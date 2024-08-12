from diaspora_event_sdk import KafkaConsumer
from kafka import TopicPartition
import json
from logging import Logger
import re
import subprocess
import time
from typing import Tuple

from parsl.dataflow.taskrecord import TaskRecord
from parsl.providers import SlurmProvider

from exception_msg_lib import ERROR_LIST
from utils import *

def is_terminate(error_info: str) -> bool: # TODO: exception?
    """
    Determine whether a failure is terminate 
    by compare the error_info with exception_msg_lib.
    """
    if any(pattern in error_info for pattern in ERROR_LIST):
        return True
    else:
        return False

    
class Resource_Analyzer():
    """
    Analyze resource profile data and tell:
    1. Whether the root cause is resource starvation or machine shutdown.
    2. If resource starvation, which type(s) of resource?
    3. If machine shutdown, which machine?
    """
    def __init__(self, taskrecord: TaskRecord, logger: Logger) -> None:
        self.hostname = None
        self.taskrecord = taskrecord
        self.logger = logger
        self.root_cause = None # "resource_starvation" or "machine_shutdown"
        self.starved_type_list = [] # subset of self.STARVE
        self.STARVE = [
            "CPU", 
            "DISK", 
            "MEMORY", 
            "WALLTIME",
            "ULIMIT", 
            "UNKNOW"
        ] # TODO: 1. Make configurable; 2. Write to enum.
        self.bad_machine_list = [] # node
        self.logger.info("resource analyzer initialized")

    def add2starved(self, type: str) -> bool:
        """
        Add starved type to self.starved_type_list.
        Return whether it's a successful append.
        """
        if type.upper() in self.STARVE:
            self.starved_type_list.append(type.upper())
            return True
        else:
            return False
        
    def get_error_info(self) -> str:
        """
        Create a KafkaConsumer and fetch error info.
        """
        consumer, last_offset = get_consumer_and_last_offset(
            taskrecord=self.taskrecord, 
            topic="failure-info"
        )

        # Fetch error info
        for message in consumer:
            message_dict = json.loads(message.value.decode('utf-8'))
            # Jump out of the loop or jump into next loop if it's not the task we want
            if 'task_id' in message_dict:
                if int(message_dict['task_id']) != self.taskrecord['id']:
                    if message.offset >= last_offset:
                        break
                    else:
                        continue
            
            # Found corresponding error info, return
            if 'task_fail_history' in message_dict:
                error_info = message_dict['task_fail_history']
                return error_info

            # Finish traversing
            if message.offset >= last_offset:
                break

        return None

    def which_root_cause(self) -> str:
        """
        "resource_starvation" or "machine_shutdown"
        """
        error_info = self.get_error_info()
        if error_info:
            if 'ManagerLost' in error_info:
                self.root_cause = "machine_shutdown"
            else:
                self.root_cause = "resource_starvation"
        return self.root_cause


    def get_node_memory(self, node_name) -> dict:
        """
        For memory usage comparison.
        """
        result = subprocess.run(['scontrol', 'show', 'node', node_name], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Failed to get node information for {node_name}")
        output = result.stdout
        memory_info = {
            'RealMemory': None,
            'AllocMem': None,
        }
        real_memory_match = re.search(r'RealMemory=(\d+)', output)
        alloc_memory_match = re.search(r'AllocMem=(\d+)', output)
        if real_memory_match:
            memory_info['RealMemory'] = int(real_memory_match.group(1))
        if alloc_memory_match:
            memory_info['AllocMem'] = int(alloc_memory_match.group(1))
        return memory_info
    
    def time_str_to_seconds(self, time_str) -> int:
        """
        For Walltime comparison.
        """
        h, m, s = map(int, time_str.split(':'))
        return h * 3600 + m * 60 + s

    
    def which_resources(self) -> list:
        """
        Compare the already used value with the overall value of:
        [] CPU, 
        [] disk, 
        [x] memory, 
        [x] walltime, 
        [] ulimit, 
        [] unknown
        """
        if self.root_cause != "resource_starvation":
            return []
        else:
            consumer, last_offset = get_consumer_and_last_offset(
                taskrecord=self.taskrecord, 
                topic="resilience-executor"
            )

        # Fetch current usage value of each type of resource
        last_executor_info = {}
        max_values = {}
        for message in consumer:
            self.logger.info(f"msg: {message}")
            message_key = message.key.decode('utf-8')
            message_dict = json.loads(message.value.decode('utf-8'))
            
            if message_key != self.taskrecord['executor']:
                continue

            # Get node name
            self.hostname = message_dict['hostname']
            
            # Keep the last one for walltime verification
            last_executor_info[message_key] = message_dict
            # Get the max value of each type
            for key, value in message_dict.items():
                if key not in max_values or value > max_values[key]:
                    max_values[key] = value

            if message.offset >= last_offset:
                break
        
        self.logger.info(f"max_values: {max_values}")
        # Compare with overall value
        mem_total = self.get_node_memory(self.hostname)['AllocMem']/1024
        mem_used = int(max_values['psutil_process_memory_resident'])/(1024**3)
        if mem_used > mem_total*0.6: # TODO: make threshold configurable? other methods to determine out-of-mem?
            self.add2starved("MEMORY")

        current_provider = self.taskrecord['dfk'].executors[self.taskrecord['executor']].provider
        if isinstance(current_provider, SlurmProvider): # TODO: other providers
            run_time = time.time() - last_executor_info[self.taskrecord['executor']]['start_time']
            walltime = self.time_str_to_seconds(current_provider.walltime)
            if run_time > walltime:
                self.add2starved("WALLTIME")

        return self.starved_type_list
    

    def which_machines(self) -> list:
        """
        Add bad node name to list.
        """
        if self.root_cause != "machine_shutdown":
            return []
        else:
            consumer, last_offset = get_consumer_and_last_offset(
                taskrecord=self.taskrecord,
                topic="resilience-manager"
            )

            for message in consumer:
                message_dict = json.loads(message.value.decode('utf-8'))
                self.hostname = message_dict['hostname']
                
                if message.offset >= last_offset:
                    break
            
            self.bad_machine_list.append(self.hostname)

        return self.bad_machine_list


    def get_rootcause_and_list(self) -> Tuple[str, list]:
        root_cause = self.which_root_cause()
        if root_cause == "resource_starvation":
            resource_list = self.which_resources()
            self.logger.info(f"resource error: {resource_list}")
            return root_cause, resource_list
        elif root_cause == "machine_shutdown":
            machine_list = self.which_machines()
            self.logger.info(f"machine error: {machine_list}")
            return root_cause, machine_list
