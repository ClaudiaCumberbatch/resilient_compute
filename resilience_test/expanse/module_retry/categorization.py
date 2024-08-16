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

def is_terminate(error_info: str) -> bool:
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
        self.hostname = get_task_hostname(taskrecord)
        self.taskrecord = taskrecord
        self.logger = logger
        self.error_info = None
        self.root_cause = None # "resource_starvation" or "machine_shutdown"
        self.starved_type_dict = {} # the key of this dict is the subset of self.STARVE
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

    def add2starved(self, type: str, peak_value: float) -> bool:
        """
        Add starved type to self.starved_type_dict.
        Return whether it's a successful append.
        """
        if type.upper() in self.STARVE:
            self.starved_type_dict[type.upper()] = peak_value
            return True
        else:
            return False
        
    def get_error_info(self) -> str:
        """
        Create a KafkaConsumer and fetch error info.
        """
        if self.error_info:
            return self.error_info
        
        consumer, last_offset = get_consumer_and_last_offset(
            taskrecord=self.taskrecord, 
            topic="failure-info"
        )
        if not consumer:
            return None

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
        # TODO: ping test needs hostname too!!!
        if not ping_test(self.hostname):
            self.root_cause = "machine_shutdown"
        else:
            self.root_cause = "resource_starvation"

        # error_info = self.get_error_info()
        # if error_info:
        #     # TODO: need other info to determine machine_shutdown
        #     # walltime limit will also trigger ManagerLost
        #     if 'ManagerLost' in error_info: 
        #         self.root_cause = "machine_shutdown"
        #     else:
        #         self.root_cause = "resource_starvation"
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
    
    
    def which_resources(self) -> dict:
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
                topic="resilience-manager"
            )
            self.logger.info(f"last offset is {last_offset}")

        # Fetch current usage value of each type of resource
        last_executor_info = {}
        max_values = {}
        for message in consumer:
            self.logger.info(f"msg: {message}")
            message_key = message.key.decode('utf-8')
            message_dict = json.loads(message.value.decode('utf-8'))
            
            # if message_key != self.taskrecord['executor']:
            #     continue
            
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
            self.add2starved("MEMORY", mem_used)

        current_provider = self.taskrecord['dfk'].executors[self.taskrecord['executor']].provider
        if isinstance(current_provider, SlurmProvider): # TODO: other providers
            run_time = time.time() - last_executor_info[self.hostname]['start_time']
            walltime = time_str_to_seconds(current_provider.walltime)
            if run_time > walltime:
                self.add2starved("WALLTIME", run_time)

        return self.starved_type_dict
    

    def which_machines(self) -> list:
        """
        !! deprecate this
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
                # TODO: this is inaccurate. we don't know the hostname of the node for this task.
                self.hostname = message_dict['hostname']
                
                if message.offset >= last_offset:
                    break
            
            self.bad_machine_list.append(self.hostname)

        return self.bad_machine_list


    def get_rootcause_and_list(self) -> Tuple[str, list] | Tuple[str, dict]:
        root_cause = self.which_root_cause()
        if root_cause == "resource_starvation":
            resource_dict = self.which_resources()
            self.logger.info(f"resource error: {resource_dict}")
            return root_cause, resource_dict
        elif root_cause == "machine_shutdown": # TODO: deprecate this
            machine_list = self.which_machines()
            self.logger.info(f"machine error: {machine_list}")
            return root_cause, machine_list
        else:
            return None