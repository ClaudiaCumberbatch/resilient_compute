import json
from logging import Logger
import time
from typing import Tuple

from parsl.dataflow.taskrecord import TaskRecord
from parsl.providers import SlurmProvider

from utils import *

node_denylist = []

executor_denylist = []

class Resource_Controller():
    """
    For resource starvation, do resource reallocation:
        1. Get resource utilization in each node of the current executor;
        2. Compare to the peak utilization of the current task;
        3. Choose from those satisfying the requirements, with fewer tasks in the queue. If still more than one, choose randomly.
        4. If no satisfying node, switch to another executor and do the same thing.
    For hardware failure, do denylist:
        1. Ping those in the denylist, move them out if they reply;
        2. Ping the ones which reported error, if not reply, add to denylist.
    """
    def __init__(self, taskrecord: TaskRecord, logger: Logger) -> None:
        self.taskrecord = taskrecord
        self.logger = logger
        self.node_list = []
        self.executor_list = []
        self.logger.info("resource controller initialized")

    def get_satisfying_list(self, is_node: bool, bad_dict: dict) -> list:
        """
        is_node = True: get satisfying node list
        is_node = False: get satisfying executor list
        """
        if is_node:
            topic = "resilience-manager"
            l = self.node_list
        else:
            topic = "resilience-executor"
            l = self.executor_list

        # Get resource utilization
        consumer, last_offset = get_consumer_and_last_offset(
            taskrecord=self.taskrecord,
            topic=topic
        )
        # Get last record of each node
        last_node_info = {}
        for message in consumer:
            if message.offset > last_offset:
                break

            # self.logger.info(f"msg in get_satisfying_node: {message}")
            message_key = message.key.decode('utf-8')
            message_dict = json.loads(message.value.decode('utf-8'))
            if is_node and message_dict['executor_label'] != self.taskrecord['executor']:
                continue

            last_node_info[message_key] = message_dict

        # Choose the satisfying ones according to bad_list
        for hostname, msg_dict in last_node_info.items():
            if "MEMORY" in bad_dict.keys():
                mem_used = int(msg_dict['psutil_process_memory_resident'])/(1024**3)
                mem_total = mem_used/msg_dict['psutil_process_memory_percent']*100
                self.logger.info(f"mem_rest is {mem_total - mem_used}, mem requires is {bad_dict['MEMORY']}")
                if mem_total - mem_used > bad_dict["MEMORY"]: # the rest mem is enough
                    l.append(hostname)
                elif hostname in l:
                    l.remove(hostname)
            if "WALLTIME" in bad_dict.keys():
                current_provider = self.taskrecord['dfk'].executors[self.taskrecord['executor']].provider
                if isinstance(current_provider, SlurmProvider): # TODO: other providers
                    run_time = time.time() - msg_dict['start_time']
                    walltime = time_str_to_seconds(current_provider.walltime)
                    self.logger.info(f"time_rest is {walltime - run_time}, time requires is {bad_dict['WALLTIME']}")
                    if walltime - run_time > bad_dict["WALLTIME"]:
                        l.append(hostname)
                    elif hostname in l:
                        l.remove(hostname) 
        
        if is_node:
            self.node_list = l
        else:
            self.executor_list = l
        
        return l

    def update_denylist(self, bad_list) -> list:
        for node in node_denylist:
            res = ping_test(node)
            if res:
                node_denylist.remove(node)
        
        for node in bad_list:
            res = ping_test(node)
            if not res:
                node_denylist.append(node)
        
        return node_denylist

    def get_suggestions(self, root_cause: str, bad_list: list | dict) -> Tuple[list, list]:
        """
        Return suggested executor list and node list.
        """
        self.logger.info(f"root cause is {root_cause}, bad_list is {bad_list}")
        if root_cause == "resource_starvation":
            # now the bad_list is a dict containing starved resource types and peak values, but can also be empty
            if bad_list is not {}:
                # this node_list is a subset of current executor's node list
                node_list = self.get_satisfying_list(is_node=True, bad_dict=bad_list) 
                executor_list = self.get_satisfying_list(is_node=False, bad_dict=bad_list) 
            return node_list, executor_list
   
        elif root_cause == "machine_shutdown":
            # TODO: this can get a denylist, but how to get a full list?
            l = self.update_denylist(bad_list)
            return [], []