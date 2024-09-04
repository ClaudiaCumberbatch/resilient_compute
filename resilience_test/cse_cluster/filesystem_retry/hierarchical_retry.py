from logging import Logger

from parsl.dataflow.taskrecord import TaskRecord

from utils import *

class Retry_Controller():

    def __init__(self, taskrecord: TaskRecord, logger: Logger) -> None:
        self.taskrecord = taskrecord
        self.logger = logger
        self.cost = 1
        self.logger.info("retry controller initialized")

    def update_taskrecord(self, node_list: list, executor_list: list) -> TaskRecord:
        """
        Reschedule by modifying taskrecord.
        1. For executor, update taskrecord['executor'].
        2. For manager, update taskrecord['resource_specification'].

        Priority desc:
        node_list (same type of avaliable resource)
        executor_list (different type of avaliable resource)
        node denylist (if the 1st time retry)
        executor denylist (others)
        """
        if len(node_list) >= 1:
            self.logger.info(f"attempt to switch to a new node according to suggested node_list: {node_list}")
            # find manager uid in node table in monitoring.db according to hostname
            message_df = get_messages_from_db(
                taskrecord=self.taskrecord,
                query=f"SELECT uid FROM node WHERE hostname IS '{node_list[0]}'"
            )
            if len(message_df) > 0:
                manager_id = message_df.iloc[0]['uid'].encode('utf-8')
                self.taskrecord['resource_specification']['manager_id'] = manager_id
                self.logger.info(f"set manager_id to {manager_id}")

        elif len(executor_list) >= 1:
            self.logger.info(f"attempt to switch to a new executor according to suggested executor_list: {executor_list}")
            self.taskrecord['executor'] = executor_list[0] # we can do this because it's a pointer
            self.logger.info(f"set executor to {self.taskrecord['executor']}")
        
        elif self.taskrecord['fail_count'] <= 1:
            current_executor_label = self.taskrecord['executor']
            denylist = self.taskrecord['dfk'].executors[current_executor_label].denylist
            if len(denylist) == 0:
                return self.taskrecord
            
            self.logger.info(f"attempt to switch to a new node according to node success rate: {denylist}")

            hostname = next(iter(denylist)) # get the first element
            message_df = get_messages_from_db(
                taskrecord=self.taskrecord,
                query=f"SELECT uid FROM node WHERE hostname IS '{hostname}'"
            )
            if len(message_df) > 0:
                manager_id = message_df.iloc[0]['uid'].encode('utf-8')
                self.taskrecord['resource_specification']['manager_id'] = manager_id
                self.logger.info(f"set manager_id to {manager_id}")
        
        else:
            denylist = self.taskrecord['dfk'].denylist
            if len(denylist) == 0:
                return self.taskrecord
            
            self.logger.info(f"attempt to switch to a new executor according to executor success rate: {denylist}")

            self.taskrecord['executor'] = next(iter(denylist)) # get the first element
            self.logger.info(f"set executor to {self.taskrecord['executor']}")

        return self.taskrecord
    
    def get_cost(self) -> int:
        return self.cost