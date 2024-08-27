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
        # TODO: how to suggest node to a task?
        # 1. directly update the executor
        # 2. put suggested node_list into taskrecord. what about resource specification?
        if len(node_list) >= 1:
            self.logger.info(f"node list in retry controller is {node_list}")
            # find manager uid in node table in monitoring.db according to hostname
            message_df = get_messages_from_db(
                taskrecord=self.taskrecord,
                query=f"SELECT uid FROM node WHERE hostname IS '{node_list[0]}'"
            )
            if len(message_df) > 0:
                manager_id = message_df.iloc[0]['uid'].encode('utf-8')
                self.taskrecord['resource_specification']['manager_id'] = manager_id
                self.logger.info(f"set manager_id to {manager_id}")

        if len(executor_list) >= 1:
            self.taskrecord['executor'] = executor_list[0] # we can do this because it's a pointer
            self.logger.info(f"taskrecord has been updated to {self.taskrecord}")

        return self.taskrecord
    
    def get_cost(self) -> int:
        return self.cost