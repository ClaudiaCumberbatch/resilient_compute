from logging import Logger

from parsl.dataflow.taskrecord import TaskRecord

class Retry_Controller():

    def __init__(self, taskrecord: TaskRecord, logger: Logger) -> None:
        self.taskrecord = taskrecord
        self.logger = logger
        self.cost = 1

    def update_taskrecord(self, node_list: list, executor_list: list) -> TaskRecord:
        # TODO: how to suggest node to a task?
        # 1. directly update the executor
        # 2. put suggested node_list into taskrecord. what about resource specification?
        return self.taskrecord
    
    def get_cost(self) -> int:
        return self.cost