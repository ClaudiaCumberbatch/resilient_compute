from logging import Logger

from parsl.dataflow.taskrecord import TaskRecord

class Retry_Controller():

    def __init__(self, taskrecord: TaskRecord, logger: Logger) -> None:
        self.taskrecord = taskrecord
        self.logger = logger
        self.cost = 1

    def update_taskrecord(self, node_list: list, executor_list: list) -> TaskRecord:
        # TODO: how to suggest node to a task?
        # TODO: can we add retry history in taskrecord?
        return self.taskrecord
    
    def get_cost(self) -> int:
        return self.cost