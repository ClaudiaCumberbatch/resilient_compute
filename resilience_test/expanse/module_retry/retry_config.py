from parsl.dataflow.taskrecord import TaskRecord
from categorization import *
from resource_control import *
from hierarchical_retry import *

import logging
import os
import sys

logger_init_flag = False

def start_file_logger(filename, name=__name__, level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d " \
                        "%(process)d %(threadName)s " \
                        "[%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers = []

    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

def init_logger(taskrecord: TaskRecord):
    global logger, logger_init_flag
    if not logger_init_flag:
        log_file = "{}/resilient_retry.log".format(taskrecord['dfk'].run_dir)
        # if exist, clear it
        if os.path.isfile(log_file):
            with open(log_file, 'w') as file:
                pass      
        logger = start_file_logger(log_file, level=logging.INFO)
        logger_init_flag = True

def resilient_retry(e: Exception,
                    taskrecord: TaskRecord) -> float:
    init_logger(taskrecord)
    # TODO: despite of dependency error, is there anything else that will cause a task not being launched?
    if taskrecord['try_time_launched'] is None:
        logger.info("Task not launched, return to user")
        return sys.maxsize
    
    # Invoke Categorization Module
    error_info = repr(e)
    logger.info(f"error info is {error_info}")
    if is_terminate(error_info):
        logger.info("permanent error, return to user")
        return sys.maxsize
    else:
        resource_analyzer = Resource_Analyzer(taskrecord, logger)
        root_cause, bad_list = resource_analyzer.get_rootcause_and_list()
        if not root_cause:
            return sys.maxsize
        
        # Invoke Resource Control Module
        resource_controler = Resource_Controller(taskrecord, logger)
        node_list, executor_list = resource_controler.get_suggestions(root_cause, bad_list)
        logger.info(f"node list: {node_list}")
        logger.info(f"executor list: {executor_list}")

        # Invoke Retry Module
        retry_controller = Retry_Controller(taskrecord, logger)
        taskrecord = retry_controller.update_taskrecord(node_list, executor_list)
        return retry_controller.get_cost()
