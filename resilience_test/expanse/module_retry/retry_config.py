from parsl.dataflow.taskrecord import TaskRecord
from categorization import *

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
    
    resource_analyzer = Resource_Analyzer(taskrecord, logger)
    error_info = resource_analyzer.get_error_info()
    if is_terminate(error_info):
        logger.info("permanent error, return to user")
        return sys.maxsize
    else:
        logger.info("not permanent error")
        root_cause = resource_analyzer.which_root_cause()
        logger.info(f"root cause is {root_cause}")
        if root_cause == "resource_starvation":
            resource_list = resource_analyzer.which_resources()
            logger.info(f"resource error: {resource_list}")
        elif root_cause == "machine_shutdown":
            machine_list = resource_analyzer.which_machines()
            logger.info(f"machine error: {machine_list}")
    # return 1
    return sys.maxsize