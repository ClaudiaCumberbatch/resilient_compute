import logging
import sys
import json
import os
import time

from parsl.dataflow.taskrecord import TaskRecord
from parsl.providers import SlurmProvider
from diaspora_event_sdk import KafkaConsumer
from kafka import TopicPartition

# TODO: 1. Get a full list from python errors
# 2. Dynamically update this pattern list?
permanent_error_pattern_list = ['OSError',
                                'ZeroDivisionError']

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

# set logger
log_file = 'resilient_retry.log'
if os.path.isfile(log_file):
    with open(log_file, 'w') as file:
        pass
logger = start_file_logger(log_file, level=logging.INFO)

def retry_different_executor(e: Exception,
                             taskrecord: TaskRecord) -> float:
    import random
    dfk = taskrecord['dfk']
    choices = {k: v for k, v in dfk.executors.items() if k != '_parsl_internal' and k!= taskrecord['executor']}
    new_exe = random.choice(list(choices.keys()))
    taskrecord['executor'] = new_exe
    return 1

def coarse_category(taskrecord: TaskRecord) -> str:
    # unknown, permanent, resource
    topic = "failure-info"
    consumer = KafkaConsumer(topic)
    logger.warning(f"Creating Kafka consumer for {topic}")

    partition = 0
    topic_partition = TopicPartition(topic, partition)
    start_time = taskrecord['try_time_launched'].timestamp()
    logger.warning(f"start time = {int(start_time * 1000)}")
    start_offsets = consumer.offsets_for_times({topic_partition: start_time*1000})
    start_offset = start_offsets[topic_partition].offset if start_offsets[topic_partition] else None
    logger.warning(f"start offset = {start_offset}")
    if start_offset:
        consumer.seek(topic_partition, start_offset)
    else:
        # no failure-info record after start_time, temporarily assume that it's not a permanent error
        return 'unknown'
    
    end_offsets = consumer.end_offsets([topic_partition])
    last_offset = end_offsets[topic_partition] - 1
    logger.warning(f"last offset = {last_offset}")

    for message in consumer:
        logger.warning("Received message: {}".format(message))
        message_key = message.key.decode('utf-8')
        message_dict = json.loads(message.value.decode('utf-8'))
        if 'task_id' in message_dict:
            if int(message_dict['task_id']) != taskrecord['id']:
                if message.offset >= last_offset:
                    break
                else:
                    continue

        if 'task_fail_history' in message_dict:
            error_info = message_dict['task_fail_history']
            if any(pattern in error_info for pattern in permanent_error_pattern_list):
                logger.info(f"{error_info} is a permanent error")
                return 'permanent'
            elif 'loss' in error_info:
                logger.info(f"{error_info} is a resource error")
                return 'resource'

        if message.offset >= last_offset:
            break

    return 'unknown'

def time_str_to_seconds(time_str):
    h, m, s = map(int, time_str.split(':'))
    return h * 3600 + m * 60 + s

def which_resource_category(taskrecord: TaskRecord) -> str:
    # unknown, walltime, memory
    topic = "radio-test"
    consumer = KafkaConsumer(topic)
    logger.warning(f"Creating Kafka consumer for {topic}")

    partition = 0
    topic_partition = TopicPartition(topic, partition)
    start_time = taskrecord['try_time_launched'].timestamp()
    logger.warning(f"start time = {int(start_time * 1000)}")
    start_offsets = consumer.offsets_for_times({topic_partition: start_time*1000})
    start_offset = start_offsets[topic_partition].offset if start_offsets[topic_partition] else None
    logger.warning(f"start offset = {start_offset}")
    if start_offset:
        consumer.seek(topic_partition, start_offset)
    else:
        # no resource record after start_time
        return 'unknown'
    
    end_offsets = consumer.end_offsets([topic_partition])
    last_offset = end_offsets[topic_partition] - 1
    logger.warning(f"last offset = {last_offset}")

    # get executor resource info
    # TODO: here the traversal is slow, should find better method
    last_executor_info = {}
    peak_mem_info = {}
    for message in consumer:
        logger.warning("Received message: {}".format(message))
        message_key = message.key.decode('utf-8')
        message_dict = json.loads(message.value.decode('utf-8'))
        # only focus on executor info
        if 'pid' in message_dict:
            if message.offset >= last_offset:
                break
            else:
                continue

        last_executor_info[message_key] = message_dict
        if message_key in peak_mem_info:
            if peak_mem_info[message_key]['psutil_process_memory_resident'] < message_dict['psutil_process_memory_resident']:
                peak_mem_info[message_key] = message_dict
        else:
            peak_mem_info[message_key] = message_dict
            
        if message.offset >= last_offset:
            break
    
    current_provider = taskrecord['dfk'].executors[taskrecord['executor']].provider
    if isinstance(current_provider, SlurmProvider):
        run_time = time.time() - last_executor_info[taskrecord['executor']]['start_time']
        walltime = time_str_to_seconds(current_provider.walltime)
        logger.info(f"run_time = {run_time}, walltime = {walltime}")
        if run_time > walltime:
            logger.info(f"{run_time} > {walltime}, executor hit walltime limit")
            return 'walltime'
        
        # one run out of memory, the process will be killed, so the value of used mem will drop
        # so here use peak mem instead of last mem to determine whether it's a mem error
        mem_used = int(peak_mem_info[taskrecord['executor']]['psutil_process_memory_resident'])/(1024**3)
        mem = current_provider.mem_per_node # unit G
        logger.info(f"mem_used = {mem_used}, mem = {mem}")
        if mem_used > mem*0.6: # TODO: make threshold configurable? other methods to determine out-of-mem?
            logger.info(f"{mem_used} > {mem}*0.6, run out of memory")
            large_mem_executor(taskrecord, last_executor_info)
            return 'memory'

    return 'unknown'

def large_mem_executor(taskrecord: TaskRecord, last_executor_info: dict) -> None:
    # switch to the executor with the max rest memory
    rest_mem_dic = {}
    max_rest = -sys.maxsize
    exe_res = None

    for executor_name, info in last_executor_info.items():
        executor = taskrecord['dfk'].executors[executor_name]
        current_provider = executor.provider
        if isinstance(current_provider, SlurmProvider):
            mem_used = int(info['psutil_process_memory_resident']) / (1024**3)
            mem = current_provider.mem_per_node # unit G
            rest_mem_dic[executor_name] = mem - mem_used
            if rest_mem_dic[executor_name] > max_rest:
                max_rest = rest_mem_dic[executor_name]
                exe_res = executor_name
    if exe_res:
        taskrecord['executor'] = exe_res
        logger.info(f"switch to {exe_res}")
    return

def resilient_retry(e: Exception,
                    taskrecord: TaskRecord) -> float:
    '''
    1. Go through diaspora failure-info to determine whether it's a permenant error.
    If so, directly return a large cost, so Parsl will return this bad task to the user.
    2. If it's potentially a resource error, go through diaspora radio-test (resource info) 
    to figure out the more concrete reason.
    '''
    cat = coarse_category(taskrecord)
    if cat == 'permanent':
        logger.info("permanent error, return to user")
        return sys.maxsize
    elif cat == 'resource':
        res_cat = which_resource_category(taskrecord)
        if res_cat == 'walltime': 
            logger.info("walltime error, retry at the same place")
            return 1
        elif res_cat == 'memory':
            return 1
        else:
            logger.info("unknown resource error")
            return 1
    else:
        logger.info(f"category is {cat}")
        return 1