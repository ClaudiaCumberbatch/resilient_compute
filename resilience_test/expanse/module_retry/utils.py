from diaspora_event_sdk import KafkaConsumer
import json
from kafka import TopicPartition
import subprocess
from typing import Tuple

from parsl.dataflow.taskrecord import TaskRecord


def get_consumer_and_last_offset(
        taskrecord: TaskRecord, 
        topic: str
    ) -> Tuple[KafkaConsumer, int]:
    
    # Create consumer
    consumer = KafkaConsumer(topic)

    # Fix start and end offset
    partition = 0
    topic_partition = TopicPartition(topic, partition)
    # TODO: when comes to resources, start_time should be the-last-gasp-of-death time
    start_time = taskrecord['try_time_launched'].timestamp() 
    start_offsets = consumer.offsets_for_times({topic_partition: start_time*1000})
    start_offset = start_offsets[topic_partition].offset if start_offsets[topic_partition] else None
    if start_offset:
        consumer.seek(topic_partition, start_offset)
    else:
        # no record after start_time
        return None
        
    end_offsets = consumer.end_offsets([topic_partition])
    last_offset = end_offsets[topic_partition] - 1

    return consumer, last_offset


def time_str_to_seconds(time_str) -> int:
    """
    For Walltime comparison.
    """
    h, m, s = map(int, time_str.split(':'))
    return h * 3600 + m * 60 + s


def ping_test(hostname: str) -> bool:
    """
    Check whether the node is alive.
    """
    if not hostname:
        return False
    
    for _ in range(3):
        result = subprocess.run(['ping', '-c', '1', hostname], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode == 0:
            return True
        
    return False

def get_task_hostname(taskrecord: TaskRecord) -> str:
    """
    Read radio-test topic in DEF, get hostname according to current task_id and try_id.
    TODO: possibly be a list
    """
    consumer, last_offset = get_consumer_and_last_offset(
        taskrecord=taskrecord,
        topic="radio-test"
    )

    for message in consumer:
        message_dict = json.loads(message.value.decode('utf-8'))
        if message_dict['task_id'] == taskrecord['id'] and message_dict['try_id'] == taskrecord['try_id']:
            return message_dict['hostname']

        if message.offset >= last_offset:
                break
        
    return None