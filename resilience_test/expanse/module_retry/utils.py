from kafka import TopicPartition
from diaspora_event_sdk import KafkaConsumer
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