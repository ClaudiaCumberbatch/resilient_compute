from typing import Tuple
from kafka import TopicPartition
from diaspora_event_sdk import KafkaConsumer

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