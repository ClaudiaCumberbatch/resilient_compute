from diaspora_event_sdk import KafkaConsumer
from kafka import TopicPartition
import time
import pprint
import json

topic = "failure-info"
consumer = KafkaConsumer(topic, auto_offset_reset="earliest")

partition = 0
topic_partition = TopicPartition(topic, partition)

end_offsets = consumer.end_offsets([topic_partition])
last_offset = end_offsets[topic_partition] - 1
print(f"The last offset is {last_offset}")

for message in consumer:
    message_dict = json.loads(message.value.decode('utf-8'))
    print(message_dict['task_executor'])