from diaspora_event_sdk import KafkaConsumer
# from diaspora_event_sdk import Client as GlobusClient
from kafka import TopicPartition
import time

# c = GlobusClient()
topic = "radio-test"
consumer = KafkaConsumer(topic)

# 设定想要回溯到的时间点（例如，24小时前）
one_day_ago = int((time.time() - 10*24*3600) * 1000)  # 当前时间减去24小时，转换为毫秒
print(one_day_ago)

# 指定topic和partition
partition = 0
topic_partition = TopicPartition(topic, partition)

# 获取最早（earliest）和最新（latest）的offset
earliest_offset = consumer.beginning_offsets([topic_partition])[topic_partition]
latest_offset = consumer.end_offsets([topic_partition])[topic_partition]

print(f"Earliest offset: {earliest_offset}")
print(f"Latest offset: {latest_offset}")


# # 定位到最新的offset（这会跳过当前所有未消费的消息）
# consumer.seek_to_end(topic_partition)

# # 回退一个offset，以便读取最后一条消息
# last_offset = consumer.position(topic_partition) - 1
# consumer.seek(topic_partition, last_offset)

# # 尝试读取一条消息
# for message in consumer:
#     print(f"Offset: {message.offset}, Timestamp: {message.timestamp}")
#     break  # 只读取一条消息

# # 关闭consumer
# consumer.close()

# 查找指定时间戳之后的offset
offsets = consumer.offsets_for_times({topic_partition: one_day_ago})
print(offsets)

# 获取offset，并配置consumer从该offset开始消费
offset = offsets[topic_partition].offset if offsets[topic_partition] else None
if offset:
    consumer.seek(topic_partition, offset)

# 现在可以开始消费消息
for message in consumer:
    print(message)
    # 根据需要处理消息

# 不要忘记关闭consumer
consumer.close()
