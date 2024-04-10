topics = ['topic1', 'topic2'] # Change these to your topic names

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=[kafka_server],
    auto_offset_reset='earliest', # Start reading at the earliest message
    enable_auto_commit=True, # Automatically commit offsets
    group_id='my-group', # Consumer group id
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Message deserializer
)

# Process messages
try:
    print(f"Listening for messages on topics: {topics}")
    for message in consumer:
        print(f"Received message from topic {message.topic}: {message.value}")
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()