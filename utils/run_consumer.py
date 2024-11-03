import json
from kafka import KafkaConsumer

def run_consumer(topic, group_id):
    # Kafka Consumer Setup
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:29092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer
