import json
from kafka import KafkaProducer

def run_producer():
    # Kafka producer setup
    producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
    return producer