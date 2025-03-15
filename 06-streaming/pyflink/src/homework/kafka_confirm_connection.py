import json
import time
from kafka import KafkaProducer

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers = [server],
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

producer_status = producer.bootstrap_connected()

producer.close()

print(f'Producer is currently connected: {producer_status}')