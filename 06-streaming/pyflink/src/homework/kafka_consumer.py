import json
from argparse import ArgumentParser
from kafka import KafkaConsumer

def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

def consume_messages(topic, total_messages, bootstrap_servers='localhost:9092'):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='manual-kafka-consumer',
        value_deserializer=json_deserializer
    )

    i = 0
    for message in consumer:
        print(f'received message: \n {json.dumps(message.value,indent=2)}')
        i+=1
        if i == total_messages:
            break

if __name__ == "__main__":
    
    parser = ArgumentParser(
        prog='Kafka Consumer',
        description='Program consumes a few messages from a topic'    
    )

    parser.add_argument('-t','--topic', help='Name of the topic to consume from',default='green-trips')
    parser.add_argument('-m','--messages', help='Total number of messages to consume',default=5)

    args = parser.parse_args()

    print(f"Consuming a few messages from the topic: {args.topic} to check structure")

    consume_messages(topic=args.topic, total_messages=args.messages)