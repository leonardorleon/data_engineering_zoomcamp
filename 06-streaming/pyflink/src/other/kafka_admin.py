from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from argparse import ArgumentParser
import json


admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='test'
)


if __name__ == '__main__':
    '''
    Simple python script to use the kafka admin client to list or describe topics.

    Kafka admin is not able to read the contents of the topic, so it's only administrative info in this case.
    '''
    parser = ArgumentParser()

    parser.add_argument("--topic", help="Topic name to describe")
    parser.add_argument("--list", help="List all topics", action="store_true")

    args = parser.parse_args()

    if args.topic:
        topic_description = admin_client.describe_topics(args.topic)
        print(json.dumps(topic_description, indent=2))
    elif args.list:
        admin_client.list_topics()
    else:
        print(parser.print_help())    
