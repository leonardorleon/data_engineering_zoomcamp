from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from argparse import ArgumentParser
import json


admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='test'
)


if __name__ == '__main__':
    '''
    Simple python script to use the kafka admin client to handle administrative tasks on topics.

    Kafka admin is not able to read the contents of the topic, so it's only administrative info in this case.
    '''
    parser = ArgumentParser()

    parser.add_argument("--topic", nargs='+' , help="Topic name to describe")
    parser.add_argument("--list", help="List all topics", action="store_true")
    parser.add_argument("--delete_topics", nargs='+', help="Delete topics from the cluster")

    args = parser.parse_args()

    if args.topic:
        topic_description = admin_client.describe_topics(args.topic)
        print(json.dumps(topic_description, indent=2))
    elif args.list:
        print(admin_client.list_topics())
    elif args.delete_topics:
        admin_client.delete_topics(args.delete_topics)
        print(f"Deleted topics: {args.delete_topics}")
    else:
        print(parser.print_help())    
