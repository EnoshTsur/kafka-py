import os
from dotenv import load_dotenv
from flask import Flask
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

load_dotenv(verbose=True)

app = Flask(__name__)


def init_topics():
    client = KafkaAdminClient(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])
    topic = NewTopic(
        name=os.environ['TOPIC_PEOPLE_BASIC_NAME'],
        num_partitions=int(os.environ['TOPIC_PEOPLE_BASIC_PARTITIONS']),
        replication_factor=int(os.environ['TOPIC_PEOPLE_BASIC_REPLICAS']),
        topic_configs={"retention.ms": "60000"})
    try:
        client.create_topics([topic])
    except TopicAlreadyExistsError as e:
        print(str(e))
    finally:
        client.close()


if __name__ == '__main__':
    init_topics()
    app.run()
