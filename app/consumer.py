import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv(verbose=True)


def consume_fake_person():
    consumer = KafkaConsumer(
        os.environ['TOPIC_PEOPLE_BASIC_NAME'],
        bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',  # Read messages from the beginning of the topic
    )
    print(f"Listening to topic: {os.environ['TOPIC_PEOPLE_BASIC_NAME']}")

    # Continuously listen for messages
    for message in consumer:
        print(f"Consumed message: {message.value['first_name']}")


if __name__ == '__main__':
    try:
        consume_fake_person()
    except KeyboardInterrupt:
        pass
