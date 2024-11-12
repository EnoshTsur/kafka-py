import json
import os

from dotenv import load_dotenv
from faker import Faker
from flask import Flask
from kafka import KafkaProducer

fake = Faker()

load_dotenv(verbose=True)

app = Flask(__name__)


def produce_fake_person():
    producer = KafkaProducer(
        bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    fake_person = {
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'address': fake.address(),
        'birth_date': fake.date_of_birth().isoformat()
    }
    producer.send(
        os.environ['TOPIC_PEOPLE_BASIC_NAME'],
        value=fake_person,
        key=fake_person['email'].encode('utf-8')
    )
    producer.flush()


if __name__ == '__main__':
    produce_fake_person()
    app.run()




