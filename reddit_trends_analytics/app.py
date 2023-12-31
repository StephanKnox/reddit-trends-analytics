import confluent_kafka
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
import json
import time
import logging
import sys
import requests
from dotenv import load_dotenv
import os


load_dotenv() 

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')  


# Create Kafka Producer instance
def configure_kafka(servers=KAFKA_BOOTSTRAP_SERVERS) -> Producer:
    """Creates and returns a Kafka producer instance."""
    settings = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'producer_instance'  
    }
    return Producer(settings)


def publish_to_kafka(producer, topic, data):
    """Sends data to a Kafka topic."""
    producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=on_delivery)
    producer.flush()

# Callback function for kafka producer
def on_delivery(err, msg):
    """Reports the delivery status of the message to Kafka."""
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), '[Partition: {}]'.format(msg.partition()))


def retrieve_user_data():
    """Formats the fetched dummy user data to test out producing to Kafka topic."""
    return {
        "name": "Ms. Jane Doe",
        "gender": "Female",
        "email": "jane_doe@gmail.com",
        "job_title": "Product Manager"
    }


def main():
    PAUSE_INTERVAL = 10

    #sr = SchemaRegistryClient({"url": 'http://schemaregistry0:8085'})
    #subjects = sr.get_subjects()
    #for subject in subjects:
    #    schema = sr.get_latest_version(subject)
    #    print(schema.version)
    #    print(schema.schema_id)
    #    print(schema.schema.schema_str)

    """Initiates the process to stream user data to Kafka."""
    kafka_producer = configure_kafka()
    for _ in range(10):
        raw_data = retrieve_user_data()
        publish_to_kafka(kafka_producer, KAFKA_TOPIC, raw_data)
        time.sleep(PAUSE_INTERVAL)


if __name__ == "__main__":
    main()  


