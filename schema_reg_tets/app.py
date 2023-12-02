import confluent_kafka
from confluent_kafka import Producer
import json
import time
#import fastavro
import logging
import sys


KAFKA_BOOTSTRAP_SERVERS = ['kafka_broker_2:19092']
KAFKA_TOPIC = "names_topic" 
# launch stream in console command via Net Cat (utility for TCP and UDP on Mac)
# nc -lk 9999    


def configure_kafka(servers=KAFKA_BOOTSTRAP_SERVERS):
    """Creates and returns a Kafka producer instance."""
    settings = {
        'bootstrap.servers': 'kafka_broker_2:19092',
        'client.id': 'producer_instance'  
    }
    return Producer(settings)


def publish_to_kafka(producer, topic, data):
    """Sends data to a Kafka topic."""
    producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=on_delivery)
    producer.flush()


def on_delivery(err, msg):
    """Reports the delivery status of the message to Kafka."""
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), '[Partition: {}]'.format(msg.partition()))


def retrieve_user_data():
    """Formats the fetched user data for Kafka streaming."""
    return {
        "name": "Mr Stanislav Kulchitskiy",
        "gender": "Male",
        "email": "stas_cool@gmail.com"
    }


def main():
  
    #logging.INFO("START")
    PAUSE_INTERVAL = 10

    #kafka_config = configure_kafka()
    #producer = Producer(kafka_config)

    #producer.produce(
    #    topic="",
    #    key=""
    #    value=value,
    #    on_delivery=on_delivery
    #)
    """Initiates the process to stream user data to Kafka."""
    kafka_producer = configure_kafka()
    for _ in range(10):
        raw_data = retrieve_user_data()
        publish_to_kafka(kafka_producer, KAFKA_TOPIC, raw_data)
        time.sleep(PAUSE_INTERVAL)
    
main()  