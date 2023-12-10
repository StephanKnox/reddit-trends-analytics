import confluent_kafka
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
import json
import time
#import fastavro
import logging
import sys
import requests


KAFKA_BOOTSTRAP_SERVERS = ['kafka_broker:19092']
KAFKA_TOPIC = "names_topic" 
# launch stream in console command via Net Cat (utility for TCP and UDP on Mac)
# nc -lk 9999    

# Create Kafka Producer instance
def configure_kafka(servers=KAFKA_BOOTSTRAP_SERVERS) -> Producer:
    """Creates and returns a Kafka producer instance."""
    settings = {
        'bootstrap.servers': 'kafka_broker:19092',
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
    """Formats the fetched user data for Kafka streaming."""
    return {
        "name": "Ms. Grace Cho",
        "gender": "Female",
        "email": "grace_cho@gmail.com",
        "job_title": "Product Manager"
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

   

    sr = SchemaRegistryClient({"url": 'http://schemaregistry0:8085'})
    subjects = sr.get_subjects()
    for subject in subjects:
        schema = sr.get_latest_version(subject)
        print(schema.version)
        print(schema.schema_id)
        print(schema.schema.schema_str)


    """Initiates the process to stream user data to Kafka."""
    kafka_producer = configure_kafka()
    for _ in range(10):
        raw_data = retrieve_user_data()
        publish_to_kafka(kafka_producer, KAFKA_TOPIC, raw_data)
        time.sleep(PAUSE_INTERVAL)
    
main()  


# TO-DO
# 2. Figure out how to package docker container in order for spark to work without any futher manual set up
# 3. Consume topic from Kafka via Spark and try some stream transformation functions
# 4. Select a data source for the project
# 5. Develop a code to retrieve source data and load it to kafka (set up kafka producer)
# 6. Learn and then set up Airflow, Airlow db and maybe Airflow scheduler
# 7. Develop and Test: 
#   a) Data Retrieval from source
#   b) Load retrieved data into Kafka Topic 
#   c) Consuming the data from Kakfa Topic via Spark Structured Streaming
#   d) Perform some stream transformations via Spark SS
#   e) Finally load the data from Spark SS into Minio S3 bucker
# 8. Automate and schedule point 7. via Airflow
# 9. Add unit tests to the project
# 10. Figure our "real-time" dashboard use case, what will be the data source (Cassandara?) and data viz tool  (Apache Druid?) 
