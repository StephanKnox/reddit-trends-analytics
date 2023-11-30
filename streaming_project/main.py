import confluent_kafka
from confluent_kafka import Producer
import fastavro
import logging
import sys

# launch stream in console command via Net Cat (utility for TCP and UDP on Mac)
# nc -lk 9999    
def configure_kafka():
      pass

def on_delivery(err, record):
        pass


def main():
    logging.INFO("START")

    kafka_config = configure_kafka()
    producer = Producer(kafka_config)

    producer.produce(
        topic="",
        key=""
        value=value,
        on_delivery=on_delivery
    )
    
    
if __name__ == "main":
      logging.basicConfig(level=logging.DEBUG)
      sys.exit(main())



