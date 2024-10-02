from attr import dataclass
from confluent_kafka import Producer
import json

from helpers.logger import setup_logger

logger = setup_logger(__name__)

KAFKA_TOPIC = "wikipedia"
KAFKA_BROKER = "localhost:9092"

producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "wikipedia-producer",
}

producer = Producer(producer_conf)


async def produce_event_to_kafka(data, key):
    logger.info(f"Flushing data for {key} into Kafka...")
    producer.produce(KAFKA_TOPIC, key=str(key), value=json.dumps(data))
    producer.flush()
    logger.info("Success!")
