from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from confluent_kafka import Consumer
import json
import uuid
from helpers.logger import setup_logger

logger = setup_logger(__name__)

KAFKA_TOPIC = "wikipedia"
KAFKA_BROKER = "localhost:9092"

# Initialize the Kafka Consumer
consumer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "wikipedia-consumer",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

# Initialize Cassandra connection
cluster = Cluster(["localhost"])
session = cluster.connect("wikipedia_stream")


def store_event(event):
    id = uuid.uuid4()
    event_time = event.get("timestamp")
    event_type = event.get("meta", {}).get("stream", "unknown")
    event_data = json.dumps(event)

    insert_statement = SimpleStatement(
        """
        INSERT INTO events (id, event_time, event_type, event_data)
        VALUES (%s, %s, %s, %s)
    """
    )
    session.execute(insert_statement, (id, event_time, event_type, event_data))
    logger.info(f"Stored event {id} into Cassandra")


if __name__ == "__main__":
    logger.info("Starting Kafka consumer...")

    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                logger.error(f"Consumer error: {message.error()}")
                continue

            event = json.loads(message.value().decode("utf-8"))
            print(event)
            store_event(event)

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        session.shutdown()
        cluster.shutdown()
