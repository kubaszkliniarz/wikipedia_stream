from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

from helpers.logger import setup_logger

logger = setup_logger(__name__)


def init_cassandra():
    cluster = Cluster(["localhost"])
    session = cluster.connect()

    session.execute(
        """
    CREATE KEYSPACE IF NOT EXISTS wikipedia_stream
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """
    )

    # Use the newly created keyspace
    session.set_keyspace("wikipedia_stream")

    # Create a table to store Wikipedia events
    session.execute(
        """
    CREATE TABLE IF NOT EXISTS events (
        id UUID PRIMARY KEY,
        event_time TIMESTAMP,
        event_type TEXT,
        event_data TEXT
    )
    """
    )

    logger.info("Cassandra keyspace and table initialized.")


if __name__ == "__main__":
    init_cassandra()
