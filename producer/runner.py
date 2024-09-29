from wikipedia_stream.producer.wikipedia_listener import run_wikipedia_listener
import asyncio
from helpers.logger import setup_logger

logger = setup_logger(__name__)

# Runner that will later on be transformed to Kafka Producer.
# Essentially, a producer will run wiki listener and save it to log for a given Kafka topic.
# No clue for now how to transfer data since it's inside wikipedia listener and does not leave it.

async def main():
    await run_wikipedia_listener()

if __name__ == '__main__':
    asyncio.run(main())