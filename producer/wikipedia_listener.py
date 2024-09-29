import aiohttp
import json
import ssl

from config.config_helpers import get_config
from helpers.logger import setup_logger

logger = setup_logger(__name__)

# Async implementation of stream_listener without using `SSEClient`. 
# Since asynchronous function, forced to use `aiohttp.ClientSession()`

async def stream_listener(url, wikis_to_scrape):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    logger.info("Connecting...")

    async with aiohttp.ClientSession() as session:
        async with session.get(url, ssl=ssl_context) as response:
            logger.info(f"Connection established to {url}")
            buffer = ""
            async for line in response.content:
                buffer += line.decode("utf-8")

                # Check if we have received a complete event
                while "\n\n" in buffer:
                    event_data, buffer = buffer.split("\n\n", 1)  # Split into event data and remaining buffer

                    # Split lines in the event
                    lines = event_data.strip().split("\n")
                    event_info = {}
                    
                    for line in lines:
                        if line.startswith("event:"):
                            event_info['event'] = line[len("event:"):].strip()
                        elif line.startswith("id:"):
                            event_info['id'] = json.loads(line[len("id:"):].strip())
                        elif line.startswith("data:"):
                            event_info['data'] = json.loads(line[len("data:"):].strip())

                    # Process the event if it is a data event
                    if event_info.get("event") == "message":
                        if "meta" in event_info["data"]:
                            # Implementing filtering logic
                            data = event_info["data"]
                            if data["meta"]["domain"] == "canary":
                                continue
                            if data["meta"]["stream"] == "mediawiki.recentchange":
                                if data["wiki"] not in wikis_to_scrape:
                                    continue
                            if data["meta"]["stream"] == "mediawiki.revision-create":
                                if data["database"] not in wikis_to_scrape:
                                    continue
                            
                            logger.info(f"Received {data["meta"]["stream"]} event: {event_data[:50]}")

async def run_wikipedia_listener():
    config = get_config("wikipedia_stream/config/ingestion.yaml")["wikipedia-listener"]
    wikis_to_scrape = config["accepted_wikis"]
    url = f"{config['url']}/{','.join(config['endpoint'])}"
    await stream_listener(url, wikis_to_scrape)
