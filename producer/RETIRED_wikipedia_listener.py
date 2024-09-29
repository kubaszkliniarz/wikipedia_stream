import requests
import asyncio
import aiohttp
import json
from sseclient import SSEClient as EventSource
from config.config_helpers import get_config

# Implementation of stream listener using supported by Wikipedia `SSEClient`.
# Simple stream where response can be send somewhere.

def stream_listener(url, wikis_to_scrape):
    for event in EventSource(url):
        if event.event == "message":
            try:
                response = json.loads(event.data)
            except ValueError:
                continue
            else:
                if response["meta"]["domain"] == "canary":
                    continue
                if response["meta"]["stream"] == "mediawiki.recentchange":
                    if not response["wiki"] in wikis_to_scrape:
                        continue
                if response["meta"]["stream"] == "mediawiki.revision-create":
                    if not response["database"] in wikis_to_scrape:
                        continue

        print(response)


def main():
    config = get_config("wikipedia_stream/config/ingestion.yaml")["wikipedia-listener"]
    wikis_to_scrape = config["accepted_wikis"]
    url = f"{config['url']}/{','.join(config['endpoint'])}"
    stream_listener(url, wikis_to_scrape)


if __name__ == "__main__":
    main()
