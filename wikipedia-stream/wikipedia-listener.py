import requests
import json
from sseclient import SSEClient as EventSource
import yaml

config = yaml.safe_load(open("config/ingestion.yaml"))
wikis_to_scrape = config['streaming-listener']['accepted_wikis']

url = "https://stream.wikimedia.org/v2/stream/recentchange,revision-create"
output = []

for event in EventSource(url):
    if event.event == 'message':
        try:
            response = json.loads(event.data)
        except ValueError:
            continue
        else:
            if response['meta']['domain'] == 'canary':
                continue
            if response['meta']['stream'] == 'mediawiki.recentchange':
                if not response['wiki'] in wikis_to_scrape:
                    continue
            if response['meta']['stream'] == 'mediawiki.revision-create':
                if not response['database'] in wikis_to_scrape:
                    continue
    
    output.append(response)
    print(response)
    sd