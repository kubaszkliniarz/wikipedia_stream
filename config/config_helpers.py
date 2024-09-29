import yaml

def get_config(path):
    return yaml.safe_load(open(path))