import yaml

conf = {}

def load_config(file_str: str):
    with open(file_str) as f:
        conf.update(yaml.safe_load(f))
