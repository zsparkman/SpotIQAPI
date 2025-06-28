import json

REGISTRY_FILE = "parsers_registry.json"

def get_parser_for_file(file_name):
    try:
        with open(REGISTRY_FILE, "r") as f:
            registry = json.load(f)
        return registry.get(file_name)
    except Exception:
        return None
