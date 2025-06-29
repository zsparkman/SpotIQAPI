import json
import hashlib

REGISTRY_FILE = "parsers_registry.json"

def compute_fingerprint(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def load_registry():
    try:
        with open(REGISTRY_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def get_parser_for_content(text):
    registry = load_registry()
    fingerprint = compute_fingerprint(text[:2048])
    return registry.get(fingerprint)

def register_parser(fingerprint, parser_name):
    registry = load_registry()
    registry[fingerprint] = parser_name
    with open(REGISTRY_FILE, "w") as f:
        json.dump(registry, f, indent=2)
