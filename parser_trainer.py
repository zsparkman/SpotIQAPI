import os
import json
import importlib.util
from datetime import datetime
from openai import OpenAI
import traceback

UNHANDLED_DIR = "unhandled_logs"
PARSERS_DIR = "parsers"
REGISTRY_FILE = "parsers_registry.json"
OPENAI_MODEL = "gpt-4"

client = OpenAI()

def load_registry():
    if not os.path.exists(REGISTRY_FILE):
        return {}
    with open(REGISTRY_FILE, "r") as f:
        return json.load(f)

def save_registry(registry):
    with open(REGISTRY_FILE, "w") as f:
        json.dump(registry, f, indent=2)

def generate_parser_code(sample_text):
    prompt = f"""
You are a Python engineer. Write a function that parses the following log data and returns a list of dictionaries with all useful fields extracted.
Each dictionary should represent one row or impression. Here is the sample data:

