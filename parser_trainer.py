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
    prompt = (
        "You are a Python engineer. Write a function that parses the following log data "
        "and returns a list of dictionaries with all useful fields extracted.\n"
        "Each dictionary should represent one row or impression.\n\n"
        "Here is the sample data:\n\n"
        f"{sample_text}\n\n"
        "The function should be named `parse` and accept a single argument: `file_path`.\n"
        "It should return a list of dictionaries.\n"
        "Only return the function code — no explanation, no comments."
    )

    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content

def test_parser(parser_path, log_file):
    try:
        spec = importlib.util.spec_from_file_location("parser_module", parser_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        results = module.parse(log_file)
        return isinstance(results, list) and isinstance(results[0], dict)
    except Exception:
        traceback.print_exc()
        return False

def write_parser_code(code, parser_name):
    parser_path = os.path.join(PARSERS_DIR, f"{parser_name}.py")
    with open(parser_path, "w") as f:
        f.write(code)
    return parser_path

def main():
    os.makedirs(PARSERS_DIR, exist_ok=True)
    os.makedirs(UNHANDLED_DIR, exist_ok=True)

    registry = load_registry()

    for file_name in os.listdir(UNHANDLED_DIR):
        log_path = os.path.join(UNHANDLED_DIR, file_name)

        try:
            with open(log_path, "r") as f:
                sample = "".join([next(f) for _ in range(20)])

            parser_name = f"parser_{file_name.replace('.', '_')}_{int(datetime.utcnow().timestamp())}"
            code = generate_parser_code(sample)
            parser_path = write_parser_code(code, parser_name)

            if test_parser(parser_path, log_path):
                registry[file_name] = parser_name
                save_registry(registry)
                print(f"[✓] Added parser: {parser_name}")
                os.remove(log_path)
            else:
                print(f"[x] Failed to validate parser for: {file_name}")

        except Exception as e:
            print(f"[!] Error handling {file_name}: {e}")

if __name__ == "__main__":
    main()
