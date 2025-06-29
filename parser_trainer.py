import os
import json
import base64
import importlib.util
from datetime import datetime
from openai import OpenAI
import traceback
import requests
import hashlib

UNHANDLED_DIR = "unhandled_logs"
PARSERS_DIR = "parsers"
REGISTRY_FILE = "parsers_registry.json"
OPENAI_MODEL = "gpt-4"

client = OpenAI()


def compute_fingerprint(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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


def push_file_to_github(local_path, remote_path, commit_message):
    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPO")
    branch = os.getenv("GITHUB_BRANCH", "main")

    url = f"https://api.github.com/repos/{repo}/contents/{remote_path}"
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")

    data = {
        "message": commit_message,
        "content": content,
        "branch": branch
    }

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }

    response = requests.put(url, headers=headers, json=data)
    if response.status_code not in (200, 201):
        print(f"[x] GitHub upload failed: {response.status_code} {response.text}")
    else:
        print(f"[✓] Committed to GitHub: {remote_path}")


def main():
    os.makedirs(PARSERS_DIR, exist_ok=True)
    os.makedirs(UNHANDLED_DIR, exist_ok=True)

    registry = load_registry()

    for file_name in os.listdir(UNHANDLED_DIR):
        log_path = os.path.join(UNHANDLED_DIR, file_name)
        print(f"[DEBUG] Handling file: {file_name}")

        try:
            try:
                with open(log_path, "rb") as f:
                    sample = f.read(2048).decode("utf-8", errors="ignore")
            except Exception as read_err:
                print(f"[!] Failed to read {file_name}: {read_err}")
                continue

            fingerprint = compute_fingerprint(sample)
            if fingerprint in registry:
                print(f"[~] Fingerprint already registered: {fingerprint}")
                continue

            parser_name = f"parser_{file_name.replace('.', '_')}_{int(datetime.utcnow().timestamp())}"
            code = generate_parser_code(sample)
            parser_path = write_parser_code(code, parser_name)

            if test_parser(parser_path, log_path):
                registry[fingerprint] = parser_name
                save_registry(registry)

                push_file_to_github(
                    local_path=parser_path,
                    remote_path=f"parsers/{parser_name}.py",
                    commit_message=f"Add parser: {parser_name}"
                )

                push_file_to_github(
                    local_path=REGISTRY_FILE,
                    remote_path="parsers_registry.json",
                    commit_message="Update parser registry"
                )

                os.remove(log_path)
                print(f"[✓] Added and pushed parser: {parser_name}")
            else:
                print(f"[x] Failed to validate parser for: {file_name}")

        except Exception as e:
            print(f"[!] Error handling {file_name}: {e}")


if __name__ == "__main__":
    main()
