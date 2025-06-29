# parser_trainer.py

import os
import openai
import pandas as pd
import hashlib
from main_parser import fingerprint_csv, save_parser_to_repo
from datetime import datetime
import base64
from github import Github

UNHANDLED_DIR = "unhandled_logs"
HANDLED_DIR = "handled_logs"
PARSERS_DIR = "parsers"
REPO_NAME = os.getenv("GITHUB_REPO")  # e.g., "zSparkman/SpotIQAPI"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

os.makedirs(UNHANDLED_DIR, exist_ok=True)
os.makedirs(HANDLED_DIR, exist_ok=True)

client = openai.OpenAI()

def generate_parser_code(columns: list) -> str:
    col_args = ", ".join([f'"{col}"' for col in columns])
    prompt = f"""You are a Python developer.

Write a Python function called `parse` that takes a pandas DataFrame as input and returns a new DataFrame with only these columns: {columns}.

The function should:
- Drop any completely empty rows
- Normalize column names to lowercase
- Rename them exactly to: {columns}

Return ONLY the Python code that defines the function."""
    
    response = client.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that writes clean and functional pandas code."},
            {"role": "user", "content": prompt}
        ]
    )
    
    return response.choices[0].message.content.strip()

def commit_to_github(filepath: str, content: str, commit_msg: str):
    gh = Github(GITHUB_TOKEN)
    repo = gh.get_repo(REPO_NAME)

    try:
        existing_file = repo.get_contents(filepath)
        repo.update_file(
            existing_file.path,
            commit_msg,
            content,
            existing_file.sha
        )
    except Exception:
        repo.create_file(filepath, commit_msg, content)

def handle_unprocessed_files():
    for filename in os.listdir(UNHANDLED_DIR):
        if not filename.lower().endswith(".csv"):
            continue

        file_path = os.path.join(UNHANDLED_DIR, filename)
        try:
            print(f"[trainer] Handling {filename}")
            df = pd.read_csv(file_path)
            if df.empty or df.shape[1] < 2:
                print(f"[trainer] Skipped {filename} - empty or invalid structure.")
                continue

            columns = list(df.columns)
            parser_code = generate_parser_code(columns)

            fingerprint = fingerprint_csv(df)
            parser_filename = f"{fingerprint}.py"
            parser_path = os.path.join(PARSERS_DIR, parser_filename)

            with open(parser_path, "w") as f:
                f.write(parser_code)

            # Commit parser to GitHub
            with open(parser_path, "r") as f:
                content = f.read()
                commit_to_github(
                    filepath=f"{PARSERS_DIR}/{parser_filename}",
                    content=content,
                    commit_msg=f"Add parser for {fingerprint}"
                )

            # Move handled file
            new_handled_path = os.path.join(HANDLED_DIR, filename)
            os.rename(file_path, new_handled_path)

            # Commit moved log to GitHub
            with open(new_handled_path, "rb") as f:
                encoded_log = base64.b64encode(f.read()).decode("utf-8")
                commit_to_github(
                    filepath=f"{HANDLED_DIR}/{filename}",
                    content=base64.b64decode(encoded_log).decode("utf-8", errors="ignore"),
                    commit_msg=f"Move handled log {filename}"
                )

            print(f"[trainer] Trained and committed parser: {parser_filename}")

        except Exception as e:
            print(f"[trainer] ERROR handling {filename}: {e}")

if __name__ == "__main__":
    handle_unprocessed_files()
