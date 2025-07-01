# parser_trainer.py

import os
import openai
import pandas as pd
import hashlib
from main_parser import fingerprint_csv, save_parser_to_repo
from datetime import datetime
from github import Github
import boto3
from io import BytesIO
import re

# === Environment setup ===
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
UNHANDLED_PREFIX = "unhandled_logs/"
HANDLED_PREFIX = "handled_logs/"

REPO_NAME = os.getenv("GITHUB_REPO")  # e.g., "zSparkman/SpotIQAPI"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

PARSERS_DIR = "parsers"
os.makedirs(PARSERS_DIR, exist_ok=True)

# === AWS S3 setup ===
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
)

# === GitHub setup ===
gh = Github(GITHUB_TOKEN)
repo = gh.get_repo(REPO_NAME)

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
    try:
        existing_file = repo.get_contents(filepath)
        repo.update_file(existing_file.path, commit_msg, content, existing_file.sha)
    except Exception:
