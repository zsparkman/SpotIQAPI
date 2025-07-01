import os
import openai
import pandas as pd
import hashlib
from main_parser import fingerprint_csv
from s3_utils import upload_parser_module
import boto3
from io import BytesIO
import re

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
UNHANDLED_PREFIX = "spotiq-data/unhandled_logs/"
HANDLED_PREFIX = "spotiq-data/handled_logs/"

PARSERS_DIR = "parsers"
os.makedirs(PARSERS_DIR, exist_ok=True)

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
)

client = openai.OpenAI()

def generate_parser_code(columns: list) -> str:
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

def move_s3_object(old_key, new_key):
    s3_client.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': old_key}, Key=new_key)
    s3_client.delete_object(Bucket=S3_BUCKET, Key=old_key)

def sanitize_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^\w\-.()]", "_", name)
    name = name.replace("..", "_")
    return name

def handle_unprocessed_files():
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=UNHANDLED_PREFIX)
    if "Contents" not in response:
        print("[trainer] No unhandled logs in S3.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.lower().endswith(".csv") or key == UNHANDLED_PREFIX:
            continue

        filename = key.split("/")[-1]
        safe_filename = sanitize_filename(filename)
        print(f"[trainer] Handling {filename}")

        try:
            file_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            raw_bytes = file_obj["Body"].read()
            df = pd.read_csv(BytesIO(raw_bytes))

            if df.empty or df.shape[1] < 2:
                print(f"[trainer] Skipped {filename} - empty or invalid structure.")
                continue

            columns = list(df.columns)
            parser_code = generate_parser_code(columns)

            # Validate before saving
            try:
                compile(parser_code, "<generated_parser>", "exec")
            except SyntaxError as e:
                print(f"[trainer] Invalid parser skipped: {e}")
                continue

            fingerprint = fingerprint_csv(df)
            parser_filename = f"{fingerprint}.py"
            parser_path = os.path.join(PARSERS_DIR, parser_filename)

            with open(parser_path, "w") as f:
                f.write(parser_code)

            with open(parser_path, "rb") as f:
                upload_parser_module(parser_filename, f.read())

            new_key = f"{HANDLED_PREFIX}{safe_filename}"
            move_s3_object(key, new_key)

            print(f"[trainer] Trained and uploaded parser: {parser_filename}")

        except Exception as e:
            print(f"[trainer] ERROR handling {filename}: {e}")

if __name__ == "__main__":
    handle_unprocessed_files()
