import os
import importlib.util
import boto3
import tempfile

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
PARSERS_PREFIX = "parser_modules/"

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
)

def load_all_parsers():
    parser_map = {}
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=PARSERS_PREFIX)

    if "Contents" not in response:
        return parser_map

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".py"):
            continue

        filename = key.split("/")[-1]
        module_name = filename[:-3]

        with tempfile.NamedTemporaryFile("wb", delete=False, suffix=".py") as tmp:
            s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            tmp.write(s3_obj["Body"].read())
            tmp_path = tmp.name

        spec = importlib.util.spec_from_file_location(module_name, tmp_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        parser_map[module_name] = module.parse

    return parser_map
