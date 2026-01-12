# lambda_handlers/ingest_lambda.py
import os
from html_to_s3_loader import main as html_loader
from json_to_s3_loader import main as json_loader

BUCKET = os.environ["BUCKET"]
BLS_PREFIX = os.environ.get("BLS_PREFIX", "bls/pr/")
POP_PREFIX = os.environ.get("POP_PREFIX", "demographics")

def handler(event, context):
    html_loader(bucket=BUCKET, prefix=BLS_PREFIX)
    json_loader(bucket=BUCKET, prefix=POP_PREFIX)
    return {"status": "ok"}
