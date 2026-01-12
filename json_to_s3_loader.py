import json
import argparse
from datetime import datetime, timezone

import boto3
import requests

URL = ("https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
       "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population")


def main(bucket: str, key: str):
    #Fetch JSON from API
    r = requests.get(URL, timeout=30, headers={"User-Agent": "datausa-s3-uploader/1.0"})
    r.raise_for_status()
    data = r.json()

    data["_fetched_at_utc"] = datetime.now(timezone.utc).isoformat()

    #Upload JSON to S3
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Uploaded to s3://{bucket}/{key}")


if __name__ == "__main__":
    main("s3-quest-bls-dataset-neelima", "demographics")