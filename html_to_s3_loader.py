import re
import boto3
import requests

SRC = "https://download.bls.gov/pub/time.series/pr/"
UA = "BLS S3 sync (Neelima Naladimmu; neelima.naladimmu1106@gmail.com)"  # put real contact info

def list_src(s):
    html = s.get(SRC, timeout=30).text
    return sorted(set(re.findall(r'/pub/time\.series/pr/([^"<]+)', html)))

def list_s3_buckets():
    s3 = boto3.client("s3")
    response = s3.list_buckets()

    print("S3 Buckets:")
    for bucket in response.get("Buckets", []):
        print(f"- {bucket['Name']}")

def s3_keys(s3, bucket, prefix):
    out, token = {}, None
    while True:
        r = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=token) if token \
            else s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for o in r.get("Contents", []):
            out[o["Key"]] = o["Size"]
        if not r.get("IsTruncated"): break
        token = r["NextContinuationToken"]
    return out

def main(bucket, prefix="bls/pr/"):
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "*/*"})
    s3 = boto3.client("s3")

    files = list_src(s)
    existing = s3_keys(s3, bucket, prefix)
    keep = set()

    for f in files:
        url = SRC + f
        h = s.head(url, timeout=30); h.raise_for_status()
        size = int(h.headers.get("Content-Length", "0"))
        lm = h.headers.get("Last-Modified", "")
        key = prefix + f
        keep.add(key)

        try:
            obj = s3.head_object(Bucket=bucket, Key=key)
            meta = obj.get("Metadata", {})
            same = (meta.get("src_lm", "") == lm and meta.get("src_size", "") == str(size))
            if same:
                print("skip ", f); continue
        except s3.exceptions.ClientError:
            pass

        with s.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            s3.upload_fileobj(
                r.raw, bucket, key,
                ExtraArgs={"Metadata": {"src_lm": lm, "src_size": str(size), "src_url": url}}
            )
        print("up   ", f)

    for k in existing:
        if k not in keep:
            s3.delete_object(Bucket=bucket, Key=k)
            print("del  ", k[len(prefix):])

if __name__ == "__main__":
    main(bucket="s3-quest-bls-dataset-neelima", prefix="bls/pr/")
    """s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "*/*"})
    print(list_src(s))
    list_s3_buckets()"""
