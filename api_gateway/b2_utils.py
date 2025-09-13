# shared/b2_utils.py
import os
import urllib.parse
import requests
from b2sdk.v2 import InMemoryAccountInfo, B2Api

B2_APPLICATION_KEY_ID = os.getenv("B2_APPLICATION_KEY_ID")
B2_APPLICATION_KEY = os.getenv("B2_APPLICATION_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")

if not (B2_APPLICATION_KEY_ID and B2_APPLICATION_KEY and B2_BUCKET_NAME):
    raise RuntimeError("Missing Backblaze B2 env vars")

info = InMemoryAccountInfo()
b2_api = B2Api(info)
b2_api.authorize_account("production", B2_APPLICATION_KEY_ID, B2_APPLICATION_KEY)
bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)

def upload_to_b2(local_path, b2_filename, content_type="application/octet-stream"):
    with open(local_path, "rb") as f:
        data = f.read()
    bucket.upload_bytes(data, b2_filename, content_type=content_type)
    return b2_filename

def get_signed_url(file_name, valid_seconds=3600):
    auth_token = bucket.get_download_authorization(
        file_name_prefix=file_name,
        valid_duration_in_seconds=valid_seconds
    )
    base_url = b2_api.account_info.get_download_url()
    download_url = f"{base_url}/file/{bucket.name}/{urllib.parse.quote(file_name)}"
    return f"{download_url}?Authorization={auth_token}"

def download_from_b2_to(local_path, b2_filename, valid_seconds=3600):
    url = get_signed_url(b2_filename, valid_seconds=valid_seconds)
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    return local_path
