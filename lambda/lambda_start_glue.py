# lambda_start_glue.py
import os
import json
import boto3
import time
import traceback
from urllib.parse import unquote_plus

# region: lấy từ env hoặc fallback
REGION = os.environ.get('AWS_REGION') or os.environ.get('ATHENA_REGION') or 'ap-southeast-1'

glue = boto3.client('glue', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

# cấu hình (set trong Lambda env vars)
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'slue_etl_parquet')
OUTPUT_S3 = os.environ.get('OUTPUT_S3', 's3://company-analytics-datalake/processed/')
ADDITIONAL_ARGS = os.environ.get('ADDITIONAL_ARGS', '')  # optional

def wait_for_object_complete(bucket, key, timeout=60, poll_interval=1):
    """
    Wait until object exists and size is stable for one poll interval.
    Returns True if stable/existing, False on timeout (we still proceed).
    """
    start = time.time()
    last_size = -1
    while True:
        try:
            resp = s3.head_object(Bucket=bucket, Key=key)
            size = resp.get('ContentLength', 0)
            # if size is stable and > 0 for two consecutive checks (approx)
            if size > 0 and size == last_size:
                return True
            last_size = size
        except s3.exceptions.NoSuchKey:
            # object not present yet
            pass
        except Exception as e:
            # log and continue; sometimes eventual consistency
            print("warn: head_object exception:", str(e))
        if time.time() - start > timeout:
            print(f"wait_for_object_complete: timeout waiting for s3://{bucket}/{key}")
            return False
        time.sleep(poll_interval)

def parse_additional_args(raw: str):
    """
    Parse ADDITIONAL_ARGS env var into dict.
    Supports:
      - "--KEY val --KEY2 val2"
      - "KEY=val,KEY2=val2"
    Returns dict of form {'--KEY': 'val', ...}
    """
    out = {}
    if not raw:
        return out
    raw = raw.strip()
    # try key=val comma sep
    if '=' in raw and (',' in raw or '=' in raw):
        # parse comma separated or space separated key=value
        toks = [t.strip() for t in raw.replace(',', ' ').split() if t.strip()]
        for t in toks:
            if '=' in t:
                k, v = t.split('=', 1)
                k = k.strip().lstrip('-')
                out[f'--{k}'] = v.strip()
        return out
    # fallback parse as alternating tokens
    toks = raw.split()
    i = 0
    while i < len(toks) - 1:
        k = toks[i].lstrip('-')
        v = toks[i+1]
        out[f'--{k}'] = v
        i += 2
    return out

def lambda_handler(event, context):
    """
    Lambda entrypoint for S3 event -> start Glue job.
    Expects S3 put notification event format.
    """
    print("DEBUG: EVENT:", json.dumps(event))
    try:
        print("DEBUG: REQUEST_ID:", getattr(context, 'aws_request_id', None))
    except Exception:
        pass

    results = []
    # build extra args once
    extra_args = parse_additional_args(ADDITIONAL_ARGS)

    records = event.get('Records', [])
    if not records:
        print("No Records in event; exiting.")
        return {'statusCode': 400, 'message': 'no records'}

    for rec in records:
        try:
            s3info = rec.get('s3') or rec.get('S3')  # be tolerant with casing
            if not s3info:
                print("Skipping record without s3 info:", rec)
                results.append({'status': 'skipped', 'reason': 'no s3 info'})
                continue

            bucket = s3info['bucket']['name']
            # URL decode key (S3 event gives URL-encoded key)
            key = unquote_plus(s3info['object']['key'])
            print(f"Record -> bucket: {bucket}, key: {key}")

            # ignore folder markers
            if key.endswith('/'):
                print("Ignoring folder key:", key)
                results.append({'status': 'ignored', 'key': key})
                continue

            # optional: only accept prefix raw/
            # if not key.startswith('raw/'):
            #     print("Ignoring key not under raw/:", key)
            #     results.append({'status': 'ignored_prefix', 'key': key})
            #     continue

            # wait until upload complete (helps avoid partial multipart trigger)
            wait_for_object_complete(bucket, key, timeout=30)

            raw_s3_path = f"s3://{bucket}/{key}"
            # build arguments: must start with "--"
            args = {'--RAW_S3': raw_s3_path, '--OUTPUT_S3': OUTPUT_S3}
            args.update(extra_args)

            print("Starting Glue job", GLUE_JOB_NAME, "with args", args)
            resp = glue.start_job_run(JobName=GLUE_JOB_NAME, Arguments=args)
            jobrunid = resp.get('JobRunId')
            print("Started Glue job:", jobrunid, "for", raw_s3_path)
            results.append({'status': 'started', 'jobRunId': jobrunid, 'raw': raw_s3_path})
        except Exception as e:
            tb = traceback.format_exc()
            print("Exception processing record:", str(e))
            print(tb)
            results.append({'status': 'error', 'error': str(e), 'trace': tb, 'record': rec})

    return {'statusCode': 200, 'results': results}
