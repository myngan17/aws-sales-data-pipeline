# =====================================================
# IMPORT THƯ VIỆN
# =====================================================

import os
# dùng đọc environment variables

import json
# dùng print event dạng JSON cho dễ debug

import boto3
# AWS SDK cho Python
# dùng gọi Glue, S3, Athena...

import time
# dùng sleep / timeout

import traceback
# dùng in lỗi chi tiết

from urllib.parse import unquote_plus
# decode URL encoded string từ S3 event
# ví dụ:
# file%20name.csv -> file name.csv



# =====================================================
# REGION
# =====================================================

REGION = (
    os.environ.get('AWS_REGION')
    or
    os.environ.get('ATHENA_REGION')
    or
    'ap-southeast-1'
)

# lấy AWS region từ Lambda environment variables
#
# ưu tiên:
# 1. AWS_REGION
# 2. ATHENA_REGION
# 3. default ap-southeast-1



# =====================================================
# TẠO AWS CLIENTS
# =====================================================

glue = boto3.client(
    'glue',
    region_name=REGION
)

# client dùng để start Glue Job


s3 = boto3.client(
    's3',
    region_name=REGION
)

# client dùng check object trên S3



# =====================================================
# CẤU HÌNH
# =====================================================

GLUE_JOB_NAME = os.environ.get(
    'GLUE_JOB_NAME',
    'glue_etl_parquet'
)

# tên Glue job cần trigger


OUTPUT_S3 = os.environ.get(
    'OUTPUT_S3',
    's3://company-analytics-datalake/processed/'
)

# nơi Glue ETL ghi parquet output


ADDITIONAL_ARGS = os.environ.get(
    'ADDITIONAL_ARGS',
    ''
)

# extra arguments truyền thêm cho Glue Job



# =====================================================
# CHỜ FILE S3 UPLOAD XONG
# =====================================================

def wait_for_object_complete(
    bucket,
    key,
    timeout=60,
    poll_interval=1
):

    """
    Chờ cho tới khi:
    - object tồn tại
    - size ổn định

    để tránh trigger khi file upload chưa xong
    """

    start = time.time()

    last_size = -1

    while True:

        try:

            # lấy metadata object
            resp = s3.head_object(
                Bucket=bucket,
                Key=key
            )

            # lấy file size
            size = resp.get(
                'ContentLength',
                0
            )

            # nếu size ổn định
            if (
                size > 0
                and
                size == last_size
            ):

                return True

            # update last size
            last_size = size


        except s3.exceptions.NoSuchKey:

            # object chưa xuất hiện
            pass


        except Exception as e:

            print(
                "warn: head_object exception:",
                str(e)
            )


        # nếu timeout
        if time.time() - start > timeout:

            print(
                f"timeout waiting for "
                f"s3://{bucket}/{key}"
            )

            return False


        # đợi rồi check tiếp
        time.sleep(poll_interval)



# =====================================================
# PARSE ADDITIONAL ARGS
# =====================================================

def parse_additional_args(raw: str):

    """
    Parse env var ADDITIONAL_ARGS
    thành dict cho Glue Job

    ví dụ:

    "--KEY val"

    ->
    {
      '--KEY': 'val'
    }
    """

    out = {}

    if not raw:
        return out

    raw = raw.strip()


    # =====================================================
    # FORMAT KEY=VALUE
    # =====================================================

    if '=' in raw and (',' in raw or '=' in raw):

        toks = [

            t.strip()

            for t in raw.replace(',', ' ').split()

            if t.strip()
        ]

        for t in toks:

            if '=' in t:

                k, v = t.split('=', 1)

                k = k.strip().lstrip('-')

                out[f'--{k}'] = v.strip()

        return out


    # =====================================================
    # FORMAT "--KEY value"
    # =====================================================

    toks = raw.split()

    i = 0

    while i < len(toks) - 1:

        k = toks[i].lstrip('-')

        v = toks[i+1]

        out[f'--{k}'] = v

        i += 2

    return out



# =====================================================
# MAIN LAMBDA HANDLER
# =====================================================

def lambda_handler(event, context):

    """
    Hàm chính Lambda

    Trigger từ:
    S3 PUT EVENT

    Khi upload file lên S3
    Lambda sẽ:
    -> start Glue ETL Job
    """

    print(
        "DEBUG EVENT:",
        json.dumps(event)
    )


    # log request id
    try:

        print(
            "REQUEST_ID:",
            getattr(
                context,
                'aws_request_id',
                None
            )
        )

    except Exception:
        pass


    results = []


    # parse extra args
    extra_args = parse_additional_args(
        ADDITIONAL_ARGS
    )


    # lấy records từ S3 event
    records = event.get('Records', [])


    # nếu không có records
    if not records:

        print("No Records in event")

        return {
            'statusCode': 400,
            'message': 'no records'
        }



    # =====================================================
    # LOOP TỪNG FILE EVENT
    # =====================================================

    for rec in records:

        try:

            s3info = (
                rec.get('s3')
                or
                rec.get('S3')
            )

            if not s3info:

                print(
                    "Skipping record:",
                    rec
                )

                results.append({
                    'status': 'skipped'
                })

                continue


            # =====================================================
            # LẤY BUCKET + FILE KEY
            # =====================================================

            bucket = s3info['bucket']['name']

            key = unquote_plus(
                s3info['object']['key']
            )

            print(
                f"bucket={bucket}, key={key}"
            )


            # =====================================================
            # BỎ QUA FOLDER
            # =====================================================

            if key.endswith('/'):

                print(
                    "Ignoring folder:",
                    key
                )

                results.append({
                    'status': 'ignored'
                })

                continue



            # =====================================================
            # CHỜ FILE UPLOAD XONG
            # =====================================================

            wait_for_object_complete(
                bucket,
                key,
                timeout=30
            )



            # =====================================================
            # TẠO S3 PATH FILE RAW
            # =====================================================

            raw_s3_path = (
                f"s3://{bucket}/{key}"
            )



            # =====================================================
            # TẠO ARGUMENTS CHO GLUE JOB
            # =====================================================

            args = {

                '--RAW_S3': raw_s3_path,

                '--OUTPUT_S3': OUTPUT_S3
            }

            # add extra args
            args.update(extra_args)



            # =====================================================
            # START GLUE JOB
            # =====================================================

            print(
                "Starting Glue Job",
                GLUE_JOB_NAME
            )

            resp = glue.start_job_run(

                JobName=GLUE_JOB_NAME,

                Arguments=args
            )


            # lấy Glue Job Run ID
            jobrunid = resp.get('JobRunId')


            print(
                "Started Glue job:",
                jobrunid
            )



            # =====================================================
            # LƯU RESULT
            # =====================================================

            results.append({

                'status': 'started',

                'jobRunId': jobrunid,

                'raw': raw_s3_path
            })


        # =====================================================
        # HANDLE ERROR
        # =====================================================

        except Exception as e:

            tb = traceback.format_exc()

            print("Exception:", str(e))

            print(tb)

            results.append({

                'status': 'error',

                'error': str(e),

                'trace': tb
            })



    # =====================================================
    # RETURN RESPONSE
    # =====================================================

    return {

        'statusCode': 200,

        'results': results
    }
