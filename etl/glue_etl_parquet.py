# ================================
# IMPORT THƯ VIỆN
# ================================

import sys
# dùng để lấy arguments truyền vào Glue Job

import os
# dùng đọc environment variables

import time
# dùng sleep/wait khi polling Athena query

import boto3
# AWS SDK for Python
# dùng gọi Athena API

import traceback
# in lỗi chi tiết khi exception xảy ra


from awsglue.context import GlueContext
# Glue wrapper cho Spark

from pyspark.context import SparkContext
# Spark engine chính

from awsglue.job import Job
# quản lý Glue Job lifecycle

from awsglue.utils import getResolvedOptions
# lấy job parameters từ Glue

from pyspark.sql.functions import (
    col,                # lấy column
    to_date,            # convert string -> date
    year,               # lấy year từ date
    month,              # lấy month từ date
    when,               # if else
    regexp_replace,     # replace regex
    round,              # làm tròn số
    to_timestamp,       # string -> timestamp
    lit                 # tạo literal value
)

from pyspark.sql import DataFrame
# DataFrame type


# ================================
# LẤY JOB PARAMETERS
# ================================

args = getResolvedOptions(sys.argv, [

    'JOB_NAME',
    # tên Glue Job

    'RAW_S3',
    # input CSV path trên S3

    'OUTPUT_S3',
    # output parquet path

    'ATHENA_DATABASE',
    # Athena database name

    'ATHENA_QUERY_RESULTS_BUCKET',
    # nơi Athena lưu query result

    'ATHENA_REGION',
    # AWS region của Athena

    'ATHENA_TABLE'
    # Athena table name
])


# ================================
# KHỞI TẠO SPARK + GLUE
# ================================

sc = SparkContext()
# tạo SparkContext

glueContext = GlueContext(sc)
# tạo GlueContext từ Spark

spark = glueContext.spark_session
# lấy SparkSession


# ================================
# KHỞI TẠO GLUE JOB
# ================================

job = Job(glueContext)

job.init(args['JOB_NAME'], args)
# start Glue Job


# ================================
# INPUT / OUTPUT PATH
# ================================

raw_s3 = args['RAW_S3']
# input CSV location

out_s3 = args['OUTPUT_S3'].rstrip('/') + '/'
# output parquet location
# rstrip('/') để tránh ////


# ================================
# ATHENA CONFIG
# ================================

ATHENA_DATABASE = (
    args.get('ATHENA_DATABASE')
    or os.environ.get('ATHENA_DATABASE')
)

# lấy Athena database từ:
# 1. job parameter
# 2. environment variable


ATHENA_QUERY_RESULTS_BUCKET = (
    args.get('ATHENA_QUERY_RESULTS_BUCKET')
    or os.environ.get('ATHENA_QUERY_RESULTS_BUCKET')
)

# nơi Athena lưu query output


ATHENA_REGION = (
    args.get('ATHENA_REGION')
    or os.environ.get('ATHENA_REGION', 'ap-southeast-2')
)

# AWS region
# default ap-southeast-2


ATHENA_TABLE = (
    args.get('ATHENA_TABLE')
    or os.environ.get('ATHENA_TABLE', 'processed_sales')
)

# tên Athena table


# ================================
# NORMALIZE ATHENA RESULT PATH
# ================================

if (
    ATHENA_QUERY_RESULTS_BUCKET
    and
    not ATHENA_QUERY_RESULTS_BUCKET.endswith('/')
):

    ATHENA_QUERY_RESULTS_BUCKET += '/'

# thêm / cuối path nếu chưa có


# ================================
# LOG CONFIG
# ================================

print(f"JOB_NAME={args['JOB_NAME']}")
print(f"RAW_S3={raw_s3}")
print(f"OUTPUT_S3={out_s3}")

print(f"ATHENA_DATABASE={ATHENA_DATABASE}")
print(f"ATHENA_TABLE={ATHENA_TABLE}")
print(f"ATHENA_RESULTS={ATHENA_QUERY_RESULTS_BUCKET}")
print(f"ATHENA_REGION={ATHENA_REGION}")

# log config để debug


# ================================
# ĐỌC CSV TỪ S3
# ================================

df = (
    spark.read

    .option("header", True)
    # CSV có header

    .option("multiLine", False)
    # không hỗ trợ multiline CSV

    .csv(raw_s3)
    # đọc CSV từ S3
)

print("Initial columns:", df.columns)


# ================================
# HELPER FUNCTION
# ================================

def col_exists(df: DataFrame, name: str) -> bool:

    return name in df.columns

# check column có tồn tại không


# ================================
# NORMALIZE COLUMN NAMES
# ================================

for c in df.columns:

    normalized = (
        c.strip()
        # xóa space đầu/cuối

        .lower()
        # lowercase

        .replace(" ", "_")
        # replace space -> underscore
    )

    if normalized != c:

        df = df.withColumnRenamed(c, normalized)

# ví dụ:
# " Unit Price "
# ->
# "unit_price"

print("Columns after normalization:", df.columns)


# ================================
# CLEAN TOTALPRICE
# ================================

if (
    col_exists(df, "totalprice")
    or
    col_exists(df, "total_price")
):

    tp_col = (
        "totalprice"
        if col_exists(df, "totalprice")
        else "total_price"
    )

    # xác định tên cột đúng

    if tp_col != "totalprice":

        df = df.withColumnRenamed(
            tp_col,
            "totalprice"
        )

    # rename total_price -> totalprice


    df = df.withColumn(
        "totalprice_tmp",

        regexp_replace(
            col("totalprice"),
            ",",
            ""
        )
    )

    # xóa dấu phẩy
    # ví dụ:
    # "1,200"
    # ->
    # "1200"


    df = df.withColumn(

        "totalprice",

        when(
            col("totalprice_tmp").isin("", None),
            None
        )

        .otherwise(
            col("totalprice_tmp").cast("double")
        )
    )

    # convert string -> double

    df = df.drop("totalprice_tmp")
    # xóa cột temp


# ================================
# CLEAN UNIT_PRICE
# ================================

if (
    col_exists(df, "unit_price")
    or
    col_exists(df, "unitprice")
):

    up = (
        "unit_price"
        if col_exists(df, "unit_price")
        else "unitprice"
    )

    # xác định tên column

    if up != "unit_price":

        df = df.withColumnRenamed(
            up,
            "unit_price"
        )

    # rename unitprice -> unit_price


    df = df.withColumn(
        "unit_price",

        regexp_replace(
            col("unit_price"),
            ",",
            ""
        )
    )

    # xóa dấu phẩy


    df = df.withColumn(

        "unit_price",

        when(
            col("unit_price").isin("", None),
            None
        )

        .otherwise(
            col("unit_price").cast("double")
        )
    )

    # convert -> double


# ================================
# CLEAN QUANTITY
# ================================

if col_exists(df, "quantity"):

    df = df.withColumn(

        "quantity",

        when(
            col("quantity").isin("", None),
            None
        )

        .otherwise(
            col("quantity").cast("int")
        )
    )

# convert quantity -> int


# ================================
# CLEAN DISCOUNT
# ================================

if col_exists(df, "discount"):

    df = df.withColumn(

        "discount",

        when(
            col("discount").isin("", None),
            None
        )

        .otherwise(
            col("discount").cast("double")
        )
    )

    # convert -> double


    df = df.withColumn(

        "discount",

        when(

            (col("discount").isNotNull())
            &
            (col("discount") > 1),

            col("discount") / 100.0
        )

        .otherwise(col("discount"))
    )

    # nếu discount = 10
    # ->
    # 0.1


    df = df.withColumn(
        "discount",
        round(col("discount"), 4)
    )

    # làm tròn 4 số thập phân


# ================================
# PARSE ORDER_DATE
# ================================

if col_exists(df, "order_date"):

    df = df.withColumn(

        "order_date_parsed",

        to_date(
            col("order_date"),
            "yyyy-MM-dd"
        )
    )

    # thử parse format:
    # yyyy-MM-dd


    df = df.withColumn(

        "order_date_parsed",

        when(
            col("order_date_parsed").isNull(),

            to_date(
                to_timestamp(
                    col("order_date"),
                    "dd/MM/yyyy"
                )
            )
        )

        .otherwise(col("order_date_parsed"))
    )

    # nếu fail
    # thử dd/MM/yyyy


    df = df.withColumn(

        "order_date_parsed",

        when(
            col("order_date_parsed").isNull(),

            to_date(
                to_timestamp(
                    col("order_date"),
                    "dd-MM-yyyy"
                )
            )
        )

        .otherwise(col("order_date_parsed"))
    )

    # nếu vẫn fail
    # thử dd-MM-yyyy


    df = df.withColumn(
        "year",
        year(col("order_date_parsed"))
    )

    # tạo year partition column


    df = df.withColumn(
        "month",
        month(col("order_date_parsed"))
    )

    # tạo month partition column


else:

    # nếu không có order_date

    if not col_exists(df, "year"):

        df = df.withColumn(
            "year",
            lit(None).cast("int")
        )

    if not col_exists(df, "month"):

        df = df.withColumn(
            "month",
            lit(None).cast("int")
        )

    # tạo year/month NULL


# ================================
# RECOMPUTE TOTALPRICE
# ================================

if (

    col_exists(df, "totalprice")

    and

    (
        col_exists(df, "unit_price")
        and
        col_exists(df, "quantity")
        and
        col_exists(df, "discount")
    )
):

    df = df.withColumn(

        "totalprice",

        when(

            col("totalprice").isNull(),

            (
                col("unit_price")
                *
                col("quantity")
                *
                (1 - col("discount"))
            )
        )

        .otherwise(col("totalprice"))
    )

    # nếu totalprice NULL
    # thì tự tính:
    #
    # unit_price * quantity * (1-discount)


    df = df.withColumn(
        "totalprice",
        round(col("totalprice"), 2)
    )

    # làm tròn 2 số


# ================================
# CHỌN OUTPUT COLUMNS
# ================================

selected_cols = []

col_map = [

    ("order_id","order_id"),
    ("customer_id","customer_id"),
    ("customer_type","customer_type"),
    ("product","product"),
    ("category","category"),
    ("unit_price","unit_price"),
    ("quantity","quantity"),
    ("discount","discount"),
    ("totalprice","totalprice"),
    ("region","region"),
    ("order_date_parsed","order_date"),
    ("year","year"),
    ("month","month")
]

# mapping source -> target columns


for src, tgt in col_map:

    if col_exists(df, src):

        selected_cols.append(
            col(src).alias(tgt)
        )

    else:

        selected_cols.append(
            lit(None).alias(tgt)
        )

# nếu thiếu cột
# tạo NULL column


out_df = df.select(*selected_cols)

print("Output schema columns:", out_df.columns)


# ================================
# TÁCH DATA THEO PARTITION
# ================================

part_df = out_df.filter(

    col("year").isNotNull()
    &
    col("month").isNotNull()
)

# dữ liệu có partition key hợp lệ


other_df = out_df.filter(

    col("year").isNull()
    |
    col("month").isNull()
)

# dữ liệu parse date lỗi


# ================================
# COUNT RECORDS
# ================================

try:

    print(
        "Records with partition keys:",
        part_df.count(),

        "Records without date:",
        other_df.count()
    )

except Exception:

    print(
        "Skipping counts to avoid expensive operation"
    )

# count để debug
# nhưng dataset lớn có thể rất tốn


# ================================
# REPARTITION
# ================================

if (
    col_exists(part_df, "year")
    and
    col_exists(part_df, "month")
):

    part_df = part_df.repartition(

        col("year"),
        col("month")
    )

# shuffle data
# gom cùng year/month về cùng partition


# ================================
# WRITE PARQUET
# ================================

part_df.write \

    .mode("append") \
    # append thêm parquet files mới

    .partitionBy("year", "month") \
    # tạo folder:
    # year=2025/month=5/

    .parquet(out_s3)

# ghi parquet lên S3


# ================================
# WRITE INVALID DATE DATA
# ================================

if other_df.rdd.isEmpty() is False:

    other_df.write \
        .mode("append") \
        .parquet(out_s3 + "no_date/")

# dữ liệu parse date fail
# ghi riêng vào no_date/


# =====================================================
# ATHENA: TỰ ĐỘNG ADD PARTITION VÀO ATHENA
# =====================================================

# Vì table Athena là external table nên:
#
# Athena KHÔNG tự biết trên S3 có folder mới.
#
# Ví dụ Glue vừa ghi:
#
# s3://processed/year=2025/month=5/
#
# thì Athena chưa query được ngay.
#
# Cần phải add metadata partition:
#
# ALTER TABLE sales_table
# ADD PARTITION (...)
#
# Đoạn code dưới tự động làm việc đó.


# =====================================================
# CHẠY ATHENA QUERY VÀ ĐỢI HOÀN THÀNH
# =====================================================

def run_athena_and_wait(
    query,
    database,
    result_s3,
    region,
    timeout_seconds=300
):

    # tạo Athena client
    athena = boto3.client(
        'athena',
        region_name=region
    )

    # submit query lên Athena
    resp = athena.start_query_execution(

        QueryString=query,
        # câu SQL cần chạy

        QueryExecutionContext={
            'Database': database
        },
        # database Athena

        ResultConfiguration={
            'OutputLocation': result_s3
        }
        # nơi Athena lưu kết quả query
    )

    # lấy query execution id
    qid = resp['QueryExecutionId']

    # lưu thời gian bắt đầu
    start = time.time()


    # loop liên tục để check query đã chạy xong chưa
    while True:

        resp2 = athena.get_query_execution(
            QueryExecutionId=qid
        )

        # lấy trạng thái query
        st = resp2['QueryExecution']['Status']['State']


        # nếu query chạy xong
        if st in (
            'SUCCEEDED',
            'FAILED',
            'CANCELLED'
        ):

            # lấy lý do lỗi (nếu có)
            reason = (
                resp2['QueryExecution']
                ['Status']
                .get('StateChangeReason', '')
            )

            # trả về status
            return st, qid, reason


        # nếu chạy quá timeout
        if time.time() - start > timeout_seconds:

            return 'TIMEOUT', qid, 'timeout'


        # đợi 2 giây rồi check tiếp
        time.sleep(2)



# =====================================================
# ADD PARTITIONS VÀO ATHENA
# =====================================================

def add_partitions_with_athena(
    out_s3_prefix,
    db,
    table,
    results_bucket,
    region
):

    """
    Hàm này sẽ:

    1. Lấy các partition year/month vừa được ghi
    2. Chạy ALTER TABLE ADD PARTITION
    3. Để Athena nhìn thấy data mới trên S3
    """


    # nếu thiếu Athena config
    # thì skip
    if not (db and results_bucket):

        print(
            "ATHENA_DATABASE or "
            "ATHENA_QUERY_RESULTS_BUCKET "
            "not provided"
        )

        return


    # =====================================================
    # LẤY DANH SÁCH PARTITIONS MỚI
    # =====================================================

    parts = (

        part_df

        .select("year", "month")

        .distinct()

        .collect()
    )

    # ví dụ:
    #
    # [
    #   (2025, 5),
    #   (2025, 6)
    # ]
    #
    # nghĩa là ETL vừa ghi:
    #
    # year=2025/month=5/
    # year=2025/month=6/


    # nếu không có partition nào
    if not parts:

        print("No partitions found to add.")

        return


    print(
        "Starting Athena ALTER TABLE "
        "partition adds:",
        parts
    )


    # =====================================================
    # LOOP TỪNG PARTITION
    # =====================================================

    for r in parts:

        y = r['year']
        m = r['month']

        # bỏ qua nếu null
        if y is None or m is None:
            continue


        # convert sang int cho chắc chắn
        try:

            yi = int(y)
            mi = int(m)

        except Exception:

            yi = y
            mi = m


        # =====================================================
        # TẠO S3 LOCATION CỦA PARTITION
        # =====================================================

        loc = (
            f"{out_s3_prefix.rstrip('/')}"
            f"/year={yi}/month={mi}/"
        )

        # ví dụ:
        #
        # s3://bucket/processed/
        #       year=2025/
        #       month=5/


        # =====================================================
        # TẠO CÂU SQL ADD PARTITION
        # =====================================================

        query = (

            f"ALTER TABLE {table} "

            f"ADD IF NOT EXISTS "

            f"PARTITION "
            f"(year={yi}, month={mi}) "

            f"LOCATION '{loc}';"
        )

        # ví dụ SQL:
        #
        # ALTER TABLE sales_table
        # ADD IF NOT EXISTS
        # PARTITION (year=2025, month=5)
        # LOCATION
        # 's3://bucket/processed/year=2025/month=5/'


        print("Submitting Athena query:", query)


        # =====================================================
        # CHẠY QUERY ATHENA
        # =====================================================

        try:

            status, qid, reason = run_athena_and_wait(

                query,

                database=db,

                result_s3=results_bucket,

                region=region,

                timeout_seconds=300
            )

            # log kết quả
            print(
                f"Athena ALTER partition result:"
                f" status={status}"
                f" queryId={qid}"
                f" reason={reason}"
            )


            # nếu query fail
            if status != 'SUCCEEDED':

                print(
                    "Check Athena console "
                    "for queryId:",
                    qid
                )


        except Exception as e:

            print(
                "Failed to submit ALTER PARTITION:",
                str(e)
            )

            # in stacktrace lỗi
            traceback.print_exc()



# =====================================================
# GỌI HÀM ADD PARTITIONS
# =====================================================

add_partitions_with_athena(

    out_s3,
    # output parquet path

    ATHENA_DATABASE,
    # Athena database

    ATHENA_TABLE,
    # Athena table

    ATHENA_QUERY_RESULTS_BUCKET,
    # nơi Athena lưu query results

    ATHENA_REGION
    # AWS region
)

# Sau bước này:
#
# Athena sẽ nhìn thấy partition mới
#
# Ví dụ query:
#
# SELECT *
# FROM sales_table
# WHERE year=2025 AND month=5;
#
# sẽ chạy được ngay.



# =====================================================
# KẾT THÚC GLUE JOB
# =====================================================

job.commit()

# commit job
# đánh dấu ETL hoàn thành thành công


print("Job finished successfully.")
