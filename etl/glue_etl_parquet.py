# glue_etl_parquet.py
import sys
import os
import time
import boto3
import traceback

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    col, to_date, year, month, when, regexp_replace, round, to_timestamp, lit
)
from pyspark.sql import DataFrame

# -----------------------
# Lấy args (bắt buộc) - NOTE: thêm các ATHENA_* để đọc từ Job parameters (--ARG)
# -----------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'RAW_S3', 'OUTPUT_S3',
    'ATHENA_DATABASE', 'ATHENA_QUERY_RESULTS_BUCKET', 'ATHENA_REGION', 'ATHENA_TABLE'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

raw_s3 = args['RAW_S3']
out_s3 = args['OUTPUT_S3'].rstrip('/') + '/'

# Lấy ATHENA config từ args trước, nếu không có thì fallback sang environment variables (tiện cho cả 2 cách cấu hình)
ATHENA_DATABASE = args.get('ATHENA_DATABASE') or os.environ.get('ATHENA_DATABASE')
ATHENA_QUERY_RESULTS_BUCKET = args.get('ATHENA_QUERY_RESULTS_BUCKET') or os.environ.get('ATHENA_QUERY_RESULTS_BUCKET')
ATHENA_REGION = args.get('ATHENA_REGION') or os.environ.get('ATHENA_REGION', 'ap-southeast-2')
ATHENA_TABLE = args.get('ATHENA_TABLE') or os.environ.get('ATHENA_TABLE', 'processed_sales')

# normalize results bucket path
if ATHENA_QUERY_RESULTS_BUCKET and not ATHENA_QUERY_RESULTS_BUCKET.endswith('/'):
    ATHENA_QUERY_RESULTS_BUCKET = ATHENA_QUERY_RESULTS_BUCKET + '/'

print(f"JOB_NAME={args['JOB_NAME']}, RAW_S3={raw_s3}, OUTPUT_S3={out_s3}")
print(f"ATHENA_DATABASE={ATHENA_DATABASE}, ATHENA_TABLE={ATHENA_TABLE}, ATHENA_RESULTS={ATHENA_QUERY_RESULTS_BUCKET}, ATHENA_REGION={ATHENA_REGION}")

# -----------------------
# Đọc CSV
# -----------------------
df = spark.read.option("header", True).option("multiLine", False).csv(raw_s3)
print("Initial columns:", df.columns)

# helper
def col_exists(df: DataFrame, name: str) -> bool:
    return name in df.columns

# Normalize column names
for c in df.columns:
    normalized = c.strip().lower().replace(" ", "_")
    if normalized != c:
        df = df.withColumnRenamed(c, normalized)
print("Columns after normalization:", df.columns)

# ---- totalprice
if col_exists(df, "totalprice") or col_exists(df, "total_price"):
    tp_col = "totalprice" if col_exists(df, "totalprice") else "total_price"
    if tp_col != "totalprice":
        df = df.withColumnRenamed(tp_col, "totalprice")
    df = df.withColumn("totalprice_tmp", regexp_replace(col("totalprice"), ",", ""))
    df = df.withColumn("totalprice",
                       when(col("totalprice_tmp").isin("", None), None)
                       .otherwise(col("totalprice_tmp").cast("double")))
    df = df.drop("totalprice_tmp")

# ---- unit_price, quantity, discount
if col_exists(df, "unit_price") or col_exists(df, "unitprice"):
    up = "unit_price" if col_exists(df, "unit_price") else "unitprice"
    if up != "unit_price":
        df = df.withColumnRenamed(up, "unit_price")
    df = df.withColumn("unit_price", regexp_replace(col("unit_price"), ",", ""))
    df = df.withColumn("unit_price", when(col("unit_price").isin("", None), None).otherwise(col("unit_price").cast("double")))

if col_exists(df, "quantity"):
    # keep as int (or change to double here if you prefer)
    df = df.withColumn("quantity", when(col("quantity").isin("", None), None).otherwise(col("quantity").cast("int")))

if col_exists(df, "discount"):
    df = df.withColumn("discount", when(col("discount").isin("", None), None).otherwise(col("discount").cast("double")))
    df = df.withColumn("discount", when((col("discount").isNotNull()) & (col("discount") > 1), col("discount") / 100.0).otherwise(col("discount")))
    df = df.withColumn("discount", round(col("discount"), 4))

# ---- order_date parse
if col_exists(df, "order_date"):
    df = df.withColumn("order_date_parsed", to_date(col("order_date"), "yyyy-MM-dd"))
    df = df.withColumn("order_date_parsed",
                       when(col("order_date_parsed").isNull(),
                            to_date(to_timestamp(col("order_date"), "dd/MM/yyyy")))
                       .otherwise(col("order_date_parsed")))
    df = df.withColumn("order_date_parsed",
                       when(col("order_date_parsed").isNull(),
                            to_date(to_timestamp(col("order_date"), "dd-MM-yyyy")))
                       .otherwise(col("order_date_parsed")))
    df = df.withColumn("year", year(col("order_date_parsed")))
    df = df.withColumn("month", month(col("order_date_parsed")))
else:
    if not col_exists(df, "year"):
        df = df.withColumn("year", lit(None).cast("int"))
    if not col_exists(df, "month"):
        df = df.withColumn("month", lit(None).cast("int"))

# ---- recompute totalprice
if col_exists(df, "totalprice") and (col_exists(df, "unit_price") and col_exists(df, "quantity") and col_exists(df, "discount")):
    df = df.withColumn(
        "totalprice",
        when(col("totalprice").isNull(),
             (col("unit_price") * col("quantity") * (1 - col("discount"))))
        .otherwise(col("totalprice"))
    )
    df = df.withColumn("totalprice", round(col("totalprice"), 2))

# ---- select output columns
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

for src, tgt in col_map:
    if col_exists(df, src):
        selected_cols.append(col(src).alias(tgt))
    else:
        selected_cols.append(lit(None).alias(tgt))

out_df = df.select(*selected_cols)
print("Output schema columns:", out_df.columns)

# -----------------------
# Partition handling and write
# -----------------------
part_df = out_df.filter(col("year").isNotNull() & col("month").isNotNull())
other_df = out_df.filter(col("year").isNull() | col("month").isNull())

# OPTIONAL: remove .count() if dataset large
try:
    print("Records with partition keys:", part_df.count(), "Records without date:", other_df.count())
except Exception:
    print("Skipping counts to avoid expensive operation on large datasets")

# repartition by year/month (don't force 1)
if col_exists(part_df, "year") and col_exists(part_df, "month"):
    part_df = part_df.repartition(col("year"), col("month"))

part_df.write.mode("append").partitionBy("year", "month").parquet(out_s3)

if other_df.rdd.isEmpty() is False:
    other_df.write.mode("append").parquet(out_s3 + "no_date/")

# -----------------------
# Athena: ALTER TABLE ADD IF NOT EXISTS PARTITION (fast for single/new partitions)
# -----------------------
def run_athena_and_wait(query, database, result_s3, region, timeout_seconds=300):
    athena = boto3.client('athena', region_name=region)
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': result_s3}
    )
    qid = resp['QueryExecutionId']
    start = time.time()
    while True:
        resp2 = athena.get_query_execution(QueryExecutionId=qid)
        st = resp2['QueryExecution']['Status']['State']
        if st in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            # return full status and optional reason
            reason = resp2['QueryExecution']['Status'].get('StateChangeReason', '')
            return st, qid, reason
        if time.time() - start > timeout_seconds:
            return 'TIMEOUT', qid, 'timeout'
        time.sleep(2)

def add_partitions_with_athena(out_s3_prefix, db, table, results_bucket, region):
    """
    Thêm partition cụ thể bằng ALTER TABLE ADD IF NOT EXISTS PARTITION (year=..., month=...) LOCATION '...';
    Dùng cho trường hợp upload 1 file / 1 partition để nhanh chóng ghi metadata.
    """
    if not (db and results_bucket):
        print("ATHENA_DATABASE or ATHENA_QUERY_RESULTS_BUCKET not provided — skipping ALTER partition step.")
        return

    # get distinct partitions written in this run
    parts = part_df.select("year", "month").distinct().collect()
    if not parts:
        print("No partitions found to add.")
        return

    print("Starting Athena ALTER TABLE partition adds for partitions:", parts)
    for r in parts:
        y = r['year']
        m = r['month']
        if y is None or m is None:
            continue
        # ensure numeric values in SQL
        try:
            yi = int(y)
            mi = int(m)
        except Exception:
            yi = y
            mi = m
        loc = f"{out_s3_prefix.rstrip('/')}/year={yi}/month={mi}/"
        query = f"ALTER TABLE {table} ADD IF NOT EXISTS PARTITION (year={yi}, month={mi}) LOCATION '{loc}';"
        print("Submitting Athena query:", query)
        try:
            status, qid, reason = run_athena_and_wait(query, database=db, result_s3=results_bucket, region=region, timeout_seconds=300)
            print(f"Athena ALTER partition result for year={yi},month={mi}: status={status}, queryId={qid}, reason={reason}")
            if status != 'SUCCEEDED':
                print("Check Athena console for queryId:", qid, "reason:", reason)
        except Exception as e:
            print("Failed to submit ALTER PARTITION for", yi, mi, "error:", str(e))
            traceback.print_exc()

# call add_partitions_with_athena if configured
add_partitions_with_athena(out_s3, ATHENA_DATABASE, ATHENA_TABLE, ATHENA_QUERY_RESULTS_BUCKET, ATHENA_REGION)

# finish
job.commit()
print("Job finished successfully.")
