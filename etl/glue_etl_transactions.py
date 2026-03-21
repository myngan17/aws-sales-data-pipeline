import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ----- config -----
db = "beverage_sales_db"
table = "beverage_sales_data_csv"
output_path = "s3://company-analytics-datalake/processed/"
audit_order_date_null = "s3://company-analytics-datalake/audit/"
audit_totalprice_mismatch = "s3://company-analytics-datalake/audit/"

# Read from Glue Catalog
dyf = glueContext.create_dynamic_frame.from_catalog(database=db, table_name=table)
df = dyf.toDF()

# Debug: schema & row count
print("SCHEMA:", df.schema.simpleString())
rows_after_read = df.count()
print("ROWS_AFTER_READ:", rows_after_read)

# ---------- CLEAN & CAST ----------
# Trim whitespace on strings (Order_Date and any string columns)
df = df.withColumn("Order_Date", F.trim(F.col("Order_Date")))

# Normalize Discount if contains '%' and strip currency/thousands in Unit_Price
df = df.withColumn("Discount",
                   F.when(F.col("Discount").isNotNull() & F.col("Discount").rlike(".*%$"),
                          (F.regexp_replace(F.col("Discount"), "%", "").cast("double") / 100.0)
                   ).otherwise(F.col("Discount"))
                  )

# Remove currency symbols/commas from Unit_Price then cast
df = df.withColumn("Unit_Price",
                   F.regexp_replace(F.col("Unit_Price").cast("string"), r"[^\d\.\-]", "")) \
       .withColumn("Unit_Price", F.when(F.col("Unit_Price") == "", None).otherwise(F.col("Unit_Price").cast("double")))

# Cast numeric fields with safe defaults
df = df.withColumn("Quantity", F.coalesce(F.col("Quantity").cast("double"), F.lit(0.0))) \
       .withColumn("Unit_Price", F.coalesce(F.col("Unit_Price").cast("double"), F.lit(0.0))) \
       .withColumn("Discount", F.coalesce(F.col("Discount").cast("double"), F.lit(0.0)))

# Robust date parsing
df = df.withColumn(
    "Order_Date_parsed",
    F.coalesce(
        F.to_date(F.col("Order_Date"), "yyyy-MM-dd"),
        F.to_date(F.col("Order_Date"), "yyyy/MM/dd"),
        F.to_date(F.col("Order_Date"), "dd-MM-yyyy"),
        F.to_date(F.col("Order_Date"), "dd/MM/yyyy"),
        F.to_date(F.col("Order_Date"), "MM/dd/yyyy"),
        F.to_date(F.to_timestamp(F.col("Order_Date"), "yyyy-MM-dd HH:mm:ss")),
        F.to_date(F.to_timestamp(F.col("Order_Date"), "yyyy/MM/dd HH:mm:ss")),
        F.to_date(F.regexp_replace(F.col("Order_Date"), r"(\+.*$)|(\s.*$)", ""), "yyyy-MM-dd")
    )
)
df = df.drop("Order_Date").withColumnRenamed("Order_Date_parsed", "Order_Date")

# ---------- COMPUTE TotalPrice (if missing) ----------
# Compute TotalPrice (rounded)
df = df.withColumn("ComputedTotal_tmp", F.round(F.col("Quantity") * F.col("Unit_Price") * (1 - F.col("Discount")), 6))

if "TotalPrice" in df.columns:
    df = df.withColumn("TotalPrice", F.col("TotalPrice").cast("double"))
    # fill nulls in source TotalPrice by computed
    df = df.withColumn("TotalPrice", F.when(F.col("TotalPrice").isNull(), F.col("ComputedTotal_tmp")).otherwise(F.col("TotalPrice")))
else:
    df = df.withColumn("TotalPrice", F.col("ComputedTotal_tmp"))

# ---------- VALIDATE TotalPrice (flag mismatches) ----------
df = df.withColumn(
    "TotalPrice_Mismatch",
    F.when(
        (F.col("TotalPrice").isNotNull()) &
        (F.abs(F.col("TotalPrice") - F.col("ComputedTotal_tmp")) > 0.01),
        True
    ).otherwise(False)
)

# Export mismatches (if any) for audit
mismatches = df.filter(F.col("TotalPrice_Mismatch") == True)
mismatch_count = mismatches.count()
print("MISMATCH_COUNT:", mismatch_count)
if mismatch_count > 0:
    mismatches.select("Order_ID","Customer_ID","Unit_Price","Quantity","Discount","TotalPrice","ComputedTotal_tmp") \
              .write.mode("overwrite").parquet(audit_totalprice_mismatch)
    print("WROTE TotalPrice mismatches to:", audit_totalprice_mismatch)
else:
    print("No TotalPrice mismatches found.")

# ---------- HANDLE rows with missing Order_Date ----------
null_dates = df.filter(F.col("Order_Date").isNull())
num_null_dates = null_dates.count()
print("NUM_NULL_ORDER_DATE:", num_null_dates)
if num_null_dates > 0:
    null_dates.limit(1000).write.mode("overwrite").parquet(audit_order_date_null)
    print("WROTE samples with NULL Order_Date to:", audit_order_date_null)

# ---------- ADD PARTITION COLUMNS ----------
df = df.withColumn("year", F.year("Order_Date")) \
       .withColumn("month", F.month("Order_Date")) \
       .withColumn("day", F.dayofmonth("Order_Date"))

# Prepare final df (drop temp cols if you don't want them)
df_out = df.drop("ComputedTotal_tmp", "TotalPrice_Mismatch")

# ---------- WRITE PARQUET partitioned by year/month with snappy ----------
# DEV: overwrite output path. PROD: consider .mode("append") or write to temp then atomic replace.
(df_out.write
   .mode("overwrite")
   .format("parquet")
   .option("compression", "snappy")
   .partitionBy("year","month")
   .save(output_path))

# Log counts
print("Total rows written (df_out count):", df_out.count())
