# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: Raw Ingestion
# MAGIC
# MAGIC Lands the daily transaction CSV batches (and the sanctions watchlist)
# MAGIC from a Unity Catalog Volume into append-only **Bronze** Delta tables —
# MAGIC the entry point of the medallion architecture. Data is kept as close
# MAGIC to the source as possible (schema enforced, but no business logic yet)
# MAGIC with ingestion metadata added for lineage.
# MAGIC
# MAGIC Source system being simulated: a core-banking feed dropping one
# MAGIC `batch_date=YYYY-MM-DD/transactions.csv` file per day, exactly like a
# MAGIC nightly extract you'd see in a real Transaction Monitoring pipeline.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Setup — catalog / schema / volume
# MAGIC Uses Unity Catalog's three-level namespace (`catalog.schema.table`),
# MAGIC available by default on Databricks Free Edition. Run once.

# COMMAND ----------
CATALOG = "workspace"
SCHEMA = "aml_demo"
VOLUME = "raw_data"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/raw"
print("Upload the contents of data/raw/ (from this repo) into:", RAW_PATH)
print("Expected layout: raw/watchlist.csv and raw/batch_date=YYYY-MM-DD/transactions.csv")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Explicit schema
# MAGIC Explicit `StructType` instead of `inferSchema` — avoids silent type
# MAGIC drift on a production ingestion path and fails fast if the source
# MAGIC feed changes shape.

# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.sql import functions as F

transactions_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("sender_account_id", StringType(), True),
    StructField("sender_name", StringType(), True),
    StructField("sender_country", StringType(), True),
    StructField("receiver_account_id", StringType(), True),
    StructField("receiver_name", StringType(), True),
    StructField("receiver_country", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("source_system", StringType(), True),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Read all batches with partition discovery
# MAGIC `basePath` tells Spark to treat `batch_date=...` as a Hive-style
# MAGIC partition column instead of a literal folder name, so `batch_date`
# MAGIC comes back as a first-class column for free. The hidden
# MAGIC `_metadata.file_path` column keeps per-row lineage back to the
# MAGIC source file (Unity Catalog blocks the older `input_file_name()`
# MAGIC function on serverless compute — this is its supported replacement).

# COMMAND ----------
bronze_transactions = (
    spark.read
    .option("header", True)
    .option("basePath", RAW_PATH)
    .schema(transactions_schema)
    .csv(f"{RAW_PATH}/batch_date=*/transactions.csv")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

display(bronze_transactions.limit(10))
print("Rows read:", bronze_transactions.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Write to Bronze (append, schema evolution allowed)
# MAGIC `mergeSchema` means a new column showing up in tomorrow's source feed
# MAGIC (e.g. a new `channel` value or an added field) won't break the job —
# MAGIC it gets absorbed into the Delta table schema automatically.

# COMMAND ----------
(
    bronze_transactions.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("batch_date")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_transactions")
)

print(f"Appended {bronze_transactions.count()} rows to {CATALOG}.{SCHEMA}.bronze_transactions")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Watchlist (small reference table — overwrite each run)

# COMMAND ----------
watchlist = (
    spark.read.option("header", True).csv(f"{RAW_PATH}/watchlist.csv")
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    watchlist.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.watchlist")
)

display(watchlist)

# COMMAND ----------
# MAGIC %md
# MAGIC Next: `02_silver_transform.py` cleans, dedups and upserts this into
# MAGIC the Silver layer using a Delta `MERGE`.
