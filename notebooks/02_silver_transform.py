# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: Clean, Deduplicate, Upsert
# MAGIC
# MAGIC Takes Bronze as-is data and turns it into a trustworthy, query-ready
# MAGIC table: drops incomplete rows, standardizes formats, removes duplicates
# MAGIC (the source feed can legitimately resend a row), and **upserts** into
# MAGIC Silver via a Delta `MERGE` so the notebook is safe to re-run on the
# MAGIC same batch (idempotent) and can later run incrementally.

# COMMAND ----------
CATALOG = "workspace"
SCHEMA = "aml_demo"

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read Bronze
# MAGIC In a real incremental pipeline this would filter on `_ingested_at` >
# MAGIC last successful watermark (or better: Delta's `readStream` with
# MAGIC `Trigger.AvailableNow`). Kept as a full read here for clarity — the
# MAGIC `MERGE` below is what makes re-runs idempotent either way.

# COMMAND ----------
bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_transactions")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Deduplicate
# MAGIC The source feed occasionally resends the same `transaction_id`
# MAGIC (observed in bronze). Keep the most recently ingested version of
# MAGIC each id using a ranking window function rather than a blind
# MAGIC `dropDuplicates`, so a genuine correction/resend is handled correctly.

# COMMAND ----------
dedup_window = Window.partitionBy("transaction_id").orderBy(F.col("_ingested_at").desc())

deduped = (
    bronze
    .withColumn("_rank", F.row_number().over(dedup_window))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

print("Bronze rows:", bronze.count(), "-> Deduplicated rows:", deduped.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Clean & standardize
# MAGIC - Drop rows missing a required key field (can't process what we can't
# MAGIC   identify or route).
# MAGIC - Standardize country codes and currency to uppercase.
# MAGIC - Guard against non-positive amounts (data entry errors upstream).

# COMMAND ----------
required_cols = ["transaction_id", "sender_account_id", "receiver_account_id", "amount"]

silver_batch = (
    deduped
    .dropna(subset=required_cols)
    .withColumn("sender_country", F.upper(F.trim("sender_country")))
    .withColumn("receiver_country", F.upper(F.trim("receiver_country")))
    .withColumn("currency", F.upper(F.trim("currency")))
    .withColumn("sender_name", F.trim("sender_name"))
    .withColumn("receiver_name", F.trim("receiver_name"))
    .filter(F.col("amount") > 0)
    .withColumn("_processed_at", F.current_timestamp())
)

print("Clean rows ready to merge:", silver_batch.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Upsert into Silver via Delta `MERGE`
# MAGIC ACID merge on `transaction_id`: updates the row if it already exists
# MAGIC (e.g. a correction landed in a later batch), inserts it otherwise.
# MAGIC This is the core Delta Lake capability the pipeline leans on for
# MAGIC incremental processing.

# COMMAND ----------
target_table = f"{CATALOG}.{SCHEMA}.silver_transactions"

if not spark.catalog.tableExists(target_table):
    (
        silver_batch.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )
    print(f"Created {target_table} with {silver_batch.count()} rows")
else:
    delta_target = DeltaTable.forName(spark, target_table)
    (
        delta_target.alias("t")
        .merge(silver_batch.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Merged {silver_batch.count()} rows into {target_table}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Sanity check

# COMMAND ----------
display(spark.table(target_table).limit(10))
print("Silver row count:", spark.table(target_table).count())

# COMMAND ----------
# MAGIC %md
# MAGIC Next: `03_gold_aggregates.py` builds the reporting/detection layer
# MAGIC (structuring alerts, sanctions screening, daily volumes).
