# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data Quality Checks
# MAGIC
# MAGIC Runs a small set of pass/fail checks against Silver and Gold and logs
# MAGIC every run to `dq_log`, so quality is auditable over time rather than
# MAGIC just eyeballed once. The last cell **raises** if any check flagged as
# MAGIC `critical` failed — wired into the Databricks Job (see
# MAGIC `jobs/databricks_job.json`) so a broken run stops the pipeline and
# MAGIC alerts, instead of silently publishing bad data downstream.

# COMMAND ----------
CATALOG = "workspace"
SCHEMA = "aml_demo"

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_transactions")
bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_transactions")

results = []

def run_check(name, passed, details, critical=True):
    results.append({
        "check_name": name,
        "status": "PASS" if passed else "FAIL",
        "critical": critical,
        "details": details,
    })
    flag = "PASS" if passed else "FAIL"
    print(f"[{flag}] {name} — {details}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Row count reconciliation
# MAGIC Deduplicated bronze count should equal silver count — nothing should
# MAGIC silently disappear (or duplicate) between layers.

# COMMAND ----------
bronze_distinct = bronze.select("transaction_id").distinct().count()
silver_count = silver.count()

run_check(
    "row_count_reconciliation",
    bronze_distinct == silver_count,
    f"bronze_distinct={bronze_distinct}, silver_count={silver_count}",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. No nulls in required columns

# COMMAND ----------
required_cols = ["transaction_id", "sender_account_id", "receiver_account_id", "amount"]
null_counts = silver.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in required_cols
]).collect()[0].asDict()

total_nulls = sum(null_counts.values())
run_check(
    "no_nulls_in_required_columns",
    total_nulls == 0,
    f"null counts: {null_counts}",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. No duplicate transaction_id in Silver

# COMMAND ----------
dup_count = (
    silver.groupBy("transaction_id").count().filter("count > 1").count()
)
run_check(
    "no_duplicate_transaction_id",
    dup_count == 0,
    f"duplicate transaction_ids found: {dup_count}",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Country codes are valid ISO2

# COMMAND ----------
bad_country_count = silver.filter(
    ~F.col("sender_country").rlike("^[A-Z]{2}$")
    | ~F.col("receiver_country").rlike("^[A-Z]{2}$")
).count()

run_check(
    "valid_iso2_country_codes",
    bad_country_count == 0,
    f"rows with malformed country code: {bad_country_count}",
    critical=False,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Positive amounts only

# COMMAND ----------
non_positive = silver.filter(F.col("amount") <= 0).count()
run_check(
    "positive_amounts_only",
    non_positive == 0,
    f"non-positive amount rows: {non_positive}",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Persist results to `dq_log`

# COMMAND ----------
dq_schema = StructType([
    StructField("check_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("critical", BooleanType(), False),
    StructField("details", StringType(), True),
])

dq_df = (
    spark.createDataFrame(results, schema=dq_schema)
    .withColumn("run_ts", F.current_timestamp())
)

(
    dq_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.dq_log")
)

display(spark.table(f"{CATALOG}.{SCHEMA}.dq_log").orderBy(F.col("run_ts").desc()))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Fail the job on critical failures
# MAGIC This is what actually makes the Databricks Job stop and notify —
# MAGIC without it, a red cell in a notebook is easy to miss but a raised
# MAGIC exception fails the job run and can trigger the configured alert.

# COMMAND ----------
critical_failures = [r for r in results if r["critical"] and r["status"] == "FAIL"]
if critical_failures:
    raise Exception(f"Data quality gate failed: {critical_failures}")

print("All critical data quality checks passed.")
