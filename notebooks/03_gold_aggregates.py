# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: Reporting & AML Detection
# MAGIC
# MAGIC Curated, business-facing tables built on top of Silver:
# MAGIC 1. **Daily volume by country** — standard reporting aggregate.
# MAGIC 2. **Structuring alerts** — classic AML "smurfing" pattern: a sender
# MAGIC    splits a transfer into several transactions, each individually
# MAGIC    under the reporting threshold, within a short time window.
# MAGIC 3. **Sanctions screening** — exact + fuzzy (Soundex) name matching
# MAGIC    against a watchlist, split into a high-confidence table and a
# MAGIC    "for review" queue to manage the false-positive/false-negative
# MAGIC    trade-off any AFC screening process has to make.

# COMMAND ----------
CATALOG = "workspace"
SCHEMA = "aml_demo"
REPORTING_THRESHOLD = 10000  # EUR-equivalent, matches data/generate_data.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_transactions")
watchlist = spark.table(f"{CATALOG}.{SCHEMA}.watchlist")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Daily volume by country
# MAGIC Partitioned by `tx_day` — a natural query/reporting filter, and the
# MAGIC dimension `OPTIMIZE ... ZORDER` targets below.

# COMMAND ----------
gold_daily_volume = (
    silver
    .withColumn("tx_day", F.to_date("transaction_date"))
    .groupBy("tx_day", "sender_country")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("transaction_count"),
    )
)

(
    gold_daily_volume.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("tx_day")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_daily_volume_by_country")
)

display(gold_daily_volume.orderBy("tx_day"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Structuring detection
# MAGIC For each sender, look at a rolling 24h *forward* window (in seconds,
# MAGIC via `rangeBetween` on a unix timestamp — Spark window ranges only
# MAGIC work on numeric/time-as-long columns). Flag the sender if, within
# MAGIC that window, they have **3+ transactions that are each individually
# MAGIC under the threshold but sum above it** — money split into pieces to
# MAGIC dodge a reporting threshold.

# COMMAND ----------
w = (
    Window.partitionBy("sender_account_id")
    .orderBy(F.col("transaction_date").cast("long"))
    .rangeBetween(0, 24 * 3600)
)

structuring_candidates = (
    silver
    .withColumn("window_sum", F.sum("amount").over(w))
    .withColumn("window_count", F.count("*").over(w))
    .withColumn("window_max_amount", F.max("amount").over(w))
)

structuring_flags = structuring_candidates.filter(
    (F.col("window_count") >= 3)
    & (F.col("window_max_amount") < REPORTING_THRESHOLD)
    & (F.col("window_sum") > REPORTING_THRESHOLD)
)

gold_structuring_alerts = (
    structuring_flags
    .groupBy("sender_account_id", "sender_name")
    .agg(
        F.min("transaction_date").alias("window_start"),
        F.max("transaction_date").alias("window_end"),
        F.max("window_sum").alias("total_amount_in_window"),
        F.max("window_count").alias("transaction_count_in_window"),
    )
    .withColumn("alert_type", F.lit("STRUCTURING"))
    .withColumn("flagged_at", F.current_timestamp())
)

(
    gold_structuring_alerts.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_structuring_alerts")
)

print("Structuring rings flagged:", gold_structuring_alerts.count())
display(gold_structuring_alerts)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Sanctions screening — exact match (high confidence)

# COMMAND ----------
wl_names = watchlist.select(F.lower(F.col("watchlist_name")).alias("wl_name"), "risk_level", "list_source")

exact_sender_hits = (
    silver
    .join(wl_names, F.lower(F.col("sender_name")) == F.col("wl_name"), "inner")
    .select("transaction_id", "transaction_date", F.lit("SENDER").alias("matched_party"),
            "sender_name", "sender_account_id", "risk_level", "list_source")
)

exact_receiver_hits = (
    silver
    .join(wl_names, F.lower(F.col("receiver_name")) == F.col("wl_name"), "inner")
    .select(
        "transaction_id", "transaction_date", F.lit("RECEIVER").alias("matched_party"),
        F.col("receiver_name").alias("sender_name"),
        F.col("receiver_account_id").alias("sender_account_id"),
        "risk_level", "list_source",
    )
)

gold_sanctions_hits = exact_sender_hits.unionByName(exact_receiver_hits).withColumn(
    "match_type", F.lit("EXACT")
).withColumn("flagged_at", F.current_timestamp())

(
    gold_sanctions_hits.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_sanctions_hits")
)

print("Exact sanctions hits:", gold_sanctions_hits.count())
display(gold_sanctions_hits)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Sanctions screening — fuzzy match (review queue)
# MAGIC `soundex()` catches phonetically-similar names (transliteration
# MAGIC variants, typos) that an exact match would miss. Kept in a separate
# MAGIC "for review" table rather than merged into the high-confidence hits —
# MAGIC fuzzy matching trades false negatives for false positives, so an
# MAGIC analyst should triage this queue rather than auto-escalate it.

# COMMAND ----------
wl_soundex = watchlist.select(
    F.soundex(F.col("watchlist_name")).alias("wl_soundex"),
    F.col("watchlist_name").alias("wl_name_display"),
)

review_queue = (
    silver
    .withColumn("sender_soundex", F.soundex("sender_name"))
    .join(wl_soundex, F.col("sender_soundex") == F.col("wl_soundex"), "inner")
    .select(
        "transaction_id", "transaction_date", "sender_name",
        F.col("wl_name_display").alias("possible_watchlist_match"),
    )
    .withColumn("match_type", F.lit("FUZZY_SOUNDEX"))
    .withColumn("flagged_at", F.current_timestamp())
)

(
    review_queue.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_sanctions_review_queue")
)

print("Fuzzy-match candidates for manual review:", review_queue.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Table maintenance
# MAGIC `OPTIMIZE` compacts small files; `ZORDER` co-locates rows commonly
# MAGIC filtered together (here, by country) to speed up point/range queries.
# MAGIC On Free Edition serverless SQL warehouses this runs automatically in
# MAGIC the background, but it's shown explicitly here since a classic job
# MAGIC cluster would need it scheduled.

# COMMAND ----------
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.gold_daily_volume_by_country ZORDER BY (sender_country)")
