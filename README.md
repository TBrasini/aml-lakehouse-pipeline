# AML Lakehouse Pipeline (Databricks / PySpark / Delta Lake)

A small end-to-end data engineering project simulating a Transaction
Monitoring / Anti-Money-Laundering pipeline on a lakehouse architecture —
built to demonstrate hands-on Databricks/Delta Lake skills in the same
domain (Anti Financial Crime) as PwC's FCU Technology team.

Synthetic banking transactions are ingested, cleaned, and turned into two
concrete AML detections (structuring / smurfing, and sanctions-list
screening) using the medallion (Bronze → Silver → Gold) pattern.

## Why this project exists

My background is IT Governance / Business Analysis (ITGC, COBIT, data
governance, financial-services audit) rather than hands-on data
engineering. This project is a deliberate, scoped way to build real
Databricks/PySpark/Delta Lake experience in the exact domain (AML/AFC)
the FCU Technology team works in, so I can speak to actual pipeline code
in interview rather than only transferable skills.

## Architecture

```
data/raw/                      Synthetic source files (this repo)
  watchlist.csv                 Sanctions watchlist (reference data)
  batch_date=YYYY-MM-DD/         One CSV per "day" — mimics a nightly
    transactions.csv             core-banking extract

        │  upload to a Unity Catalog Volume
        ▼
┌───────────────┐   01_bronze_ingestion.py
│    BRONZE     │   Schema-enforced, append-only, partitioned by
│  (raw, as-is) │   batch_date. Adds _source_file / _ingested_at lineage.
└───────┬───────┘
        │  02_silver_transform.py
        ▼
┌───────────────┐   Dedup (row_number window), null/format cleaning,
│    SILVER     │   Delta MERGE upsert on transaction_id (idempotent,
│  (clean, dedup)│  incremental-safe).
└───────┬───────┘
        │  03_gold_aggregates.py
        ▼
┌───────────────────────────────────────────────────────────┐
│                          GOLD                              │
│  gold_daily_volume_by_country   reporting aggregate         │
│  gold_structuring_alerts        rolling-24h window detection│
│  gold_sanctions_hits            exact watchlist match        │
│  gold_sanctions_review_queue    fuzzy (Soundex) match queue  │
└───────────────────────────────────────────────────────────┘
        │  04_data_quality_checks.py
        ▼
   dq_log table (row-count reconciliation, null checks, dedup
   checks, format checks) — raises and fails the job on critical
   failures, which is what the Databricks Job alert hooks into.
```

`jobs/databricks_job.json` orchestrates all four notebooks as a daily
Databricks Job with explicit task dependencies (`bronze → silver → gold →
dq`), matching the "notebooks, jobs and workflows" requirement in the job
posting.

## Repo structure

```
aml-lakehouse-pipeline/
├── data/
│   ├── generate_data.py       # synthetic data generator (run locally)
│   └── raw/                   # generated output — upload this to Databricks
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_aggregates.py
│   └── 04_data_quality_checks.py
├── jobs/
│   └── databricks_job.json    # Jobs API 2.1 workflow definition
├── requirements.txt           # local dev only, not needed on Databricks
└── README.md
```

## Setup on Databricks Free Edition (from scratch)

1. **Create an account.** Go to databricks.com and sign up for the Free
   Edition (no credit card, no cloud billing). Verify your email and log
   in — you land in a workspace with Unity Catalog and serverless compute
   already configured.
2. **Regenerate the data (optional).** The `data/raw/` folder in this
   repo already contains generated CSVs, so you can skip this. To
   regenerate: `python data/generate_data.py`.
3. **Create a Volume and upload the data.** In the workspace, go to
   **Catalog** → `workspace` (the default catalog on Free Edition — run
   `SHOW CATALOGS` first if yours has a different name and adjust the
   `CATALOG` variable at the top of each notebook accordingly) → create
   schema `aml_demo` → create a Volume
   named `raw_data` (or just run the setup cell at the top of
   `01_bronze_ingestion.py`, which creates the schema/volume for you via
   SQL). Then, in **Catalog Explorer**, open the volume and upload the
   entire `data/raw/` folder (drag-and-drop preserves the
   `batch_date=YYYY-MM-DD/` subfolders and `watchlist.csv`).
4. **Import the notebooks.** Workspace → your user folder → **Import** →
   select all four files in `notebooks/`. Databricks recognizes the
   `# Databricks notebook source` header and imports them as proper
   notebooks with cells already split out.
5. **Run them in order**, top to bottom: `01_bronze_ingestion` →
   `02_silver_transform` → `03_gold_aggregates` → `04_data_quality_checks`.
   Free Edition attaches serverless compute automatically — no cluster to
   configure.
6. **(Optional) Set up the Job.** Workflows → Jobs → Create Job → switch
   to JSON view and paste `jobs/databricks_job.json`, updating the
   `notebook_path` values to wherever you imported the notebooks, and the
   notification email. This gives you a scheduled, dependency-aware
   pipeline you can point to directly in an interview.
7. **Explore the results.** Query the Gold tables directly in a SQL
   editor or notebook, e.g.:
   ```sql
   SELECT * FROM workspace.aml_demo.gold_structuring_alerts;
   SELECT * FROM workspace.aml_demo.gold_sanctions_hits;
   SELECT * FROM workspace.aml_demo.dq_log ORDER BY run_ts DESC;
   ```

## Validation

The detection logic was first validated with an equivalent **pandas
prototype** run against the generated data (pyspark/delta-spark couldn't
be installed in the sandbox this project was drafted in — no network
budget for the ~300MB download), then the PySpark/Delta notebooks were
**executed end-to-end on a live Databricks Free Edition workspace**.
Results matched the pandas prototype exactly:

| Check | Result |
|---|---|
| Bronze rows ingested | 1,241 |
| Duplicate `transaction_id` dropped in Silver | 12 (expected: 4/day × 3 days) |
| Silver row count | 1,229 |
| Structuring rings flagged | 6 / 6 expected |
| Sanctions hits flagged | 9 / 9 expected |
| Data quality checks (`dq_log`) | 5 / 5 PASS |

Two portability fixes were needed to get from "code that compiles" to
"code that runs on Databricks", both already applied in this repo:
- The default Unity Catalog on Databricks Free Edition is named
  `workspace`, not `main` — the `CATALOG` variable at the top of each
  notebook reflects this (run `SHOW CATALOGS` on your own workspace to
  confirm, in case it differs).
- `F.input_file_name()` is blocked on Unity-Catalog-enabled serverless
  compute; replaced with the supported `_metadata.file_path` column for
  the same per-row lineage in `01_bronze_ingestion.py`.

## How this maps to the job posting

| Job posting requirement | Where it's demonstrated |
|---|---|
| ETL/ELT, lakehouse architecture | Full Bronze/Silver/Gold medallion pipeline |
| Databricks: Spark batch pipelines, notebooks/jobs/workflows | 4 notebooks + `databricks_job.json` |
| Delta Lake: ACID, schema evolution, incremental processing, merges | `mergeSchema` in Bronze, `MERGE INTO` upsert in Silver |
| Performance optimization (partitioning, compaction) | `partitionBy` on Bronze/Gold, `OPTIMIZE ... ZORDER` |
| SQL — designing/writing/optimizing queries | `spark.sql()` DDL/OPTIMIZE calls; extend Gold queries as practice |
| Python for data processing (pandas, PySpark) | Generator script (pandas-style), all 4 notebooks in PySpark |
| Data modelling, data quality, data governance | Explicit schemas, `dq_log` gate, lineage columns |
| Cloud-based data platforms | Databricks Free Edition (Unity Catalog); Azure specifically still to build (see below) |
| Financial services / AML / regulatory-driven environments | Entire project's domain, plus real PwC audit/ITGC background |
| Translate business requirements into technical solutions | This project itself — plus real PwC Business Analyst experience |

**Still open:** the posting prefers **Azure** specifically (this project
uses Databricks' own Free Edition rather than Azure Databricks), and
**streaming** (Spark Structured Streaming) is untouched — both listed as
"nice to have" rather than required, but worth a follow-up iteration if
there's time before the interview (e.g. swap the daily-batch read for
`readStream` + `Trigger.AvailableNow`, or spin up an Azure Databricks
trial workspace instead of Free Edition to get the same code running on
Azure).

## Possible extensions

- Convert `02_silver_transform.py` to `spark.readStream` with
  `Trigger.AvailableNow` to demonstrate streaming ingestion.
- Add a Databricks Asset Bundle (`databricks.yml`) for CI/CD-style
  deploy of notebooks + job definition.
- Swap the Soundex fuzzy match for a proper Jaro-Winkler/Levenshtein
  distance (or a `jellyfish`/`rapidfuzz` UDF) for closer-to-production
  sanctions screening.
- Point at an Azure Databricks workspace instead of Free Edition to
  directly address the "Azure preferred" requirement.
