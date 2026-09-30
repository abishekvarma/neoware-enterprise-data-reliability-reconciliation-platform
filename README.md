# Enterprise Data Reliability & Readiness Platform

A source-agnostic Data Engineering framework for turning heterogeneous enterprise data into trusted, analytics-ready datasets.

## Core design

**The source is not the pipeline.**

A customer may provide data as CSV, JSON, JSONL, Parquet, HTTP/REST, Amazon S3, Azure Blob/ADLS, a SQL query, or a local path during development.

Only the ingestion adapter changes. The downstream reliability engine remains common.

## Processing flow

SOURCE → ADAPTER → BRONZE → PROFILE → QUALITY GATE → QUARANTINE → SILVER → RELIABILITY SCORE → GOLD

Controls include raw-source preservation, profiling, required-column checks, uniqueness checks, optional referential-integrity checks, quarantine with failure reasons, standardized Silver output, Gold trusted output, batch IDs, file hashes and audit records.

Reliability score weights:
- Completeness 30%
- Validity 25%
- Uniqueness 20%
- Referential integrity 15%
- Freshness 10%

## Run locally

python -m pip install -r requirements.txt
python -m streamlit run app.py

CLI:
python -m src.run_pipeline --manifest data/run_manifest.json --output output

## Credentials

Cloud/database credentials are not stored in the repository. Use environment variables or Streamlit secrets. Never commit .streamlit/secrets.toml, .env, access keys, passwords or tokens.

## Windows + PySpark

Local Spark writes use Hadoop filesystem APIs. On Windows, configure the Hadoop Windows runtime (HADOOP_HOME / winutils.exe) or run the project in WSL/Linux/Databricks. The app surfaces the runtime error instead of pretending the pipeline completed.

## Production mapping

The reference implementation maps naturally to Azure Data Factory/orchestration, ADLS Gen2, Azure Databricks + PySpark, Delta Lake Bronze/Silver/Gold, catalog/lineage/access control, CI/CD, monitoring and alerting. These are architecture targets unless explicitly marked as implemented.

## Engineering question

If a completely new customer arrives with a different source type and different schema tomorrow, how much pipeline code must be rewritten?

The intended answer: the source adapter and configuration may change; common reliability behavior does not.
