# Enterprise Data Reliability & Readiness Platform

Production-style Data Engineering reference implementation — no AI/ML dependency.

## Purpose

A reusable reliability control plane for heterogeneous enterprise data. The source can change without changing the common processing engine.

## Source → trusted data flow

SOURCE → ADAPTER → RAW BRONZE → PARSED BRONZE → PROFILE → QUALITY GATE → QUARANTINE → SILVER → RELIABILITY SCORE → GOLD → AUDIT

Supported sources:
- File upload: CSV, Excel, JSON, JSONL, Parquet
- HTTP/REST GET or POST
- Amazon S3 object
- Azure Blob / ADLS object or SAS URL
- Google Cloud Storage object
- SQL database query (SQLAlchemy)
- Local path for development

## Source contract

Every registered source must provide three core release-contract fields: required columns, a unique key (including composite keys), and a freshness column with an SLA. Optional controls include expected schema, numeric ranges, regex validation, referential integrity, and rejection of unexpected columns. Authenticated connectors receive secret references/environment-variable names; secret values are never entered into the UI or committed to Git.

## Ten engineering controls

1. Preserve original source bytes in Raw Bronze.
2. Profile row count, schema, columns and null/blank counts.
3. Apply configurable required-field, uniqueness, range and regex rules.
4. Quarantine failed records with failure reasons.
5. Promote only quality-passing records to Silver.
6. Validate configured foreign-key/reference relationships.
7. Create Gold only when the reliability gate passes.
8. Write auditable batch, fingerprint, row-count, metric and duration records.
9. Calculate reliability from completeness 30%, validity 25%, uniqueness 20%, referential integrity 15%, freshness 10%.
10. Enforce idempotency using SHA-256 fingerprints; use --force for explicit reprocessing.

## Run

python -m pip install -r requirements.txt
python -m streamlit run app.py

CLI:
python -m src.run_pipeline --manifest data/run_manifest.json --output output
python -m src.run_pipeline --manifest data/run_manifest.json --output output --force

## Reliability formula

score = 0.30*C + 0.25*V + 0.20*U + 0.15*RI + 0.10*F

The UI and CLI expose the individual dimensions so a reviewer can trace the score to actual controls.

## Security

Credentials are never committed. API tokens and database connection strings are referenced through environment variables; local Streamlit secrets can be used outside Git. Do not commit .env or .streamlit/secrets.toml.

## Production deployment mapping

The local implementation is runnable and intentionally keeps cloud services optional. The same contracts map to Azure Data Factory/orchestration, ADLS Gen2 or S3, Azure Databricks/Spark, Delta Lake Bronze/Silver/Gold, catalog/lineage, CI/CD, monitoring, alerting and secret management. These are architecture targets until actually deployed and tested.

## Run monitoring

The Streamlit monitor uses a persistent DAG-style execution view inspired by modern pipeline monitoring UIs: each stage changes state while the run executes, shows duration, records event messages, and turns red/orange when a schema, quality, relationship, or release-gate problem occurs. The final run view exposes row counts, quality dimensions, quarantine reasons, and the exact release decision. This is a local reference implementation, not a Databricks UI clone.

## What to show a reviewer

1. Start the UI.
2. Register a source.
3. Configure its data contract.
4. Execute the pipeline.
5. Show Raw Bronze preservation.
6. Show profiling.
7. Show quarantined rows and failure reasons.
8. Show Silver/Gold outputs.
9. Show reliability dimensions and score.
10. Re-run the same source and demonstrate idempotent skip; then use Force reprocess for recovery.

The differentiator is reusable engineering behavior: a new source type or schema should require an adapter/configuration change, not a rewritten reliability pipeline.
