# Interview Notes

**30-second story:** I studied publicly described Neoware Data Engineering challenges and built an independent reliability/reconciliation POC to demonstrate multi-source integration, standardization, data quality, cross-system reconciliation, quarantine and trusted-data promotion.

**Why Pandas locally?** Reproducibility. The same logic can scale to PySpark/Databricks.

**Why reconciliation?** A pipeline can succeed technically while two systems disagree on business facts. Reconciliation turns that risk into a measurable signal.

**Production additions:** ADF/Airflow, ADLS/S3, Databricks/Delta, metadata-driven ingestion, schema evolution, catalog/lineage, alerting, RBAC/PII masking, CI/CD and data contracts.
