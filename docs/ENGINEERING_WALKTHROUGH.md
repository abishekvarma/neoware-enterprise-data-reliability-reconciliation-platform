# Engineering Walkthrough

## 1. Ingest
Customer master, orders API-style JSON, legacy invoices and operational support data.

## 2. Standardize
Normalize column names, trim strings, parse dates and cast financial fields.

## 3. Validate
Apply required-field, date, positive-value and referential-integrity checks.

## 4. Detect
Identify duplicate invoice representations and source-level anomalies.

## 5. Reconcile
Aggregate by business key and compare order vs invoice facts.

## 6. Quarantine
Keep failures visible with source, record key, rule, severity and explanation.

## 7. Promote
Only successfully reconciled orders reach the trusted layer.

## 8. Observe
Expose source health, quality, reconciliation and trusted-data metrics through Streamlit.

## Production evolution
The local Pandas implementation can move to PySpark/Databricks with cloud storage, orchestration, metadata, lineage, alerting and governance.
