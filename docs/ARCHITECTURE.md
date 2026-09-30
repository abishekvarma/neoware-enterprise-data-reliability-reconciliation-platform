# Architecture

## Runtime separation

The system has two independent concerns:

1. Source adapters acquire/materialize data.
2. The PySpark reliability engine processes every source through the same control plane.

This prevents source-specific ingestion code from spreading into business-quality logic.

## Processing contract

Source
→ adapter
→ raw bytes
→ parsed Bronze
→ profiling
→ schema contract
→ quality rules
→ quarantine / Silver
→ reliability calculation
→ Gold promotion
→ audit

## Reliability dimensions

Completeness, validity, uniqueness, referential integrity and freshness are measured independently. The weighted score is only the final summary.

## Batch safety

Every materialized source receives a SHA-256 fingerprint. The first 16 characters form the batch identifier. A previously seen fingerprint is skipped unless the operator explicitly requests force reprocessing.

## Data preservation

Raw Bronze stores the original materialized file before parsing. Parsed Bronze stores Spark's structured representation with ingestion metadata.

## Cloud mapping

Local Parquet is used as the portable reference output. In an Azure Databricks deployment, the same layer boundaries can be implemented with ADLS Gen2 and Delta Lake without changing the source contract or reliability rules.

## Security boundary

Secrets are referenced by environment-variable name rather than embedded in source control. The repository contains no customer data, access keys or database passwords.

## Operational boundary

The repository is production-style and deployment-ready in structure, but a real production deployment still requires environment-specific networking, IAM/RBAC, secret stores, orchestration, alerting, retention policies and load testing.
