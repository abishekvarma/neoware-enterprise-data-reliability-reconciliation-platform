# Enterprise Data Reliability & Reconciliation Platform

**Company-targeted portfolio case study inspired by publicly available information about Neoware Technology Solutions.**

> Fragmented enterprise data → validated, reconciled, trusted and AI-ready data.

## Why this project?
Neoware publicly describes Data Engineering work around diverse source integration, incomplete/inconsistent data, scalable data platforms, lakehouse implementation, migration, governance and data quality. Its public solution accelerators also include Data Validation and Reconciliation (DVRC).

This independent POC focuses on one engineering thread: **making heterogeneous enterprise data trustworthy before it reaches analytics and AI consumers.**

## Demonstrates
- CSV + JSON/API-style ingestion
- Standardization
- Bronze/Silver/Gold mapping
- Data-quality rules
- Referential integrity
- Duplicate detection
- Cross-source reconciliation
- Quarantine and trusted promotion
- Source-health metrics
- Streamlit observability
- PySpark production mapping

## Run
```bash
python -m pip install -r requirements.txt
python src/pipeline.py
python -m streamlit run app.py
```

## Test
```bash
pytest -q
```

## Important boundary
All data is synthetic. This project does **not** use Neoware internal data, code, infrastructure or proprietary accelerators.

## GitHub
https://github.com/abishekvarma/neoware-enterprise-data-reliability-reconciliation-platform

## Evidence and boundaries

Public evidence used for problem selection:
- https://neoware.ai/our_services/data-engineering/
- https://neoware.ai/solution-accelerators/
- https://neoware.ai/partners/

The Streamlit app includes an evidence-to-implementation mapping and the repository includes an engineering walkthrough.

This is **not** Neoware's DVRC implementation. It does not use Neoware internal data, source code, infrastructure, credentials or proprietary accelerators.
