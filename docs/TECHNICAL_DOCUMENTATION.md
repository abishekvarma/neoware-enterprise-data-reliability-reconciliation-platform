# Technical Documentation

Sources: customer master, API-style orders, legacy invoices, support system.

Outputs: source health, quality issues, reconciliation report, trusted orders, pipeline summary and quality report.

Reconciliation aggregates order and invoice totals by order_id. A difference under 0.01 is treated as matched; mismatches and missing-side cases remain visible.
