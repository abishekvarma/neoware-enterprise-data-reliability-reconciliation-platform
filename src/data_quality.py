"""Reusable data-quality rules."""
import pandas as pd

def required_value(value) -> bool:
    return value is not None and not pd.isna(value) and str(value).strip() != ""

def positive_number(value) -> bool:
    return pd.notna(value) and float(value) > 0

def validate_orders(orders: pd.DataFrame, valid_customer_ids: set[str]) -> pd.DataFrame:
    issues = []
    for _, row in orders.iterrows():
        if not positive_number(row["order_amount"]):
            issues.append(["orders_api", row["order_id"], "positive_order_amount", "High"])
        if pd.isna(row["order_date"]):
            issues.append(["orders_api", row["order_id"], "valid_order_date", "High"])
        if row["customer_id"] not in valid_customer_ids:
            issues.append(["orders_api", row["order_id"], "customer_reference", "Critical"])
    return pd.DataFrame(issues, columns=["source","record_key","rule","severity"])
