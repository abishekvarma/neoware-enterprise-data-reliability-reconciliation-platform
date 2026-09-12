"""Cross-system reconciliation logic."""
import pandas as pd

def reconcile_orders_to_invoices(
    orders: pd.DataFrame, invoices: pd.DataFrame, tolerance: float = 0.01
) -> pd.DataFrame:
    order_totals = orders.groupby("order_id", as_index=False)["order_amount"].sum()
    invoice_totals = invoices.groupby("order_id", as_index=False)["invoice_amount"].sum()
    out = order_totals.merge(invoice_totals, on="order_id", how="outer", indicator=True)
    out["order_amount"] = out["order_amount"].fillna(0)
    out["invoice_amount"] = out["invoice_amount"].fillna(0)
    out["difference"] = (out["order_amount"] - out["invoice_amount"]).round(2)
    def classify(row):
        if row["_merge"] == "left_only":
            return "Missing Invoice"
        if row["_merge"] == "right_only":
            return "Missing Order"
        return "Matched" if abs(row["difference"]) <= tolerance else "Mismatch"
    out["status"] = out.apply(classify, axis=1)
    return out.drop(columns="_merge")
