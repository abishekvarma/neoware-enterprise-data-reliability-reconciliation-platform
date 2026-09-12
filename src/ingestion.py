"""Source ingestion layer for the portfolio POC."""
from pathlib import Path
import json
import pandas as pd

def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

def load_json(path: Path) -> pd.DataFrame:
    return pd.DataFrame(json.loads(path.read_text(encoding="utf-8")))

def load_all(raw_dir: Path):
    return {
        "customer_master": load_csv(raw_dir / "customer_master.csv"),
        "orders_api": load_json(raw_dir / "orders_api.json"),
        "legacy_invoices": load_csv(raw_dir / "legacy_invoices.csv"),
        "support_system": load_csv(raw_dir / "support_system.csv"),
    }
