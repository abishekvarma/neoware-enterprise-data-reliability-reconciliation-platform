import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
import pipeline, pandas as pd
def test_outputs():
 s=pipeline.run_pipeline(); assert s["sources"]==4; assert Path(pipeline.OUTPUT/"trusted_orders.csv").exists()
def test_reconciliation_detects_mismatch():
 d=pd.read_csv(pipeline.OUTPUT/"reconciliation_report.csv"); assert "Mismatch" in set(d.status)
def test_quality_schema():
 d=pd.read_csv(pipeline.OUTPUT/"quality_issues.csv"); assert {"rule","severity","detail"}.issubset(d.columns)
