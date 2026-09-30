from pathlib import Path
from src.reliability_pipeline import reliability_score, sha256_path

def test_score_formula():
    assert reliability_score({"completeness":98,"validity":97,"uniqueness":99,"referential_integrity":96,"freshness":100})==97.85

def test_hash_stable(tmp_path:Path):
    p=tmp_path/"x.csv"; p.write_text("id,name\n1,A\n",encoding="utf-8")
    assert sha256_path(p)==sha256_path(p)

def test_hash_changes(tmp_path:Path):
    p=tmp_path/"x.csv"; p.write_text("a",encoding="utf-8"); first=sha256_path(p); p.write_text("b",encoding="utf-8")
    assert first!=sha256_path(p)
