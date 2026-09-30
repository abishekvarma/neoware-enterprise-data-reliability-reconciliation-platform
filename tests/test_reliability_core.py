from pathlib import Path
from src.reliability_pipeline import _hash_file

def test_hash_is_stable(tmp_path: Path):
    path = tmp_path / "data.csv"
    path.write_text("id,name\n1,A\n", encoding="utf-8")
    first = _hash_file(path)
    second = _hash_file(path)
    assert first == second
    assert len(first) == 64

def test_manifest_shape():
    manifest = {"sources":[{"name":"x","type":"File upload","materialized_path":"x.csv"}]}
    assert manifest["sources"][0]["name"] == "x"
