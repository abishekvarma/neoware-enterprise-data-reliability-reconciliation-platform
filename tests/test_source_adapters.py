from src.source_adapters import extension_for, _safe_name

def test_extension_for_supported_formats():
    assert extension_for("https://example.com/a.json") == ".json"
    assert extension_for("s3://bucket/a.parquet") == ".parquet"
    assert extension_for("file-without-extension") == ".csv"

def test_safe_name():
    assert _safe_name("Customer Master / 2026") == "Customer_Master_2026"
