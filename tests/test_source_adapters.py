from src.source_adapters import safe_name

def test_safe_name():
    assert safe_name("Customer Master / 2026")=="Customer_Master_2026"

def test_safe_name_preserves_safe_chars():
    assert safe_name("erp.orders")=="erp.orders"
