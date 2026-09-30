"""Source-agnostic ingestion adapters."""
from __future__ import annotations
import os
import re
from pathlib import Path
from urllib.parse import urlparse
import requests

SUPPORTED_EXTENSIONS = {".csv", ".json", ".jsonl", ".parquet"}

def _safe_name(name: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", name.strip())
    return value.strip("._") or "source"

def extension_for(name: str, default: str = ".csv") -> str:
    suffix = Path(urlparse(name).path).suffix.lower()
    return suffix if suffix in SUPPORTED_EXTENSIONS else default

def save_uploaded(uploaded_file, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(uploaded_file.getvalue())
    return destination

def fetch_http(url: str, destination_dir: Path) -> Path:
    response = requests.get(url, timeout=60, stream=True)
    response.raise_for_status()
    suffix = extension_for(url)
    content_type = response.headers.get("content-type", "").lower()
    if "json" in content_type and suffix == ".csv":
        suffix = ".json"
    destination = destination_dir / f"remote{suffix}"
    with destination.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)
    return destination

def fetch_s3(uri: str, destination_dir: Path) -> Path:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("S3 support requires boto3.") from exc
    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError("Use s3://bucket/path/file.csv")
    suffix = Path(key).suffix.lower() or ".csv"
    destination = destination_dir / f"s3_object{suffix}"
    boto3.client("s3").download_file(bucket, key, str(destination))
    return destination

def fetch_azure(uri: str, destination_dir: Path) -> Path:
    if uri.startswith("https://"):
        return fetch_http(uri, destination_dir)
    if not uri.startswith("az://"):
        raise ValueError("Use an Azure Blob HTTPS/SAS URL or az://container/blob")
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise RuntimeError("Azure Blob support requires azure-storage-blob.") from exc
    connection = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection:
        raise RuntimeError("Set AZURE_STORAGE_CONNECTION_STRING for az:// sources.")
    parsed = urlparse(uri)
    container, blob = parsed.netloc, parsed.path.lstrip("/")
    if not container or not blob:
        raise ValueError("Use az://container/path/file.csv")
    suffix = Path(blob).suffix.lower() or ".csv"
    destination = destination_dir / f"azure_blob{suffix}"
    client = BlobServiceClient.from_connection_string(connection)
    with destination.open("wb") as handle:
        handle.write(client.get_blob_client(container, blob).download_blob().readall())
    return destination

def fetch_database(connection_url: str, query: str, destination_dir: Path) -> Path:
    try:
        import pandas as pd
        from sqlalchemy import create_engine
    except ImportError as exc:
        raise RuntimeError("Database support requires pandas and SQLAlchemy.") from exc
    if not query.strip().lower().startswith(("select", "with")):
        raise ValueError("Only read-only SELECT/CTE queries are allowed.")
    destination = destination_dir / "database_query.csv"
    pd.read_sql_query(query, create_engine(connection_url)).to_csv(destination, index=False)
    return destination

def materialize_source(spec: dict, uploaded_file=None, work_dir: str | Path = "data/incoming") -> Path:
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    source_name = _safe_name(spec["name"])
    source_type = spec["type"]
    if source_type == "File upload":
        if uploaded_file is None:
            raise ValueError(f"No file supplied for source '{source_name}'.")
        suffix = Path(uploaded_file.name).suffix.lower()
        if suffix not in SUPPORTED_EXTENSIONS:
            raise ValueError("Supported file formats: CSV, JSON, JSONL and Parquet.")
        return save_uploaded(uploaded_file, work_dir / f"{source_name}{suffix}")
    if source_type == "API / HTTP":
        return fetch_http(spec["url"], work_dir)
    if source_type == "Amazon S3":
        return fetch_s3(spec["uri"], work_dir)
    if source_type == "Azure Blob / ADLS":
        return fetch_azure(spec["uri"], work_dir)
    if source_type == "Database":
        return fetch_database(spec["connection_url"], spec["query"], work_dir)
    if source_type == "Local path":
        path = Path(spec["path"]).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(path)
        return path
    raise ValueError(f"Unsupported source type: {source_type}")
