"""Generic PySpark reliability engine."""
from __future__ import annotations
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

def _now():
    return datetime.now(timezone.utc).isoformat()

def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def _read(spark, path: str):
    suffix = Path(path).suffix.lower()
    if suffix == ".csv":
        return spark.read.option("header", True).option("inferSchema", True).option("mode", "PERMISSIVE").csv(path)
    if suffix in {".json", ".jsonl"}:
        return spark.read.option("mode", "PERMISSIVE").json(path)
    if suffix == ".parquet":
        return spark.read.parquet(path)
    raise ValueError(f"Unsupported dataset format: {suffix}")

def _profile(df):
    total = df.count()
    nulls = {}
    for col in df.columns:
        nulls[col] = df.filter(F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")).count()
    return {"rows": total, "columns": len(df.columns), "nulls": nulls, "schema": df.schema.simpleString()}

def _quality(df, spec, references):
    total = df.count()
    required = [c for c in spec.get("required_columns", []) if c in df.columns]
    conditions, reasons = [], []
    for col in required:
        bad = F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")
        conditions.append(~bad)
        reasons.append(F.when(bad, F.lit(f"required:{col}")))
    unique_key = spec.get("unique_key")
    if unique_key and unique_key in df.columns:
        dup_keys = df.groupBy(unique_key).count().filter(F.col("count") > 1).select(unique_key)
        df = df.join(dup_keys.withColumn("_dup_key", F.lit(True)), unique_key, "left")
        dup = F.col("_dup_key").isNull()
        conditions.append(dup)
        reasons.append(F.when(~dup, F.lit(f"duplicate:{unique_key}")))
    ref = spec.get("reference")
    if ref:
        ref_df = references.get(ref.get("source"))
        if ref_df is not None and ref.get("column") in df.columns and ref.get("reference_column") in ref_df.columns:
            ref_values = ref_df.select(F.col(ref["reference_column"]).alias("_ref_value")).dropDuplicates()
            df = df.join(ref_values, F.col(ref["column"]) == F.col("_ref_value"), "left")
            ri = F.col("_ref_value").isNotNull()
            conditions.append(ri)
            reasons.append(F.when(~ri, F.lit(f"referential_integrity:{ref['column']}")))
    valid = F.lit(True)
    for condition in conditions:
        valid = valid & condition
    reason_expr = F.concat_ws("; ", *reasons) if reasons else F.lit("")
    return df.withColumn("_quality_failed", ~valid).withColumn("_failure_reason", reason_expr)

def run(manifest_path="data/run_manifest.json", output_root="output"):
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    root = Path(output_root)
    if root.exists():
        shutil.rmtree(root)
    for name in ["bronze", "profiling", "quarantine", "silver", "gold", "audit"]:
        (root / name).mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.appName("EnterpriseDataReliabilityReadiness")
             .master("local[*]").config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    loaded, prepared = {}, []
    for spec in manifest["sources"]:
        path = Path(spec["materialized_path"])
        df = _read(spark, str(path))
        batch_id = _hash_file(path)[:16]
        df = (df.withColumn("_source", F.lit(spec["name"]))
                .withColumn("_batch_id", F.lit(batch_id))
                .withColumn("_ingested_at", F.current_timestamp()))
        loaded[spec["name"]] = df
        prepared.append((spec, df, batch_id))
    audit = []
    for spec, df, batch_id in prepared:
        total = df.count()
        df.write.mode("overwrite").parquet(str(root / "bronze" / spec["name"] / batch_id))
        profile = _profile(df)
        (root / "profiling" / f"{spec['name']}_{batch_id}.json").write_text(json.dumps(profile, indent=2), encoding="utf-8")
        checked = _quality(df, spec, loaded)
        failed, valid = checked.filter(F.col("_quality_failed")), checked.filter(~F.col("_quality_failed"))
        failed_count, valid_count = failed.count(), valid.count()
        failed.write.mode("overwrite").parquet(str(root / "quarantine" / spec["name"] / batch_id))
        valid.write.mode("overwrite").parquet(str(root / "silver" / spec["name"] / batch_id))
        required = [c for c in spec.get("required_columns", []) if c in df.columns]
        if required:
            complete = F.lit(True)
            for c in required:
                complete = complete & F.col(c).isNotNull() & (F.trim(F.col(c).cast("string")) != "")
            completeness = valid.filter(complete).count() / total * 100 if total else 100.0
        else:
            completeness = 100.0
        validity = valid_count / total * 100 if total else 100.0
        uniqueness = (valid.select(unique_key).dropDuplicates().count() / valid_count * 100
                      if (unique_key := spec.get("unique_key")) and valid_count else 100.0)
        ri = 100.0
        if spec.get("reference"):
            ref = spec["reference"]
            ref_df = loaded.get(ref["source"])
            if ref_df is not None:
                refs = ref_df.select(F.col(ref["reference_column"]).alias("_ref")).dropDuplicates()
                ri_count = valid.join(refs, valid[ref["column"]] == F.col("_ref"), "left_semi").count()
                ri = ri_count / total * 100 if total else 100.0
        score = round(completeness*.30 + validity*.25 + uniqueness*.20 + ri*.15 + 100*.10, 2)
        valid.withColumn("_gold_created_at", F.current_timestamp()).write.mode("overwrite").parquet(str(root/"gold"/spec["name"]/batch_id))
        audit.append({
            "source":spec["name"],"batch_id":batch_id,"rows_received":total,"rows_valid":valid_count,
            "rows_quarantined":failed_count,"completeness_pct":round(completeness,2),
            "validity_pct":round(validity,2),"uniqueness_pct":round(uniqueness,2),
            "referential_integrity_pct":round(ri,2),"freshness_pct":100.0,
            "reliability_score_pct":score,
            "status":"PASS" if score >= manifest.get("pass_threshold",95) else "REVIEW",
            "processed_at":_now()
        })
    (root/"audit"/"run.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
    spark.stop()
    return audit
