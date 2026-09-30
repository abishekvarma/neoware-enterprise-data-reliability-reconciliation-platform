"""Production-style, source-agnostic PySpark reliability engine."""
from __future__ import annotations
import hashlib, json, logging, shutil, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from pyspark.sql import DataFrame, SparkSession, functions as F, Window

LOG=logging.getLogger("reliability")
WEIGHTS={"completeness":.30,"validity":.25,"uniqueness":.20,"referential_integrity":.15,"freshness":.10}

def utc_now()->str: return datetime.now(timezone.utc).isoformat()

def sha256_path(path:Path)->str:
    digest=hashlib.sha256()
    if path.is_file():
        with path.open("rb") as fh:
            for chunk in iter(lambda:fh.read(1024*1024),b""): digest.update(chunk)
    else:
        for item in sorted(p for p in path.rglob("*") if p.is_file()):
            digest.update(str(item.relative_to(path)).encode())
            with item.open("rb") as fh:
                for chunk in iter(lambda:fh.read(1024*1024),b""): digest.update(chunk)
    return digest.hexdigest()

def read_source(spark:SparkSession,path:str,fmt:str|None=None)->DataFrame:
    p=Path(path); fmt=(fmt or p.suffix.lstrip(".")).lower()
    if fmt=="csv": return spark.read.option("header",True).option("inferSchema",True).option("mode","PERMISSIVE").csv(path)
    if fmt in {"json","jsonl"}: return spark.read.option("mode","PERMISSIVE").json(path)
    if fmt=="parquet": return spark.read.parquet(path)
    raise ValueError(f"Unsupported dataset format: {fmt}")

def profile(df:DataFrame)->dict[str,Any]:
    total=df.count()
    if not df.columns: return {"rows":total,"columns":0,"nulls":{},"schema":""}
    expr=[F.sum(F.when(F.col(c).isNull()|(F.trim(F.col(c).cast("string"))==""),1).otherwise(0)).alias(c) for c in df.columns]
    row=df.agg(*expr).first().asDict()
    return {"rows":total,"columns":len(df.columns),"nulls":{k:int(v or 0) for k,v in row.items()},"schema":df.schema.json(),"column_names":df.columns}

def schema_check(df:DataFrame,spec:dict)->list[str]:
    expected=spec.get("expected_schema",{})
    if not expected: return []
    actual={f.name:f.dataType.simpleString() for f in df.schema.fields}; errors=[]
    for name,dtype in expected.items():
        if name not in actual: errors.append(f"missing_column:{name}")
        elif dtype and actual[name]!=dtype: errors.append(f"type_mismatch:{name}:{actual[name]}!={dtype}")
    if spec.get("reject_unexpected_columns"):
        errors += [f"unexpected_column:{n}" for n in actual if n not in expected]
    return errors

def apply_quality(df:DataFrame,spec:dict,references:dict[str,DataFrame])->tuple[DataFrame,dict]:
    checks=[]
    for c in spec.get("required_columns",[]):
        if c in df.columns:
            bad=F.col(c).isNull()|(F.trim(F.col(c).cast("string"))==""); checks.append((f"required:{c}",~bad))
        else: checks.append((f"missing_column:{c}",F.lit(False)))
    key=spec.get("unique_key")
    if key:
        if key not in df.columns: checks.append((f"missing_unique_key:{key}",F.lit(False)))
        else:
            df=df.withColumn("_dup_count",F.count("*").over(Window.partitionBy(key)))
            checks.append((f"unique:{key}",F.col("_dup_count")==1))
    for rule in spec.get("numeric_rules",[]):
        c=rule.get("column")
        if c in df.columns:
            value=F.col(c).cast("double"); ok=F.lit(True)
            if rule.get("min") is not None: ok=ok&(value>=float(rule["min"]))
            if rule.get("max") is not None: ok=ok&(value<=float(rule["max"]))
            checks.append((f"range:{c}",ok))
    for rule in spec.get("regex_rules",[]):
        c=rule.get("column")
        if c in df.columns: checks.append((f"regex:{c}",F.col(c).cast("string").rlike(rule["pattern"])))
    ref=spec.get("reference")
    if ref:
        ref_df=references.get(ref.get("source"))
        if ref_df is not None and ref.get("column") in df.columns and ref.get("reference_column") in ref_df.columns:
            vals=ref_df.select(F.col(ref["reference_column"]).alias("_ref_value")).dropDuplicates()
            df=df.join(vals,F.col(ref["column"])==F.col("_ref_value"),"left")
            checks.append((f"referential_integrity:{ref['column']}",F.col("_ref_value").isNotNull()))
    passed=F.lit(True); reasons=[]
    for name,condition in checks:
        passed=passed&condition; reasons.append(F.when(~condition,F.lit(name)))
    reason=F.concat_ws("; ",*reasons) if reasons else F.lit("")
    return df.withColumn("_quality_failed",~passed).withColumn("_failure_reason",reason),{"rule_count":len(checks),"rules":[x[0] for x in checks]}

def freshness_pct(df:DataFrame,column:str|None,sla_hours:float|None)->float:
    if not column or column not in df.columns or not sla_hours: return 100.0
    latest=df.select(F.max(F.to_timestamp(F.col(column))).alias("latest")).first()["latest"]
    if latest is None: return 0.0
    age=(datetime.now(timezone.utc).replace(tzinfo=None)-latest).total_seconds()/3600
    return 100.0 if age<=float(sla_hours) else 0.0

def reliability_score(metrics:dict[str,float])->float:
    return round(sum(metrics[k]*WEIGHTS[k] for k in WEIGHTS),2)

def _append_jsonl(path:Path,row:dict):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("a",encoding="utf-8") as fh: fh.write(json.dumps(row,default=str)+"\n")

def _read_history(path:Path)->set[str]:
    if not path.exists(): return set()
    return {json.loads(x)["fingerprint"] for x in path.read_text(encoding="utf-8").splitlines() if x.strip() and "fingerprint" in json.loads(x)}

def run(manifest_path="data/run_manifest.json",output_root="output",force=False)->dict:
    manifest=json.loads(Path(manifest_path).read_text(encoding="utf-8")); run_id=manifest.get("run_id") or uuid.uuid4().hex[:16]
    started=time.time(); root=Path(output_root); root.mkdir(parents=True,exist_ok=True)
    history=root/"audit"/"history.jsonl"; seen=_read_history(history)
    spark=(SparkSession.builder.appName("EnterpriseDataReliabilityReadiness").master(manifest.get("spark_master","local[*]")).config("spark.sql.shuffle.partitions",str(manifest.get("shuffle_partitions",8))).config("spark.sql.adaptive.enabled","true").getOrCreate())
    spark.sparkContext.setLogLevel("WARN"); results=[]; references={}; prepared=[]
    try:
        for spec in manifest["sources"]:
            path=Path(spec["materialized_path"])
            if not path.exists(): raise FileNotFoundError(f"Source does not exist: {path}")
            fingerprint=sha256_path(path)
            if fingerprint in seen and not force:
                results.append({"source":spec["name"],"status":"SKIPPED_IDEMPOTENT","fingerprint":fingerprint}); continue
            df=read_source(spark,str(path),spec.get("format"))
            batch=fingerprint[:16]
            df=df.withColumn("_source",F.lit(spec["name"])).withColumn("_batch_id",F.lit(batch)).withColumn("_ingested_at",F.current_timestamp())
            prepared.append((spec,path,fingerprint,batch,df)); references[spec["name"]]=df
        if not prepared and results: return {"run_id":run_id,"status":"NOOP","results":results,"duration_seconds":round(time.time()-started,2)}
        for spec,path,fingerprint,batch,df in prepared:
            base=root/"runs"/run_id
            for layer in ("bronze_raw","bronze","profiling","quarantine","silver","gold"): (base/layer/spec["name"]/batch).mkdir(parents=True,exist_ok=True)
            if path.is_file(): shutil.copy2(path,base/"bronze_raw"/spec["name"]/batch/path.name)
            total=df.count(); df.write.mode("overwrite").parquet(str(base/"bronze"/spec["name"]/batch))
            prof=profile(df); (base/"profiling"/spec["name"]/batch/"profile.json").write_text(json.dumps(prof,indent=2,default=str),encoding="utf-8")
            schema_errors=schema_check(df,spec); checked,rule_meta=apply_quality(df,spec,references)
            failed=checked.filter(F.col("_quality_failed")); valid=checked.filter(~F.col("_quality_failed"))
            failed_count,valid_count=failed.count(),valid.count()
            failed_out=failed.drop("_dup_count","_ref_value","_quality_failed")
            valid_out=valid.drop("_dup_count","_ref_value","_quality_failed","_failure_reason")
            failed_out.write.mode("overwrite").parquet(str(base/"quarantine"/spec["name"]/batch)); valid_out.write.mode("overwrite").parquet(str(base/"silver"/spec["name"]/batch))
            required=[c for c in spec.get("required_columns",[]) if c in df.columns]
            if required:
                complete=F.lit(True)
                for c in required: complete=complete&F.col(c).isNotNull()&(F.trim(F.col(c).cast("string"))!="")
                completeness=valid.filter(complete).count()/total*100 if total else 100.0
            else: completeness=100.0
            validity=valid_count/total*100 if total else 100.0
            key=spec.get("unique_key")
            uniqueness=valid.select(key).dropDuplicates().count()/valid_count*100 if key and key in valid.columns and valid_count else 100.0
            ri=100.0; ref=spec.get("reference")
            if ref and ref.get("source") in references and ref.get("column") in valid.columns:
                ref_df=references[ref["source"]]
                if ref.get("reference_column") in ref_df.columns:
                    vals=ref_df.select(F.col(ref["reference_column"]).alias("_ref")).dropDuplicates()
                    ri=valid.join(vals,valid[ref["column"]]==F.col("_ref"),"left_semi").count()/total*100 if total else 100.0
            fresh=freshness_pct(df,spec.get("freshness_column"),spec.get("freshness_sla_hours"))
            metrics={"completeness":round(completeness,2),"validity":round(validity,2),"uniqueness":round(uniqueness,2),"referential_integrity":round(ri,2),"freshness":round(fresh,2)}
            score=reliability_score(metrics); threshold=float(spec.get("pass_threshold",manifest.get("pass_threshold",95)))
            status="SCHEMA_REVIEW" if schema_errors else ("PASS" if score>=threshold else "REVIEW")
            if status=="PASS": valid_out.withColumn("_gold_created_at",F.current_timestamp()).write.mode("overwrite").parquet(str(base/"gold"/spec["name"]/batch))
            audit={"run_id":run_id,"source":spec["name"],"source_type":spec["type"],"batch_id":batch,"fingerprint":fingerprint,"rows_received":total,"rows_valid":valid_count,"rows_quarantined":failed_count,"metrics":metrics,"reliability_score_pct":score,"threshold_pct":threshold,"status":status,"schema_errors":schema_errors,"quality_rules":rule_meta,"processed_at":utc_now(),"duration_seconds":round(time.time()-started,2)}
            _append_jsonl(history,audit); results.append(audit)
        summary={"run_id":run_id,"status":"COMPLETED","sources_processed":len(prepared),"finished_at":utc_now(),"duration_seconds":round(time.time()-started,2)}
        (root/"audit"/"latest.json").write_text(json.dumps({"run":summary,"results":results},indent=2,default=str),encoding="utf-8")
        return {"run_id":run_id,"status":"COMPLETED","results":results,"duration_seconds":summary["duration_seconds"]}
    finally: spark.stop()
