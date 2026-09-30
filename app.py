from __future__ import annotations
import json,subprocess,sys,uuid
from pathlib import Path
import streamlit as st
from src.source_adapters import materialize_source
BASE=Path(__file__).resolve().parent; INCOMING=BASE/"data"/"incoming"; OUTPUT=BASE/"output"
st.set_page_config(page_title="Enterprise Data Reliability",page_icon="◆",layout="wide")
st.markdown("""<style>
.block-container{max-width:1280px;padding:2rem 2.5rem 4rem}.hero{padding:34px 40px;border-radius:22px;background:linear-gradient(135deg,#0b1f33,#164b63);color:#fff;margin-bottom:22px}.hero h1{font-size:2.55rem;margin:.3rem 0 .5rem}.hero p{font-size:1.05rem;max-width:1000px;line-height:1.6;color:#d8e6ee}.card{border:1px solid #dce5eb;border-radius:16px;padding:18px 20px;background:#fff;margin:8px 0}.small{color:#607080;font-size:.9rem}
</style>""",unsafe_allow_html=True)
if "sources" not in st.session_state:st.session_state.sources=[]
if "last_result" not in st.session_state:st.session_state.last_result=None
st.markdown("""<div class="hero"><div style="font-size:.8rem;letter-spacing:.15em;color:#a9c3d0">DATA ENGINEERING • PRODUCTION-STYLE REFERENCE IMPLEMENTATION</div><h1>Enterprise Data Reliability & Readiness Platform</h1><p>Connect a source. Preserve the raw batch. Profile it. Enforce configurable quality controls. Quarantine failures. Produce trusted Silver/Gold data. Record an auditable reliability score. The reliability engine is independent of the source type.</p></div>""",unsafe_allow_html=True)
a,b,c,d=st.columns(4); a.metric("Source adapters","6"); b.metric("Formats","4"); c.metric("Quality dimensions","5"); d.metric("Pipeline controls","10")
st.divider(); st.markdown("## 1 · Register a source"); st.caption("CSV, Excel, JSON, JSONL and Parquet uploads are accepted. API, cloud and database sources use the same downstream reliability engine.")
with st.form("source"):
    c1,c2=st.columns(2); name=c1.text_input("Source name",placeholder="erp_orders"); kind=c2.selectbox("Source type",["File upload","API / HTTP","Amazon S3","Azure Blob / ADLS","Database","Local path"])
    uploaded=None; spec={"name":name.strip(),"type":kind}
    if kind=="File upload":uploaded=st.file_uploader("Upload data",type=["csv","xlsx","xlsm","json","jsonl","parquet"],max_upload_size=4096)
    elif kind=="API / HTTP":
        spec["url"]=st.text_input("GET endpoint",placeholder="https://api.example.com/v1/orders"); spec["auth_env"]=st.text_input("Auth token environment variable",placeholder="API_TOKEN"); spec["auth_header"]=st.text_input("Auth header",value="Authorization")
    elif kind=="Amazon S3":spec["uri"]=st.text_input("S3 object",placeholder="s3://bucket/path/data.parquet")
    elif kind=="Azure Blob / ADLS":spec["uri"]=st.text_input("Azure Blob URI or SAS URL",placeholder="az://container/path/data.parquet")
    elif kind=="Database":spec["connection_env"]=st.text_input("Connection-string environment variable",placeholder="ERP_DB_URL"); spec["query"]=st.text_area("Read-only SQL",placeholder="SELECT * FROM schema.table")
    else:spec["path"]=st.text_input("Local path",placeholder="C:/data/source.parquet")
    c1,c2,c3=st.columns(3); req=c1.text_input("Required columns",placeholder="id,name,updated_at"); spec["required_columns"]=[x.strip() for x in req.split(",") if x.strip()]; spec["unique_key"]=c2.text_input("Unique key",placeholder="id").strip() or None; spec["freshness_column"]=c3.text_input("Freshness column",placeholder="updated_at").strip() or None
    c1,c2,c3=st.columns(3); spec["freshness_sla_hours"]=c1.number_input("Freshness SLA (hours)",0.0,8760.0,24.0,1.0); spec["pass_threshold"]=c2.number_input("Reliability threshold",0.0,100.0,95.0,.5); spec["reject_unexpected_columns"]=c3.checkbox("Reject unexpected columns",value=False)
    with st.expander("Advanced quality rules"):
        spec["expected_schema"]=json.loads(st.text_area("Expected schema JSON",value="{}")); spec["numeric_rules"]=json.loads(st.text_area("Numeric rules JSON",value="[]")); spec["regex_rules"]=json.loads(st.text_area("Regex rules JSON",value="[]"))
        rs=st.text_input("Reference source name"); rc=st.text_input("Foreign-key column"); rk=st.text_input("Reference key column")
        if rs and rc and rk:spec["reference"]={"source":rs,"column":rc,"reference_column":rk}
    add=st.form_submit_button("Add source to run plan",type="primary")
if add:
    if not spec["name"]:st.error("Source name is required.")
    else:st.session_state.sources.append((spec,uploaded)); st.success(f"Added {spec['name']}.")
if st.session_state.sources:
    st.markdown("## 2 · Run plan")
    for i,(s,_) in enumerate(st.session_state.sources,1):st.markdown(f'<div class="card"><b>{i}. {s["name"]}</b> · {s["type"]}<br><span class="small">Required: {", ".join(s["required_columns"]) or "none"} · Key: {s["unique_key"] or "none"} · SLA: {s["freshness_sla_hours"]}h</span></div>',unsafe_allow_html=True)
    c1,c2=st.columns([1,5]); force=c1.checkbox("Force reprocess")
    if c1.button("Clear plan"):st.session_state.sources=[]; st.rerun()
    if c2.button("▶ Execute full reliability pipeline",type="primary"):
        try:
            INCOMING.mkdir(parents=True,exist_ok=True)
            for p in INCOMING.iterdir():
                if p.is_file():p.unlink()
            runtime=[]
            for s,up in st.session_state.sources:
                materialized=materialize_source(s,up,INCOMING); item=dict(s); item["materialized_path"]=str(materialized); item["format"]=Path(materialized).suffix.lstrip("."); runtime.append(item)
            run_id=uuid.uuid4().hex[:16]; manifest={"run_id":run_id,"sources":runtime,"pass_threshold":min(x.get("pass_threshold",95) for x in runtime),"shuffle_partitions":8}
            mp=BASE/"data"/"run_manifest.json"; mp.write_text(json.dumps(manifest,indent=2),encoding="utf-8")
            with st.status("Executing Bronze → Profile → Quality → Quarantine → Silver → Gold → Audit",expanded=True) as status:
                result=subprocess.run([sys.executable,"-m","src.run_pipeline","--manifest",str(mp),"--output",str(OUTPUT)]+(["--force"] if force else []),cwd=BASE,capture_output=True,text=True)
                if result.returncode:status.update(label="Pipeline failed",state="error"); st.code(result.stdout+"\n"+result.stderr)
                else:status.update(label="Pipeline completed",state="complete")
            if result.returncode==0:st.session_state.last_result=json.loads((OUTPUT/"audit"/"latest.json").read_text())
        except Exception as exc:st.error(str(exc)); st.exception(exc)
if st.session_state.last_result:
    p=st.session_state.last_result; st.markdown("## 3 · Run result"); st.caption(f"Run ID: {p['run']['run_id']} · Duration: {p['run']['duration_seconds']}s")
    for row in p["results"]:
        if row.get("status")=="SKIPPED_IDEMPOTENT":st.info(f"{row['source']}: skipped safely because the same fingerprint was already processed."); continue
        m1,m2,m3,m4,m5=st.columns(5); m1.metric("Rows received",row["rows_received"]); m2.metric("Quarantined",row["rows_quarantined"]); m3.metric("Completeness",f"{row['metrics']['completeness']}%"); m4.metric("Reliability",f"{row['reliability_score_pct']}%"); m5.metric("Status",row["status"]); st.progress(min(max(row["reliability_score_pct"]/100,0),1))
        with st.expander(f"{row['source']} · quality and audit details"):st.json({"metrics":row["metrics"],"schema_errors":row["schema_errors"],"quality_rules":row["quality_rules"],"batch_id":row["batch_id"],"fingerprint":row["fingerprint"]})
    st.success("Artifacts: output/runs/<run_id>/{bronze_raw,bronze,profiling,quarantine,silver,gold} plus audit history.")
else:st.info("Add one or more sources to create a run plan. Nothing executes until you click the pipeline button.")
