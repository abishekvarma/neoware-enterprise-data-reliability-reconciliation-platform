from __future__ import annotations
import json
import subprocess
import sys
from pathlib import Path
import streamlit as st
from src.source_adapters import materialize_source

BASE=Path(__file__).resolve().parent
INCOMING=BASE/"data"/"incoming"
OUTPUT=BASE/"output"
st.set_page_config(page_title="Data Reliability & Readiness", page_icon="◆", layout="wide")
st.markdown("""<style>
.block-container{max-width:1250px;padding:2rem 2.2rem 4rem}
.hero{padding:34px 38px;border:1px solid #dbe4ec;border-radius:22px;background:linear-gradient(135deg,#0b1f33,#123d55);color:white}
.hero h1{font-size:2.7rem;margin:.2rem 0 .6rem}.hero p{max-width:950px;font-size:1.05rem;line-height:1.65;opacity:.9}
.pill{display:inline-block;border:1px solid rgba(255,255,255,.25);padding:5px 10px;border-radius:99px;margin:4px 5px 0 0;font-size:.78rem}
.source-card{border:1px solid #dce5eb;border-radius:16px;padding:18px;background:#fff;margin-bottom:12px}
</style>""",unsafe_allow_html=True)
if "sources" not in st.session_state: st.session_state.sources=[]
st.markdown("""<div class="hero"><div style="color:#9fb5c5">SOURCE-AGNOSTIC DATA ENGINEERING</div>
<h1>Enterprise Data Reliability & Readiness Platform</h1>
<p>Bring data from the source that actually exists — files, APIs, cloud object storage, databases or a local path. The ingestion adapter changes; the reliability engine does not.</p>
<span class="pill">CSV</span><span class="pill">JSON / JSONL</span><span class="pill">Parquet</span><span class="pill">HTTP / API</span><span class="pill">S3</span><span class="pill">Azure Blob / ADLS</span><span class="pill">SQL databases</span><span class="pill">PySpark</span></div>""",unsafe_allow_html=True)
st.markdown("### 1. Add data sources")
st.caption("There is no fixed customer/order/payment/product form. Each source is registered by name and connection type.")
with st.form("source_form",clear_on_submit=True):
    c1,c2=st.columns(2)
    name=c1.text_input("Source name",placeholder="customer_master")
    source_type=c2.selectbox("How will the data arrive?",["File upload","API / HTTP","Amazon S3","Azure Blob / ADLS","Database","Local path"])
    uploaded=None
    spec={"name":name.strip(),"type":source_type}
    if source_type=="File upload":
        uploaded=st.file_uploader("Data file",type=["csv","json","jsonl","parquet"],help="CSV is optional; JSON and Parquet are supported too.")
    elif source_type=="API / HTTP":
        spec["url"]=st.text_input("API / HTTP URL",placeholder="https://example.com/api/data")
    elif source_type=="Amazon S3":
        spec["uri"]=st.text_input("S3 object URI",placeholder="s3://bucket/path/data.parquet")
    elif source_type=="Azure Blob / ADLS":
        spec["uri"]=st.text_input("Azure URI",placeholder="az://container/path/data.parquet or HTTPS/SAS URL")
    elif source_type=="Database":
        spec["connection_url"]=st.text_input("Database connection URL",type="password",help="Use secrets/env vars in real deployments; never commit credentials.")
        spec["query"]=st.text_area("Read-only SQL query",placeholder="SELECT * FROM schema.table")
    else:
        spec["path"]=st.text_input("Local data path",placeholder="C:/data/customer_master.parquet")
    c1,c2,c3=st.columns(3)
    spec["required_columns"]=[x.strip() for x in c1.text_input("Required columns (optional)",placeholder="id,name,updated_at").split(",") if x.strip()]
    spec["unique_key"]=c2.text_input("Unique key (optional)",placeholder="id").strip() or None
    spec["pass_threshold"]=c3.number_input("Reliability pass threshold",0.0,100.0,95.0,0.5)
    add=st.form_submit_button("＋ Add source",type="primary")
if add:
    if not name.strip(): st.error("Give the source a name.")
    else:
        st.session_state.sources.append((spec,uploaded))
        st.success(f"Registered source: {name.strip()}")
if st.session_state.sources:
    st.markdown("### Registered sources")
    for i,(spec,uploaded) in enumerate(st.session_state.sources):
        st.markdown(f'<div class="source-card"><b>{i+1}. {spec["name"]} · {spec["type"]}</b><br><span>Required: {", ".join(spec.get("required_columns",[])) or "none"} · Unique key: {spec.get("unique_key") or "none"}</span></div>',unsafe_allow_html=True)
    if st.button("Clear source list"):
        st.session_state.sources=[]; st.rerun()
    st.markdown("### 2. Run the common reliability engine")
    st.write("Adapter materializes the source. The same engine then profiles → validates → quarantines → standardizes → scores → promotes trusted output.")
    if st.button("Run Data Reliability Pipeline",type="primary"):
        try:
            INCOMING.mkdir(parents=True,exist_ok=True)
            for item in INCOMING.iterdir():
                if item.is_file(): item.unlink()
            runtime_specs=[]
            for spec,uploaded in st.session_state.sources:
                materialized=materialize_source(spec,uploaded,INCOMING)
                item=dict(spec); item["materialized_path"]=str(materialized)
                runtime_specs.append(item)
            manifest={"sources":runtime_specs,"pass_threshold":min(x.get("pass_threshold",95) for x in runtime_specs)}
            manifest_path=BASE/"data"/"run_manifest.json"
            manifest_path.parent.mkdir(parents=True,exist_ok=True)
            manifest_path.write_text(json.dumps(manifest,indent=2),encoding="utf-8")
            with st.status("Running PySpark reliability engine...",expanded=True) as status:
                result=subprocess.run([sys.executable,"-m","src.run_pipeline","--manifest",str(manifest_path),"--output",str(OUTPUT)],cwd=str(BASE),capture_output=True,text=True)
                if result.returncode!=0:
                    status.update(label="Pipeline failed",state="error"); st.code(result.stdout+"\n"+result.stderr)
                else:
                    status.update(label="Pipeline completed",state="complete"); st.code(result.stdout)
            if result.returncode==0 and (OUTPUT/"audit"/"run.json").exists():
                audit=json.loads((OUTPUT/"audit"/"run.json").read_text())
                st.markdown("### 3. Reliability results")
                for row in audit:
                    a,b,c,d,e=st.columns(5)
                    a.metric("Source",row["source"]); b.metric("Rows",row["rows_received"]); c.metric("Quarantined",row["rows_quarantined"]); d.metric("Reliability",f'{row["reliability_score_pct"]}%'); e.metric("Status",row["status"])
                st.success("The same reliability controls were applied regardless of where each source came from.")
        except Exception as exc:
            st.error(str(exc)); st.exception(exc)
else:
    st.info("Register one or more sources. A source can be a file, API, cloud object, database query or local path.")
