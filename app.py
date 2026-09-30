from __future__ import annotations
import json,sys,uuid
from pathlib import Path
import streamlit as st
from src.source_adapters import materialize_source
from src.reliability_pipeline import run as run_pipeline
BASE=Path(__file__).resolve().parent; INCOMING=BASE/"data"/"incoming"; OUTPUT=BASE/"output"
st.set_page_config(page_title="Enterprise Data Reliability",page_icon="◆",layout="wide")
st.markdown("""<style>
.block-container{max-width:1280px;padding:2rem 2.5rem 4rem}.hero{padding:34px 40px;border-radius:22px;background:linear-gradient(135deg,#0b1f33,#164b63);color:#fff;margin-bottom:22px}.hero h1{font-size:2.55rem;margin:.3rem 0 .5rem}.hero p{font-size:1.05rem;max-width:1000px;line-height:1.6;color:#d8e6ee}.card{border:1px solid #dce5eb;border-radius:16px;padding:18px 20px;background:#fff;margin:8px 0}.small{color:#607080;font-size:.9rem}.pipeline{display:flex;align-items:stretch;overflow-x:auto;padding:18px 4px 22px}.stage{min-width:112px;border:1px solid #d8e0e7;border-radius:14px;padding:12px 10px;text-align:center;background:#f7f9fb}.stage .num{font-size:10px;color:#738291;font-weight:700}.stage .title{font-weight:700;font-size:13px;margin-top:4px}.stage .state{font-size:11px;margin-top:6px}.stage.running{border:2px solid #1677ff;background:#eef6ff}.stage.done{border-color:#26a269;background:#effaf4}.stage.done .state{color:#16824f}.stage.warn{border-color:#d99b24;background:#fff9e8}.stage.error{border:2px solid #e5484d;background:#fff1f1}.detail{border-left:4px solid #1677ff;background:#f6f9fc;padding:12px 16px;border-radius:8px;margin:8px 0}
</style>""",unsafe_allow_html=True)
if "sources" not in st.session_state:st.session_state.sources=[]
if "last_result" not in st.session_state:st.session_state.last_result=None
if "live_state" not in st.session_state:st.session_state.live_state={}
st.markdown("""<div class="hero"><div style="font-size:.8rem;letter-spacing:.15em;color:#a9c3d0">DATA ENGINEERING • PRODUCTION-STYLE REFERENCE IMPLEMENTATION</div><h1>Enterprise Data Reliability & Readiness Platform</h1><p>Connect a source. Preserve the raw batch. Profile it. Enforce configurable quality controls. Quarantine failures. Produce trusted Silver/Gold data. Record an auditable reliability score. The reliability engine is independent of the source type.</p></div>""",unsafe_allow_html=True)
a,b,c,d=st.columns(4); a.metric("Source adapters","6"); b.metric("Formats","4"); c.metric("Quality dimensions","5"); d.metric("Pipeline controls","10")
STAGES=[("01","Ingest"),("02","Bronze"),("03","Profile"),("04","Schema"),("05","Quality"),("06","Quarantine"),("07","Silver"),("08","Integrity"),("09","Gold"),("10","Audit")]

def render_graph(state=None,row=None):
    state=state or {}
    html="<div class=\"pipeline\">"
    for i,(num,title) in enumerate(STAGES):
        cls=""; label="Waiting"
        if row:
            if title in ("Ingest","Bronze","Profile","Silver","Audit"):
                cls="done"
                label={"Ingest":"Complete","Bronze":"Preserved","Profile":f"{row.get('rows_received',0):,} rows","Silver":f"{row.get('rows_valid',0):,} valid","Audit":f"{row.get('reliability_score_pct',0)}%"}[title]
            elif title=="Schema": cls="error" if row.get("schema_errors") else "done"; label="Failed" if cls=="error" else "Passed"
            elif title=="Quality": cls="warn" if row.get("rows_quarantined",0)>0 else "done"; label=f"{row.get('quality_rules',{}).get('rule_count',0)} rules"
            elif title=="Quarantine": cls="warn" if row.get("rows_quarantined",0)>0 else "done"; label=f"{row.get('rows_quarantined',0):,} rejected"
            elif title=="Integrity": cls="warn" if row.get("metrics",{}).get("referential_integrity",100)<100 else "done"; label=f"{row.get('metrics',{}).get('referential_integrity',0)}%"
            elif title=="Gold": cls="done" if row.get("status")=="PASS" else "error"; label="Released" if cls=="done" else "Blocked"
        else:
            if i==state.get("error",-1): cls="error"; label="Failed"
            elif i==state.get("stage",-1): cls="running"; label="Running"
            elif i<=state.get("done",-1): cls="done"; label="Complete"
        html+=f'<div class="stage {cls}"><div class="num">{num}</div><div class="title">{title}</div><div class="state">{label}</div></div>'
        if i<len(STAGES)-1: html+="<div style=\"min-width:18px\"></div>"
    return html+"</div>"

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
    try:
        if not spec["name"]: raise ValueError("Source name is required.")
        if kind=="File upload" and uploaded is None: raise ValueError("Select a file first.")
        contract_present=bool(spec["required_columns"] or spec.get("expected_schema") or spec.get("unique_key") or spec.get("numeric_rules") or spec.get("regex_rules") or (ref_source and ref_column and ref_key))
        if not contract_present: raise ValueError("A release contract is required. Add required columns, a unique key, expected schema, numeric/regex rules, or a reference rule. Uncontracted data cannot be released.")
        st.session_state.sources.append((spec,uploaded))
        st.success(f"Added {spec['name']} with a release contract.")
    except Exception as exc:
        st.error(f"Configuration error: {exc}")
if st.session_state.sources:
    st.markdown("## 2 · Run plan")
    for i,(s,_) in enumerate(st.session_state.sources,1):st.markdown(f'<div class="card"><b>{i}. {s["name"]}</b> · {s["type"]}<br><span class="small">Required: {", ".join(s["required_columns"]) or "none"} · Key: {s["unique_key"] or "none"} · SLA: {s["freshness_sla_hours"]}h</span></div>',unsafe_allow_html=True)
    c1,c2=st.columns([1,5]); force=c1.checkbox("Force reprocess")
    if c1.button("Clear plan"):st.session_state.sources=[]; st.rerun()
    if c2.button("▶ Execute full reliability pipeline",type="primary"):
        INCOMING.mkdir(parents=True,exist_ok=True)
        for p in INCOMING.iterdir():
            if p.is_file():p.unlink()
        try:
            runtime=[]
            for s,up in st.session_state.sources:
                materialized=materialize_source(s,up,INCOMING)
                item=dict(s); item["materialized_path"]=str(materialized); item["format"]=Path(materialized).suffix.lstrip(".").lower(); runtime.append(item)
            run_id=uuid.uuid4().hex[:16]
            mp=BASE/"data"/"run_manifest.json"
            mp.write_text(json.dumps({"run_id":run_id,"sources":runtime,"pass_threshold":min(x.get("pass_threshold",95) for x in runtime),"shuffle_partitions":8},indent=2),encoding="utf-8")
            st.markdown("### Live execution")
            graph_box=st.empty(); event_box=st.empty()
            state={"stage":-1,"done":-1,"error":-1}
            graph_box.markdown(render_graph(state),unsafe_allow_html=True)
            def on_event(event):
                state["stage"]=event.get("stage_index",-1)
                if event.get("event")=="stage_done": state["done"]=max(state["done"],event.get("stage_index",-1))
                if event.get("event")=="stage_error": state["error"]=event.get("stage_index",-1)
                graph_box.markdown(render_graph(state),unsafe_allow_html=True)
                prefix="✓" if event.get("event")=="stage_done" else ("✕" if event.get("event")=="stage_error" else "●")
                event_box.markdown(f'<div class="detail"><b>{prefix} {event.get("stage","Stage")}</b> · {event.get("source","")}<br><span class="small">{event.get("message","")}</span></div>',unsafe_allow_html=True)
            run_pipeline(str(mp),str(OUTPUT),bool(force),on_event=on_event)
            st.session_state.last_result=json.loads((OUTPUT/"audit"/"latest.json").read_text(encoding="utf-8"))
            st.rerun()
        except Exception as exc:
            event_box.error(f"{type(exc).__name__}: {exc}")
            st.exception(exc)
if st.session_state.last_result:
    p=st.session_state.last_result; st.markdown("## 3 · Run result"); st.caption(f"Run ID: {p['run']['run_id']} · Duration: {p['run']['duration_seconds']}s")
    for row in p["results"]:
        if row.get("status")=="SKIPPED_IDEMPOTENT":st.info(f"{row['source']}: skipped safely because the same fingerprint was already processed."); continue
        st.markdown(f"### {row['source']} · execution graph")
        st.markdown(render_graph(row=row),unsafe_allow_html=True)
        m1,m2,m3,m4,m5=st.columns(5)
        m1.metric("Rows received",f"{row['rows_received']:,}")
        m2.metric("Rows valid",f"{row['rows_valid']:,}")
        m3.metric("Quarantined",f"{row['rows_quarantined']:,}")
        m4.metric("Reliability",f"{row['reliability_score_pct']}%")
        m5.metric("Status",row["status"])
        st.progress(min(max(row["reliability_score_pct"]/100,0),1))
        q=st.columns(5)
        for col,(label,key) in zip(q,[("Completeness","completeness"),("Validity","validity"),("Uniqueness","uniqueness"),("Referential integrity","referential_integrity"),("Freshness","freshness")]):
            col.metric(label,f"{row['metrics'][key]}%")
        if row["rows_quarantined"]>0:
            st.warning(f"{row['rows_quarantined']:,} records failed quality controls and were quarantined. They were not promoted to trusted data.")
            with st.expander("Exact quarantine reasons"):
                st.json(row.get("failure_reason_counts",{}))
        if row.get("schema_errors"):
            st.error("Schema contract failed — Gold release is blocked.")
            st.json(row["schema_errors"])
        if row.get("status")=="NO_CONTRACT":
            st.error("No release contract was defined. The pipeline refuses to publish ungoverned data.")
        with st.expander(f"{row['source']} · full audit and contract evidence"):
            st.json({"metrics":row["metrics"],"schema_errors":row["schema_errors"],"quality_rules":row["quality_rules"],"failure_reason_counts":row.get("failure_reason_counts",{}),"batch_id":row["batch_id"],"fingerprint":row["fingerprint"],"threshold_pct":row.get("threshold_pct")})
    st.success("Artifacts: output/runs/<run_id>/{bronze_raw,bronze,profiling,quarantine,silver,gold} plus audit history.")
else:st.info("Add one or more sources to create a run plan. Nothing executes until you click the pipeline button.")
