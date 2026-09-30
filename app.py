from __future__ import annotations
import json,sys,uuid,time,traceback
from pathlib import Path
import streamlit as st
from src.source_adapters import materialize_source
from src.reliability_pipeline import run as run_pipeline

BASE=Path(__file__).resolve().parent
INCOMING=BASE/"data"/"incoming"
OUTPUT=BASE/"output"

st.set_page_config(page_title="Data Reliability Control Plane",page_icon="◆",layout="wide",initial_sidebar_state="collapsed")

st.markdown("""
<style>
:root{--ink:#17212b;--muted:#647383;--line:#dfe7ee;--blue:#1677ff;--green:#159a63;--amber:#c88600;--red:#d64545;--bg:#f5f7fa}
.block-container{max-width:1380px;padding:28px 42px 60px}
.hero{padding:34px 42px;border-radius:22px;background:linear-gradient(135deg,#0b1724 0%,#12384b 55%,#15546b 100%);color:white;box-shadow:0 10px 32px rgba(18,45,65,.12);margin-bottom:22px}
.hero .eyebrow{font-size:11px;letter-spacing:.18em;color:#a9c7d4;font-weight:800}.hero h1{font-size:39px;line-height:1.1;margin:8px 0 10px}.hero p{max-width:1050px;color:#d9e7ed;font-size:16px;line-height:1.55;margin:0}
.section{font-size:24px;font-weight:800;color:var(--ink);margin:26px 0 10px}
.sub{color:var(--muted);font-size:14px;margin-bottom:14px}
.card{background:#fff;border:1px solid var(--line);border-radius:16px;padding:18px 20px;margin:9px 0;box-shadow:0 3px 12px rgba(20,40,60,.035)}
.source-card{display:flex;justify-content:space-between;gap:18px;align-items:center}.source-name{font-weight:800;font-size:16px}.pill{display:inline-block;padding:5px 9px;border-radius:999px;background:#eef5ff;color:#165db8;font-size:11px;font-weight:800}
.contract{background:#f8fafc;border:1px solid #e5ebf0;border-radius:14px;padding:15px;margin-top:12px}.contract-title{font-weight:800;margin-bottom:7px}.contract-row{color:#536373;font-size:13px;margin:3px 0}
.monitor{background:#0b1724;border-radius:20px;padding:20px 20px 24px;color:white;box-shadow:0 12px 34px rgba(5,20,30,.14)}
.monitor-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:15px}.monitor-title{font-size:18px;font-weight:800}.monitor-meta{color:#a8bdc8;font-size:12px}
.dag{display:flex;align-items:center;overflow-x:auto;padding:18px 2px 14px}
.node{min-width:128px;min-height:92px;border:1px solid #40515d;border-radius:14px;background:#14232f;padding:12px 11px;position:relative;transition:.15s}
.node:not(:last-child){margin-right:28px}.node:not(:last-child):after{content:"→";position:absolute;right:-25px;top:31px;color:#78909d;font-size:20px;font-weight:700}
.node-num{font-size:10px;color:#91a8b5;font-weight:800;letter-spacing:.08em}.node-title{font-size:13px;font-weight:800;margin-top:5px}.node-state{font-size:11px;margin-top:8px;color:#b5c4cc}.node.running{border:2px solid #3d91ff;background:#102d47;box-shadow:0 0 0 3px rgba(61,145,255,.12)}.node.running .node-state{color:#75b4ff}.node.done{border-color:#28a66d;background:#102a22}.node.done .node-state{color:#79d3a9}.node.warn{border-color:#c88600;background:#302710}.node.warn .node-state{color:#f1c766}.node.error{border:2px solid #e25555;background:#341b1b}.node.error .node-state{color:#ff9c9c}.node.wait{opacity:.68}
.live-row{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0 0}.event{background:#10212c;border:1px solid #2a414e;border-radius:10px;padding:9px 11px;color:#c6d4db;font-size:12px}
.error-box{border:1px solid #efb0b0;background:#fff3f3;border-radius:12px;padding:14px 16px;color:#7d2222;margin-top:12px}.warn-box{border:1px solid #efd69a;background:#fff9e8;border-radius:12px;padding:14px 16px;color:#755000}
.metric-card{border:1px solid var(--line);background:#fff;border-radius:14px;padding:14px 16px}.metric-label{font-size:11px;color:var(--muted);font-weight:700}.metric-value{font-size:24px;color:var(--ink);font-weight:800;margin-top:3px}
.guide{background:#f7fafc;border:1px solid #e1e9ef;border-radius:16px;padding:18px 20px}.guide h4{margin:0 0 8px}.guide li{margin:6px 0;color:#4d5d6a;font-size:13px}
.stButton>button{border-radius:10px;font-weight:750}
</style>
""",unsafe_allow_html=True)

if "sources" not in st.session_state: st.session_state.sources=[]
if "last_result" not in st.session_state: st.session_state.last_result=None
if "last_run_id" not in st.session_state: st.session_state.last_run_id=None
# Restore the latest completed run after a browser refresh/restart.
if st.session_state.last_result is None:
    _latest=OUTPUT/"audit"/"latest.json"
    if _latest.exists():
        try:
            st.session_state.last_result=json.loads(_latest.read_text(encoding="utf-8"))
            st.session_state.last_run_id=st.session_state.last_result.get("run",{}).get("run_id")
        except Exception:
            pass

STAGES=[
("01","Ingest"),("02","Raw Bronze"),("03","Profile"),("04","Schema"),
("05","Quality"),("06","Quarantine"),("07","Silver"),("08","Integrity"),
("09","Gold"),("10","Audit")
]

def parse_numeric_rules(text:str)->list[dict]:
    rules=[]
    for line in text.splitlines():
        line=line.strip()
        if not line: continue
        parts=[x.strip() for x in line.split("|")]
        if len(parts)<2: raise ValueError("Numeric rule format: column|min=0|max=100")
        rule={"column":parts[0]}
        for item in parts[1:]:
            if not item: continue
            k,v=[x.strip() for x in item.split("=",1)]
            if k not in {"min","max"}: raise ValueError(f"Unknown numeric rule option: {k}")
            rule[k]=float(v)
        rules.append(rule)
    return rules

def parse_regex_rules(text:str)->list[dict]:
    rules=[]
    for line in text.splitlines():
        line=line.strip()
        if not line: continue
        parts=line.split("|",1)
        if len(parts)!=2: raise ValueError("Regex rule format: column|regular_expression")
        rules.append({"column":parts[0].strip(),"pattern":parts[1].strip()})
    return rules

def contract_present(spec):
    return bool(spec.get("required_columns") and spec.get("unique_key") and spec.get("freshness_column"))

def node_html(state=None,row=None,stage_durations=None):
    state=state or {}
    stage_durations=stage_durations or {}
    html='<div class="dag">'
    for i,(num,title) in enumerate(STAGES):
        cls="wait"; label="Waiting"; detail=""
        if row:
            if title=="Ingest": cls="done"; label="Complete"
            elif title=="Raw Bronze": cls="done"; label="Preserved"
            elif title=="Profile": cls="done"; label=f"{row.get('rows_received',0):,} rows"
            elif title=="Schema":
                cls="error" if row.get("schema_errors") else "done"; label="Failed" if cls=="error" else "Passed"
            elif title=="Quality":
                cls="warn" if row.get("rows_quarantined",0)>0 else "done"; label=f"{row.get('quality_rules',{}).get('rule_count',0)} rules"
            elif title=="Quarantine":
                cls="warn" if row.get("rows_quarantined",0)>0 else "done"; label=f"{row.get('rows_quarantined',0):,} rejected"
            elif title=="Silver":
                cls="error" if row.get("schema_errors") else "done"
                label="Blocked" if cls=="error" else f"{row.get('rows_valid',0):,} valid"
            elif title=="Integrity":
                ri=row.get("metrics",{}).get("referential_integrity",100)
                cls="warn" if ri<100 else "done"; label=f"RI {ri}%"
            elif title=="Gold":
                cls="done" if row.get("status")=="PASS" else "error"; label="Released" if cls=="done" else "Blocked"
            elif title=="Audit": cls="done"; label=f"{row.get('reliability_score_pct',0)}%"
        else:
            if i==state.get("error",-1): cls="error"; label="Failed"
            elif i==state.get("stage",-1): cls="running"; label="Running"
            elif i<=state.get("done",-1): cls="done"; label="Complete"
            if i==state.get("stage",-1): detail=state.get("message","")
        dur=stage_durations.get(i)
        if dur is not None: label=f"{label} · {dur:.1f}s"
        html+=f'<div class="node {cls}"><div class="node-num">{num}</div><div class="node-title">{title}</div><div class="node-state">{label}</div>'
        if detail: html+=f'<div style="font-size:9px;color:#9fc2d4;margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{detail}</div>'
        html+='</div>'
    html+="</div>"
    return html

st.markdown("""<div class="hero">
<div class="eyebrow">DATA ENGINEERING • SOURCE-AGNOSTIC • QUALITY-GATED</div>
<h1>Enterprise Data Reliability & Readiness Platform</h1>
<p>Connect heterogeneous enterprise sources through adapters, preserve the original batch, validate a source-specific contract, quarantine failures, promote trusted records and release Gold only when the reliability gate passes.</p>
</div>""",unsafe_allow_html=True)

m1,m2,m3,m4=st.columns(4)
m1.metric("Source adapters","7")
m2.metric("File formats","5")
m3.metric("Quality dimensions","5")
m4.metric("Release controls","10")

st.markdown('<div class="section">1 · Register a source</div>',unsafe_allow_html=True)
st.markdown('<div class="sub">Choose where the data lives. The source adapter handles acquisition; the same Spark reliability engine handles profiling, quality, quarantine and release.</div>',unsafe_allow_html=True)

with st.form("source_form",clear_on_submit=False):
    left,right=st.columns([1,1])
    name=left.text_input("Source name *",placeholder="erp_orders")
    kind=right.selectbox("Source type *",["File upload","API / HTTP","Amazon S3","Azure Blob / ADLS","Google Cloud Storage","Database","Local path"])

    uploaded=None
    spec={"name":name.strip(),"type":kind}

    if kind=="File upload":
        uploaded=st.file_uploader("Data file *",type=["csv","xlsx","xlsm","json","jsonl","parquet"],max_upload_size=4096)
        st.caption("Accepted: CSV, Excel, JSON, JSONL, Parquet. Excel is normalized before Spark processing.")
    elif kind=="API / HTTP":
        a1,a2=st.columns([1,3])
        spec["method"]=a1.selectbox("Method",["GET","POST"])
        spec["url"]=a2.text_input("Endpoint URL *",placeholder="https://api.example.com/v1/orders")
        a1,a2,a3=st.columns([1,1,2])
        spec["auth_type"]=a1.selectbox("Authentication",["None","Bearer","API key"])
        spec["auth_env"]=a2.text_input("Secret env var",placeholder="API_TOKEN")
        spec["auth_header"]=a3.text_input("Auth header",value="Authorization" if spec["auth_type"]=="Bearer" else "X-API-Key")
        if spec["method"]=="POST":
            body=st.text_area("POST JSON body",value="{}",height=90)
            try: spec["body_json"]=json.loads(body)
            except Exception: st.warning("POST body must be valid JSON.")
        st.caption("Do not paste secrets here. Store the token in an environment/secret store and provide only its variable name.")
    elif kind=="Amazon S3":
        spec["uri"]=st.text_input("S3 object URI *",placeholder="s3://bucket/path/data.parquet")
        st.info("Credential: boto3 default credential chain — IAM role, profile, or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables.")
    elif kind=="Azure Blob / ADLS":
        spec["uri"]=st.text_input("Azure object URI *",placeholder="https://account.blob.core.windows.net/container/path/data.parquet?...")
        spec["credential_env"]=st.text_input("Connection-string env var",value="AZURE_STORAGE_CONNECTION_STRING")
        st.info("Use a SAS URL in the object URI or configure the connection string through the environment. Never commit the secret.")
    elif kind=="Google Cloud Storage":
        spec["uri"]=st.text_input("GCS object URI *",placeholder="gs://bucket/path/data.parquet")
        spec["credential_env"]=st.text_input("Credential environment",value="GOOGLE_APPLICATION_CREDENTIALS")
        st.info("Use Application Default Credentials or a service-account credential file referenced by the environment.")
    elif kind=="Database":
        dbtype=st.selectbox("Database",["PostgreSQL","MySQL","SQL Server","Other SQLAlchemy-compatible"])
        spec["connection_env"]=st.text_input("Connection-string env var *",placeholder="ERP_DB_URL")
        spec["query"]=st.text_area("Read-only SQL query *",placeholder="SELECT * FROM schema.table",height=100)
        st.caption("Only SELECT / CTE queries are accepted. The connection string stays in the environment.")
    else:
        spec["path"]=st.text_input("Local dataset path *",placeholder="C:/data/customer.parquet")

    st.markdown("#### Release contract")
    st.caption("These fields define what the platform is allowed to call trustworthy. A source cannot enter the release gate without the three core contract fields.")

    c1,c2,c3=st.columns(3)
    spec["required_columns"]=[x.strip() for x in c1.text_input("Required columns *",placeholder="id,name,updated_at").split(",") if x.strip()]
    spec["unique_key"]=c2.text_input("Unique key / composite key *",placeholder="id or customer_id,order_id").strip()
    spec["freshness_column"]=c3.text_input("Freshness column *",placeholder="updated_at").strip()

    c1,c2,c3=st.columns(3)
    spec["freshness_sla_hours"]=c1.number_input("Freshness SLA (hours)",1.0,8760.0,24.0,1.0)
    spec["pass_threshold"]=c2.number_input("Reliability release threshold (%)",1.0,100.0,95.0,.5)
    spec["reject_unexpected_columns"]=c3.checkbox("Reject unexpected columns",value=False)

    with st.expander("Optional quality rules — use only what the source requires"):
        ntext=st.text_area("Numeric rules (one per line)",placeholder="amount|min=0\nquantity|min=1|max=100000",height=90)
        rtext=st.text_area("Regex rules (one per line)",placeholder="email|^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$",height=90)
        x1,x2,x3=st.columns(3)
        ref_source=x1.text_input("Reference source")
        ref_col=x2.text_input("Foreign-key column")
        ref_key=x3.text_input("Reference key column")
        expected=st.text_area("Expected schema JSON (optional)",placeholder='{"id":"string","amount":"double"}',height=90)

    add=st.form_submit_button("＋ Add source to run plan",type="primary")

if add:
    try:
        if not spec["name"]: raise ValueError("Source name is required.")
        if kind=="File upload" and uploaded is None: raise ValueError("Select a data file.")
        if kind=="API / HTTP" and not spec.get("url"): raise ValueError("Endpoint URL is required.")
        if kind in {"Amazon S3","Azure Blob / ADLS","Google Cloud Storage"} and not spec.get("uri"): raise ValueError("Cloud object URI is required.")
        if kind=="Database" and (not spec.get("connection_env") or not spec.get("query")): raise ValueError("Database connection env var and read-only SQL are required.")
        if kind=="Local path" and not spec.get("path"): raise ValueError("Local path is required.")
        if not contract_present(spec): raise ValueError("Release contract incomplete. Required columns, unique key and freshness column are mandatory.")
        spec["numeric_rules"]=parse_numeric_rules(ntext)
        spec["regex_rules"]=parse_regex_rules(rtext)
        if expected.strip():
            spec["expected_schema"]=json.loads(expected)
        else: spec["expected_schema"]={}
        if ref_source and ref_col and ref_key:
            spec["reference"]={"source":ref_source.strip(),"column":ref_col.strip(),"reference_column":ref_key.strip()}
        elif any([ref_source,ref_col,ref_key]):
            raise ValueError("Reference rule requires source name, foreign-key column and reference key column.")
        if kind=="API / HTTP" and spec.get("auth_type")!="None" and not spec.get("auth_env"):
            raise ValueError("Secret environment-variable name is required for authenticated APIs.")
        st.session_state.sources.append((spec,uploaded))
        st.success(f"Source '{spec['name']}' added. Contract is ready.")
    except Exception as exc:
        st.error(f"Configuration error: {exc}")

st.markdown('<div class="section">Contract rules — exactly what the source owner must provide</div>',unsafe_allow_html=True)
g1,g2,g3=st.columns(3)
with g1:
    st.markdown("""<div class="guide"><h4>Core contract</h4><ul>
<li><b>Required columns:</b> fields that must exist and contain a value.</li>
<li><b>Unique key:</b> one key or a comma-separated composite key that must be unique.</li>
<li><b>Freshness column:</b> timestamp/date used to enforce the SLA.</li>
<li><b>Freshness SLA:</b> maximum accepted age in hours.</li>
</ul></div>""",unsafe_allow_html=True)
with g2:
    st.markdown("""<div class="guide"><h4>Optional data rules</h4><ul>
<li><b>Expected schema:</b> exact column types when schema drift must be rejected.</li>
<li><b>Numeric rules:</b> minimum/maximum business limits.</li>
<li><b>Regex rules:</b> formats such as email, phone or ID patterns.</li>
<li><b>Unexpected columns:</b> reject schema additions when strict mode is required.</li>
</ul></div>""",unsafe_allow_html=True)
with g3:
    st.markdown("""<div class="guide"><h4>Relationship rule</h4><ul>
<li><b>Reference source:</b> another registered source in the same run.</li>
<li><b>Foreign-key column:</b> field in the current dataset.</li>
<li><b>Reference key:</b> key field in the parent dataset.</li>
<li>Example: <b>orders.customer_id → customers.customer_id</b>.</li>
</ul></div>""",unsafe_allow_html=True)

if st.session_state.sources:
    st.markdown('<div class="section">2 · Run plan</div>',unsafe_allow_html=True)
    for i,(s,_) in enumerate(st.session_state.sources,1):
        rules=len(s.get("required_columns",[]))+bool(s.get("unique_key"))+bool(s.get("freshness_column"))+len(s.get("numeric_rules",[]))+len(s.get("regex_rules",[]))+bool(s.get("reference"))
        st.markdown(f'''<div class="card source-card"><div><div class="source-name">{i}. {s["name"]} <span class="pill">{s["type"]}</span></div><div class="contract"><div class="contract-title">Release contract · {rules} controls</div><div class="contract-row"><b>Required:</b> {", ".join(s["required_columns"])}</div><div class="contract-row"><b>Key:</b> {s["unique_key"]}</div><div class="contract-row"><b>Freshness:</b> {s["freshness_column"]} ≤ {s["freshness_sla_hours"]}h</div></div></div></div>''',unsafe_allow_html=True)

    b1,b2,b3=st.columns([1,1,4])
    force=b1.checkbox("Force reprocess")
    if b2.button("Clear run plan"): st.session_state.sources=[]; st.session_state.last_result=None; st.rerun()
    execute=b3.button("▶ Run reliability pipeline",type="primary")

    if execute:
        INCOMING.mkdir(parents=True,exist_ok=True)
        for p in INCOMING.iterdir():
            if p.is_file(): p.unlink()
        monitor=st.empty()
        detail=st.empty()
        progress=st.progress(0)
        state={"stage":-1,"done":-1,"error":-1,"message":""}
        stage_started={}
        stage_durations={}
        current_stage_name="Preparing"
        events=[]

        def draw_monitor():
            elapsed=time.time()-run_started
            body='<div class="monitor"><div class="monitor-head"><div><div class="monitor-title">Pipeline execution</div><div class="monitor-meta">Spark reliability engine · live run · '+current_stage_name+'</div></div><div class="monitor-meta">'+f"{elapsed:.1f}s elapsed"+'</div></div>'
            body+=node_html(state,stage_durations=stage_durations)
            body+='<div class="live-row">'
            for ev in events[-5:]:
                body+=f'<div class="event">{ev}</div>'
            body+='</div></div>'
            monitor.markdown(body,unsafe_allow_html=True)

        try:
            run_started=time.time()
            runtime=[]
            for s,up in st.session_state.sources:
                materialized=materialize_source(s,up,INCOMING)
                item=dict(s)
                item["materialized_path"]=str(materialized)
                item["format"]=Path(materialized).suffix.lstrip(".").lower()
                runtime.append(item)

            run_id=uuid.uuid4().hex[:16]
            manifest_path=BASE/"data"/"run_manifest.json"
            manifest_path.write_text(json.dumps({"run_id":run_id,"sources":runtime,"pass_threshold":min(x.get("pass_threshold",95) for x in runtime),"shuffle_partitions":8},indent=2),encoding="utf-8")

            draw_monitor()

            def on_event(event):
                idx=event.get("stage_index",-1)
                current_stage_name=event.get("stage","Pipeline")
                state["stage"]=idx
                state["message"]=event.get("message","")
                if event.get("event")=="stage_start":
                    stage_started[idx]=time.time()
                    events.append("● "+current_stage_name+" · "+event.get("source","")+" · "+event.get("message",""))
                elif event.get("event")=="stage_done":
                    state["done"]=max(state["done"],idx)
                    if idx in stage_started: stage_durations[idx]=time.time()-stage_started[idx]
                    events.append("✓ "+current_stage_name+" · "+event.get("source","")+" · "+event.get("message",""))
                elif event.get("event")=="stage_error":
                    state["error"]=idx
                    if idx in stage_started: stage_durations[idx]=time.time()-stage_started[idx]
                    events.append("✕ "+current_stage_name+" · "+event.get("source","")+" · "+event.get("message",""))
                progress.progress(min((state["done"]+1)/len(STAGES),1.0))
                draw_monitor()
                detail.markdown(f'<div class="card"><b>{current_stage_name}</b><br><span class="small">{event.get("message","")}</span></div>',unsafe_allow_html=True)

            result=run_pipeline(str(manifest_path),str(OUTPUT),bool(force),on_event=on_event)
            st.session_state.last_result=json.loads((OUTPUT/"audit"/"latest.json").read_text(encoding="utf-8"))
            progress.progress(1.0)
            st.rerun()
        except Exception as exc:
            state["error"]=max(state.get("stage",-1),0)
            events.append("✕ "+current_stage_name+" · "+type(exc).__name__)
            draw_monitor()
            detail.markdown(f'<div class="error-box"><b>Pipeline stopped in: {current_stage_name}</b><br>{type(exc).__name__}: {exc}</div>',unsafe_allow_html=True)
            st.code(traceback.format_exc(),language="text")

if st.session_state.last_result:
    p=st.session_state.last_result
    st.markdown('<div class="section">3 · Run result</div>',unsafe_allow_html=True)
    st.caption(f"Run ID: {p['run']['run_id']} · Duration: {p['run']['duration_seconds']}s")

    for row in p["results"]:
        if row.get("status")=="SKIPPED_IDEMPOTENT":
            st.info(f"{row['source']}: safely skipped — identical source fingerprint already processed.")
            continue

        st.markdown(f"### {row['source']} · pipeline DAG")
        st.markdown('<div class="monitor">'+node_html(row=row)+'</div>',unsafe_allow_html=True)

        q1,q2,q3,q4,q5=st.columns(5)
        for col,label,value in [
            (q1,"Rows received",f"{row['rows_received']:,}"),
            (q2,"Rows valid",f"{row['rows_valid']:,}"),
            (q3,"Quarantined",f"{row['rows_quarantined']:,}"),
            (q4,"Reliability",f"{row['reliability_score_pct']}%"),
            (q5,"Release status",row["status"])]:
            col.markdown(f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>',unsafe_allow_html=True)

        st.progress(min(max(row["reliability_score_pct"]/100,0),1))
        d1,d2,d3,d4,d5=st.columns(5)
        for col,(label,key) in zip([d1,d2,d3,d4,d5],[("Completeness","completeness"),("Validity","validity"),("Uniqueness","uniqueness"),("Referential integrity","referential_integrity"),("Freshness","freshness")]):
            col.metric(label,f"{row['metrics'][key]}%")

        if row["rows_quarantined"]>0:
            st.markdown(f'<div class="warn-box"><b>{row["rows_quarantined"]:,} records were quarantined.</b><br>They failed one or more configured data-quality controls and were not promoted as trusted records.</div>',unsafe_allow_html=True)
            with st.expander("Open exact failure reasons"):
                st.json(row.get("failure_reason_counts",{}))
        if row.get("schema_errors"):
            st.markdown('<div class="error-box"><b>Schema contract failed — Gold release is blocked.</b></div>',unsafe_allow_html=True)
            st.json(row["schema_errors"])
        if row.get("status")=="NO_CONTRACT":
            st.markdown('<div class="error-box"><b>Release blocked — no valid source contract.</b></div>',unsafe_allow_html=True)

        with st.expander("Audit, batch fingerprint and contract evidence"):
            st.json({"source_type":row["source_type"],"batch_id":row["batch_id"],"fingerprint":row["fingerprint"],"metrics":row["metrics"],"quality_rules":row["quality_rules"],"failure_reason_counts":row.get("failure_reason_counts",{}),"threshold_pct":row.get("threshold_pct")})

    st.success("Artifacts written under output/runs/<run_id> and audit history. Re-running the same fingerprint is idempotently skipped unless Force reprocess is selected.")
else:
    st.markdown('<div class="card"><b>Nothing executes until you add a source and click Run reliability pipeline.</b><br><span class="small">The platform is designed so the source connector and source schema can change without rewriting the common reliability engine.</span></div>',unsafe_allow_html=True)

st.markdown('<div class="section">Source connection cheat sheet</div>',unsafe_allow_html=True)
st.table([
{"Source":"File upload","What the source owner gives":"The actual CSV / Excel / JSON / JSONL / Parquet file","Secret":"None"},
{"Source":"API / HTTP","What the source owner gives":"Endpoint URL + GET/POST method + authentication type + secret environment-variable name","Secret":"Token stored outside Git"},
{"Source":"Amazon S3","What the source owner gives":"s3://bucket/object URI","Secret":"IAM role/profile or AWS environment credentials"},
{"Source":"Azure Blob / ADLS","What the source owner gives":"Blob/ADLS object URI or SAS URL","Secret":"Connection string environment variable or SAS"},
{"Source":"Google Cloud Storage","What the source owner gives":"gs://bucket/object URI","Secret":"Application Default Credentials / service account"},
{"Source":"Database","What the source owner gives":"Database connection environment-variable name + read-only SELECT/CTE","Secret":"Connection string stored outside Git"},
{"Source":"Local path","What the developer gives":"Path to the dataset on the runtime machine","Secret":"None"},
])
st.caption("The UI intentionally asks for secret references, not secret values. This keeps credentials out of the Git repository and aligns the source adapter boundary with production secret-management practices.")


# LAYER_EXPLORER_V2
# ---------------------------------------------------------------------------
# Data-proof UI: inspect persisted datasets without loading the full dataset
# into the browser. Spark performs the expensive work; Streamlit receives a
# bounded sample and aggregate metadata.
@st.cache_resource(show_spinner=False)
def _explorer_spark():
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.appName("ReliabilityLayerExplorer")
           .master("local[*]")
           .config("spark.sql.shuffle.partitions","8")
           .config("spark.sql.adaptive.enabled","true")
           .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def _layer_path(run_id,row,layer):
    return OUTPUT/"runs"/run_id/layer/row["source"]/row["batch_id"]

def _layer_snapshot(run_id,row,layer,sample_rows=200):
    from pyspark.sql import functions as F
    path=_layer_path(run_id,row,layer)
    if not path.exists():
        return {"available":False,"error":f"{layer} was not produced for this batch."}
    try:
        df=_explorer_spark().read.parquet(str(path))
        rows=df.count()
        cols=df.columns
        nulls={}
        if cols:
            expr=[F.sum(F.when(F.col(col).isNull() | (F.trim(F.col(col).cast("string"))==""),1).otherwise(0)).alias(col) for col in cols]
            nulls={k:int(v or 0) for k,v in df.agg(*expr).first().asDict().items()}
        return {
            "available":True,
            "rows":rows,
            "columns":len(cols),
            "column_names":cols,
            "nulls":nulls,
            "sample":df.limit(sample_rows).toPandas(),
        }
    except Exception as exc:
        return {"available":False,"error":f"{type(exc).__name__}: {exc}"}

def _render_layer_tab(run_id,row,layer):
    snap=_layer_snapshot(run_id,row,layer,200)
    if not snap["available"]:
        st.warning(snap["error"])
        return
    a,b,c=st.columns(3)
    a.metric("Rows",f"{snap['rows']:,}")
    b.metric("Columns",snap["columns"])
    c.metric("Sample shown",min(200,snap["rows"]))
    st.dataframe(snap["sample"],use_container_width=True,height=360)
    st.caption("The complete dataset remains in Parquet. Only a bounded sample is brought into the browser.")
    with st.expander("Schema + full null profile"):
        st.json({"columns":snap["column_names"],"nulls":snap["nulls"]})
    if layer=="quarantine" and "_failure_reason" in snap["sample"].columns:
        reasons=(snap["sample"]["_failure_reason"].value_counts()
                 .rename_axis("failure_reason").reset_index(name="sample_count"))
        st.dataframe(reasons,use_container_width=True)

def _render_layer_explorer():
    payload=st.session_state.get("last_result")
    if not payload or not payload.get("results"):
        return
    run_id=payload.get("run",{}).get("run_id") or st.session_state.get("last_run_id")
    rows=[r for r in payload["results"] if r.get("batch_id") and r.get("source")]
    if not run_id or not rows:
        return

    st.markdown('<div class="section">4 · Layer Explorer & Data Proof</div>',unsafe_allow_html=True)
    st.markdown(
        '<div class="sub">The pipeline graph answers <b>what happened</b>. '
        'This explorer answers <b>what data was actually produced</b>. '
        'It is safe for 1M+ row datasets because Spark counts/aggregates the full '
        'Parquet data while the browser receives only a small sample.</div>',
        unsafe_allow_html=True,
    )

    source_names=[r["source"] for r in rows]
    source=st.selectbox("Inspect source",source_names,key="layer_explorer_source")
    row=next(r for r in rows if r["source"]==source)

    tabs=st.tabs(["Raw Bronze","Bronze","Profile","Quarantine","Silver","Gold","Compare","Lineage"])

    with tabs[0]:
        raw_dir=_layer_path(run_id,row,"bronze_raw")
        files=list(raw_dir.iterdir()) if raw_dir.exists() else []
        if not files:
            st.warning("Raw Bronze artifact was not found.")
        else:
            f=files[0]
            st.markdown(
                f'<div class="success-box"><b>Raw Bronze preserved</b><br>'
                f'Original artifact: <b>{f.name}</b><br>'
                f'Size: {f.stat().st_size:,} bytes<br>'
                f'Batch: {row["batch_id"]}</div>',
                unsafe_allow_html=True,
            )
            st.caption("This is the immutable source copy. The next tab is the Spark-readable Bronze dataset.")

    with tabs[1]:
        _render_layer_tab(run_id,row,"bronze")

    with tabs[2]:
        profile_file=OUTPUT/"runs"/run_id/"profiling"/row["source"]/row["batch_id"]/"profile.json"
        if profile_file.exists():
            prof=json.loads(profile_file.read_text(encoding="utf-8"))
            a,b,c=st.columns(3)
            a.metric("Rows",f'{prof.get("rows",0):,}')
            b.metric("Columns",prof.get("columns",0))
            c.metric("Profiled fields",len(prof.get("column_names",[])))
            st.json(prof)
        else:
            st.warning("Profile artifact was not found.")

    with tabs[3]:
        _render_layer_tab(run_id,row,"quarantine")

    with tabs[4]:
        _render_layer_tab(run_id,row,"silver")

    with tabs[5]:
        _render_layer_tab(run_id,row,"gold")

    with tabs[6]:
        layer_names=["bronze","quarantine","silver","gold"]
        ca,cb=st.columns(2)
        left=ca.selectbox("Compare layer A",layer_names,index=0,key="compare_layer_a")
        right=cb.selectbox("Compare layer B",layer_names,index=2,key="compare_layer_b")
        if left==right:
            st.info("Choose two different layers.")
        else:
            sa=_layer_snapshot(run_id,row,left,100)
            sb=_layer_snapshot(run_id,row,right,100)
            if not sa["available"] or not sb["available"]:
                st.warning(sa.get("error") or sb.get("error"))
            else:
                common=sorted(set(sa["column_names"]) & set(sb["column_names"]))
                added=sorted(set(sb["column_names"])-set(sa["column_names"]))
                removed=sorted(set(sa["column_names"])-set(sb["column_names"]))
                m=st.columns(4)
                m[0].metric("Rows A",f'{sa["rows"]:,}')
                m[1].metric("Rows B",f'{sb["rows"]:,}')
                m[2].metric("Row delta",f'{sb["rows"]-sa["rows"]:+,}')
                m[3].metric("Common columns",len(common))
                st.markdown(
                    f'<div class="card"><b>Schema movement</b><br>'
                    f'<span style="color:#168454;font-weight:800">Added in {right}: {", ".join(added) if added else "None"}</span><br>'
                    f'<span style="color:#c33c3c;font-weight:800">Removed in {right}: {", ".join(removed) if removed else "None"}</span></div>',
                    unsafe_allow_html=True,
                )
                null_rows=[]
                for col in common:
                    null_rows.append({
                        "column":col,
                        f"{left}_nulls":sa["nulls"].get(col,0),
                        f"{right}_nulls":sb["nulls"].get(col,0),
                        "null_delta":sb["nulls"].get(col,0)-sa["nulls"].get(col,0),
                    })
                st.dataframe(null_rows,use_container_width=True,height=330)
                x,y=st.columns(2)
                x.caption(f"{left} sample")
                x.dataframe(sa["sample"],use_container_width=True,height=260)
                y.caption(f"{right} sample")
                y.dataframe(sb["sample"],use_container_width=True,height=260)

    with tabs[7]:
        st.markdown(
            '<div class="card"><b>Batch lineage</b><br><br>'
            'Source → <b>Raw Bronze</b> → <b>Bronze</b> → <b>Profile / Schema</b> '
            '→ <b>Quality</b> → <b>Quarantine / Silver</b> → <b>Integrity</b> '
            '→ <b>Gold</b> → <b>Audit</b></div>',
            unsafe_allow_html=True,
        )
        st.code(
            f"output/runs/{run_id}/\n"
            f"  bronze_raw/{row['source']}/{row['batch_id']}/\n"
            f"  bronze/{row['source']}/{row['batch_id']}/\n"
            f"  profiling/{row['source']}/{row['batch_id']}/profile.json\n"
            f"  quarantine/{row['source']}/{row['batch_id']}/\n"
            f"  silver/{row['source']}/{row['batch_id']}/\n"
            f"  gold/{row['source']}/{row['batch_id']}/",
            language="text",
        )

_render_layer_explorer()
