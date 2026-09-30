from __future__ import annotations

import json
import time
import traceback
import uuid
from pathlib import Path

import streamlit as st

from src.reliability_pipeline import run as run_pipeline
from src.source_adapters import materialize_source

BASE = Path(__file__).resolve().parent
INCOMING = BASE / "data" / "incoming"
OUTPUT = BASE / "output"

st.set_page_config(
    page_title="Data Reliability Control Plane",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Visual language: enterprise data-quality / pipeline-control dashboard
# ---------------------------------------------------------------------------
st.markdown(
    """
<style>
:root{
 --bg:#f7f8fa;--card:#fff;--ink:#17202a;--muted:#697681;--line:#e5e8ec;
 --nav:#101820;--nav2:#19242e;--blue:#2f6fed;--green:#35a879;--red:#df5a5a;
 --amber:#e9b63f;--purple:#7257d7;
}
html,body,[class*="css"]{font-family:Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}
.stApp{background:var(--bg)}
.block-container{max-width:1500px;padding:20px 30px 55px}
[data-testid="stSidebar"]{background:var(--nav);border-right:1px solid #24313b}
[data-testid="stSidebar"] *{color:#dce5eb}
[data-testid="stSidebar"] .stButton button{
 background:transparent!important;border:0!important;box-shadow:none!important;
 color:#b8c5ce!important;text-align:left;width:100%;padding:9px 12px;border-radius:7px;
 font-size:13px;font-weight:650
}
[data-testid="stSidebar"] .stButton button:hover{background:#1d2a34!important;color:#fff!important}
.brand{padding:5px 4px 20px;border-bottom:1px solid #28353f;margin-bottom:12px}
.brand-mark{display:inline-flex;width:30px;height:30px;border-radius:8px;background:#2f6fed;
 align-items:center;justify-content:center;color:#fff;font-weight:900;margin-right:8px}
.brand-name{font-size:17px;font-weight:850;color:#fff;vertical-align:middle}
.brand-sub{font-size:9px;color:#8294a1;margin:6px 0 0 39px;letter-spacing:.11em}
.nav-label{font-size:9px;font-weight:850;letter-spacing:.14em;color:#657783;margin:18px 5px 6px}
.top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:17px}
.h1{font-size:27px;font-weight:850;color:var(--ink);line-height:1.1}
.sub{font-size:12px;color:var(--muted);margin-top:5px}
.toolbar{display:flex;gap:6px;align-items:center}
.toolbar .stButton button{border:1px solid var(--line);background:#fff;border-radius:6px;font-size:11px}
.card{background:#fff;border:1px solid var(--line);border-radius:8px;box-shadow:0 1px 4px rgba(20,30,40,.035);padding:16px}
.card-title{font-size:14px;font-weight:800;color:var(--ink)}
.card-sub{font-size:11px;color:var(--muted);margin-top:3px;margin-bottom:13px}
.metric{background:#fff;border:1px solid var(--line);border-radius:8px;padding:15px 16px;min-height:104px}
.metric-label{font-size:10px;color:#71808a;font-weight:800;text-transform:uppercase;letter-spacing:.04em}
.metric-value{font-size:28px;font-weight:850;color:var(--ink);margin-top:5px}
.metric-foot{font-size:10px;color:#89959d;margin-top:3px}
.metric-green{border-left:4px solid var(--green)}
.metric-blue{border-left:4px solid var(--blue)}
.metric-amber{border-left:4px solid var(--amber)}
.metric-red{border-left:4px solid var(--red)}
.section{font-size:17px;font-weight:850;color:var(--ink);margin:22px 0 7px}
.pill{display:inline-block;padding:3px 7px;border-radius:12px;font-size:9px;font-weight:800}
.pill-ok{background:#e9f7f0;color:#197650}.pill-warn{background:#fff6dd;color:#8c6907}
.pill-err{background:#ffeded;color:#a73b3b}.pill-blue{background:#edf3ff;color:#2c60c7}
.pipeline{display:flex;align-items:center;overflow-x:auto;padding:9px 2px 13px}
.node{min-width:118px;border:1px solid #dfe4e8;background:#fbfcfd;border-radius:7px;padding:10px}
.node.ok{border-color:#b9ddcd;background:#f5fbf8}.node.warn{border-color:#ead49a;background:#fffbf0}
.node.err{border-color:#e9bcbc;background:#fff7f7}
.node-num{font-size:8px;color:#8c99a2;font-weight:850}.node-title{font-size:11px;font-weight:850;color:var(--ink);margin-top:4px}
.node-state{font-size:9px;color:#75838d;margin-top:6px}.node.ok .node-state{color:#26976a}
.node.warn .node-state{color:#9a760d}.node.err .node-state{color:#c04a4a}
.arrow{font-size:16px;color:#9ba7ae;font-weight:800;padding:0 8px}
.layer-card{border:1px solid var(--line);border-radius:8px;background:#fff;padding:14px}
.layer-head{display:flex;justify-content:space-between;align-items:center}.layer-name{font-weight:850;font-size:14px}
.layer-count{font-size:11px;color:#71808a}.bar{height:5px;background:#edf0f2;border-radius:9px;margin:13px 0 10px;overflow:hidden}
.bar>div{height:100%;background:var(--green);border-radius:9px}.row{display:flex;justify-content:space-between;font-size:10px;color:#687680;margin:7px 0}
.event{display:inline-block;border:1px solid #e4e8eb;background:#fafbfc;border-radius:5px;padding:5px 7px;margin:2px;font-size:9px;color:#596873}
.expect{border:1px solid var(--line);border-radius:7px;overflow:hidden;background:#fff}
.expect-head,.expect-row{display:grid;grid-template-columns:2fr 1fr 1fr 1.1fr;padding:9px 10px;font-size:10px}
.expect-head{background:#f8f9fa;color:#77848d;font-weight:800}.expect-row{border-top:1px solid #eef0f2;color:#34414b}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:6px}.dot-green{background:var(--green)}
.dot-red{background:var(--red)}.dot-amber{background:var(--amber)}
.release{padding:13px 15px;border-radius:8px;border:1px solid;margin-top:12px}
.release-pass{background:#eaf7f0;border-color:#b9ddcd}.release-review{background:#fff8e5;border-color:#ecd99d}
.release-block{background:#fff0f0;border-color:#e8bebe}
.release-title{font-weight:850;font-size:13px}.release-sub{font-size:10px;color:#66757e;margin-top:3px}
.stProgress>div>div>div{height:7px;border-radius:8px}
div[data-testid="stMetric"]{background:#fff;border:1px solid var(--line);border-radius:8px}
div[data-testid="stMetric"] label{font-size:10px}
.small{font-size:10px;color:#7b8891}
</style>
""",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
if "page" not in st.session_state:
    st.session_state.page = "Dashboard"
if "sources" not in st.session_state:
    st.session_state.sources = []
if "last_result" not in st.session_state:
    st.session_state.last_result = None
if "last_run_id" not in st.session_state:
    st.session_state.last_run_id = None

if st.session_state.last_result is None:
    latest = OUTPUT / "audit" / "latest.json"
    if latest.exists():
        try:
            st.session_state.last_result = json.loads(latest.read_text(encoding="utf-8"))
            st.session_state.last_run_id = st.session_state.last_result.get("run", {}).get("run_id")
        except Exception:
            pass

STAGES = [
    ("01", "Ingest"), ("02", "Bronze"), ("03", "Profile"), ("04", "Schema"),
    ("05", "Quality"), ("06", "Quarantine"), ("07", "Silver"), ("08", "Integrity"),
    ("09", "Gold"), ("10", "Audit"),
]

def results():
    return (st.session_state.last_result or {}).get("results", [])

def parse_numeric_rules(text):
    out=[]
    for line in text.splitlines():
        line=line.strip()
        if not line: continue
        parts=[x.strip() for x in line.split("|")]
        if len(parts)<2: raise ValueError("Numeric rule: column|min=0|max=100")
        item={"column":parts[0]}
        for p in parts[1:]:
            k,v=[x.strip() for x in p.split("=",1)]
            if k not in {"min","max"}: raise ValueError(f"Unknown numeric option: {k}")
            item[k]=float(v)
        out.append(item)
    return out

def parse_regex_rules(text):
    out=[]
    for line in text.splitlines():
        line=line.strip()
        if not line: continue
        p=line.split("|",1)
        if len(p)!=2: raise ValueError("Regex rule: column|regular_expression")
        out.append({"column":p[0].strip(),"pattern":p[1].strip()})
    return out

def contract_present(spec):
    return bool(spec.get("required_columns") and spec.get("unique_key") and spec.get("freshness_column"))

def status_info(r):
    s=r.get("status","REVIEW")
    if s=="PASS": return "SUCCESS","pill-ok"
    if s in {"REVIEW","SCHEMA_REVIEW","NO_CONTRACT"}: return s.replace("_"," "),"pill-warn"
    return s,"pill-err"

def pipeline_html(r):
    html='<div class="pipeline">'
    for i,(num,title) in enumerate(STAGES):
        cls="ok"; label="Complete"
        if not r: cls,label="warn","Waiting"
        else:
            if title=="Ingest": label="Source read"
            elif title=="Bronze": label="Raw preserved"
            elif title=="Profile": label=f"{int(r.get('rows_received',0)):,} rows"
            elif title=="Schema": cls="err" if r.get("schema_errors") else "ok"; label="Failed" if r.get("schema_errors") else "Passed"
            elif title=="Quality": cls="warn" if r.get("rows_quarantined",0) else "ok"; label=f"{len(r.get('quality_rules',{}).get('rules',[]))} rules"
            elif title=="Quarantine": cls="warn" if r.get("rows_quarantined",0) else "ok"; label=f"{int(r.get('rows_quarantined',0)):,} rows"
            elif title=="Silver": cls="err" if r.get("schema_errors") else "ok"; label="Blocked" if r.get("schema_errors") else f"{int(r.get('rows_valid',0)):,} valid"
            elif title=="Integrity":
                ri=float(r.get("metrics",{}).get("referential_integrity",100)); cls="warn" if ri<100 else "ok"; label=f"RI {ri:.1f}%"
            elif title=="Gold": cls="ok" if r.get("status")=="PASS" else "err"; label="Released" if r.get("status")=="PASS" else "Blocked"
            elif title=="Audit": label=f"{float(r.get('reliability_score_pct',0)):.2f}%"
        html+=f'<div class="node {cls}"><div class="node-num">{num}</div><div class="node-title">{title}</div><div class="node-state">{label}</div></div>'
        if i<len(STAGES)-1: html+='<div class="arrow">›</div>'
    return html+"</div>"

def layer_card(name,count,pct,state,detail):
    p=max(0,min(100,float(pct)))
    cls="pill-ok" if state=="ok" else "pill-warn" if state=="warn" else "pill-err"
    label="Active" if state=="ok" else "Review" if state=="warn" else "Blocked"
    return f'''<div class="layer-card"><div class="layer-head"><span class="layer-name">{name}</span><span class="layer-count">{int(count):,} rows</span></div>
    <div class="bar"><div style="width:{p}%"></div></div><div class="row"><span>Processing status</span><b>{p:.0f}%</b></div>
    <div class="row"><span>Control state</span><span class="pill {cls}">{label}</span></div><div class="row"><span>Details</span><span>{detail}</span></div></div>'''

def render_quality(r):
    m=r.get("metrics",{})
    rows=[
        ("Completeness",float(m.get("completeness",0))),
        ("Validity",float(m.get("validity",0))),
        ("Uniqueness",float(m.get("uniqueness",0))),
        ("Referential integrity",float(m.get("referential_integrity",0))),
        ("Freshness",float(m.get("freshness",0))),
    ]
    st.markdown('<div class="card"><div class="card-title">Data quality</div><div class="card-sub">Metric health from the executed batch</div>',unsafe_allow_html=True)
    for name,val in rows:
        st.progress(max(0,min(1,val/100)),text=f"{name}  ·  {val:.2f}%")
    st.markdown('</div>',unsafe_allow_html=True)

def render_expectations(r):
    reasons=r.get("failure_reason_counts",{})
    rules=r.get("quality_rules",{}).get("rules",[])
    st.markdown('<div class="card"><div class="card-title">Expectations</div><div class="card-sub">Configured controls and failed-record evidence</div><div class="expect"><div class="expect-head"><span>Name</span><span>Action</span><span>Fail %</span><span>Failed records</span></div>',unsafe_allow_html=True)
    total=max(1,int(r.get("rows_received",0)))
    for rule in rules[:12]:
        count=0
        for reason,n in reasons.items():
            if rule in reason: count+=int(n)
        fail=count/total*100
        action="FAIL" if count else "ALLOW"
        dot="dot-red" if count else "dot-green"
        st.markdown(f'<div class="expect-row"><span><i class="dot {dot}"></i>{rule}</span><span>{action}</span><span>{fail:.1f}%</span><span>{count:,}</span></div>',unsafe_allow_html=True)
    if not rules:
        st.markdown('<div class="expect-row"><span>No configured expectations</span><span>—</span><span>0%</span><span>0</span></div>',unsafe_allow_html=True)
    st.markdown('</div></div>',unsafe_allow_html=True)

def render_result(r):
    score=float(r.get("reliability_score_pct",0)); threshold=float(r.get("threshold_pct",0))
    total=int(r.get("rows_received",0)); valid=int(r.get("rows_valid",0)); bad=int(r.get("rows_quarantined",0))
    st.markdown('<div class="section">Pipeline run</div>',unsafe_allow_html=True)
    st.markdown(f'<div class="small">Source <b>{r.get("source","—")}</b> · Batch <code>{r.get("batch_id","—")}</code> · Fingerprint <code>{str(r.get("fingerprint",""))[:16]}</code></div>',unsafe_allow_html=True)
    st.markdown(pipeline_html(r),unsafe_allow_html=True)
    a,b,c,d=st.columns(4)
    a.metric("Rows received",f"{total:,}"); b.metric("Rows valid",f"{valid:,}"); c.metric("Quarantined",f"{bad:,}"); d.metric("Reliability",f"{score:.2f}%")
    q1,q2=st.columns([1,1])
    with q1: render_quality(r)
    with q2: render_expectations(r)
    if r.get("schema_errors"):
        st.markdown(f'<div class="release release-block"><div class="release-title">Gold blocked — schema contract failed</div><div class="release-sub">{"; ".join(r.get("schema_errors",[]))}</div></div>',unsafe_allow_html=True)
    elif r.get("status")=="PASS":
        st.markdown(f'<div class="release release-pass"><div class="release-title">Gold released — {score:.2f}% meets {threshold:.2f}% threshold</div><div class="release-sub">Trusted records are eligible for downstream consumption.</div></div>',unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="release release-review"><div class="release-title">Release requires review — {score:.2f}% vs {threshold:.2f}% threshold</div><div class="release-sub">The quality gate did not release Gold for this batch.</div></div>',unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown('<div class="brand"><span class="brand-mark">◈</span><span class="brand-name">Data Reliability</span><div class="brand-sub">ENTERPRISE DATA CONTROL PLANE</div></div>',unsafe_allow_html=True)
    st.markdown('<div class="nav-label">MONITOR</div>',unsafe_allow_html=True)
    nav=["Dashboard","Catalog","Issues","Collections","Deltas","Data Quality","Pipeline Runs"]
    for item in nav:
        if st.button(item,key="nav_"+item):
            st.session_state.page=item; st.rerun()
    st.markdown('<div class="nav-label">DATA</div>',unsafe_allow_html=True)
    for item in ["Data Ingestion","Layer Comparison","Audit & Lineage","Configuration"]:
        if st.button(item,key="nav_"+item):
            st.session_state.page=item; st.rerun()
    st.markdown('<div class="nav-label">ENGINE</div>',unsafe_allow_html=True)
    st.caption("Spark processing")
    st.caption("Contract-driven quality")
    st.caption("Bronze / Silver / Gold")

# ---------------------------------------------------------------------------
# Dashboard — visual composition based on the supplied enterprise monitoring
# references.
# ---------------------------------------------------------------------------
def dashboard():
    rs=results(); latest=rs[-1] if rs else None
    total=sum(int(x.get("rows_received",0)) for x in rs)
    bad=sum(int(x.get("rows_quarantined",0)) for x in rs)
    avg=sum(float(x.get("reliability_score_pct",0)) for x in rs)/len(rs) if rs else 0
    released=sum(1 for x in rs if x.get("status")=="PASS")
    st.markdown('<div class="top"><div><div class="h1">Dashboard</div><div class="sub">All important data reliability insights on a single screen</div></div><div class="toolbar"></div></div>',unsafe_allow_html=True)
    f1,f2,f3,f4,f5=st.columns([1,1,1,1,1.7])
    for col,label in zip([f1,f2,f3,f4],["30 days","60 days","90 days","All"]):
        col.button(label,key="period_"+label)
    f5.markdown('<div style="text-align:right;padding-top:5px;color:#7b8790;font-size:10px">Updated from latest local run</div>',unsafe_allow_html=True)
    m1,m2,m3,m4=st.columns(4)
    m1.markdown(f'<div class="metric metric-green"><div class="metric-label">Pipeline reliability</div><div class="metric-value">{avg:.0f}%</div><div class="metric-foot">{len(rs)} source runs in latest batch</div></div>',unsafe_allow_html=True)
    m2.markdown(f'<div class="metric metric-blue"><div class="metric-label">Records processed</div><div class="metric-value">{total:,}</div><div class="metric-foot">records received</div></div>',unsafe_allow_html=True)
    m3.markdown(f'<div class="metric metric-amber"><div class="metric-label">Records quarantined</div><div class="metric-value">{bad:,}</div><div class="metric-foot">isolated by quality rules</div></div>',unsafe_allow_html=True)
    m4.markdown(f'<div class="metric metric-green"><div class="metric-label">Released datasets</div><div class="metric-value">{released}</div><div class="metric-foot">Gold release gates passed</div></div>',unsafe_allow_html=True)

    st.markdown('<div class="section">Monitoring coverage</div>',unsafe_allow_html=True)
    c1,c2=st.columns([1.55,1])
    with c1:
        st.markdown('<div class="card"><div class="card-title">Data quality coverage</div><div class="card-sub">Coverage across the reliability dimensions</div>',unsafe_allow_html=True)
        dims=[("Completeness",30),("Validity",25),("Uniqueness",20),("Referential integrity",15),("Freshness",10)]
        for n,w in dims:
            st.markdown(f'<div class="row"><span>{n}</span><b>{w}% weight</b></div>',unsafe_allow_html=True)
            st.progress(w/30)
        st.markdown('</div>',unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="card"><div class="card-title">Pipeline reliability</div><div class="card-sub">Current release-readiness score</div>',unsafe_allow_html=True)
        st.metric("Reliability score",f"{avg:.2f}%")
        st.markdown(f'<div class="small">Threshold-based release gate · {released}/{len(rs) or 1} released</div>',unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

    if latest:
        st.markdown('<div class="section">Pipeline monitoring</div>',unsafe_allow_html=True)
        st.markdown('<div class="card"><div class="card-title">Latest execution</div><div class="card-sub">Source-to-Gold control path</div>',unsafe_allow_html=True)
        st.markdown(pipeline_html(latest),unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)
        st.markdown('<div class="section">Recent pipeline runs</div>',unsafe_allow_html=True)
        table=[]
        for r in rs:
            table.append({"Pipeline name":r.get("source"),"Rows":f'{int(r.get("rows_received",0)):,}',"Alerts":f'{int(r.get("rows_quarantined",0)):,}',"Reliability":f'{float(r.get("reliability_score_pct",0)):.2f}%',"Status":r.get("status"),"Duration":f'{float(r.get("duration_seconds",0)):.2f}s'})
        st.dataframe(table,use_container_width=True,hide_index=True)
    else:
        st.info("No run yet. Use Data Ingestion to register a source and execute the reliability pipeline.")

    st.markdown('<div class="section">Issue response</div>',unsafe_allow_html=True)
    i1,i2,i3=st.columns(3)
    i1.markdown(f'<div class="card"><div class="card-title">Open quality issues</div><div class="metric-value">{bad:,}</div><div class="small">records requiring investigation</div></div>',unsafe_allow_html=True)
    i2.markdown(f'<div class="card"><div class="card-title">Sources monitored</div><div class="metric-value">{len(rs)}</div><div class="small">source contracts evaluated</div></div>',unsafe_allow_html=True)
    i3.markdown(f'<div class="card"><div class="card-title">Audit records</div><div class="metric-value">{len(rs)}</div><div class="small">batch evidence available</div></div>',unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Data ingestion / registration
# ---------------------------------------------------------------------------
def ingestion():
    st.markdown('<div class="top"><div><div class="h1">Data Ingestion</div><div class="sub">Connect files, APIs, cloud objects and databases to the same reliability contract.</div></div></div>',unsafe_allow_html=True)
    st.markdown('<div class="card"><div class="card-title">Source connection</div><div class="card-sub">Credentials stay outside the repository; the adapter materializes a local batch for Spark.</div>',unsafe_allow_html=True)
    with st.form("source_form"):
        c1,c2=st.columns(2)
        name=c1.text_input("Source name *",placeholder="erp_orders")
        kind=c2.selectbox("Source type *",["File upload","API / HTTP","Amazon S3","Azure Blob / ADLS","Google Cloud Storage","Database","Local path"])
        uploaded=None; spec={"name":name.strip(),"type":kind}
        if kind=="File upload":
            uploaded=st.file_uploader("Dataset *",type=["csv","xlsx","xlsm","json","jsonl","parquet"],max_upload_size=4096)
        elif kind=="API / HTTP":
            a,b=st.columns([1,3]); spec["method"]=a.selectbox("Method",["GET","POST"]); spec["url"]=b.text_input("Endpoint URL *")
            a,b,c=st.columns(3); spec["auth_type"]=a.selectbox("Authentication",["None","Bearer","API key"]); spec["auth_env"]=b.text_input("Secret env var"); spec["auth_header"]=c.text_input("Header",value="Authorization" if spec["auth_type"]=="Bearer" else "X-API-Key")
            if spec["method"]=="POST":
                body=st.text_area("POST JSON body","{}",height=70)
                try: spec["body_json"]=json.loads(body)
                except Exception: st.warning("POST body must be valid JSON.")
        elif kind in {"Amazon S3","Azure Blob / ADLS","Google Cloud Storage"}:
            spec["uri"]=st.text_input("Object URI *",placeholder="s3://bucket/path/data.parquet or gs://bucket/path/data.parquet")
            spec["credential_env"]=st.text_input("Credential env var")
        elif kind=="Database":
            st.selectbox("Database",["PostgreSQL","MySQL","SQL Server","Other SQLAlchemy-compatible"])
            spec["connection_env"]=st.text_input("Connection-string env var *"); spec["query"]=st.text_area("Read-only SQL query *",placeholder="SELECT * FROM schema.table",height=70)
        else:
            spec["path"]=st.text_input("Local dataset path *",placeholder="C:/data/customer.parquet")
        st.markdown("#### Release contract")
        a,b,c=st.columns(3)
        spec["required_columns"]=[x.strip() for x in a.text_input("Required columns *",placeholder="id,name,updated_at").split(",") if x.strip()]
        spec["unique_key"]=b.text_input("Unique key / composite key *",placeholder="id or customer_id,order_id").strip()
        spec["freshness_column"]=c.text_input("Freshness column *",placeholder="updated_at").strip()
        a,b,c=st.columns(3)
        spec["freshness_sla_hours"]=a.number_input("Freshness SLA (hours)",1.0,8760.0,24.0,1.0)
        spec["pass_threshold"]=b.number_input("Reliability threshold (%)",1.0,100.0,95.0,.5)
        spec["reject_unexpected_columns"]=c.checkbox("Reject unexpected columns")
        with st.expander("Quality and relationship controls"):
            nt=st.text_area("Numeric rules",placeholder="amount|min=0\nquantity|min=1|max=100000",height=70)
            rt=st.text_area("Regex rules",placeholder="email|^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$",height=70)
            a,b,c=st.columns(3); ref_source=a.text_input("Reference source"); ref_col=b.text_input("Foreign-key column"); ref_key=c.text_input("Reference key")
            expected=st.text_area("Expected schema JSON",placeholder='{"id":"string","amount":"double"}',height=70)
        add=st.form_submit_button("＋ Add source",type="primary")
    st.markdown('</div>',unsafe_allow_html=True)

    if add:
        try:
            if not spec["name"]: raise ValueError("Source name is required.")
            if kind=="File upload" and uploaded is None: raise ValueError("Select a data file.")
            if kind=="API / HTTP" and not spec.get("url"): raise ValueError("Endpoint URL is required.")
            if kind in {"Amazon S3","Azure Blob / ADLS","Google Cloud Storage"} and not spec.get("uri"): raise ValueError("Object URI is required.")
            if kind=="Database" and (not spec.get("connection_env") or not spec.get("query")): raise ValueError("Database connection env var and query are required.")
            if kind=="Local path" and not spec.get("path"): raise ValueError("Local path is required.")
            if not contract_present(spec): raise ValueError("Required columns, unique key and freshness column are mandatory.")
            spec["numeric_rules"]=parse_numeric_rules(nt); spec["regex_rules"]=parse_regex_rules(rt)
            spec["expected_schema"]=json.loads(expected) if expected.strip() else {}
            if ref_source and ref_col and ref_key: spec["reference"]={"source":ref_source.strip(),"column":ref_col.strip(),"reference_column":ref_key.strip()}
            elif any([ref_source,ref_col,ref_key]): raise ValueError("Reference rule requires all three fields.")
            st.session_state.sources.append((spec,uploaded)); st.success(f"'{spec['name']}' added.")
        except Exception as e: st.error(str(e))

    if st.session_state.sources:
        st.markdown('<div class="section">Run plan</div>',unsafe_allow_html=True)
        for i,(spec,_) in enumerate(st.session_state.sources,1):
            st.markdown(f'<div class="card" style="margin-bottom:8px"><b>{i}. {spec["name"]}</b> <span class="pill pill-blue">{spec["type"]}</span><div class="small">Required: {", ".join(spec["required_columns"])} · Key: {spec["unique_key"]} · Freshness: {spec["freshness_column"]}</div></div>',unsafe_allow_html=True)
        a,b=st.columns(2); force=a.checkbox("Force reprocess")
        if b.button("Clear run plan"): st.session_state.sources=[]; st.rerun()
        if st.button("▶ Execute reliability pipeline",type="primary",use_container_width=True):
            INCOMING.mkdir(parents=True,exist_ok=True)
            for p in INCOMING.iterdir():
                if p.is_file(): p.unlink()
            monitor=st.empty(); progress=st.progress(0); events=[]; active={"index":-1,"started":None}
            def draw_live():
                current=active["index"]; html='<div class="card"><div class="card-title">Pipeline run monitor</div><div class="card-sub">Live stage execution</div><div class="pipeline">'
                for i,(num,title) in enumerate(STAGES):
                    cls="ok" if i<current else "warn" if i==current else "warn"; label="Complete" if i<current else "Running" if i==current else "Waiting"
                    html+=f'<div class="node {cls}"><div class="node-num">{num}</div><div class="node-title">{title}</div><div class="node-state">{label}</div></div>'
                    if i<len(STAGES)-1: html+='<div class="arrow">›</div>'
                html+='</div><div>'+''.join(f'<span class="event">{e}</span>' for e in events[-8:])+'</div></div>'; monitor.markdown(html,unsafe_allow_html=True)
            try:
                runtime=[]
                for spec,upload in st.session_state.sources:
                    materialized=materialize_source(spec,upload,INCOMING); item=dict(spec); item["materialized_path"]=str(materialized); item["format"]=Path(materialized).suffix.lstrip(".").lower(); runtime.append(item)
                manifest=BASE/"data"/"run_manifest.json"; run_id=uuid.uuid4().hex[:16]
                manifest.write_text(json.dumps({"run_id":run_id,"sources":runtime,"spark_master":"local[*]","shuffle_partitions":8},indent=2),encoding="utf-8")
                def on_event(ev):
                    idx=int(ev.get("stage_index",-1))
                    if ev.get("event")=="stage_start": active["index"]=idx; active["started"]=time.time()
                    msg=ev.get("message","")
                    if msg: events.append(f'{ev.get("stage","Stage")}: {msg}')
                    progress.progress(min(100,max(0,int(((idx+1)/len(STAGES))*100)))); draw_live()
                out=run_pipeline(manifest_path=str(manifest),output_root=str(OUTPUT),force=force,on_event=on_event)
                st.session_state.last_result={"run":{"run_id":run_id},"results":out.get("results",[])}; st.session_state.last_run_id=run_id
                progress.progress(100); st.success("Pipeline execution completed."); st.rerun()
            except Exception as e:
                monitor.error("Pipeline execution failed"); st.error(str(e)); st.code(traceback.format_exc())

# ---------------------------------------------------------------------------
# Other pages
# ---------------------------------------------------------------------------
def pipeline_runs():
    st.markdown('<div class="top"><div><div class="h1">Pipeline Runs</div><div class="sub">Execution logs, stage health and release decisions</div></div></div>',unsafe_allow_html=True)
    rs=results()
    if not rs: st.info("No pipeline run available."); return
    for r in rs:
        st.markdown(f'<div class="card"><div class="card-title">{r.get("source")} <span class="pill pill-blue">{r.get("status")}</span></div><div class="card-sub">Batch {r.get("batch_id","—")} · {r.get("processed_at","—")}</div>{pipeline_html(r)}</div>',unsafe_allow_html=True)
        render_result(r)

def data_quality():
    st.markdown('<div class="top"><div><div class="h1">Data Quality</div><div class="sub">Quality health, expectations and failed-record evidence</div></div></div>',unsafe_allow_html=True)
    rs=results()
    if not rs: st.info("Run a source to populate quality evidence."); return
    for r in rs: render_result(r)

def layer_comparison():
    st.markdown('<div class="top"><div><div class="h1">Layer Comparison</div><div class="sub">Bronze → Quarantine → Silver → Gold record movement</div></div></div>',unsafe_allow_html=True)
    rs=results()
    if not rs: st.info("Run a pipeline to compare layers."); return
    for r in rs:
        total=int(r.get("rows_received",0)); bad=int(r.get("rows_quarantined",0)); valid=int(r.get("rows_valid",0)); score=float(r.get("reliability_score_pct",0))
        st.markdown(f'<div class="card"><div class="card-title">{r.get("source")}</div><div class="card-sub">Batch {r.get("batch_id","—")}</div></div>',unsafe_allow_html=True)
        a,b,c,d=st.columns(4)
        a.markdown(layer_card("Bronze",total,100,"ok","raw records preserved"),unsafe_allow_html=True)
        b.markdown(layer_card("Quarantine",bad,bad/total*100 if total else 0,"warn" if bad else "ok","failed quality rules"),unsafe_allow_html=True)
        c.markdown(layer_card("Silver",valid,valid/total*100 if total else 0,"err" if r.get("schema_errors") else "ok","trusted records"),unsafe_allow_html=True)
        d.markdown(layer_card("Gold",valid if r.get("status")=="PASS" else 0,score,"ok" if r.get("status")=="PASS" else "err","release gated"),unsafe_allow_html=True)

def audit_lineage():
    st.markdown('<div class="top"><div><div class="h1">Audit & Lineage</div><div class="sub">Batch fingerprints, counts, metrics and release evidence</div></div></div>',unsafe_allow_html=True)
    history=OUTPUT/"audit"/"history.jsonl"; rows=[]
    if history.exists():
        for line in history.read_text(encoding="utf-8").splitlines():
            if line.strip(): rows.append(json.loads(line))
    if rows: st.dataframe(rows,use_container_width=True,hide_index=True)
    else: st.info("No audit records yet.")

def configuration():
    st.markdown('<div class="top"><div><div class="h1">Configuration</div><div class="sub">Reliability dimensions and release behavior</div></div></div>',unsafe_allow_html=True)
    a,b=st.columns(2)
    with a:
        st.markdown('<div class="card"><div class="card-title">Reliability model</div><div class="card-sub">Weighted score derived from executed controls</div>',unsafe_allow_html=True)
        for n,w in [("Completeness",30),("Validity",25),("Uniqueness",20),("Referential integrity",15),("Freshness",10)]:
            st.markdown(f'<div class="row"><span>{n}</span><b>{w}%</b></div>',unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)
    with b:
        st.markdown('<div class="card"><div class="card-title">Release behavior</div><div class="card-sub">Gold is gated by contract and reliability threshold</div><div class="row"><span>Schema failure</span><span class="pill pill-err">Gold blocked</span></div><div class="row"><span>Score below threshold</span><span class="pill pill-warn">Review</span></div><div class="row"><span>Score meets threshold</span><span class="pill pill-ok">Gold released</span></div></div>',unsafe_allow_html=True)

page=st.session_state.page
if page=="Dashboard": dashboard()
elif page=="Data Ingestion": ingestion()
elif page=="Pipeline Runs": pipeline_runs()
elif page=="Data Quality": data_quality()
elif page=="Layer Comparison": layer_comparison()
elif page=="Audit & Lineage": audit_lineage()
elif page=="Configuration": configuration()
else: dashboard()
