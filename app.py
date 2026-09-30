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
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------
# Visual system
# ---------------------------------------------------------------------
st.markdown(
    """
<style>
:root {
  --nav:#0f1720;
  --nav2:#17222d;
  --ink:#18222d;
  --muted:#71808d;
  --line:#e5ebef;
  --bg:#f5f7f9;
  --card:#ffffff;
  --blue:#2d6cdf;
  --blue-soft:#edf4ff;
  --green:#24a36b;
  --green-soft:#eaf8f1;
  --yellow:#f2c94c;
  --yellow-soft:#fff8df;
  --red:#e05252;
  --red-soft:#fff0f0;
  --purple:#7656d6;
  --shadow:0 5px 18px rgba(24,38,50,.055);
}
html,body,[class*="css"] { font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
.stApp { background:var(--bg); }
.block-container { max-width:1480px; padding:22px 34px 50px; }
[data-testid="stSidebar"] { background:linear-gradient(180deg,var(--nav),#101b25); }
[data-testid="stSidebar"] * { color:#dbe5ec; }
[data-testid="stSidebar"] .stButton button {
  width:100%; text-align:left; border:0; background:transparent; color:#c8d5de;
  border-radius:9px; padding:10px 12px; font-weight:650;
}
[data-testid="stSidebar"] .stButton button:hover { background:#1c2b38; color:white; }
.brand { padding:8px 6px 22px; border-bottom:1px solid #263542; margin-bottom:16px; }
.brand-mark {
  display:inline-flex; width:34px; height:34px; border-radius:10px; align-items:center;
  justify-content:center; background:#2d6cdf; color:white; font-weight:900; margin-right:9px;
}
.brand-name { color:#fff; font-size:19px; font-weight:850; vertical-align:middle; }
.brand-sub { color:#8fa3b2; font-size:11px; margin:6px 0 0 44px; }
.nav-label { color:#718897; font-size:10px; font-weight:850; letter-spacing:.12em; margin:18px 6px 7px; }
.topbar {
  display:flex; justify-content:space-between; align-items:center; gap:20px; margin-bottom:18px;
}
.page-title { font-size:28px; font-weight:850; color:var(--ink); margin:0; }
.page-sub { color:var(--muted); font-size:13px; margin-top:4px; }
.health {
  background:white; border:1px solid var(--line); border-radius:12px; padding:10px 15px;
  box-shadow:var(--shadow); font-size:12px; color:#52616d;
}
.health-dot { display:inline-block; width:9px; height:9px; border-radius:50%; background:var(--green); margin-right:7px; }
.metric-card {
  background:var(--card); border:1px solid var(--line); border-radius:13px; padding:16px 17px;
  box-shadow:var(--shadow); min-height:102px;
}
.metric-label { color:var(--muted); font-size:11px; font-weight:750; }
.metric-value { color:var(--ink); font-size:27px; font-weight:850; margin-top:5px; }
.metric-foot { color:#8a98a3; font-size:11px; margin-top:4px; }
.panel {
  background:var(--card); border:1px solid var(--line); border-radius:14px; padding:18px;
  box-shadow:var(--shadow); margin-top:16px;
}
.panel-title { color:var(--ink); font-size:16px; font-weight:850; }
.panel-sub { color:var(--muted); font-size:12px; margin-top:3px; margin-bottom:13px; }
.section-title { color:var(--ink); font-size:21px; font-weight:850; margin:24px 0 5px; }
.section-sub { color:var(--muted); font-size:13px; margin-bottom:10px; }
.pipeline {
  display:flex; align-items:stretch; overflow-x:auto; padding:7px 3px 8px;
}
.pipe-node {
  min-width:118px; border:1px solid #dfe6eb; border-radius:11px; background:#fbfcfd;
  padding:11px; position:relative;
}
.pipe-node:not(:last-child) { margin-right:28px; }
.pipe-node:not(:last-child):after {
  content:"→"; position:absolute; right:-23px; top:29px; color:#a5b1ba; font-size:18px; font-weight:800;
}
.pipe-num { font-size:9px; color:#95a3ad; font-weight:850; }
.pipe-title { font-size:12px; font-weight:850; color:var(--ink); margin-top:4px; }
.pipe-state { font-size:10px; margin-top:7px; color:#71808d; }
.pipe-node.ok { border-color:#bde4cf; background:#f5fcf8; }
.pipe-node.ok .pipe-state { color:var(--green); }
.pipe-node.warn { border-color:#efd98d; background:#fffaf0; }
.pipe-node.warn .pipe-state { color:#a97800; }
.pipe-node.err { border-color:#efb8b8; background:#fff5f5; }
.pipe-node.err .pipe-state { color:var(--red); }
.layer {
  border:1px solid var(--line); border-radius:12px; background:#fff; padding:15px;
  box-shadow:0 2px 9px rgba(20,35,50,.035); min-height:170px;
}
.layer-head { display:flex; justify-content:space-between; align-items:center; }
.layer-name { font-size:15px; font-weight:850; color:var(--ink); }
.layer-count { font-size:12px; color:#667680; }
.layer-bar { height:5px; background:#edf1f4; border-radius:20px; margin:14px 0 11px; overflow:hidden; }
.layer-bar > div { height:100%; border-radius:20px; background:var(--green); }
.layer-row { display:flex; justify-content:space-between; font-size:11px; margin:7px 0; color:#667681; }
.badge { display:inline-block; border-radius:999px; padding:4px 8px; font-size:10px; font-weight:800; }
.badge.ok { color:#17754d; background:var(--green-soft); }
.badge.warn { color:#916800; background:var(--yellow-soft); }
.badge.err { color:#9d3030; background:var(--red-soft); }
.table-wrap { overflow-x:auto; }
.small-note { font-size:11px; color:#7c8a95; }
.event {
  display:inline-block; border:1px solid #e1e8ed; border-radius:8px; background:#f8fafb;
  padding:7px 9px; margin:3px 4px 0 0; color:#556571; font-size:10px;
}
.release {
  border-radius:13px; padding:15px 17px; margin-top:14px; border:1px solid;
}
.release.pass { background:var(--green-soft); border-color:#bde4cf; }
.release.review { background:var(--yellow-soft); border-color:#ecd58b; }
.release.blocked { background:var(--red-soft); border-color:#efb8b8; }
.release-title { font-weight:850; font-size:15px; }
.kicker { font-size:10px; font-weight:850; letter-spacing:.08em; color:#7c8b95; text-transform:uppercase; }
.stButton>button { border-radius:9px; font-weight:750; }
div[data-testid="stMetric"] { background:white; border:1px solid var(--line); border-radius:13px; padding:12px; }
</style>
""",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------
# State
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------
with st.sidebar:
    st.markdown(
        """
        <div class="brand">
          <span class="brand-mark">◆</span><span class="brand-name">Data Reliability</span>
          <div class="brand-sub">ENTERPRISE DATA CONTROL PLANE</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="nav-label">WORKSPACE</div>', unsafe_allow_html=True)
    for item in ["Dashboard", "Data Ingestion", "Pipeline Runs", "Data Quality", "Layer Comparison", "Audit & Lineage", "Configuration"]:
        if st.button(item, key=f"nav_{item}"):
            st.session_state.page = item
            st.rerun()

    st.markdown('<div class="nav-label">ENGINE</div>', unsafe_allow_html=True)
    st.caption("Spark reliability engine")
    st.caption("Source-agnostic adapters")
    st.caption("Contract-driven release")

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
STAGES = [
    ("01", "Ingest"),
    ("02", "Bronze"),
    ("03", "Profile"),
    ("04", "Schema"),
    ("05", "Quality"),
    ("06", "Quarantine"),
    ("07", "Silver"),
    ("08", "Integrity"),
    ("09", "Gold"),
    ("10", "Audit"),
]


def parse_numeric_rules(text: str) -> list[dict]:
    rules = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [x.strip() for x in line.split("|")]
        if len(parts) < 2:
            raise ValueError("Numeric rule format: column|min=0|max=100")
        rule = {"column": parts[0]}
        for item in parts[1:]:
            k, v = [x.strip() for x in item.split("=", 1)]
            if k not in {"min", "max"}:
                raise ValueError(f"Unknown numeric rule option: {k}")
            rule[k] = float(v)
        rules.append(rule)
    return rules


def parse_regex_rules(text: str) -> list[dict]:
    rules = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|", 1)
        if len(parts) != 2:
            raise ValueError("Regex rule format: column|regular_expression")
        rules.append({"column": parts[0].strip(), "pattern": parts[1].strip()})
    return rules


def contract_present(spec: dict) -> bool:
    return bool(spec.get("required_columns") and spec.get("unique_key") and spec.get("freshness_column"))


def latest_results() -> list[dict]:
    data = st.session_state.last_result or {}
    return data.get("results", [])


def source_status(result: dict | None) -> tuple[str, str]:
    if not result:
        return "WAITING", "warn"
    status = result.get("status", "REVIEW")
    if status == "PASS":
        return "RELEASED", "ok"
    if status in {"SCHEMA_REVIEW", "REVIEW", "NO_CONTRACT"}:
        return status.replace("_", " "), "warn"
    return status, "err"


def pipeline_html(result: dict | None) -> str:
    html = '<div class="pipeline">'
    for num, title in STAGES:
        state = "ok"
        label = "Complete"
        if not result:
            state, label = "warn", "Waiting"
        else:
            if title == "Ingest":
                label = "Source read"
            elif title == "Bronze":
                label = "Raw preserved"
            elif title == "Profile":
                label = f"{result.get('rows_received', 0):,} rows"
            elif title == "Schema":
                state = "err" if result.get("schema_errors") else "ok"
                label = "Failed" if result.get("schema_errors") else "Passed"
            elif title == "Quality":
                state = "warn" if result.get("rows_quarantined", 0) else "ok"
                label = f"{len(result.get('quality_rules', {}).get('rules', []))} rules"
            elif title == "Quarantine":
                state = "warn" if result.get("rows_quarantined", 0) else "ok"
                label = f"{result.get('rows_quarantined', 0):,} rows"
            elif title == "Silver":
                state = "err" if result.get("schema_errors") else "ok"
                label = "Blocked" if result.get("schema_errors") else f"{result.get('rows_valid', 0):,} valid"
            elif title == "Integrity":
                ri = result.get("metrics", {}).get("referential_integrity", 100)
                state = "warn" if ri < 100 else "ok"
                label = f"RI {ri:.1f}%"
            elif title == "Gold":
                state = "ok" if result.get("status") == "PASS" else "err"
                label = "Released" if result.get("status") == "PASS" else "Blocked"
            elif title == "Audit":
                label = f"{result.get('reliability_score_pct', 0):.2f}%"
        html += (
            f'<div class="pipe-node {state}"><div class="pipe-num">{num}</div>'
            f'<div class="pipe-title">{title}</div><div class="pipe-state">{label}</div></div>'
        )
    html += "</div>"
    return html


def layer_card(name: str, count: int, pct: float, state: str, detail: str) -> str:
    badge = "ok" if state == "ok" else "warn" if state == "warn" else "err"
    label = {"ok": "Active", "warn": "Review", "err": "Blocked"}[badge]
    return f"""
    <div class="layer">
      <div class="layer-head">
        <div class="layer-name">{name}</div>
        <div class="layer-count">{count:,} rows</div>
      </div>
      <div class="layer-bar"><div style="width:{max(0,min(100,pct))}%"></div></div>
      <div class="layer-row"><span>Processing status</span><b>{pct:.0f}%</b></div>
      <div class="layer-row"><span>Control state</span><span class="badge {badge}">{label}</span></div>
      <div class="layer-row"><span>Details</span><span>{detail}</span></div>
    </div>
    """


def render_result(result: dict):
    status, status_class = source_status(result)
    metrics = result.get("metrics", {})
    total = int(result.get("rows_received", 0))
    valid = int(result.get("rows_valid", 0))
    bad = int(result.get("rows_quarantined", 0))
    score = float(result.get("reliability_score_pct", 0))
    threshold = float(result.get("threshold_pct", 0))

    st.markdown('<div class="section-title">Pipeline execution</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="section-sub">Source: <b>{result.get("source","unknown")}</b> · '
        f'Batch: <code>{result.get("batch_id","")}</code> · '
        f'Fingerprint: <code>{result.get("fingerprint","")[:16]}</code></div>',
        unsafe_allow_html=True,
    )
    st.markdown(pipeline_html(result), unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total records", f"{total:,}")
    c2.metric("Valid records", f"{valid:,}")
    c3.metric("Quarantined", f"{bad:,}")
    c4.metric("Reliability score", f"{score:.2f}%")

    a, b = st.columns([1.45, 1])
    with a:
        st.markdown(
            '<div class="panel"><div class="panel-title">Data quality</div>'
            '<div class="panel-sub">Actual checks from the executed batch</div>',
            unsafe_allow_html=True,
        )
        rows = [
            ("Completeness", metrics.get("completeness", 0)),
            ("Validity", metrics.get("validity", 0)),
            ("Uniqueness", metrics.get("uniqueness", 0)),
            ("Referential integrity", metrics.get("referential_integrity", 0)),
            ("Freshness", metrics.get("freshness", 0)),
        ]
        for name, value in rows:
            st.progress(max(0.0, min(1.0, float(value) / 100.0)), text=f"{name} · {float(value):.2f}%")
        st.markdown("</div>", unsafe_allow_html=True)

    with b:
        reasons = result.get("failure_reason_counts", {})
        st.markdown(
            '<div class="panel"><div class="panel-title">Failure distribution</div>'
            '<div class="panel-sub">Why records were quarantined</div>',
            unsafe_allow_html=True,
        )
        if reasons:
            for reason, count in list(reasons.items())[:8]:
                st.markdown(f'<div class="layer-row"><span>{reason}</span><b>{count:,}</b></div>', unsafe_allow_html=True)
        else:
            st.success("No quarantined records.")
        st.markdown("</div>", unsafe_allow_html=True)

    state = "pass" if status == "RELEASED" else "blocked" if result.get("schema_errors") else "review"
    headline = (
        f"Gold released · reliability {score:.2f}% ≥ threshold {threshold:.2f}%"
        if state == "pass"
        else "Gold blocked · schema contract failed"
        if result.get("schema_errors")
        else f"Release requires review · reliability {score:.2f}% vs threshold {threshold:.2f}%"
    )
    st.markdown(
        f'<div class="release {state}"><div class="release-title">{headline}</div>'
        f'<div class="small-note">Release decision: {result.get("status","UNKNOWN")} · '
        f'processed in {result.get("duration_seconds",0):.2f}s</div></div>',
        unsafe_allow_html=True,
    )

    st.markdown('<div class="section-title">Data layers</div>', unsafe_allow_html=True)
    p1, p2, p3, p4 = st.columns(4)
    p1.markdown(layer_card("Bronze", total, 100, "ok", "raw batch preserved"), unsafe_allow_html=True)
    q_pct = (bad / total * 100) if total else 0
    p2.markdown(layer_card("Quarantine", bad, q_pct, "warn" if bad else "ok", "failed quality rules"), unsafe_allow_html=True)
    s_pct = (valid / total * 100) if total else 0
    p3.markdown(layer_card("Silver", valid, s_pct, "err" if result.get("schema_errors") else "ok", "trusted records"), unsafe_allow_html=True)
    g_state = "ok" if result.get("status") == "PASS" else "err"
    p4.markdown(layer_card("Gold", valid if g_state == "ok" else 0, score, g_state, "release-gated dataset"), unsafe_allow_html=True)


# ---------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------
def dashboard():
    results = latest_results()
    latest = results[-1] if results else None
    avg_score = sum(float(x.get("reliability_score_pct", 0)) for x in results) / len(results) if results else 0
    total_rows = sum(int(x.get("rows_received", 0)) for x in results)
    total_bad = sum(int(x.get("rows_quarantined", 0)) for x in results)
    released = sum(1 for x in results if x.get("status") == "PASS")

    st.markdown(
        f"""
        <div class="topbar">
          <div><div class="page-title">Dashboard</div>
          <div class="page-sub">Enterprise data reliability, pipeline execution and release readiness</div></div>
          <div class="health"><span class="health-dot"></span><b>Control plane healthy</b><br>
          <span style="margin-left:16px">Local Spark engine · {len(results)} source(s) in latest run</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.markdown(f'<div class="metric-card"><div class="metric-label">RELIABILITY SCORE</div><div class="metric-value">{avg_score:.2f}%</div><div class="metric-foot">latest completed run</div></div>', unsafe_allow_html=True)
    with m2:
        st.markdown(f'<div class="metric-card"><div class="metric-label">TOTAL RECORDS</div><div class="metric-value">{total_rows:,}</div><div class="metric-foot">processed in latest run</div></div>', unsafe_allow_html=True)
    with m3:
        st.markdown(f'<div class="metric-card"><div class="metric-label">QUARANTINED</div><div class="metric-value">{total_bad:,}</div><div class="metric-foot">isolated for review</div></div>', unsafe_allow_html=True)
    with m4:
        st.markdown(f'<div class="metric-card"><div class="metric-label">RELEASED DATASETS</div><div class="metric-value">{released}</div><div class="metric-foot">Gold release gates passed</div></div>', unsafe_allow_html=True)
    with m5:
        st.markdown(f'<div class="metric-card"><div class="metric-label">REGISTERED SOURCES</div><div class="metric-value">{len(st.session_state.sources)}</div><div class="metric-foot">current run plan</div></div>', unsafe_allow_html=True)

    st.markdown('<div class="panel"><div class="panel-title">Pipeline overview</div><div class="panel-sub">Ingest → Bronze → Profile → Quality → Quarantine → Silver → Integrity → Gold → Audit</div>', unsafe_allow_html=True)
    if latest:
        st.markdown(pipeline_html(latest), unsafe_allow_html=True)
    else:
        st.info("No completed run yet. Register a source and execute the pipeline.")
    st.markdown("</div>", unsafe_allow_html=True)

    if results:
        st.markdown('<div class="panel"><div class="panel-title">Recent pipeline runs</div><div class="panel-sub">Source-level release and reliability evidence</div>', unsafe_allow_html=True)
        table = []
        for r in results:
            table.append({
                "Source": r.get("source"),
                "Rows": r.get("rows_received"),
                "Valid": r.get("rows_valid"),
                "Quarantined": r.get("rows_quarantined"),
                "Reliability": f"{r.get('reliability_score_pct',0):.2f}%",
                "Status": r.get("status"),
                "Duration": f"{r.get('duration_seconds',0):.2f}s",
            })
        st.dataframe(table, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    if latest:
        render_result(latest)


# ---------------------------------------------------------------------
# Source registration
# ---------------------------------------------------------------------
def ingestion():
    st.markdown('<div class="topbar"><div><div class="page-title">Data Ingestion</div><div class="page-sub">Register heterogeneous sources against the same reliability contract.</div></div></div>', unsafe_allow_html=True)

    st.markdown('<div class="panel"><div class="panel-title">Source connection</div><div class="panel-sub">The adapter acquires the data; the Spark engine applies the common controls.</div>', unsafe_allow_html=True)

    with st.form("source_form", clear_on_submit=False):
        c1, c2 = st.columns(2)
        name = c1.text_input("Source name *", placeholder="erp_orders")
        kind = c2.selectbox("Source type *", [
            "File upload", "API / HTTP", "Amazon S3", "Azure Blob / ADLS",
            "Google Cloud Storage", "Database", "Local path"
        ])

        uploaded = None
        spec = {"name": name.strip(), "type": kind}

        if kind == "File upload":
            uploaded = st.file_uploader("Dataset *", type=["csv", "xlsx", "xlsm", "json", "jsonl", "parquet"], max_upload_size=4096)
            st.caption("CSV · Excel · JSON · JSONL · Parquet")
        elif kind == "API / HTTP":
            a1, a2 = st.columns([1, 3])
            spec["method"] = a1.selectbox("Method", ["GET", "POST"])
            spec["url"] = a2.text_input("Endpoint URL *", placeholder="https://api.example.com/v1/orders")
            a1, a2, a3 = st.columns([1, 1, 2])
            spec["auth_type"] = a1.selectbox("Authentication", ["None", "Bearer", "API key"])
            spec["auth_env"] = a2.text_input("Secret env var", placeholder="API_TOKEN")
            spec["auth_header"] = a3.text_input("Header", value="Authorization" if spec["auth_type"] == "Bearer" else "X-API-Key")
            if spec["method"] == "POST":
                body = st.text_area("POST JSON body", value="{}", height=80)
                try:
                    spec["body_json"] = json.loads(body)
                except Exception:
                    st.warning("POST body must be valid JSON.")
        elif kind == "Amazon S3":
            spec["uri"] = st.text_input("S3 object URI *", placeholder="s3://bucket/path/data.parquet")
        elif kind == "Azure Blob / ADLS":
            spec["uri"] = st.text_input("Azure object URI *", placeholder="https://account.blob.core.windows.net/container/data.parquet")
            spec["credential_env"] = st.text_input("Credential env var", value="AZURE_STORAGE_CONNECTION_STRING")
        elif kind == "Google Cloud Storage":
            spec["uri"] = st.text_input("GCS object URI *", placeholder="gs://bucket/path/data.parquet")
            spec["credential_env"] = st.text_input("Credential env var", value="GOOGLE_APPLICATION_CREDENTIALS")
        elif kind == "Database":
            st.selectbox("Database", ["PostgreSQL", "MySQL", "SQL Server", "Other SQLAlchemy-compatible"])
            spec["connection_env"] = st.text_input("Connection-string env var *", placeholder="ERP_DB_URL")
            spec["query"] = st.text_area("Read-only SQL query *", placeholder="SELECT * FROM schema.table", height=90)
        else:
            spec["path"] = st.text_input("Local dataset path *", placeholder="C:/data/customer.parquet")

        st.markdown("#### Release contract")
        c1, c2, c3 = st.columns(3)
        spec["required_columns"] = [x.strip() for x in c1.text_input("Required columns *", placeholder="id,name,updated_at").split(",") if x.strip()]
        spec["unique_key"] = c2.text_input("Unique key / composite key *", placeholder="id or customer_id,order_id").strip()
        spec["freshness_column"] = c3.text_input("Freshness column *", placeholder="updated_at").strip()

        c1, c2, c3 = st.columns(3)
        spec["freshness_sla_hours"] = c1.number_input("Freshness SLA (hours)", 1.0, 8760.0, 24.0, 1.0)
        spec["pass_threshold"] = c2.number_input("Reliability threshold (%)", 1.0, 100.0, 95.0, .5)
        spec["reject_unexpected_columns"] = c3.checkbox("Reject unexpected columns", value=False)

        with st.expander("Optional quality and relationship controls"):
            ntext = st.text_area("Numeric rules", placeholder="amount|min=0\nquantity|min=1|max=100000", height=75)
            rtext = st.text_area("Regex rules", placeholder="email|^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", height=75)
            x1, x2, x3 = st.columns(3)
            ref_source = x1.text_input("Reference source")
            ref_col = x2.text_input("Foreign-key column")
            ref_key = x3.text_input("Reference key")
            expected = st.text_area("Expected schema JSON", placeholder='{"id":"string","amount":"double"}', height=75)

        add = st.form_submit_button("＋ Add source", type="primary")

    if add:
        try:
            if not spec["name"]:
                raise ValueError("Source name is required.")
            if kind == "File upload" and uploaded is None:
                raise ValueError("Select a data file.")
            if kind == "API / HTTP" and not spec.get("url"):
                raise ValueError("Endpoint URL is required.")
            if kind in {"Amazon S3", "Azure Blob / ADLS", "Google Cloud Storage"} and not spec.get("uri"):
                raise ValueError("Cloud object URI is required.")
            if kind == "Database" and (not spec.get("connection_env") or not spec.get("query")):
                raise ValueError("Database connection env var and query are required.")
            if kind == "Local path" and not spec.get("path"):
                raise ValueError("Local path is required.")
            if not contract_present(spec):
                raise ValueError("Required columns, unique key and freshness column are mandatory.")
            spec["numeric_rules"] = parse_numeric_rules(ntext)
            spec["regex_rules"] = parse_regex_rules(rtext)
            spec["expected_schema"] = json.loads(expected) if expected.strip() else {}
            if ref_source and ref_col and ref_key:
                spec["reference"] = {"source": ref_source.strip(), "column": ref_col.strip(), "reference_column": ref_key.strip()}
            elif any([ref_source, ref_col, ref_key]):
                raise ValueError("Reference rule requires all three fields.")
            if kind == "API / HTTP" and spec.get("auth_type") != "None" and not spec.get("auth_env"):
                raise ValueError("Authenticated APIs require a secret environment-variable name.")
            st.session_state.sources.append((spec, uploaded))
            st.success(f"'{spec['name']}' added to the run plan.")
        except Exception as exc:
            st.error(str(exc))

    st.markdown("</div>", unsafe_allow_html=True)

    if st.session_state.sources:
        st.markdown('<div class="section-title">Run plan</div>', unsafe_allow_html=True)
        for idx, (spec, _) in enumerate(st.session_state.sources, 1):
            st.markdown(
                f'<div class="panel"><div class="panel-title">{idx}. {spec["name"]} '
                f'<span class="badge ok">{spec["type"]}</span></div>'
                f'<div class="panel-sub">Required: {", ".join(spec["required_columns"])} · '
                f'Key: {spec["unique_key"]} · Freshness: {spec["freshness_column"]} ≤ {spec["freshness_sla_hours"]}h</div></div>',
                unsafe_allow_html=True,
            )

        b1, b2 = st.columns([1, 1])
        force = b1.checkbox("Force reprocess", value=False)
        if b2.button("Clear run plan"):
            st.session_state.sources = []
            st.rerun()

        if st.button("▶ Execute reliability pipeline", type="primary", use_container_width=True):
            INCOMING.mkdir(parents=True, exist_ok=True)
            for p in INCOMING.iterdir():
                if p.is_file():
                    p.unlink()

            monitor = st.empty()
            progress = st.progress(0)
            events = []
            stage_names = {i: name for i, (_, name) in enumerate(STAGES)}
            started = time.time()
            stage_durations = {}
            active_stage = {"index": -1, "started": None}

            def draw_live():
                current = active_stage["index"]
                html = '<div class="panel"><div class="panel-title">Live pipeline monitor</div><div class="panel-sub">Spark reliability engine · execution in progress</div><div class="pipeline">'
                for i, (num, title) in enumerate(STAGES):
                    if i < current:
                        cls, label = "ok", "Complete"
                    elif i == current:
                        cls, label = "warn", "Running"
                    else:
                        cls, label = "warn", "Waiting"
                    html += f'<div class="pipe-node {cls}"><div class="pipe-num">{num}</div><div class="pipe-title">{title}</div><div class="pipe-state">{label}</div></div>'
                html += '</div><div style="margin-top:8px">'
                for event in events[-6:]:
                    html += f'<span class="event">{event}</span>'
                html += '</div></div>'
                monitor.markdown(html, unsafe_allow_html=True)

            try:
                runtime = []
                for spec, upload in st.session_state.sources:
                    materialized = materialize_source(spec, upload, INCOMING)
                    item = dict(spec)
                    item["materialized_path"] = str(materialized)
                    item["format"] = Path(materialized).suffix.lstrip(".").lower()
                    runtime.append(item)

                manifest = BASE / "data" / "run_manifest.json"
                run_id = uuid.uuid4().hex[:16]
                manifest.write_text(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "sources": runtime,
                            "spark_master": "local[*]",
                            "shuffle_partitions": 8,
                        },
                        indent=2,
                    ),
                    encoding="utf-8",
                )

                def on_event(ev):
                    idx = int(ev.get("stage_index", -1))
                    if ev.get("event") == "stage_start":
                        if active_stage["started"] is not None and active_stage["index"] >= 0:
                            stage_durations[active_stage["index"]] = time.time() - active_stage["started"]
                        active_stage["index"] = idx
                        active_stage["started"] = time.time()
                    elif ev.get("event") in {"stage_done", "stage_error"}:
                        if idx >= 0 and active_stage["index"] == idx and active_stage["started"] is not None:
                            stage_durations[idx] = time.time() - active_stage["started"]
                    msg = ev.get("message", "")
                    if msg:
                        events.append(f'{ev.get("stage","Stage")}: {msg}')
                    progress.progress(min(100, max(0, int(((idx + 1) / len(STAGES)) * 100))))
                    draw_live()

                result = run_pipeline(
                    manifest_path=str(manifest),
                    output_root=str(OUTPUT),
                    force=force,
                    on_event=on_event,
                )
                if active_stage["started"] is not None and active_stage["index"] >= 0:
                    stage_durations[active_stage["index"]] = time.time() - active_stage["started"]
                st.session_state.last_result = {"run": {"run_id": run_id}, "results": result.get("results", [])}
                st.session_state.last_run_id = run_id
                progress.progress(100)
                st.success("Pipeline execution completed.")
                st.rerun()
            except Exception as exc:
                monitor.error("Pipeline execution failed")
                st.error(str(exc))
                st.code(traceback.format_exc())

# ---------------------------------------------------------------------
# Other pages
# ---------------------------------------------------------------------
def pipeline_runs():
    st.markdown('<div class="topbar"><div><div class="page-title">Pipeline Runs</div><div class="page-sub">Execution status, release gates and batch evidence.</div></div></div>', unsafe_allow_html=True)
    results = latest_results()
    if not results:
        st.info("No pipeline run available.")
        return
    for result in results:
        status, cls = source_status(result)
        st.markdown(
            f'<div class="panel"><div class="panel-title">{result.get("source")} '
            f'<span class="badge {cls}">{status}</span></div>'
            f'<div class="panel-sub">Batch {result.get("batch_id","")} · {result.get("processed_at","")}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(pipeline_html(result), unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)


def data_quality():
    st.markdown('<div class="topbar"><div><div class="page-title">Data Quality</div><div class="page-sub">Quality dimensions, failed records and rule evidence.</div></div></div>', unsafe_allow_html=True)
    results = latest_results()
    if not results:
        st.info("Run a source to populate quality evidence.")
        return
    for result in results:
        render_result(result)


def layer_comparison():
    st.markdown('<div class="topbar"><div><div class="page-title">Layer Comparison</div><div class="page-sub">Compare the record movement across Raw Bronze, Quarantine, Silver and Gold.</div></div></div>', unsafe_allow_html=True)
    results = latest_results()
    if not results:
        st.info("Run a pipeline to compare layers.")
        return
    for result in results:
        total = int(result.get("rows_received", 0))
        bad = int(result.get("rows_quarantined", 0))
        valid = int(result.get("rows_valid", 0))
        score = float(result.get("reliability_score_pct", 0))
        st.markdown(f'<div class="panel"><div class="panel-title">{result.get("source")}</div><div class="panel-sub">Batch {result.get("batch_id","")}</div>', unsafe_allow_html=True)
        cols = st.columns(4)
        cols[0].markdown(layer_card("Bronze", total, 100, "ok", "all received records"), unsafe_allow_html=True)
        cols[1].markdown(layer_card("Quarantine", bad, (bad / total * 100) if total else 0, "warn" if bad else "ok", "failed controls"), unsafe_allow_html=True)
        cols[2].markdown(layer_card("Silver", valid, (valid / total * 100) if total else 0, "err" if result.get("schema_errors") else "ok", "trusted records"), unsafe_allow_html=True)
        cols[3].markdown(layer_card("Gold", valid if result.get("status") == "PASS" else 0, score, "ok" if result.get("status") == "PASS" else "err", "release gated"), unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)


def audit_lineage():
    st.markdown('<div class="topbar"><div><div class="page-title">Audit & Lineage</div><div class="page-sub">Batch fingerprints, row counts, quality metrics and release decisions.</div></div></div>', unsafe_allow_html=True)
    history = OUTPUT / "audit" / "history.jsonl"
    rows = []
    if history.exists():
        for line in history.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
    if not rows:
        st.info("No audit records yet.")
        return
    st.dataframe(rows, use_container_width=True, hide_index=True)


def configuration():
    st.markdown('<div class="topbar"><div><div class="page-title">Configuration</div><div class="page-sub">Reliability dimensions and release behavior.</div></div></div>', unsafe_allow_html=True)
    st.markdown(
        """
        <div class="panel">
          <div class="panel-title">Reliability model</div>
          <div class="panel-sub">The score is derived from executed quality dimensions.</div>
          <div class="layer-row"><span>Completeness</span><b>30%</b></div>
          <div class="layer-row"><span>Validity</span><b>25%</b></div>
          <div class="layer-row"><span>Uniqueness</span><b>20%</b></div>
          <div class="layer-row"><span>Referential integrity</span><b>15%</b></div>
          <div class="layer-row"><span>Freshness</span><b>10%</b></div>
        </div>
        <div class="panel">
          <div class="panel-title">Release behavior</div>
          <div class="panel-sub">Gold is written only when the configured reliability gate passes.</div>
          <div class="layer-row"><span>Schema contract failure</span><span class="badge err">Gold blocked</span></div>
          <div class="layer-row"><span>Score below threshold</span><span class="badge warn">Review</span></div>
          <div class="layer-row"><span>Score meets threshold</span><span class="badge ok">Gold released</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


page = st.session_state.page
if page == "Dashboard":
    dashboard()
elif page == "Data Ingestion":
    ingestion()
elif page == "Pipeline Runs":
    pipeline_runs()
elif page == "Data Quality":
    data_quality()
elif page == "Layer Comparison":
    layer_comparison()
elif page == "Audit & Lineage":
    audit_lineage()
else:
    configuration()
