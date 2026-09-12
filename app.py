
import json
from pathlib import Path
import pandas as pd
import streamlit as st

BASE = Path(__file__).resolve().parent
OUT = BASE / "data" / "output"
st.set_page_config(page_title="Neoware | Data Reliability Case Study", page_icon="◈", layout="wide")

st.markdown("""
<style>
.block-container{padding:1.4rem 2.5rem 3rem;max-width:1500px}
.hero{padding:32px 34px;border-radius:22px;background:linear-gradient(135deg,#071d36,#0c3c55 58%,#12647b);color:#fff;border:1px solid #1d7188;box-shadow:0 14px 40px rgba(5,28,50,.18)}
.hero .eyebrow{font-size:.78rem;letter-spacing:.14em;text-transform:uppercase;opacity:.72;font-weight:700}
.hero h1{font-size:2.65rem;line-height:1.05;margin:.35rem 0 .7rem}
.hero p{font-size:1.03rem;line-height:1.6;max-width:1050px;opacity:.92}
.badge{display:inline-block;padding:5px 10px;border-radius:999px;background:rgba(255,255,255,.12);margin-right:7px;font-size:.78rem}
.card{padding:20px;border:1px solid #d9e4eb;border-radius:16px;background:#fff;box-shadow:0 5px 20px rgba(10,35,55,.05);height:100%}
.step{padding:18px;border-radius:15px;background:#f5f8fa;border:1px solid #dbe6ec;height:100%}
.stepnum{font-weight:800;font-size:.8rem;color:#12647b;letter-spacing:.08em}
.flow{padding:16px;border-radius:14px;background:#071d36;color:white;text-align:center;font-weight:700;letter-spacing:.02em}
.evidence{padding:17px 19px;border-left:5px solid #12647b;background:#f4f8fa;border-radius:10px;margin:9px 0}
</style>
""", unsafe_allow_html=True)

def load(name): return pd.read_csv(OUT/name)
summary=json.loads((OUT/"pipeline_summary.json").read_text())
health=load("source_health.csv"); recon=load("reconciliation_report.csv"); issues=load("quality_issues.csv"); trusted=load("trusted_orders.csv")

with st.sidebar:
    st.markdown("## ◈ Case Study")
    st.caption("Neoware-inspired • Independent POC")
    page=st.radio("Explore the engineering",[
        "01 — Executive Story","02 — Why This Problem","03 — Evidence & Problem Mapping",
        "04 — Pipeline Walkthrough","05 — Source Health","06 — Reconciliation",
        "07 — Data Quality","08 — Trusted Data","09 — Architecture",
        "10 — Code & Engineering","11 — Case Study Summary"])
    st.divider()
    st.caption("Synthetic data only")
    st.caption("No Neoware internal data, code, infrastructure or proprietary accelerator is used.")

if page=="01 — Executive Story":
    st.markdown("""<div class="hero"><div class="eyebrow">Independent portfolio engineering case study</div>
    <h1>Enterprise Data Reliability<br>& Reconciliation Platform</h1>
    <p>I studied a publicly visible enterprise Data Engineering problem and built a runnable reliability layer that turns fragmented source data into validated, reconciled and trusted data products — with an explicit path toward analytics and AI.</p>
    <span class="badge">Python</span><span class="badge">Pandas</span><span class="badge">Data Quality</span><span class="badge">Reconciliation</span><span class="badge">PySpark Mapping</span></div>""",unsafe_allow_html=True)
    st.markdown('<div class="flow">MULTIPLE SOURCES → INGEST → STANDARDIZE → VALIDATE → RECONCILE → QUARANTINE → TRUSTED DATA → BI / AI</div>',unsafe_allow_html=True)
    cols=st.columns(5)
    for c,(label,val) in zip(cols,[("Raw records",summary["raw_records"]),("Trusted orders",summary["trusted_orders"]),("Match rate",f'{summary["reconciliation_match_rate_pct"]}%'),("Quality score",f'{summary["quality_score_pct"]}%'),("Issues surfaced",summary["quality_issues"])]): c.metric(label,val)
    a,b,c=st.columns(3)
    for col,title,body in [(a,"Make data observable","Failures are retained with source, key, rule and severity."),(b,"Prove consistency","Reconciliation turns cross-system disagreement into a measurable signal."),(c,"Protect downstream users","Only reconciled records are promoted to trusted data.")]:
        col.markdown(f'<div class="card"><h3>{title}</h3><p>{body}</p></div>',unsafe_allow_html=True)

elif page=="02 — Why This Problem":
    st.markdown("## Why I built this case study")
    st.markdown("""<div class="card"><h3>Not another generic ETL demo</h3>
    <p>The starting question was: <b>what happens when an enterprise has many data sources, but the business still needs one trustworthy view?</b></p>
    <p>Public Neoware material discusses incomplete, inconsistent and inaccurate data; integrating diverse sources such as databases, files and messages; data governance; modern data platforms; and validating and reconciling data across sources.</p>
    <p>That led me to build a focused engineering proof: a reliability layer between ingestion and consumption.</p></div>""",unsafe_allow_html=True)
    cols=st.columns(4)
    for c,(t,b) in zip(cols,[("Integration","Different schemas and source shapes"),("Reliability","Quality rules before consumption"),("Reconciliation","Cross-system business consistency"),("Intelligence-ready","Trusted Gold data for BI / AI")]):
        c.markdown(f'<div class="step"><div class="stepnum">ENGINEERING GOAL</div><h3>{t}</h3><p>{b}</p></div>',unsafe_allow_html=True)

elif page=="03 — Evidence & Problem Mapping":
    st.markdown("## Evidence: why this is a relevant engineering problem")
    st.caption("Public Neoware evidence; implementation is independent and uses synthetic data.")
    st.markdown("""<div class="evidence"><b>Public problem statement</b><br>Neoware asks whether incomplete, inconsistent or inaccurate data is undermining business decisions and how diverse sources such as databases, files and messages can be integrated into a unified view.</div>
    <div class="evidence"><b>Public solution capability</b><br>Neoware describes Data Validation and Reconciliation (DVRC) as validating and reconciling data across various sources so analytics are based on accurate and consistent information.</div>
    <div class="evidence"><b>Public platform direction</b><br>Neoware describes modern data platforms, governance, cataloging, security and data-quality capabilities as part of the Data Engineering journey.</div>""",unsafe_allow_html=True)
    st.markdown("### Public sources")
    st.link_button("Neoware — Data Engineering","https://neoware.ai/our_services/data-engineering/")
    st.link_button("Neoware — Solution Accelerators / DVRC","https://neoware.ai/solution-accelerators/")
    st.link_button("Neoware — Partners / Data Platform Context","https://neoware.ai/partners/")
    st.markdown("### Evidence → implementation mapping")
    mapping=pd.DataFrame([
        ["Diverse sources","CSV + JSON/API-style + operational files","Ingestion + standardization"],
        ["Inconsistent data","Synthetic defects intentionally injected","Quality gate + issue register"],
        ["Validate & reconcile","Orders vs invoices","Reconciliation report"],
        ["Trusted analytics","Only matched orders promoted","Gold / trusted dataset"],
        ["Data to Intelligence","Trusted output for downstream use","BI / AI-ready architecture"]],columns=["Public theme","POC evidence","Engineering response"])
    st.dataframe(mapping,use_container_width=True,hide_index=True)

elif page=="04 — Pipeline Walkthrough":
    st.markdown("## How the data was processed")
    steps=[("01","INGEST","Load CSV, JSON/API-style and operational sources."),("02","STANDARDIZE","Normalize columns, trim strings, parse dates and cast financial fields."),("03","VALIDATE","Apply required-field, date, positive-value and referential checks."),("04","DETECT","Identify duplicate invoice representations and anomalies."),("05","RECONCILE","Aggregate by business key and compare order vs invoice facts."),("06","QUARANTINE","Keep failures visible with source, key, rule, severity and detail."),("07","PROMOTE","Move only reconciled records into the trusted layer."),("08","OBSERVE","Expose health, quality and reconciliation metrics.")]
    cols=st.columns(4)
    for i,(n,t,b) in enumerate(steps): cols[i%4].markdown(f'<div class="step"><div class="stepnum">{n}</div><h3>{t}</h3><p>{b}</p></div>',unsafe_allow_html=True)
    st.info("Synthetic defects are deliberately included so the reliability controls demonstrate real failure handling.")

elif page=="05 — Source Health":
    st.markdown("## Source health"); st.dataframe(health,use_container_width=True,hide_index=True); st.bar_chart(health.set_index("source")["completeness_pct"]); st.caption("Production extension: source SLAs, freshness, schema drift alerts, lineage and run monitoring.")

elif page=="06 — Reconciliation":
    st.markdown("## Cross-source reconciliation"); counts=recon["status"].value_counts()
    a,b,c=st.columns(3); a.metric("Matched",int(counts.get("Matched",0))); b.metric("Mismatch",int(counts.get("Mismatch",0))); c.metric("Other exceptions",int(len(recon)-counts.get("Matched",0)-counts.get("Mismatch",0)))
    st.bar_chart(counts); st.dataframe(recon,use_container_width=True,hide_index=True)
    st.write("A pipeline can succeed technically while two systems disagree on business facts. Reconciliation makes that disagreement explicit before trusted data is consumed.")

elif page=="07 — Data Quality":
    st.markdown("## Data quality gate"); a,b=st.columns(2); a.metric("Quality score",f'{summary["quality_score_pct"]}%'); b.metric("Issues surfaced",summary["quality_issues"])
    st.dataframe(issues,use_container_width=True,hide_index=True)
    st.markdown("### Rules implemented"); st.write("Required business keys • valid dates • positive amounts • referential integrity • duplicate detection • source completeness • reconciliation consistency")

elif page=="08 — Trusted Data":
    st.markdown("## Gold / trusted data product"); st.success("Only reconciled orders are promoted to this layer."); st.dataframe(trusted,use_container_width=True,hide_index=True)
    st.write("Consumers should read the trusted layer rather than repeatedly implementing source-specific cleanup and reconciliation logic.")

elif page=="09 — Architecture":
    st.markdown("## From local POC to production")
    st.code("""ENTERPRISE SOURCES
RDBMS • APIs • Files • Messages • Legacy Systems
                    ↓
       INGESTION + ORCHESTRATION
                    ↓
              BRONZE / RAW
          immutable + metadata
                    ↓
           DATABRICKS / PYSPARK
                    ↓
       SILVER / STANDARDIZED DATA
       schema • quality • dedup • rules
                    ↓
          RECONCILIATION LAYER
                    ↓
            GOLD / TRUSTED DATA
                    ↓
             BI • ML • GENAI • APIs""")
    df=pd.DataFrame([["Python/Pandas ingestion","Implemented"],["Quality rules","Implemented"],["Reconciliation","Implemented"],["Trusted layer","Implemented"],["Streamlit observability","Implemented"],["PySpark mapping","Reference implementation"],["ADF / cloud orchestration","Production proposal"],["ADLS / Delta Lake","Production proposal"],["Catalog / lineage / alerting","Production proposal"]],columns=["Capability","Status"])
    st.dataframe(df,use_container_width=True,hide_index=True)

elif page=="10 — Code & Engineering":
    st.markdown("## Code map")
    df=pd.DataFrame([["src/ingestion.py","Source loaders","CSV + JSON/API-style ingestion"],["src/data_quality.py","Reusable DQ rules","Validation primitives"],["src/reconciliation.py","Reconciliation engine","Cross-source comparison"],["src/pipeline.py","Orchestrator","End-to-end processing + outputs"],["src/pyspark_pipeline.py","Scale-up mapping","PySpark/Databricks pattern"],["tests/test_pipeline.py","Automated tests","Pipeline output + defect detection"],["app.py","Observability UI","Dashboard + case-study walkthrough"]],columns=["File","Layer","Purpose"])
    st.dataframe(df,use_container_width=True,hide_index=True)
    st.markdown("### Engineering principles"); st.write("Source preservation • explicit contracts • deterministic rules • explainable failures • trusted promotion • testable transformations • production mapping without pretending local code is production infrastructure.")

else:
    st.markdown("## Case study in one page")
    st.markdown(f"""<div class="card"><h3>Problem</h3><p>Enterprise data arrives from multiple systems with different schemas, quality levels and sometimes conflicting business facts.</p>
    <h3>Why I chose it</h3><p>Public Neoware material made data integration, data quality and validation/reconciliation a concrete engineering problem to study.</p>
    <h3>What I built</h3><p>A runnable reliability layer that ingests heterogeneous data, standardizes it, validates quality, reconciles business facts, quarantines exceptions and promotes trusted records.</p>
    <h3>Result</h3><p><b>{summary["raw_records"]}</b> raw records → <b>{summary["trusted_orders"]}</b> trusted orders; <b>{summary["reconciliation_match_rate_pct"]}%</b> reconciliation match rate; <b>{summary["quality_score_pct"]}%</b> quality score; <b>{summary["quality_issues"]}</b> issues surfaced.</p>
    <h3>Boundary</h3><p>Independent portfolio case study using synthetic data. Not a reimplementation of Neoware proprietary tools and no internal access is claimed.</p></div>""",unsafe_allow_html=True)
    st.link_button("Open GitHub repository","https://github.com/abishekvarma/neoware-enterprise-data-reliability-reconciliation-platform")
