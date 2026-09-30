from __future__ import annotations
import argparse,json
from .reliability_pipeline import run
if __name__=="__main__":
    p=argparse.ArgumentParser(description="Enterprise Data Reliability engine")
    p.add_argument("--manifest",default="data/run_manifest.json"); p.add_argument("--output",default="output"); p.add_argument("--force",action="store_true")
    a=p.parse_args(); print(json.dumps(run(a.manifest,a.output,a.force),indent=2,default=str))
