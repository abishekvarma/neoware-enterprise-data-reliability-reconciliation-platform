from .reliability_pipeline import run
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="data/run_manifest.json")
    parser.add_argument("--output", default="output")
    args = parser.parse_args()
    for row in run(args.manifest, args.output):
        print(row)
