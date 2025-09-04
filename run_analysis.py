"""
Small CLI to run the pipeline. Intended as an example entrypoint.

Usage:
    python scripts/run_analysis.py --input path/to/comments.csv
"""
import argparse
import sys
from pathlib import Path

from account.data_loader import read_comments_csv, validate_dataframe
from account.analysis import AnalysisEngine


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to comments CSV")
    args = parser.parse_args(argv)

    path = Path(args.input)
    try:
        df = read_comments_csv(str(path))
    except FileNotFoundError as e:
        print(e)
        sys.exit(2)
    except ValueError as e:
        print(f"Failed to read input: {e}")
        sys.exit(2)

    ok, errors = validate_dataframe(df)
    if not ok:
        print("Validation failed:")
        for e in errors:
            print(" - ", e)
        sys.exit(2)

    engine = AnalysisEngine(df)
    engine.prepare()
    kpis = engine.compute_kpis()

    print("Daily rows:", len(kpis["daily"]))
    print("Hourly rows:", len(kpis["hourly"]))

    engine.detect_early_viral_posts()
    engine.save_viral_table("viral_post_flags.csv")
    print('Analyzed full data set to identify viral posts and wrote output to viral_post_flags.csv')

if __name__ == "__main__":
    main()
    