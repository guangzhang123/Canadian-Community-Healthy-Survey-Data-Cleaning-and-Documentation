import argparse
from pathlib import Path
import yaml

from .ingest   import load_raw
from .clean    import clean_frame
from .metadata import make_codebook
from .quality  import run_checks
from .export   import export_outputs

def main() -> None:
    parser = argparse.ArgumentParser(description="CCHS Data Cleaning Pipeline")
    parser.add_argument("--config", default="configs/default.yaml", help="YAML config path")
    args = parser.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())

    raw_df = load_raw(Path("data/raw"))
    cleaned = clean_frame(raw_df, cfg)
    make_codebook(cleaned)          # docs/codebook_YYYYMMDD.csv
    run_checks(cleaned)             # console log + reports/quality_*.csv
    export_outputs(cleaned)         # parquet/csv + sas7bdat + summary tables

if __name__ == "__main__":
    main()
