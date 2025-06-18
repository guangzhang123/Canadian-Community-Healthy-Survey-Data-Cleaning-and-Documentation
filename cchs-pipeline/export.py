from pathlib import Path
import pandas as pd
import pyreadstat
from datetime import datetime
from tabulate import tabulate

def export_outputs(df: pd.DataFrame) -> None:
    # data
    ts = datetime.now().strftime("%Y%m%d")
    df.to_parquet(f"data/processed/cleaned_{ts}.parquet")
    df.to_csv    (f"data/processed/cleaned_{ts}.csv", index=False)

    # SAS
    sas_out = Path("exports/cleaned.sas7bdat")
    pyreadstat.write_sas7bdat(df, sas_out)
    print(f"SAS dataset → {sas_out}")

    # summary tables
    summary = (
        df
        .groupby(["age_group", "sex"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    sum_path = Path(f"reports/summary_{ts}.csv")
    summary.to_csv(sum_path, index=False)

    print("\nSummary preview:\n", tabulate(summary.head(), headers="keys", tablefmt="simple"))
    print(f"\nSummary saved → {sum_path}")
