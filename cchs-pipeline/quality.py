import pandas as pd
from datetime import datetime
from pathlib import Path

def run_checks(df: pd.DataFrame) -> None:
    issues = []
    for col in df.columns:
        null_pct = df[col].isna().mean() * 100
        if null_pct > 20:
            issues.append((col, f"{null_pct:.1f}% missing"))
        if df[col].dtype == "object" and df[col].str.contains(r"\s{2,}", regex=True).any():
            issues.append((col, "inconsistent spacing"))

    qual_df = pd.DataFrame(issues, columns=["variable", "issue"])
    ts = datetime.now().strftime("%Y%m%d")
    out_path = Path(f"reports/quality_{ts}.csv")
    qual_df.to_csv(out_path, index=False)
    print(f"Quality report → {out_path}")
