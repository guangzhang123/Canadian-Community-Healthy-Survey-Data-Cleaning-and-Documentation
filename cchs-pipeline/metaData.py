from pathlib import Path
import pandas as pd
from datetime import datetime

def make_codebook(df: pd.DataFrame) -> None:
    meta_rows = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        unique = df[col].nunique(dropna=True)
        meta_rows.append({"variable": col, "dtype": dtype, "unique_values": unique})
    cb = pd.DataFrame(meta_rows)
    ts = datetime.now().strftime("%Y%m%d")
    out_path = Path(f"docs/codebook_{ts}.csv")
    cb.to_csv(out_path, index=False)
    print(f"Codebook saved → {out_path}")
