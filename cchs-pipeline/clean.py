import pandas as pd
import numpy as np
from typing import Dict, Any

def _impute_numeric(col: pd.Series, strategy: str) -> pd.Series:
    if strategy == "median":
        return col.fillna(col.median())
    elif strategy.startswith("constant:"):
        val = float(strategy.split(":", 1)[1])
        return col.fillna(val)
    return col  # default: leave as-is

def _impute_categorical(col: pd.Series, strategy: str) -> pd.Series:
    if strategy == "mode":
        return col.fillna(col.mode().iat[0])
    elif strategy.startswith("constant:"):
        return col.fillna(strategy.split(":", 1)[1])
    return col

def clean_frame(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    # 1) rename
    df = df.rename(columns=cfg["rename_map"])

    # 2) type conversions (simple heuristics)
    for c in df.select_dtypes(include="object"):
        df[c] = df[c].str.strip()

    # 3) missing-value handling
    num_strategy = cfg["impute"]["numeric"]
    cat_strategy = cfg["impute"]["categorical"]

    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = _impute_numeric(df[col], num_strategy)
        else:
            df[col] = _impute_categorical(df[col], cat_strategy)

    # 4) outlier removal
    z_thr = cfg.get("outlier_z", 4.0)
    for col in df.select_dtypes(include=[np.number]):
        z = (df[col] - df[col].mean()) / df[col].std(ddof=0)
        df.loc[z.abs() > z_thr, col] = np.nan  # mark; imputed above

    df.to_parquet("data/interim/cleaned_step.parquet")
    return df
