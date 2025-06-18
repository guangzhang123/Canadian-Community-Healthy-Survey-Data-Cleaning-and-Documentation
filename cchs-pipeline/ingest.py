from pathlib import Path
import pandas as pd

def load_raw(raw_dir: Path) -> pd.DataFrame:
    frames = []
    for fp in raw_dir.iterdir():
        if fp.suffix.lower() == ".csv":
            frames.append(pd.read_csv(fp, low_memory=False))
        elif fp.suffix.lower() in (".xls", ".xlsx"):
            frames.append(pd.read_excel(fp))
        else:
            print(f"Skip unsupported file: {fp.name}")
    df = pd.concat(frames, ignore_index=True)
    df.to_parquet("data/interim/combined_raw.parquet")
    return df
