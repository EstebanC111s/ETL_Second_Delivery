from pathlib import Path
import os
import pandas as pd
from .util_db import get_engine

HOST_BASE   = Path(__file__).resolve().parents[1] / "data" / "input"
DOCKER_BASE = Path("/opt/airflow/data/input")
BASE = DOCKER_BASE if DOCKER_BASE.exists() else HOST_BASE

DEFAULT_FILE = os.getenv("NEW_FILE", "Data_histórica_de_calidad_de_agua_20251017.csv")
DEFAULT_INPUT = BASE / DEFAULT_FILE

def extract(csv_path: Path = DEFAULT_INPUT) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"[extract_new] No existe el archivo: {csv_path}")
    try:
        df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False, on_bad_lines="skip")
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding="latin-1", low_memory=False, on_bad_lines="skip")
    return df

def run():
    df = extract()
    df.columns = (
        df.columns.astype(str).str.strip().str.lower()
        .str.normalize("NFKD").str.encode("ascii","ignore").str.decode("utf-8")
        .str.replace(r"[^\w]+","_", regex=True)
    )
    eng = get_engine()
    df.to_sql("stg_new", eng, if_exists="replace", index=False, method="multi", chunksize=5000)
    print(f"[extract_new] stg_new filas={len(df)} cols={len(df.columns)}")

if __name__ == "__main__":
    d = extract()
    print(d.shape)
