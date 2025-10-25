import os
import time
import requests
import pandas as pd
from unicodedata import normalize
from .util_db import get_engine
from typing import Dict, Union


API_URL    = (os.getenv("API_URL") or "").strip()
API_TOKEN  = (os.getenv("API_TOKEN") or "").strip()
API_SELECT = (os.getenv("API_SELECT") or "").strip()
API_WHERE  = (os.getenv("API_WHERE")  or "").strip()
LIMIT      = int(os.getenv("LIMIT", "5000"))

# Retries simples para 429/5xx
MAX_RETRIES = 3
BACKOFF_SEC = 3

def _clean_cols(cols):
    s = pd.Index(cols).astype(str).str.strip().str.lower()
    # quita acentos:
    s = s.map(lambda x: normalize("NFKD", x).encode("ascii", "ignore").decode("utf-8"))
    return s.str.replace(r"[^\w]+", "_", regex=True)



Params = Dict[str, Union[str, int]]

def _fetch_page(offset: int) -> pd.DataFrame:
    headers = {"Accept": "application/json"}
    if API_TOKEN:
        headers["X-App-Token"] = API_TOKEN

    # tipamos explícitamente para permitir str o int
    params: Params = {"$limit": LIMIT, "$offset": offset}
    if API_SELECT:
        params["$select"] = API_SELECT
    if API_WHERE:
        params["$where"] = API_WHERE

    attempt = 0
    while True:
        try:
            r = requests.get(API_URL, headers=headers, params=params, timeout=60)
            if r.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                attempt += 1
                time.sleep(BACKOFF_SEC * attempt)
                continue
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "data" in data:
                data = data["data"]
            return pd.DataFrame(data)
        except requests.RequestException as e:
            if attempt < MAX_RETRIES:
                attempt += 1
                time.sleep(BACKOFF_SEC * attempt)
                continue
            raise RuntimeError(f"[extract_api] Error al llamar API: {e}") from e


def run():
    if not API_URL:
        print("[extract_api] API_URL vacío → tarea saltada.")
        # Para no romper el flujo, crea tabla vacía
        eng = get_engine()
        pd.DataFrame().to_sql("stg_api", eng, if_exists="replace", index=False)
        return

    # Paginación
    frames = []
    offset = 0
    while True:
        df = _fetch_page(offset)
        if df.empty:
            break
        frames.append(df)
        if len(df) < LIMIT:
            break
        offset += LIMIT

    if frames:
        df_all = pd.concat(frames, ignore_index=True, sort=False)
    else:
        df_all = pd.DataFrame()

    if df_all.empty:
        print("[extract_api] Sin filas recibidas.")
    else:
        df_all.columns = _clean_cols(df_all.columns)

    eng = get_engine()
    df_all.to_sql("stg_api", eng, if_exists="replace", index=False, method="multi", chunksize=5000)
    print(f"[extract_api] stg_api → filas={len(df_all)} cols={len(df_all.columns)}")
