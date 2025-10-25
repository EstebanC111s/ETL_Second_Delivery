from sqlalchemy import text
from .util_db import get_engine

def run():
    eng = get_engine()
    with eng.begin() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM clean_staging")).scalar() or 0
        if n <= 0:
            raise RuntimeError("Validación falló: clean_staging está vacío.")
    print(f"[validate] OK → filas={n}")
