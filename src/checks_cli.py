# src/checks_cli.py
# -*- coding: utf-8 -*-
import os
import sys
from contextlib import contextmanager
from typing import Dict, List, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

# (Opcional) usa tu helper si existe
try:
    from .util_db import get_engine as _get_engine
except Exception:
    _get_engine = None


# ---------------------------
# helpers de entorno / engine
# ---------------------------

def _is_airflow() -> bool:
    """Detecta si corre dentro de un task de Airflow."""
    # Airflow setea estas env vars para cada task
    return bool(os.getenv("AIRFLOW_CTX_DAG_ID"))

def _maybe_local_db_url(url: str) -> str:
    """
    Si el script corre fuera de Docker, 'warehouse:5432' no existe.
    Fallback a 'localhost:5434' (mapeo del compose).
    """
    if not url:
        return url
    if "warehouse:5432" in url:
        return url.replace("warehouse:5432", "localhost:5434")
    return url

def _build_engine() -> Engine:
    """
    Obtiene un Engine de SQLAlchemy.
    - Si existe util_db.get_engine() lo usa (ideal dentro del contenedor).
    - Si no, construye desde DB_URL (y hace fallback a localhost:5434).
    """
    if _get_engine is not None:
        return _get_engine()

    from sqlalchemy import create_engine
    url = os.getenv("DB_URL", "")
    url = _maybe_local_db_url(url)
    if not url:
        raise RuntimeError("DB_URL no está definido en el entorno.")
    return create_engine(url, future=True)

@contextmanager
def connect():
    eng = _build_engine()
    with eng.begin() as conn:
        yield conn

def _count(conn, sql: str) -> int:
    return int(conn.execute(text(sql)).scalar() or 0)


# ---------------------------
# lógica de chequeos
# ---------------------------

def _collect(conn) -> Tuple[List[str], Dict[str, int]]:
    """
    Ejecuta todos los chequeos y devuelve:
      - problems: lista de strings con problemas (vacía si todo ok)
      - metrics:  dict con métricas para logging/XCom
    """
    problems: List[str] = []
    m: Dict[str, int] = {}

    # 1) existencia y conteos
    try:
        m["rows_clean_staging"] = _count(conn, "SELECT COUNT(*) FROM clean_staging;")
        print(f"[OK] clean_staging filas: {m['rows_clean_staging']}")
        if m["rows_clean_staging"] == 0:
            problems.append("clean_staging está vacío.")
    except Exception as e:
        problems.append(f"clean_staging no existe o error de lectura: {e}")

    try:
        m["rows_clean_calidad"] = _count(conn, "SELECT COUNT(*) FROM clean_calidad;")
        print(f"[OK] clean_calidad filas: {m['rows_clean_calidad']}")
        if m["rows_clean_calidad"] == 0:
            problems.append("clean_calidad está vacío.")
    except Exception as e:
        problems.append(f"clean_calidad no existe o error de lectura: {e}")

    if problems:
        return problems, m  # si faltan tablas, cortamos aquí

    # 2) nulos prohibidos (claves)
    m["nulls_clean_staging"] = _count(conn, """
        SELECT COUNT(*) FROM clean_staging
        WHERE provider_id IS NULL OR nombre IS NULL OR
              departamento IS NULL OR municipio IS NULL OR servicio IS NULL;
    """)
    print(f"[CHK] nulos en claves (clean_staging): {m['nulls_clean_staging']}")
    if m["nulls_clean_staging"] > 0:
        problems.append(f"clean_staging tiene {m['nulls_clean_staging']} filas con nulos en claves.")

    m["nulls_clean_calidad"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE departamento IS NULL OR municipio IS NULL OR
              fecha_muestra IS NULL OR parametro IS NULL;
    """)
    print(f"[CHK] nulos en claves (clean_calidad): {m['nulls_clean_calidad']}")
    if m["nulls_clean_calidad"] > 0:
        problems.append(f"clean_calidad tiene {m['nulls_clean_calidad']} filas con nulos en claves.")

    # 3) duplicados por llaves lógicas (prestadores)
    m["dups_clean_staging"] = _count(conn, """
        SELECT COUNT(*) FROM (
          SELECT provider_id, servicio, departamento, municipio, COUNT(*) c
          FROM clean_staging
          GROUP BY 1,2,3,4
          HAVING COUNT(*) > 1
        ) t;
    """)
    print(f"[CHK] duplicados clave (clean_staging): {m['dups_clean_staging']}")
    if m["dups_clean_staging"] > 0:
        problems.append(f"clean_staging tiene {m['dups_clean_staging']} combinaciones clave duplicadas.")

    # 3b) duplicados en calidad por punto
    m["dups_calidad_punto"] = _count(conn, """
        SELECT COUNT(*) FROM (
          SELECT departamento, municipio, parametro, fecha_muestra, COALESCE(nombre_punto,'') AS p, COUNT(*) c
          FROM clean_calidad
          GROUP BY 1,2,3,4,5
          HAVING COUNT(*) > 1
        ) t;
    """)
    print(f"[CHK] duplicados clave (clean_calidad por punto): {m['dups_calidad_punto']}")
    if m["dups_calidad_punto"] > 0:
        problems.append(
            f"clean_calidad tiene {m['dups_calidad_punto']} duplicados a nivel (dep,muni,param,fecha,punto)."
        )

    # (info) colisiones municipio/día/parámetro
    m["colisiones_calidad_muni_dia"] = _count(conn, """
        SELECT COUNT(*) FROM (
          SELECT departamento, municipio, parametro, fecha_muestra, COUNT(*) c
          FROM clean_calidad
          GROUP BY 1,2,3,4
          HAVING COUNT(*) > 1
        ) t;
    """)
    print(f"[INFO] colisiones municipio/día/parámetro: {m['colisiones_calidad_muni_dia']}")

    # 4) rango de fechas
    m["fechas_fuera_rango"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE fecha_muestra < DATE '2000-01-01' OR fecha_muestra > CURRENT_DATE;
    """)
    print(f"[CHK] fechas fuera de rango (clean_calidad): {m['fechas_fuera_rango']}")
    if m["fechas_fuera_rango"] > 0:
        problems.append(f"clean_calidad tiene {m['fechas_fuera_rango']} fechas fuera de rango.")

    # 5) coordenadas
    m["latlon_despareados"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE (latitud IS NULL) <> (longitud IS NULL);
    """)
    print(f"[CHK] lat/lon despareados: {m['latlon_despareados']}")
    if m["latlon_despareados"] > 0:
        problems.append(f"clean_calidad tiene {m['latlon_despareados']} filas con lat/lon despareados.")

    m["coords_fuera_col"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE (latitud  IS NOT NULL AND (latitud  < -5  OR latitud  > 15))
           OR (longitud IS NOT NULL AND (longitud < -82 OR longitud > -66));
    """)
    print(f"[CHK] coordenadas fuera de rango COL: {m['coords_fuera_col']}")
    if m["coords_fuera_col"] > 0:
        problems.append(f"clean_calidad tiene {m['coords_fuera_col']} coordenadas fuera de rango COL.")

    # 6) reglas por parámetro
    m["ph_fuera_0_14"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE parametro = 'PH' AND valor IS NOT NULL AND (valor < 0 OR valor > 14);
    """)
    print(f"[CHK] pH fuera de 0..14: {m['ph_fuera_0_14']}")
    if m["ph_fuera_0_14"] > 0:
        problems.append("pH fuera de 0..14 (deberían haber quedado en NULL para imputar).")

    m["cloro_fuera_0_5"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE parametro LIKE 'CLORO%' AND valor IS NOT NULL AND (valor < 0 OR valor > 5);
    """)
    print(f"[CHK] CLORO fuera de 0..5: {m['cloro_fuera_0_5']}")
    if m["cloro_fuera_0_5"] > 0:
        problems.append("CLORO fuera de 0..5 (deberían haber quedado en NULL para imputar).")

    return problems, m


# ---------------------------
# wrappers CLI / Airflow
# ---------------------------

def _fail(problems: List[str]):
    msg = "\n".join(f" - {p}" for p in problems)
    if _is_airflow():
        # En Airflow, fallamos con excepción (para que el task quede FAILED)
        raise RuntimeError("DQ QUICKCHECK FALLÓ:\n" + msg)
    else:
        print("\n❌ [DQ QUICKCHECK] FALLÓ:")
        print(msg)
        print("")
        sys.exit(1)

def run(**kwargs):
    """
    Wrapper para Airflow (PythonOperator).
    - Imprime el detalle en logs del task.
    - Devuelve un dict con métricas a XCom si todo ok.
    - Lanza RuntimeError si hay problemas.
    """
    print("\n=== DQ QUICKCHECK (Airflow Task) ===")
    with connect() as conn:
        problems, metrics = _collect(conn)

    if problems:
        _fail(problems)

    print("\n✅ [DQ QUICKCHECK] OK — datos básicos consistentes.\n")
    # Lo que retorna el callable se guarda en XCom (Airflow 2.x)
    return {"status": "ok", **metrics}

def main():
    """
    Modo CLI (python -m src.checks_cli).
    Guarda comportamiento clásico con sys.exit para devolver códigos de salida.
    """
    print("\n=== DQ QUICKCHECK (terminal) ===")
    with connect() as conn:
        problems, metrics = _collect(conn)

    if problems:
        _fail(problems)

    print("\n✅ [DQ QUICKCHECK] OK — datos básicos consistentes.\n")
    sys.exit(0)

if __name__ == "__main__":
    main()
