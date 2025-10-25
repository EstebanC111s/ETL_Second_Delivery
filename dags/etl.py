# dags/etl.py
# DAG de ETL para el proyecto de Acueductos
from datetime import datetime
import os, sys, pathlib

# Asegura que /opt/airflow/src esté importable
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Funciones en src/
from src.extract_old import run as extract_old          # stg_old (CSV viejo)
from src.extract_new import run as extract_new          # stg_new (CSV nuevo)
from src.extract_api import run as extract_api          # stg_api (API)
from src.transform   import run as transform_run        # genera clean_*
from src.checks_cli  import run as checks_cli           # validación

SCHEDULE = os.getenv("SCHEDULE", "@daily")
PSQL = "psql postgresql://etl:etl@warehouse:5432/etl -v ON_ERROR_STOP=1 -f"

with DAG(
    dag_id="etl",
    description="ETL: extracts -> transform (src) -> merge (SQL) -> validate -> build dims",
    start_date=datetime(2025, 10, 17),
    schedule_interval=SCHEDULE,
    catchup=False,
    default_args={"owner": "team", "retries": 1},
    tags=["etl", "acueductos", "dw"],
) as dag:

    # 1) EXTRACTS → stg_*
    t_extract_old = PythonOperator(task_id="extract_old", python_callable=extract_old)
    t_extract_new = PythonOperator(task_id="extract_new", python_callable=extract_new)
    t_extract_api = PythonOperator(task_id="extract_api", python_callable=extract_api)

    # 2) TRANSFORM (Python) → limpia y genera clean_*
    t_transform = PythonOperator(task_id="transform", python_callable=transform_run)

    # 3) MERGE (SQL)
    t_merge_sql = BashOperator(
    task_id="merge_clean_sql",
    bash_command="psql postgresql://etl:etl@warehouse:5432/etl -v ON_ERROR_STOP=1 -f /opt/airflow/sql/merge_pipeline.sql",
    )



    # 4) VALIDATE (checks_cli.py)
    t_validate = PythonOperator(task_id="validate", python_callable=checks_cli)

    # 5) DIMENSIONES (SQL) — directas desde clean_* (sin unified, sin stage)
    t_build_dim_calidad = BashOperator(
        task_id="build_dim_calidad",
        bash_command=f'{PSQL} /opt/airflow/sql/build_dim_calidad.sql',
    )
    t_build_dim_prestacion = BashOperator(
        task_id="build_dim_prestacion",
        bash_command=f'{PSQL} /opt/airflow/sql/build_dim_prestacion.sql',
    )
    t_build_dim_prestadores = BashOperator(
        task_id="build_dim_prestadores",
        bash_command=f'{PSQL} /opt/airflow/sql/build_dim_prestadores.sql',
    )

    # 🔗 Orquestación
    [t_extract_old, t_extract_new, t_extract_api] >> t_transform >> t_merge_sql >> t_validate >> [
        t_build_dim_calidad,
        t_build_dim_prestacion,
        t_build_dim_prestadores,
    ]
