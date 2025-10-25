import os
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.create import create_engine
from sqlalchemy.inspection import inspect as sqla_inspect

def get_engine():
    url = os.getenv("DB_URL")
    if not url:
        raise RuntimeError("DB_URL no está definido en .env")
    return create_engine(url, pool_pre_ping=True, future=False)
