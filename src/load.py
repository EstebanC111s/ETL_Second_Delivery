# src/load.py
from sqlalchemy.sql.expression import text  # ✅ Fix Pylance/SQLAlchemy 2.x
from .util_db import get_engine


def create_schema() -> None:
    """
    Crea tablas finales del DW si no existen:
      - dim_prestador(provider_id PK)
      - fact_servicio(UNIQUE provider_id,servicio,fecha)
      - fact_calidad(UNIQUE departamento,municipio,parametro,fecha)
    y sus índices / FKs.
    Compatible con PostgreSQL y SQLite.
    """
    eng = get_engine()
    dialect = eng.dialect.name  # 'postgresql', 'sqlite', etc.

    with eng.begin() as conn:
        if dialect == "postgresql":
            # --------- PostgreSQL ---------
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_prestador (
                provider_id   TEXT PRIMARY KEY,
                nombre        TEXT,
                tipo          TEXT,
                clasificacion TEXT,
                departamento  TEXT,
                municipio     TEXT,
                direccion     TEXT,
                telefono      TEXT,
                email         TEXT
            );
            """))

            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_servicio (
                id            BIGSERIAL PRIMARY KEY,
                provider_id   TEXT NOT NULL,
                servicio      TEXT,
                estado        TEXT,
                fecha         DATE NOT NULL,
                CONSTRAINT uq_fact_servicio UNIQUE (provider_id, servicio, fecha),
                CONSTRAINT fk_fact_servicio_provider
                    FOREIGN KEY (provider_id) REFERENCES dim_prestador(provider_id)
            );
            """))

            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_calidad (
                id            BIGSERIAL PRIMARY KEY,
                departamento  TEXT NOT NULL,
                municipio     TEXT NOT NULL,
                parametro     TEXT NOT NULL,
                valor         DOUBLE PRECISION,
                fecha         DATE NOT NULL,
                unidad        TEXT,
                CONSTRAINT uq_fact_calidad UNIQUE (departamento, municipio, parametro, fecha)
            );
            """))

        else:
            # --------- SQLite (u otros) ---------
            # Nota: SQLite no tiene BIGSERIAL; usamos INTEGER PRIMARY KEY AUTOINCREMENT.
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_prestador (
                provider_id   TEXT PRIMARY KEY,
                nombre        TEXT,
                tipo          TEXT,
                clasificacion TEXT,
                departamento  TEXT,
                municipio     TEXT,
                direccion     TEXT,
                telefono      TEXT,
                email         TEXT
            );
            """))

            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_servicio (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_id   TEXT NOT NULL,
                servicio      TEXT,
                estado        TEXT,
                fecha         DATE NOT NULL,
                UNIQUE (provider_id, servicio, fecha),
                FOREIGN KEY (provider_id) REFERENCES dim_prestador(provider_id)
            );
            """))

            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_calidad (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                departamento  TEXT NOT NULL,
                municipio     TEXT NOT NULL,
                parametro     TEXT NOT NULL,
                valor         REAL,
                fecha         DATE NOT NULL,
                unidad        TEXT,
                UNIQUE (departamento, municipio, parametro, fecha)
            );
            """))

        # Índices (mismos para ambos dialectos)
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_dim_prestador_depto_mpio ON dim_prestador(departamento, municipio);"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_fact_servicio_provider_fecha ON fact_servicio(provider_id, fecha);"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_fact_calidad_geo_fecha ON fact_calidad(departamento, municipio, fecha);"
        ))

    print("[load] Schema OK")


def load_to_model() -> None:
    """
    UPSERT desde *_stage hacia finales:
      - dim_prestador_stage -> dim_prestador (ON CONFLICT UPDATE)
      - fact_servicio_stage -> fact_servicio (ON CONFLICT DO NOTHING)
      - fact_calidad_stage  -> fact_calidad  (ON CONFLICT DO NOTHING)
    Funciona en PostgreSQL.
    En SQLite requiere versión 3.24+ (UPsert nativo); si tu build es muy vieja,
    cambia los DO NOTHING por INSERT OR IGNORE.
    """
    eng = get_engine()
    dialect = eng.dialect.name

    with eng.begin() as conn:
        # Asegura que las stage existan (idempotente)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_prestador_stage (
            provider_id   TEXT PRIMARY KEY,
            nombre        TEXT,
            tipo          TEXT,
            clasificacion TEXT,
            departamento  TEXT,
            municipio     TEXT,
            direccion     TEXT,
            telefono      TEXT,
            email         TEXT
        );"""))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_servicio_stage (
            provider_id   TEXT,
            servicio      TEXT,
            estado        TEXT,
            fecha         DATE
        );"""))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_calidad_stage (
            departamento  TEXT,
            municipio     TEXT,
            parametro     TEXT,
            valor         DOUBLE PRECISION,
            fecha         DATE,
            unidad        TEXT
        );"""))

        # 1) Dimensión: UPSERT por provider_id
        if dialect == "postgresql":
            conn.execute(text("""
            INSERT INTO dim_prestador AS d (provider_id, nombre, tipo, clasificacion, departamento, municipio, direccion, telefono, email)
            SELECT provider_id, nombre, tipo, clasificacion, departamento, municipio, direccion, telefono, email
            FROM dim_prestador_stage
            WHERE provider_id IS NOT NULL AND provider_id <> ''
            ON CONFLICT (provider_id) DO UPDATE SET
                nombre        = EXCLUDED.nombre,
                tipo          = EXCLUDED.tipo,
                clasificacion = EXCLUDED.clasificacion,
                departamento  = EXCLUDED.departamento,
                municipio     = EXCLUDED.municipio,
                direccion     = EXCLUDED.direccion,
                telefono      = EXCLUDED.telefono,
                email         = EXCLUDED.email;
            """))
        else:
            # SQLite: ON CONFLICT ... DO UPDATE también existe (3.24+). Usamos DO UPDATE para equiparar.
            conn.execute(text("""
            INSERT INTO dim_prestador (provider_id, nombre, tipo, clasificacion, departamento, municipio, direccion, telefono, email)
            SELECT provider_id, nombre, tipo, clasificacion, departamento, municipio, direccion, telefono, email
            FROM dim_prestador_stage
            WHERE provider_id IS NOT NULL AND provider_id <> ''
            ON CONFLICT(provider_id) DO UPDATE SET
                nombre        = excluded.nombre,
                tipo          = excluded.tipo,
                clasificacion = excluded.clasificacion,
                departamento  = excluded.departamento,
                municipio     = excluded.municipio,
                direccion     = excluded.direccion,
                telefono      = excluded.telefono,
                email         = excluded.email;
            """))

        # 2) Hecho servicio: evita duplicados por (provider_id, servicio, fecha)
        # (ambos dialectos soportan DO NOTHING con UNIQUE)
        conn.execute(text("""
        INSERT INTO fact_servicio (provider_id, servicio, estado, fecha)
        SELECT DISTINCT
            provider_id,
            COALESCE(servicio, 'acueducto') AS servicio,
            estado,
            COALESCE(fecha, CURRENT_DATE)   AS fecha
        FROM fact_servicio_stage
        WHERE provider_id IS NOT NULL AND provider_id <> ''
        ON CONFLICT (provider_id, servicio, fecha) DO NOTHING;
        """))

        # 3) Hecho calidad: evita duplicados por (depto, mpio, parametro, fecha)
        conn.execute(text("""
        INSERT INTO fact_calidad (departamento, municipio, parametro, valor, fecha, unidad)
        SELECT DISTINCT
            departamento, municipio, parametro, valor,
            COALESCE(fecha, CURRENT_DATE) AS fecha,
            unidad
        FROM fact_calidad_stage
        WHERE departamento IS NOT NULL AND municipio IS NOT NULL AND parametro IS NOT NULL
        ON CONFLICT (departamento, municipio, parametro, fecha) DO NOTHING;
        """))

    print("[load] Data upserted → dim_prestador, fact_servicio, fact_calidad")
