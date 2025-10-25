# src/transform.py
# -- coding: utf-8 --
from sqlalchemy import text
from .util_db import get_engine

# Catálogo oficial de departamentos (normalizados: UPPER, sin tildes)
DEPARTAMENTOS = (
    "AMAZONAS,ANTIOQUIA,ARAUCA,ATLANTICO,BOLIVAR,BOYACA,CALDAS,CAQUETA,"
    "CASANARE,CAUCA,CESAR,CHOCO,CORDOBA,CUNDINAMARCA,GUAINIA,GUAJIRA,GUAVIARE,"
    "HUILA,MAGDALENA,META,NARINO,NORTE DE SANTANDER,PUTUMAYO,QUINDIO,RISARALDA,"
    "SAN ANDRES Y PROVIDENCIA,SANTANDER,SUCRE,TOLIMA,VALLE DEL CAUCA,VAUPES,"
    "VICHADA,BOGOTA D.C."
).split(",")

DEP_SQL = ",".join([f"'{d}'" for d in DEPARTAMENTOS])


def run() -> None:
    """
    Transforma los staging a tablas limpias y aplica reglas de calidad:

    A) clean_staging (prestadores/servicios) ← stg_old + stg_api
       - Normalización (UPPER/TRIM/sin tildes)
       - provider_id = NIT o md5(nombre|dep|muni|servicio)
       - Catálogos: servicio/estado
       - Dominio: departamento
       - Deduplicación por (provider_id,servicio,departamento,municipio)
       - Imputación: estado (moda por servicio,departamento) / clasificacion (moda por servicio)
       - Purgado final de claves nulas/vacías y re-dedupe

    B) clean_calidad (calidad de agua) ← stg_new
       - Parseo de fecha
       - Limpieza numérica de valor (DOUBLE PRECISION)
       - Coordenadas válidas para Colombia (lat/lon) + pareo de nulidad
       - Dominio de departamento y rango de fechas
       - Reglas por parámetro (pH, CLORO, no-negativos)
       - Deduplicación por (dep,muni,parametro,fecha[,nombre_punto])
       - Imputación: unidad (moda por parametro) / valor (mediana por parametro,departamento → fallback mediana global)
    """
    eng = get_engine()
    with eng.begin() as conn:

        # =========================================================================
        # Limpieza de artefactos (idempotente)
        # =========================================================================
        conn.execute(text("DROP VIEW IF EXISTS v_clean_preview;"))
        conn.execute(text("DROP VIEW IF EXISTS v_clean_calidad_preview;"))
        conn.execute(text("DROP VIEW IF EXISTS v_clean_calidad_agg;"))

        # =========================================================================
        # A) PRESTADORES: clean_staging
        # =========================================================================
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS clean_staging(
                provider_id   TEXT NOT NULL,
                nombre        TEXT NOT NULL,
                departamento  TEXT NOT NULL,
                municipio     TEXT NOT NULL,
                servicio      TEXT NOT NULL,
                estado        TEXT,
                clasificacion TEXT
            );
        """))
        conn.execute(text("DELETE FROM clean_staging;"))

        # stg_old → clean_staging
        conn.execute(text(f"""
            INSERT INTO clean_staging(provider_id,nombre,departamento,municipio,servicio,estado,clasificacion)
            SELECT
                COALESCE(
                    NULLIF(BTRIM(nit), ''),
                    md5(
                        COALESCE(UPPER(BTRIM(translate(nombre, 'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ', 'AEIOUAEIOUaeiouaeiouNn'))),'') || '|' ||
                        COALESCE(UPPER(BTRIM(translate(departamento_prestacion,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))),'') || '|' ||
                        COALESCE(UPPER(BTRIM(translate(municipio_prestacion,  'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))),'') || '|' ||
                        COALESCE(UPPER(BTRIM(translate(servicio,              'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))),'')
                    )
                ) AS provider_id,
                UPPER(BTRIM(translate(nombre,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))) AS nombre,
                UPPER(BTRIM(translate(departamento_prestacion,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))) AS departamento,
                UPPER(BTRIM(translate(municipio_prestacion,  'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))) AS municipio,
                CASE
                  WHEN UPPER(BTRIM(servicio)) LIKE '%ACUED%' THEN 'ACUEDUCTO'
                  WHEN UPPER(BTRIM(servicio)) LIKE '%ALCANT%' THEN 'ALCANTARILLADO'
                  WHEN UPPER(BTRIM(servicio)) LIKE '%ASEO%' THEN 'ASEO'
                  ELSE UPPER(BTRIM(translate(servicio,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn')))
                END AS servicio,
                CASE
                  WHEN UPPER(BTRIM(estado)) IN ('OPERATIVA','EN OPERACION','EN OPERACIÓN','ACTIVA') THEN 'OPERATIVA'
                  WHEN UPPER(BTRIM(estado)) LIKE 'SUSPEN%' THEN 'SUSPENDIDA'
                  WHEN COALESCE(estado,'') = '' THEN 'OTRO'
                  ELSE 'OTRO'
                END AS estado,
                NULLIF(UPPER(BTRIM(translate(clasificacion,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))),'') AS clasificacion
            FROM stg_old
            WHERE nombre IS NOT NULL
              AND departamento_prestacion IS NOT NULL
              AND municipio_prestacion IS NOT NULL
              AND servicio IS NOT NULL;
        """))

        # stg_api → clean_staging
        conn.execute(text(f"""
            INSERT INTO clean_staging(provider_id,nombre,departamento,municipio,servicio,estado,clasificacion)
            SELECT
                md5(
                    COALESCE(UPPER(BTRIM(translate(nombre, 'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ', 'AEIOUAEIOUaeiouaeiouNn'))),'') || '|' ||
                    COALESCE(UPPER(BTRIM(translate(departamento_prestacion,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))),'') || '|' ||
                    COALESCE(UPPER(BTRIM(translate(municipio_prestacion,  'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))),'') || '|' ||
                    COALESCE(UPPER(BTRIM(translate(servicio,              'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))),'')
                ) AS provider_id,
                UPPER(BTRIM(translate(nombre,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))) AS nombre,
                UPPER(BTRIM(translate(departamento_prestacion,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))) AS departamento,
                UPPER(BTRIM(translate(municipio_prestacion,  'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))) AS municipio,
                CASE
                  WHEN UPPER(BTRIM(servicio)) LIKE '%ACUED%' THEN 'ACUEDUCTO'
                  WHEN UPPER(BTRIM(servicio)) LIKE '%ALCANT%' THEN 'ALCANTARILLADO'
                  WHEN UPPER(BTRIM(servicio)) LIKE '%ASEO%' THEN 'ASEO'
                  ELSE UPPER(BTRIM(translate(servicio,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn')))
                END AS servicio,
                CASE
                  WHEN UPPER(BTRIM(estado)) IN ('OPERATIVA','EN OPERACION','EN OPERACIÓN','ACTIVA') THEN 'OPERATIVA'
                  WHEN UPPER(BTRIM(estado)) LIKE 'SUSPEN%' THEN 'SUSPENDIDA'
                  WHEN COALESCE(estado,'') = '' THEN 'OTRO'
                  ELSE 'OTRO'
                END AS estado,
                NULLIF(UPPER(BTRIM(translate(clasificacion,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))),'') AS clasificacion
            FROM stg_api
            WHERE nombre IS NOT NULL
              AND departamento_prestacion IS NOT NULL
              AND municipio_prestacion IS NOT NULL
              AND servicio IS NOT NULL;
        """))

        # Dominio de departamento
        conn.execute(text(f"""
            DELETE FROM clean_staging
            WHERE departamento NOT IN ({DEP_SQL});
        """))

        # Deduplicación (1ra pasada)
        conn.execute(text("""
            WITH ranked AS (
              SELECT
                ctid,
                ROW_NUMBER() OVER (
                  PARTITION BY provider_id, servicio, departamento, municipio
                  ORDER BY provider_id
                ) AS rn
              FROM clean_staging
            )
            DELETE FROM clean_staging cs
            USING ranked r
            WHERE cs.ctid = r.ctid
              AND r.rn > 1;
        """))

        # Imputación estado (moda por servicio,departamento) → fallback 'OTRO'
        conn.execute(text("""
            WITH moda AS (
              SELECT servicio, departamento, estado,
                     ROW_NUMBER() OVER (PARTITION BY servicio, departamento ORDER BY COUNT(*) DESC, estado) AS rn
              FROM clean_staging
              WHERE estado IS NOT NULL AND estado <> ''
              GROUP BY servicio, departamento, estado
            )
            UPDATE clean_staging cs
            SET estado = m.estado
            FROM moda m
            WHERE cs.estado IS NULL
              AND cs.servicio = m.servicio
              AND cs.departamento = m.departamento
              AND m.rn = 1;
        """))
        conn.execute(text("UPDATE clean_staging SET estado = 'OTRO' WHERE estado IS NULL OR estado = '';"))

        # Imputación clasificacion (moda por servicio)
        conn.execute(text("""
            WITH moda_c AS (
              SELECT servicio, clasificacion,
                     ROW_NUMBER() OVER (PARTITION BY servicio ORDER BY COUNT(*) DESC, clasificacion) AS rn
              FROM clean_staging
              WHERE clasificacion IS NOT NULL AND clasificacion <> ''
              GROUP BY servicio, clasificacion
            )
            UPDATE clean_staging cs
            SET clasificacion = m.clasificacion
            FROM moda_c m
            WHERE (cs.clasificacion IS NULL OR cs.clasificacion = '')
              AND cs.servicio = m.servicio
              AND m.rn = 1;
        """))

        # Purgar filas con claves nulas/vacías (bloque clave)
        conn.execute(text("""
            DELETE FROM clean_staging
            WHERE provider_id IS NULL OR provider_id = ''
               OR nombre      IS NULL OR nombre      = ''
               OR departamento IS NULL OR departamento = ''
               OR municipio    IS NULL OR municipio    = ''
               OR servicio     IS NULL OR servicio     = '';
        """))

        # Re-deduplicar por seguridad (2da pasada)
        conn.execute(text("""
            WITH ranked AS (
              SELECT
                ctid,
                ROW_NUMBER() OVER (
                  PARTITION BY provider_id, servicio, departamento, municipio
                  ORDER BY provider_id
                ) AS rn
              FROM clean_staging
            )
            DELETE FROM clean_staging cs
            USING ranked r
            WHERE cs.ctid = r.ctid
              AND r.rn > 1;
        """))

        # Vista + índices
        conn.execute(text("""
            CREATE OR REPLACE VIEW v_clean_preview AS
            SELECT provider_id, nombre, departamento, municipio, clasificacion, servicio, estado
            FROM clean_staging;
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_staging_pk   ON clean_staging(provider_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_staging_key  ON clean_staging(provider_id, servicio, departamento, municipio);"))

        # =========================================================================
        # B) CALIDAD DE AGUA: clean_calidad
        # =========================================================================
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS clean_calidad (
                departamento    TEXT NOT NULL,
                municipio       TEXT NOT NULL,
                fecha_muestra   DATE NOT NULL,
                parametro       TEXT NOT NULL,
                valor           DOUBLE PRECISION,
                unidad          TEXT,
                nombre_punto    TEXT,
                latitud         DOUBLE PRECISION,
                longitud        DOUBLE PRECISION
            );
        """))
        conn.execute(text("DELETE FROM clean_calidad;"))

        # Inserción desde stg_new con normalización y casting seguro
        conn.execute(text(f"""
            WITH src AS (
              SELECT
                departamento::text                  AS departamento_t,
                municipio::text                     AS municipio_t,
                fecha::text                         AS fecha_t,
                propiedad_observada::text           AS parametro_t,
                COALESCE(resultado::text,'')        AS resultado_t,
                unidad_del_resultado::text          AS unidad_t,
                nombre_del_punto_de_monitoreo::text AS nombre_punto_t,
                latitud::text                       AS latitud_t,
                longitud::text                      AS longitud_t
              FROM stg_new
            )
            INSERT INTO clean_calidad(
                departamento, municipio, fecha_muestra, parametro, valor, unidad,
                nombre_punto, latitud, longitud
            )
            SELECT
              UPPER(BTRIM(translate(departamento_t,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))) AS departamento,
              UPPER(BTRIM(translate(municipio_t,   'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))) AS municipio,
              COALESCE(
                CASE
                  WHEN fecha_t ~ '^[0-9]{{4}}\\s+[A-Za-z]{{3}}\\s+[0-9]{{2}}\\s+[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}\\s+(AM|PM)$'
                    THEN to_timestamp(fecha_t, 'YYYY Mon DD HH12:MI:SS AM')::date
                  ELSE NULL
                END,
                NULLIF(BTRIM(fecha_t),'')::date
              ) AS fecha_muestra,
              UPPER(BTRIM(translate(parametro_t,'ÁÉÍÓÚÄËÏÖÜáéíóúäëïöüÑñ','AEIOUAEIOUaeiouaeiouNn'))) AS parametro,
              NULLIF(regexp_replace(resultado_t, '[^0-9\\.\\-]', '', 'g'),'')::double precision AS valor,
              NULLIF(BTRIM(unidad_t),'')       AS unidad,
              NULLIF(BTRIM(nombre_punto_t),'') AS nombre_punto,
              CASE
                WHEN NULLIF(BTRIM(latitud_t),'') IS NULL THEN NULL
                ELSE
                  CASE
                    WHEN (NULLIF(BTRIM(latitud_t),'')::double precision) BETWEEN -5 AND 15
                      THEN NULLIF(BTRIM(latitud_t),'')::double precision
                    ELSE NULL
                  END
              END AS latitud,
              CASE
                WHEN NULLIF(BTRIM(longitud_t),'') IS NULL THEN NULL
                ELSE
                  CASE
                    WHEN (NULLIF(BTRIM(longitud_t),'')::double precision) BETWEEN -82 AND -66
                      THEN NULLIF(BTRIM(longitud_t),'')::double precision
                    ELSE NULL
                  END
              END AS longitud
            FROM src
            WHERE
              NULLIF(BTRIM(departamento_t),'') IS NOT NULL
              AND NULLIF(BTRIM(municipio_t),'')  IS NOT NULL
              AND (
                (fecha_t ~ '^[0-9]{{4}}\\s+[A-Za-z]{{3}}\\s+[0-9]{{2}}\\s+[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}\\s+(AM|PM)$')
                OR (NULLIF(BTRIM(fecha_t),'') IS NOT NULL)
              )
              AND NULLIF(BTRIM(parametro_t),'') IS NOT NULL;
        """))

        # Dominio de depto + rango de fecha
        conn.execute(text(f"DELETE FROM clean_calidad WHERE departamento NOT IN ({DEP_SQL});"))
        conn.execute(text("DELETE FROM clean_calidad WHERE fecha_muestra < DATE '2000-01-01' OR fecha_muestra > CURRENT_DATE;"))

        # Pareo de nulidad lat/lon
        conn.execute(text("""
            UPDATE clean_calidad
            SET latitud = NULL, longitud = NULL
            WHERE (latitud IS NULL) <> (longitud IS NULL);
        """))

        # Reglas por parámetro: rangos razonables → fuera de rango = NULL (para imputar)
        conn.execute(text("""
            -- pH 0..14
            UPDATE clean_calidad
            SET valor = NULL
            WHERE parametro = 'PH' AND (valor < 0 OR valor > 14);

            -- Cloro libre/residual ~ 0..5
            UPDATE clean_calidad
            SET valor = NULL
            WHERE parametro LIKE 'CLORO%' AND (valor < 0 OR valor > 5);

            -- No negativos para parámetros frecuentes
            UPDATE clean_calidad
            SET valor = NULL
            WHERE parametro IN ('TURBIDEZ','CONDUCTIVIDAD','DUREZA','ALCALINIDAD')
              AND valor < 0;
        """))

        # Deduplicación por (dep, muni, parametro, fecha, nombre_punto)
        conn.execute(text("""
            WITH ranked AS (
              SELECT
                ctid,
                ROW_NUMBER() OVER (
                  PARTITION BY departamento, municipio, parametro, fecha_muestra, COALESCE(nombre_punto,'')
                  ORDER BY departamento
                ) AS rn
              FROM clean_calidad
            )
            DELETE FROM clean_calidad c
            USING ranked r
            WHERE c.ctid = r.ctid
              AND r.rn > 1;
        """))

        # Imputación: UNIDAD = moda por parametro
        conn.execute(text("""
            WITH moda_u AS (
              SELECT parametro, unidad,
                     ROW_NUMBER() OVER (PARTITION BY parametro ORDER BY COUNT(*) DESC, unidad) AS rn
              FROM clean_calidad
              WHERE unidad IS NOT NULL AND unidad <> ''
              GROUP BY parametro, unidad
            )
            UPDATE clean_calidad c
            SET unidad = m.unidad
            FROM moda_u m
            WHERE (c.unidad IS NULL OR c.unidad = '')
              AND c.parametro = m.parametro
              AND m.rn = 1;
        """))

        # Imputación: VALOR = mediana por (parametro, departamento) → fallback mediana global por parametro
        conn.execute(text("""
            WITH med AS (
              SELECT parametro, departamento,
                     percentile_disc(0.5) WITHIN GROUP (ORDER BY valor) AS mediana
              FROM clean_calidad
              WHERE valor IS NOT NULL
              GROUP BY parametro, departamento
            )
            UPDATE clean_calidad c
            SET valor = m.mediana
            FROM med m
            WHERE c.valor IS NULL
              AND c.parametro = m.parametro
              AND c.departamento = m.departamento;
        """))
        conn.execute(text("""
            WITH med_global AS (
              SELECT parametro,
                     percentile_disc(0.5) WITHIN GROUP (ORDER BY valor) AS mediana_g
              FROM clean_calidad
              WHERE valor IS NOT NULL
              GROUP BY parametro
            )
            UPDATE clean_calidad c
            SET valor = mg.mediana_g
            FROM med_global mg
            WHERE c.valor IS NULL
              AND c.parametro = mg.parametro;
        """))

        # Vista e índices (preview)
        conn.execute(text("""
            CREATE OR REPLACE VIEW v_clean_calidad_preview AS
            SELECT departamento, municipio, fecha_muestra, parametro, valor, unidad
            FROM clean_calidad;
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_calidad_geo_fecha ON clean_calidad(departamento, municipio, fecha_muestra);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_calidad_parametro ON clean_calidad(parametro);"))

        # (OPCIONAL) Vista agregada si luego decides fact a nivel municipio/día/parámetro:
        conn.execute(text("""
            CREATE OR REPLACE VIEW v_clean_calidad_agg AS
            SELECT
              departamento,
              municipio,
              fecha_muestra,
              parametro,
              percentile_disc(0.5) WITHIN GROUP (ORDER BY valor) AS valor_mediana,
              (ARRAY(
                 SELECT u FROM (
                   SELECT unidad AS u, COUNT(*) c
                   FROM clean_calidad c2
                   WHERE c2.departamento = c.departamento
                     AND c2.municipio    = c.municipio
                     AND c2.fecha_muestra= c.fecha_muestra
                     AND c2.parametro    = c.parametro
                     AND c2.unidad IS NOT NULL AND c2.unidad <> ''
                   GROUP BY unidad
                   ORDER BY COUNT(*) DESC, unidad
                   LIMIT 1
                 ) x
              ))[1] AS unidad_moda
            FROM clean_calidad c
            GROUP BY 1,2,3,4;
        """))

        # =========================================================================
        # Log final
        # =========================================================================
        n1 = conn.execute(text("SELECT COUNT(*) FROM clean_staging;")).scalar() or 0
        n2 = conn.execute(text("SELECT COUNT(*) FROM clean_calidad;")).scalar() or 0
        print(f"[transform] OK → clean_staging={{n1}} rows | clean_calidad={{n2}} rows")