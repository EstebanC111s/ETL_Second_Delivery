-- /opt/airflow/sql/merge_pipeline.sql
-- Combina stg_old + stg_api -> clean_staging (con contacto), normaliza y usa "DESCONOCIDO".

BEGIN;

-- 0) Asegurar tabla destino (si no existe) con todas las columnas
CREATE TABLE IF NOT EXISTS clean_staging (
  provider_id   TEXT,
  nombre        TEXT,
  departamento  TEXT,
  municipio     TEXT,
  servicio      TEXT,
  estado        TEXT,
  clasificacion TEXT,
  direccion     TEXT,
  telefono      TEXT,
  email         TEXT
);

-- 0.1) Asegurar columnas de contacto aunque la tabla ya existiera
ALTER TABLE clean_staging
  ADD COLUMN IF NOT EXISTS direccion TEXT,
  ADD COLUMN IF NOT EXISTS telefono  TEXT,
  ADD COLUMN IF NOT EXISTS email     TEXT;

-- 1) Limpiar tabla destino
TRUNCATE TABLE clean_staging;

-- 2) Insertar desde stg_old (normalización + fallback prestacion->domicilio + contacto)
INSERT INTO clean_staging (
  provider_id, nombre, departamento, municipio, servicio, estado, clasificacion,
  direccion, telefono, email
)
SELECT
  COALESCE(
    NULLIF(nit,''),
    md5(
      COALESCE(NULLIF(TRIM(nombre), ''), 'DESCONOCIDO') || '|' ||
      COALESCE(NULLIF(UPPER(TRIM(departamento_prestacion)), ''),
               NULLIF(UPPER(TRIM(departamento_domicilio)), ''),
               'DESCONOCIDO') || '|' ||
      COALESCE(NULLIF(UPPER(TRIM(municipio_prestacion)), ''),
               NULLIF(UPPER(TRIM(municipio_domicilio)), ''),
               'DESCONOCIDO') || '|' ||
      COALESCE(NULLIF(UPPER(TRIM(servicio)), ''), 'DESCONOCIDO')
    )
  ) AS provider_id,
  COALESCE(NULLIF(TRIM(COALESCE(nombre,'')), ''), 'DESCONOCIDO')             AS nombre,
  COALESCE(NULLIF(UPPER(TRIM(departamento_prestacion)), ''),
           NULLIF(UPPER(TRIM(departamento_domicilio)), ''),
           'DESCONOCIDO')                                                   AS departamento,
  COALESCE(NULLIF(UPPER(TRIM(municipio_prestacion)), ''),
           NULLIF(UPPER(TRIM(municipio_domicilio)), ''),
           'DESCONOCIDO')                                                   AS municipio,
  COALESCE(NULLIF(UPPER(TRIM(COALESCE(servicio,''))), ''), 'DESCONOCIDO')   AS servicio,
  NULLIF(UPPER(TRIM(COALESCE(estado,''))),   '')                            AS estado,
  NULLIF(UPPER(TRIM(COALESCE(clasificacion,''))), '')                       AS clasificacion,
  NULLIF(TRIM(CAST(direccion AS TEXT)), '')                                 AS direccion,
  NULLIF(TRIM(CAST(telefono  AS TEXT)), '')                                 AS telefono,
  NULLIF(TRIM(CAST(email     AS TEXT)), '')                                 AS email
FROM stg_old;

-- 3) Insertar desde stg_api (misma normalización/fallback; contacto NULL)
INSERT INTO clean_staging (
  provider_id, nombre, departamento, municipio, servicio, estado, clasificacion,
  direccion, telefono, email
)
SELECT
  md5(
    COALESCE(NULLIF(TRIM(nombre), ''), 'DESCONOCIDO') || '|' ||
    COALESCE(NULLIF(UPPER(TRIM(departamento_prestacion)), ''),
             NULLIF(UPPER(TRIM(departamento_domicilio)), ''),
             'DESCONOCIDO') || '|' ||
    COALESCE(NULLIF(UPPER(TRIM(municipio_prestacion)), ''),
             NULLIF(UPPER(TRIM(municipio_domicilio)), ''),
             'DESCONOCIDO') || '|' ||
    COALESCE(NULLIF(UPPER(TRIM(servicio)), ''), 'DESCONOCIDO')
  ) AS provider_id,
  COALESCE(NULLIF(TRIM(COALESCE(nombre,'')), ''), 'DESCONOCIDO')           AS nombre,
  COALESCE(NULLIF(UPPER(TRIM(departamento_prestacion)), ''),
           NULLIF(UPPER(TRIM(departamento_domicilio)), ''),
           'DESCONOCIDO')                                                 AS departamento,
  COALESCE(NULLIF(UPPER(TRIM(municipio_prestacion)), ''),
           NULLIF(UPPER(TRIM(municipio_domicilio)), ''),
           'DESCONOCIDO')                                                 AS municipio,
  COALESCE(NULLIF(UPPER(TRIM(COALESCE(servicio,''))), ''), 'DESCONOCIDO') AS servicio,
  NULLIF(UPPER(TRIM(COALESCE(estado,''))),   '')                          AS estado,
  NULLIF(UPPER(TRIM(COALESCE(clasificacion,''))), '')                     AS clasificacion,
  NULL::TEXT AS direccion,
  NULL::TEXT AS telefono,
  NULL::TEXT AS email
FROM stg_api;

-- 4) Saneo defensivo de claves
UPDATE clean_staging
SET departamento = COALESCE(departamento, 'DESCONOCIDO'),
    municipio    = COALESCE(municipio,    'DESCONOCIDO'),
    servicio     = COALESCE(servicio,     'DESCONOCIDO'),
    nombre       = COALESCE(NULLIF(TRIM(nombre), ''), 'DESCONOCIDO');

-- 5) Deduplicar por (provider_id, servicio, departamento, municipio)
DELETE FROM clean_staging cs
USING (
  SELECT provider_id, servicio, departamento, municipio, MIN(ctid) AS keep_ctid
  FROM clean_staging
  GROUP BY provider_id, servicio, departamento, municipio
) k
WHERE cs.provider_id = k.provider_id
  AND cs.servicio    = k.servicio
  AND cs.departamento= k.departamento
  AND cs.municipio   = k.municipio
  AND cs.ctid <> k.keep_ctid;

-- 6) Vista de previsualización
CREATE OR REPLACE VIEW v_clean_preview AS
SELECT provider_id, nombre, departamento, municipio, clasificacion, servicio, estado
FROM clean_staging;

COMMIT;
