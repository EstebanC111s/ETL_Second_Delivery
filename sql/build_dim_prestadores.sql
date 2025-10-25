BEGIN;

CREATE TABLE IF NOT EXISTS dim_prestadores (
  departamento TEXT NOT NULL,
  municipio    TEXT NOT NULL,
  provider_id  TEXT NOT NULL,
  nombre       TEXT,
  servicio     TEXT,
  estado       TEXT,
  clasificacion TEXT,
  direccion    TEXT,
  telefono     TEXT,
  email        TEXT,
  es_publica   BOOLEAN,   -- se llenará luego (clasificación pública/privada)
  PRIMARY KEY (departamento, municipio, provider_id)
);

WITH src AS (
  SELECT
    UPPER(TRIM(departamento)) AS departamento,
    UPPER(TRIM(municipio))    AS municipio,
    provider_id,
    MAX(nombre)        AS nombre,
    MAX(servicio)      AS servicio,
    MAX(estado)        AS estado,
    MAX(clasificacion) AS clasificacion,
    MAX(direccion)     AS direccion,
    MAX(CAST(telefono AS TEXT)) AS telefono,
    MAX(email)         AS email
  FROM clean_staging
  WHERE departamento IS NOT NULL AND municipio IS NOT NULL AND provider_id IS NOT NULL
  GROUP BY 1,2,3
)
INSERT INTO dim_prestadores AS d
(departamento, municipio, provider_id, nombre, servicio, estado, clasificacion, direccion, telefono, email, es_publica)
SELECT
  departamento, municipio, provider_id, nombre, servicio, estado, clasificacion, direccion, telefono, email,
  NULL::BOOLEAN AS es_publica
FROM src
ON CONFLICT (departamento, municipio, provider_id) DO UPDATE SET
  nombre        = EXCLUDED.nombre,
  servicio      = EXCLUDED.servicio,
  estado        = EXCLUDED.estado,
  clasificacion = EXCLUDED.clasificacion,
  direccion     = EXCLUDED.direccion,
  telefono      = EXCLUDED.telefono,
  email         = EXCLUDED.email;

COMMIT;
