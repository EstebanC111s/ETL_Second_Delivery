-- sql/build_dim_geo.sql
-- Crea la dimensión geográfica central y la puebla desde las tablas limpias
BEGIN;

CREATE TABLE IF NOT EXISTS dim_geo (
  departamento TEXT NOT NULL,
  municipio    TEXT NOT NULL,
  CONSTRAINT pk_dim_geo PRIMARY KEY (departamento, municipio)
);

-- Población inicial desde clean_* (normalmente ya normalizadas en transform)
WITH geos AS (
  SELECT DISTINCT departamento, municipio FROM clean_staging
  UNION
  SELECT DISTINCT departamento, municipio FROM clean_calidad
)
INSERT INTO dim_geo (departamento, municipio)
SELECT g.departamento, g.municipio
FROM geos g
ON CONFLICT (departamento, municipio) DO NOTHING;

COMMIT;
