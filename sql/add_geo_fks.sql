-- sql/add_geo_fks.sql
-- Asegura que dim_geo cubre TODAS las geos de las dimensiones, agrega FKs y valida.

BEGIN;

-- 1) Asegurar que dim_geo contiene TODAS las combinaciones (dep,mun) de las dims
INSERT INTO dim_geo (departamento, municipio)
SELECT DISTINCT departamento, municipio FROM dim_prestadores
ON CONFLICT (departamento, municipio) DO NOTHING;

INSERT INTO dim_geo (departamento, municipio)
SELECT DISTINCT departamento, municipio FROM dim_prestacion_geo
ON CONFLICT (departamento, municipio) DO NOTHING;

INSERT INTO dim_geo (departamento, municipio)
SELECT DISTINCT departamento, municipio FROM dim_calidad_geo
ON CONFLICT (departamento, municipio) DO NOTHING;

-- 2) Eliminar FKs anteriores (si existen) y agregar FKs como NOT VALID
ALTER TABLE IF EXISTS dim_prestadores
  DROP CONSTRAINT IF EXISTS fk_dim_prestadores_geo;

ALTER TABLE IF EXISTS dim_prestadores
  ADD CONSTRAINT fk_dim_prestadores_geo
  FOREIGN KEY (departamento, municipio)
  REFERENCES dim_geo (departamento, municipio)
  NOT VALID;

ALTER TABLE IF EXISTS dim_prestacion_geo
  DROP CONSTRAINT IF EXISTS fk_dim_prestacion_geo;

ALTER TABLE IF EXISTS dim_prestacion_geo
  ADD CONSTRAINT fk_dim_prestacion_geo
  FOREIGN KEY (departamento, municipio)
  REFERENCES dim_geo (departamento, municipio)
  NOT VALID;

ALTER TABLE IF EXISTS dim_calidad_geo
  DROP CONSTRAINT IF EXISTS fk_dim_calidad_geo;

ALTER TABLE IF EXISTS dim_calidad_geo
  ADD CONSTRAINT fk_dim_calidad_geo
  FOREIGN KEY (departamento, municipio)
  REFERENCES dim_geo (departamento, municipio)
  NOT VALID;

COMMIT;

-- 3) Validar fuera de la transacción (menos locks; si algo falla, te dirá el detalle)
ALTER TABLE IF EXISTS dim_prestadores    VALIDATE CONSTRAINT fk_dim_prestadores_geo;
ALTER TABLE IF EXISTS dim_prestacion_geo VALIDATE CONSTRAINT fk_dim_prestacion_geo;
ALTER TABLE IF EXISTS dim_calidad_geo    VALIDATE CONSTRAINT fk_dim_calidad_geo;
