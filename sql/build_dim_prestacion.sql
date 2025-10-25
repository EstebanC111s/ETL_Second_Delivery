BEGIN;

CREATE TABLE IF NOT EXISTS dim_prestacion_geo (
  departamento TEXT NOT NULL,
  municipio    TEXT NOT NULL,
  total_prestadores INT,
  acueducto_total INT,
  alcantarillado_total INT,
  aseo_total INT,
  operativos_total INT,
  suspendidos_total INT,
  PRIMARY KEY (departamento, municipio)
);

WITH prov AS (
  SELECT DISTINCT
    UPPER(TRIM(departamento)) AS departamento,
    UPPER(TRIM(municipio))    AS municipio,
    UPPER(TRIM(COALESCE(servicio,''))) AS servicio,
    UPPER(TRIM(COALESCE(estado,'')))   AS estado,
    provider_id
  FROM clean_staging
  WHERE departamento IS NOT NULL AND municipio IS NOT NULL AND provider_id IS NOT NULL
),
agg AS (
  SELECT
    departamento, municipio,
    COUNT(DISTINCT provider_id) AS total_prestadores,
    COUNT(DISTINCT provider_id) FILTER (WHERE servicio LIKE '%ACUED%')  AS acueducto_total,
    COUNT(DISTINCT provider_id) FILTER (WHERE servicio LIKE '%ALCANT%') AS alcantarillado_total,
    COUNT(DISTINCT provider_id) FILTER (WHERE servicio LIKE '%ASEO%')   AS aseo_total,
    COUNT(DISTINCT provider_id) FILTER (WHERE estado   LIKE '%OPER%')   AS operativos_total,
    COUNT(DISTINCT provider_id) FILTER (WHERE estado   LIKE '%SUSP%')   AS suspendidos_total
  FROM prov
  GROUP BY 1,2
)
INSERT INTO dim_prestacion_geo AS d
(departamento, municipio, total_prestadores, acueducto_total, alcantarillado_total, aseo_total, operativos_total, suspendidos_total)
SELECT * FROM agg
ON CONFLICT (departamento, municipio) DO UPDATE SET
  total_prestadores    = EXCLUDED.total_prestadores,
  acueducto_total      = EXCLUDED.acueducto_total,
  alcantarillado_total = EXCLUDED.alcantarillado_total,
  aseo_total           = EXCLUDED.aseo_total,
  operativos_total     = EXCLUDED.operativos_total,
  suspendidos_total    = EXCLUDED.suspendidos_total;

COMMIT;
