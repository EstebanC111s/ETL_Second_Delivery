BEGIN;

CREATE TABLE IF NOT EXISTS dim_calidad_geo (
  departamento TEXT NOT NULL,
  municipio    TEXT NOT NULL,
  fecha_ult_muestra DATE,
  -- estados básicos por parámetro clave (añade más si quieres)
  estado_ph     TEXT,   -- OK / ALERTA / SIN_DATO
  estado_cloro  TEXT,   -- OK / ALERTA / SIN_DATO
  -- métricas
  puntos_monitoreo INT,
  mediciones INT,
  parametros_distintos INT,
  PRIMARY KEY (departamento, municipio)
);

WITH base AS (
  SELECT
    UPPER(TRIM(departamento))  AS departamento,
    UPPER(TRIM(municipio))     AS municipio,
    parametro,
    valor,
    fecha_muestra              AS fecha
  FROM clean_calidad
  WHERE departamento IS NOT NULL AND municipio IS NOT NULL
),
ult AS (
  SELECT departamento, municipio, MAX(fecha) AS fecha_ult_muestra
  FROM base
  GROUP BY 1,2
),
m_ph AS (
  SELECT b.departamento, b.municipio,
         CASE
           WHEN MAX(CASE WHEN b.parametro ILIKE '%PH%' THEN 1 END) IS NULL THEN 'SIN_DATO'
           WHEN EXISTS (
             SELECT 1 FROM base x
             WHERE x.departamento=b.departamento AND x.municipio=b.municipio
               AND x.parametro ILIKE '%PH%'
               AND (x.valor < 0 OR x.valor > 14)
           ) THEN 'ALERTA'
           ELSE 'OK'
         END AS estado_ph
  FROM base b GROUP BY 1,2
),
m_cl AS (
  SELECT b.departamento, b.municipio,
         CASE
           WHEN MAX(CASE WHEN b.parametro ILIKE '%CLORO%' THEN 1 END) IS NULL THEN 'SIN_DATO'
           WHEN EXISTS (
             SELECT 1 FROM base x
             WHERE x.departamento=b.departamento AND x.municipio=b.municipio
               AND x.parametro ILIKE '%CLORO%' AND (x.valor < 0 OR x.valor > 5)
           ) THEN 'ALERTA'
           ELSE 'OK'
         END AS estado_cloro
  FROM base b GROUP BY 1,2
),
agg AS (
  SELECT
    departamento, municipio,
    COUNT(*)                                  AS mediciones,
    COUNT(DISTINCT parametro)                 AS parametros_distintos,
    COUNT(DISTINCT COALESCE(nombre_punto,'')) AS puntos_monitoreo
  FROM clean_calidad
  GROUP BY 1,2
)
INSERT INTO dim_calidad_geo AS d
(departamento, municipio, fecha_ult_muestra, estado_ph, estado_cloro, puntos_monitoreo, mediciones, parametros_distintos)
SELECT
  u.departamento, u.municipio, u.fecha_ult_muestra,
  p.estado_ph, c.estado_cloro,
  COALESCE(a.puntos_monitoreo,0), COALESCE(a.mediciones,0), COALESCE(a.parametros_distintos,0)
FROM ult u
LEFT JOIN m_ph p USING(departamento, municipio)
LEFT JOIN m_cl c USING(departamento, municipio)
LEFT JOIN agg  a USING(departamento, municipio)
ON CONFLICT (departamento, municipio) DO UPDATE SET
  fecha_ult_muestra    = EXCLUDED.fecha_ult_muestra,
  estado_ph            = EXCLUDED.estado_ph,
  estado_cloro         = EXCLUDED.estado_cloro,
  puntos_monitoreo     = EXCLUDED.puntos_monitoreo,
  mediciones           = EXCLUDED.mediciones,
  parametros_distintos = EXCLUDED.parametros_distintos;

COMMIT;
