
-- 1) Quick sanity checks (row counts)

SELECT 'dim_region' AS table_name, COUNT(*) AS rows FROM dim_region
UNION ALL SELECT 'dim_energy_source', COUNT(*) FROM dim_energy_source
UNION ALL SELECT 'dim_sector', COUNT(*) FROM dim_sector
UNION ALL SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL SELECT 'fact_energy_monthly', COUNT(*) FROM fact_energy_monthly
UNION ALL SELECT 'fact_emissions_monthly', COUNT(*) FROM fact_emissions_monthly
UNION ALL SELECT 'fact_carbon_intensity', COUNT(*) FROM fact_carbon_intensity;

-- 2) Monthly carbon intensity by region (join dims + fact)

SELECT
  r.region_name,
  t.year,
  t.month,
  f.total_energy_mwh,
  f.total_co2_tonnes,
  f.co2_per_mwh
FROM fact_carbon_intensity f
JOIN dim_region r ON r.region_id = f.region_id
JOIN dim_time t ON t.time_id = f.time_id
ORDER BY r.region_name, t.year, t.month;

-- 3) Top 5 highest carbon intensity months per region (WINDOW)

WITH ranked AS (
  SELECT
    r.region_name,
    t.year,
    t.month,
    f.co2_per_mwh,
    ROW_NUMBER() OVER (
      PARTITION BY r.region_name
      ORDER BY f.co2_per_mwh DESC NULLS LAST
    ) AS rn
  FROM fact_carbon_intensity f
  JOIN dim_region r ON r.region_id = f.region_id
  JOIN dim_time t ON t.time_id = f.time_id
)
SELECT *
FROM ranked
WHERE rn <= 5
ORDER BY region_name, rn;

-- 4) Average monthly energy consumption per source (CTE + join)

SELECT
  s.source_name,
  ROUND(AVG(f.avg_consumption_mwh)::numeric, 2) AS avg_monthly_consumption_mwh
FROM fact_energy_monthly f
JOIN dim_energy_source s ON s.source_id = f.source_id
GROUP BY s.source_name
ORDER BY avg_monthly_consumption_mwh DESC;

-- 5) Compare emissions vs energy: join marts via region + time

SELECT
  r.region_name,
  t.year,
  t.month,
  ci.total_energy_mwh,
  ci.total_co2_tonnes,
  ci.co2_per_mwh
FROM fact_carbon_intensity ci
JOIN dim_region r ON r.region_id = ci.region_id
JOIN dim_time t ON t.time_id = ci.time_id
ORDER BY t.year, t.month, r.region_name;

-- 6) Data quality check: look for NULL intensity values

SELECT
  COUNT(*) AS null_intensity_rows
FROM fact_carbon_intensity
WHERE co2_per_mwh IS NULL;

-- 7) Optimization mindset: check query plan (EXPLAIN)

SELECT
  r.region_name,
  t.year,
  t.month,
  f.co2_per_mwh
FROM fact_carbon_intensity f
JOIN dim_region r ON r.region_id = f.region_id
JOIN dim_time t ON t.time_id = f.time_id
WHERE t.year = 2024
ORDER BY f.co2_per_mwh DESC;
