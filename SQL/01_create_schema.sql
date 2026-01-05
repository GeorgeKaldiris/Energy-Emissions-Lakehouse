-- DIMENSIONS
CREATE TABLE IF NOT EXISTS dim_region (
  region_id SERIAL PRIMARY KEY,
  region_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_energy_source (
  source_id SERIAL PRIMARY KEY,
  source_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_sector (
  sector_id SERIAL PRIMARY KEY,
  sector_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_time (
  time_id SERIAL PRIMARY KEY,
  year INT NOT NULL,
  month INT NOT NULL,
  UNIQUE(year, month)
);

-- FACTS
CREATE TABLE IF NOT EXISTS fact_energy_monthly (
  region_id INT NOT NULL REFERENCES dim_region(region_id),
  source_id INT NOT NULL REFERENCES dim_energy_source(source_id),
  time_id INT NOT NULL REFERENCES dim_time(time_id),
  avg_consumption_mwh DOUBLE PRECISION NOT NULL,
  max_consumption_mwh DOUBLE PRECISION NOT NULL,
  avg_temp_c DOUBLE PRECISION NOT NULL,
  records INT NOT NULL,
  PRIMARY KEY(region_id, source_id, time_id)
);

CREATE TABLE IF NOT EXISTS fact_emissions_monthly (
  region_id INT NOT NULL REFERENCES dim_region(region_id),
  sector_id INT NOT NULL REFERENCES dim_sector(sector_id),
  time_id INT NOT NULL REFERENCES dim_time(time_id),
  avg_co2_tonnes DOUBLE PRECISION NOT NULL,
  total_co2_tonnes DOUBLE PRECISION NOT NULL,
  records INT NOT NULL,
  PRIMARY KEY(region_id, sector_id, time_id)
);

CREATE TABLE IF NOT EXISTS fact_carbon_intensity (
  region_id INT NOT NULL REFERENCES dim_region(region_id),
  time_id INT NOT NULL REFERENCES dim_time(time_id),
  total_energy_mwh DOUBLE PRECISION NOT NULL,
  total_co2_tonnes DOUBLE PRECISION NOT NULL,
  co2_per_mwh DOUBLE PRECISION,
  PRIMARY KEY(region_id, time_id)
);

-- INDEXES (for basic query performance)
CREATE INDEX IF NOT EXISTS idx_energy_time ON fact_energy_monthly(time_id);
CREATE INDEX IF NOT EXISTS idx_emis_time ON fact_emissions_monthly(time_id);
CREATE INDEX IF NOT EXISTS idx_intensity_time ON fact_carbon_intensity(time_id);
