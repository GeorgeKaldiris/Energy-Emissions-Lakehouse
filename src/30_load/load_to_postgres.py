import pandas as pd
from sqlalchemy import create_engine, text

# ------------------------------------------------------------
# PURPOSE
# ------------------------------------------------------------
# Load GOLD Parquet marts into PostgreSQL star schema.
#
# Steps:
# 1) Insert (upsert) dimension values (region, energy_source, sector, time)
# 2) Build mapping dicts: name -> id
# 3) Convert GOLD marts into fact tables using those IDs
# 4) Upsert fact rows using ON CONFLICT (idempotent loads)
# ------------------------------------------------------------

DB_URL = "postgresql+psycopg2://de_user:de_pass@localhost:5432/lakehouse"


def upsert_dim_table(conn, table: str, name_col: str, values):
    """Insert only missing values into a dimension table."""
    values = sorted(set(values))
    if not values:
        return

    df = pd.DataFrame({name_col: values})
    stg = f"stg_{table}"
    df.to_sql(stg, conn, if_exists="replace", index=False)

    conn.execute(text(f"""
        INSERT INTO {table} ({name_col})
        SELECT s.{name_col}
        FROM {stg} s
        LEFT JOIN {table} d ON d.{name_col} = s.{name_col}
        WHERE d.{name_col} IS NULL;
    """))

    conn.execute(text(f"DROP TABLE IF EXISTS {stg};"))


def upsert_dim_time(conn, year_month_pairs):
    """Insert only missing (year, month) into dim_time."""
    if not year_month_pairs:
        return

    df = pd.DataFrame(year_month_pairs, columns=["year", "month"]).drop_duplicates()
    df.to_sql("stg_dim_time", conn, if_exists="replace", index=False)

    conn.execute(text("""
        INSERT INTO dim_time (year, month)
        SELECT s.year, s.month
        FROM stg_dim_time s
        LEFT JOIN dim_time t ON t.year = s.year AND t.month = s.month
        WHERE t.time_id IS NULL;
    """))

    conn.execute(text("DROP TABLE IF EXISTS stg_dim_time;"))


def fetch_dim_map(conn, table: str, id_col: str, name_col: str):
    """Return dict mapping dimension value -> dimension id."""
    rows = conn.execute(text(f"SELECT {id_col}, {name_col} FROM {table};")).fetchall()
    return {r[1]: r[0] for r in rows}


def fetch_time_map(conn):
    """Return dict mapping (year, month) -> time_id."""
    rows = conn.execute(text("SELECT time_id, year, month FROM dim_time;")).fetchall()
    return {(r[1], r[2]): r[0] for r in rows}


def upsert_fact_energy(conn, df):
    df.to_sql("stg_fact_energy", conn, if_exists="replace", index=False)
    conn.execute(text("""
        INSERT INTO fact_energy_monthly
        (region_id, source_id, time_id, avg_consumption_mwh, max_consumption_mwh, avg_temp_c, records)
        SELECT region_id, source_id, time_id, avg_consumption_mwh, max_consumption_mwh, avg_temp_c, records
        FROM stg_fact_energy
        ON CONFLICT (region_id, source_id, time_id)
        DO UPDATE SET
            avg_consumption_mwh = EXCLUDED.avg_consumption_mwh,
            max_consumption_mwh = EXCLUDED.max_consumption_mwh,
            avg_temp_c = EXCLUDED.avg_temp_c,
            records = EXCLUDED.records;
    """))
    conn.execute(text("DROP TABLE IF EXISTS stg_fact_energy;"))


def upsert_fact_emissions(conn, df):
    df.to_sql("stg_fact_emissions", conn, if_exists="replace", index=False)
    conn.execute(text("""
        INSERT INTO fact_emissions_monthly
        (region_id, sector_id, time_id, avg_co2_tonnes, total_co2_tonnes, records)
        SELECT region_id, sector_id, time_id, avg_co2_tonnes, total_co2_tonnes, records
        FROM stg_fact_emissions
        ON CONFLICT (region_id, sector_id, time_id)
        DO UPDATE SET
            avg_co2_tonnes = EXCLUDED.avg_co2_tonnes,
            total_co2_tonnes = EXCLUDED.total_co2_tonnes,
            records = EXCLUDED.records;
    """))
    conn.execute(text("DROP TABLE IF EXISTS stg_fact_emissions;"))


def upsert_fact_intensity(conn, df):
    df.to_sql("stg_fact_intensity", conn, if_exists="replace", index=False)
    conn.execute(text("""
        INSERT INTO fact_carbon_intensity
        (region_id, time_id, total_energy_mwh, total_co2_tonnes, co2_per_mwh)
        SELECT region_id, time_id, total_energy_mwh, total_co2_tonnes, co2_per_mwh
        FROM stg_fact_intensity
        ON CONFLICT (region_id, time_id)
        DO UPDATE SET
            total_energy_mwh = EXCLUDED.total_energy_mwh,
            total_co2_tonnes = EXCLUDED.total_co2_tonnes,
            co2_per_mwh = EXCLUDED.co2_per_mwh;
    """))
    conn.execute(text("DROP TABLE IF EXISTS stg_fact_intensity;"))


def main():
    # Load GOLD marts
    energy_m = pd.read_parquet("data/gold/energy_monthly.parquet")
    emis_m = pd.read_parquet("data/gold/emissions_monthly.parquet")
    intensity = pd.read_parquet("data/gold/carbon_intensity.parquet")

    engine = create_engine(DB_URL)

    with engine.begin() as conn:

        # 1) Upsert dimensions

        upsert_dim_table(conn, "dim_region", "region_name", energy_m["region"].tolist())
        upsert_dim_table(conn, "dim_energy_source", "source_name", energy_m["energy_source"].tolist())
        upsert_dim_table(conn, "dim_sector", "sector_name", emis_m["sector"].tolist())

        # 2) Upsert time dimension

        ym = set(zip(energy_m["year"], energy_m["month"])) \
             | set(zip(emis_m["year"], emis_m["month"])) \
             | set(zip(intensity["year"], intensity["month"]))
        upsert_dim_time(conn, list(ym))

        # 3) Build ID maps

        region_map = fetch_dim_map(conn, "dim_region", "region_id", "region_name")
        source_map = fetch_dim_map(conn, "dim_energy_source", "source_id", "source_name")
        sector_map = fetch_dim_map(conn, "dim_sector", "sector_id", "sector_name")
        time_map = fetch_time_map(conn)

        # 4) Convert Gold marts into fact rows (replace text with IDs)

        energy_fact = pd.DataFrame({
            "region_id": energy_m["region"].map(region_map),
            "source_id": energy_m["energy_source"].map(source_map),
            "time_id": list(zip(energy_m["year"], energy_m["month"])),
            "avg_consumption_mwh": energy_m["avg_consumption_mwh"],
            "max_consumption_mwh": energy_m["max_consumption_mwh"],
            "avg_temp_c": energy_m["avg_temp_c"],
            "records": energy_m["records"]
        })
        energy_fact["time_id"] = energy_fact["time_id"].map(time_map)

        emissions_fact = pd.DataFrame({
            "region_id": emis_m["region"].map(region_map),
            "sector_id": emis_m["sector"].map(sector_map),
            "time_id": list(zip(emis_m["year"], emis_m["month"])),
            "avg_co2_tonnes": emis_m["avg_co2_tonnes"],
            "total_co2_tonnes": emis_m["total_co2_tonnes"],
            "records": emis_m["records"]
        })
        emissions_fact["time_id"] = emissions_fact["time_id"].map(time_map)

        intensity_fact = pd.DataFrame({
            "region_id": intensity["region"].map(region_map),
            "time_id": list(zip(intensity["year"], intensity["month"])),
            "total_energy_mwh": intensity["total_energy_mwh"],
            "total_co2_tonnes": intensity["total_co2_tonnes"],
            "co2_per_mwh": intensity["co2_per_mwh"]
        })
        intensity_fact["time_id"] = intensity_fact["time_id"].map(time_map)

        # 5) Fail fast if any mapping produced NULLs

        for name, df in [("energy_fact", energy_fact), ("emissions_fact", emissions_fact), ("intensity_fact", intensity_fact)]:
            if df.isna().any().any():
                bad_cols = df.columns[df.isna().any()].tolist()
                raise ValueError(f"{name} has NULLs after ID mapping. Bad columns: {bad_cols}")

        # 6) Upsert facts
        
        upsert_fact_energy(conn, energy_fact)
        upsert_fact_emissions(conn, emissions_fact)
        upsert_fact_intensity(conn, intensity_fact)

    print("✅ Loaded GOLD marts into PostgreSQL.")


if __name__ == "__main__":
    main()
