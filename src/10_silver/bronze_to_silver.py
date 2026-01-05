import pandas as pd

# ------------------------------------------------------------
# PURPOSE
# ------------------------------------------------------------
# Convert BRONZE (raw CSV) -> SILVER (clean Parquet).
#
# SILVER layer includes:
# - Deduplication
# - Correct data types (date/numeric)
# - Basic validation rules (ranges)
# - Missing value handling
# - Stored as Parquet for better analytics performance
# ------------------------------------------------------------

def clean_energy(path_in: str, path_out: str) -> None:
    # Read raw energy data (Bronze)
    df = pd.read_csv(path_in)

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Convert date string -> datetime (invalid becomes NaT)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Clean categorical text fields
    df["region"] = df["region"].astype(str).str.strip()
    df["energy_source"] = df["energy_source"].astype(str).str.strip()

    # Convert numeric fields (bad strings like "N/A" become NaN)
    df["consumption_mwh"] = pd.to_numeric(df["consumption_mwh"], errors="coerce")
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")

    # Basic validation rules (data quality filters)
    df = df[df["temperature_c"].between(-40, 60)]
    df = df[df["consumption_mwh"].between(0, 2_000_000)]

    # Handle missing consumption using median (robust to outliers)
    df["consumption_mwh"] = df["consumption_mwh"].fillna(df["consumption_mwh"].median())

    # Drop rows with invalid/missing dates
    df = df.dropna(subset=["date"])

    # Save to Silver layer as Parquet
    df.to_parquet(path_out, index=False)


def clean_emissions(path_in: str, path_out: str) -> None:
    # Read raw emissions data (Bronze)
    df = pd.read_csv(path_in)

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Convert date string -> datetime (invalid becomes NaT)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Clean categorical text fields
    df["region"] = df["region"].astype(str).str.strip()
    df["sector"] = df["sector"].astype(str).str.strip()

    # Convert numeric field (bad strings become NaN)
    df["co2_tonnes"] = pd.to_numeric(df["co2_tonnes"], errors="coerce")

    # Basic validation rule
    df = df[df["co2_tonnes"].between(0, 5_000_000)]

    # Handle missing CO2 using median
    df["co2_tonnes"] = df["co2_tonnes"].fillna(df["co2_tonnes"].median())

    # Drop rows with invalid/missing dates
    df = df.dropna(subset=["date"])

    # Save to Silver layer as Parquet
    df.to_parquet(path_out, index=False)


if __name__ == "__main__":
    clean_energy("data/bronze/energy_raw.csv", "data/silver/energy_silver.parquet")
    clean_emissions("data/bronze/emissions_raw.csv", "data/silver/emissions_silver.parquet")

    print("✅ Silver layer created:")
    print(" - data/silver/energy_silver.parquet")
    print(" - data/silver/emissions_silver.parquet")
