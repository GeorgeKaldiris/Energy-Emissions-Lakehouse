import pandas as pd

# ------------------------------------------------------------
# PURPOSE
# ------------------------------------------------------------
# Convert SILVER (clean Parquet) datasets into GOLD marts.
#
# GOLD layer means:
# - Aggregated, analytics-ready tables (marts)
# - Business KPIs
# - Stable structure that can be loaded into a warehouse (PostgreSQL)
#
# Inputs:
# - data/silver/energy_silver.parquet
# - data/silver/emissions_silver.parquet
#
# Outputs:
# - data/gold/energy_monthly.parquet
# - data/gold/emissions_monthly.parquet
# - data/gold/carbon_intensity.parquet
# ------------------------------------------------------------

if __name__ == "__main__":
    
    # Load Silver datasets
    energy = pd.read_parquet("data/silver/energy_silver.parquet")
    emissions = pd.read_parquet("data/silver/emissions_silver.parquet")

    # Add year/month columns (useful for monthly aggregations)
    for df in (energy, emissions):
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month

    # --------------------------------------------------------
    # GOLD MART 1: Monthly energy metrics by region & source
    # --------------------------------------------------------
    energy_monthly = (
        energy.groupby(["region", "energy_source", "year", "month"], as_index=False)
              .agg(
                  avg_consumption_mwh=("consumption_mwh", "mean"),
                  max_consumption_mwh=("consumption_mwh", "max"),
                  avg_temp_c=("temperature_c", "mean"),
                  records=("consumption_mwh", "size")
              )
    )

    # --------------------------------------------------------
    # GOLD MART 2: Monthly emissions metrics by region & sector
    # --------------------------------------------------------
    emissions_monthly = (
        emissions.groupby(["region", "sector", "year", "month"], as_index=False)
                 .agg(
                     avg_co2_tonnes=("co2_tonnes", "mean"),
                     total_co2_tonnes=("co2_tonnes", "sum"),
                     records=("co2_tonnes", "size")
                 )
    )

    # --------------------------------------------------------
    # GOLD MART 3: Carbon intensity KPI (CO2 per MWh)
    #   - First compute totals per month/region
    #   - Then join energy totals with emissions totals
    # --------------------------------------------------------
    energy_totals = (
        energy.groupby(["region", "year", "month"], as_index=False)
              .agg(total_energy_mwh=("consumption_mwh", "sum"))
    )

    emissions_totals = (
        emissions.groupby(["region", "year", "month"], as_index=False)
                 .agg(total_co2_tonnes=("co2_tonnes", "sum"))
    )

    carbon_intensity = energy_totals.merge(
        emissions_totals,
        on=["region", "year", "month"],
        how="inner"
    )

    # Avoid division by zero by replacing 0 with NA
    carbon_intensity["co2_per_mwh"] = (
        carbon_intensity["total_co2_tonnes"] /
        carbon_intensity["total_energy_mwh"].replace(0, pd.NA)
    )

    # Save Gold marts as Parquet
    energy_monthly.to_parquet("data/gold/energy_monthly.parquet", index=False)
    emissions_monthly.to_parquet("data/gold/emissions_monthly.parquet", index=False)
    carbon_intensity.to_parquet("data/gold/carbon_intensity.parquet", index=False)

    print("✅ Gold layer created:")
    print(" - data/gold/energy_monthly.parquet")
    print(" - data/gold/emissions_monthly.parquet")
    print(" - data/gold/carbon_intensity.parquet")
