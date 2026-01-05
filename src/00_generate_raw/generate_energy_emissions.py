import numpy as np
import pandas as pd

# ------------------------------------------------------------
# PURPOSE
# ------------------------------------------------------------
# This script generates *synthetic* (fake but realistic-looking) raw datasets
# for an Energy & Emissions data engineering project.
#
# Why synthetic data?
# 1) You can learn the end-to-end pipeline without hunting for datasets online.
# 2) We intentionally inject "dirty" data (missing values, duplicates, bad types)
#    to practice data quality discovery and remediation — exactly what junior
#    data engineering roles expect.
#
# Output (Bronze layer - raw):
# - data/bronze/energy_raw.csv
# - data/bronze/emissions_raw.csv
# ------------------------------------------------------------

# Set a random seed so results are reproducible 
np.random.seed(7)

# Example "dimension-like" values
regions = ["North", "South", "East", "West"]
energy_sources = ["Electricity", "Gas", "Oil", "Renewables"]
sectors = ["Residential", "Industry", "Transport"]

# Creating a daily date range (time series)
dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")

# Collecting rows in lists and convert them into DataFrames
rows_energy = []
rows_emissions = []

# ------------------------------------------------------------
# Generating raw energy consumption data
# ------------------------------------------------------------
for d in dates:
    # Seasonal factor: higher consumption in winter, slightly higher in summer
    month = d.month
    season_factor = 1.25 if month in [12, 1, 2] else (1.10 if month in [6, 7, 8] else 1.0)

    for region in regions:
        # Generating consumption for each energy source
        for source in energy_sources:
            # Base consumption with randomness
            base = np.random.normal(900, 180) * season_factor

            # Temperature for the day (random but plausible)
            temp = np.random.normal(12, 8)

            # Making consumption somewhat related to temperature:
            # colder -> higher consumption (e.g., heating demand)
            consumption = max(0, base + (8 - temp) * 18)

            # --------------------------------------------------------
            # Injecting "dirty" data (typical real-world data problems)
            # --------------------------------------------------------
            # 2% chance: missing consumption value (None)
            if np.random.rand() < 0.02:
                consumption = None

            # 1% chance: wrong type / bad string value
            if np.random.rand() < 0.01:
                consumption = "N/A"

            # Append one raw record (date, region, source, consumption, temperature)
            rows_energy.append([
                d.strftime("%Y-%m-%d"),
                region,
                source,
                consumption,
                round(temp, 2)
            ])

        # ------------------------------------------------------------
        # Generate emissions data per sector (still per date/region)
        # ------------------------------------------------------------
        for sector in sectors:
            # Sector multiplier: industry tends to emit more, etc.
            sector_mult = {"Residential": 0.9, "Industry": 1.3, "Transport": 1.15}[sector]

            # Emissions in tonnes CO2 (random but correlated with season + sector)
            co2 = max(0, np.random.normal(420, 90) * season_factor * sector_mult)

            # 1.5% chance: missing emissions
            if np.random.rand() < 0.015:
                co2 = None

            rows_emissions.append([
                d.strftime("%Y-%m-%d"),
                region,
                sector,
                co2
            ])

# Convert lists into DataFrames (tabular format)
df_energy = pd.DataFrame(
    rows_energy,
    columns=["date", "region", "energy_source", "consumption_mwh", "temperature_c"]
)

df_emissions = pd.DataFrame(
    rows_emissions,
    columns=["date", "region", "sector", "co2_tonnes"]
)

# ------------------------------------------------------------
# Inject duplicates
# ------------------------------------------------------------
# Add ~1% duplicated rows to the energy dataset to test deduplication logic later
df_energy = pd.concat(
    [df_energy, df_energy.sample(frac=0.01, random_state=7)],
    ignore_index=True
)

# ------------------------------------------------------------
# Save to Bronze layer (raw CSV files)
# ------------------------------------------------------------
# Bronze means: store raw data in the same shape you received it.
# In a real pipeline, Bronze would be data coming from APIs/files.
df_energy.to_csv("data/bronze/energy_raw.csv", index=False)
df_emissions.to_csv("data/bronze/emissions_raw.csv", index=False)

print("✅ Generated Bronze datasets:")
print(" - data/bronze/energy_raw.csv")
print(" - data/bronze/emissions_raw.csv")
