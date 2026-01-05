# Energy Emissions Lakehouse (PostgreSQL + Power BI)

A small end-to-end data engineering project that simulates an energy & emissions dataset, processes it through a Bronze → Silver → Gold pipeline, loads a star schema into PostgreSQL, and visualizes key KPIs in Power BI.

## Architecture
- **Bronze:** raw generated dataset (CSV)
- **Silver:** cleaned & standardized data (Parquet)
- **Gold:** aggregated marts (monthly energy, monthly emissions, carbon intensity) (Parquet)
- **Warehouse:** PostgreSQL star schema (dimensions + facts)
- **BI:** Power BI dashboard connected to PostgreSQL

## Tech Stack
Python (pandas), PostgreSQL, Docker Compose, SQL (CTEs / Window Functions), Power BI

## Repository Structure
- `src/00_generate_raw/` dataset generation (raw)
- `src/10_silver/` bronze → silver transformations
- `src/20_gold/` silver → gold marts
- `src/30_load/` load gold marts into PostgreSQL
- `sql/` schema + analysis queries
- `Dashboards/PowerBI/` Power BI report (`.pbix`)
- `Dashboards/PowerBI_screenshots/` dashboard screenshots (optional)

## How to Run

### 1) Start PostgreSQL (Docker)
From the project root:
```bash
docker compose up -ds

