# solar-radiation-analytics-pipeline

An end-to-end data engineering project that automates the ingestion, cleaning, and visualisation of **686,000+ real-world meteorological records** from the University of Bradford's weather station, transforming raw sensor output into actionable intelligence for solar energy planning and public health risk assessment. Built as coursework for COS7046-B Big Data Visualization MSc Artificial Intelligence & Machine Learning.

---

## Tech Stack

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Pandas](https://img.shields.io/badge/Pandas-2.0-lightblue)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-red)
![Plotly Dash](https://img.shields.io/badge/Plotly%20Dash-2.14-brightgreen)
![SQLite](https://img.shields.io/badge/SQLite-local-lightgrey)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-cloud-blue)

---

## Project Highlights

- **ETL Pipeline** — automated Python pipeline ingesting, cleaning, and structuring 686,000+ sensor records into a cloud-compatible database
- **Feature Engineering** — derived temporal features (season, hour, day-of-year), WHO UV risk classification, and data quality flags from raw sensor noise
- **Statistical Analysis** — identified seasonal and diurnal patterns in solar radiation and UV exposure across a full calendar year
- **Interactive Dashboard** — 8-chart Plotly Dash dashboard with date filtering, season quick-select, and WHO UV risk banding
- **Cloud-Ready** — single environment variable switches from local SQLite to PostgreSQL (Supabase, Railway, Neon)

---

## Architecture
```
DATA2.csv  (raw sensor output — 30-min intervals)
      │
      ▼
┌─────────────────────────────────┐
│        etl_pipeline.py          │
│                                 │
│  EXTRACT   → ingest & validate  │
│  TRANSFORM → clean & engineer   │
│  LOAD      → batch write to DB  │
└──────────────┬──────────────────┘
               │
               ▼
    bradford_weather.db       ← SQLite (local)
    postgresql://...          ← PostgreSQL (cloud)
               │
               ▼
┌─────────────────────────────────┐
│         dashboard.py            │
│   Plotly Dash · 8 charts        │
└─────────────────────────────────┘
```

---

## Repository Structure
```
├── etl_pipeline.py      # ETL pipeline — Extract, Transform, Load
├── dashboard.py         # Interactive Plotly Dash dashboard
├── requirements.txt     # Python dependencies
├── .gitignore
└── README.md
```

> `DATA2.csv` and `bradford_weather.db` are excluded via `.gitignore`.  
> Place your `DATA2.csv` in the project root before running.

---

## Quick Start
```bash
# 1. Clone and install
git clone https://github.com/<your-username>/solar-radiation-analytics-pipeline.git
cd solar-radiation-analytics-pipeline
pip install -r requirements.txt

# 2. Add DATA2.csv to the project root

# 3. Launch — ETL runs automatically on first start
python dashboard.py
# → http://127.0.0.1:8050/
```

---

## ETL Pipeline
```bash
# Run the pipeline independently
python etl_pipeline.py

# Use a cloud PostgreSQL database
DATABASE_URL="postgresql://user:pass@host:5432/dbname" python dashboard.py
```

| Variable | Default | Description |
|---|---|---|
| `CSV_PATH` | `DATA2.csv` | Path to raw sensor CSV |
| `DATABASE_URL` | `sqlite:///bradford_weather.db` | Database connection string |
| `BATCH_SIZE` | `5000` | Rows per DB write batch |

---

## Database Schema

| Table | Rows | Description |
|---|---|---|
| `observations` | ~18,000 | Cleaned 30-min readings with engineered features |
| `daily_stats` | ~365 | Daily aggregates + cumulative energy & UV |
| `hourly_avg` | 24 | Average diurnal pattern across all days |
| `monthly_stats` | ~12 | Monthly summaries by season |

---

## Dashboard Features

| Chart | Insight |
|---|---|
| Time Series | Daily solar radiation & UV trends with WHO risk bands |
| Diurnal Pattern | Average hourly solar/UV cycle with error bands |
| Seasonal Boxplots | Statistical spread across all four seasons |
| Heatmap | Solar intensity by hour of day × date |
| Scatter Plot | Solar radiation vs temperature, coloured by UV index |
| Cumulative Energy | Annual photovoltaic harvest potential |
| UV Risk Donut | WHO-classified exposure category breakdown |
| Monthly Overview | Solar energy and mean UV index by month |

---

## Dataset

| Property | Detail |
|---|---|
| Source | University of Bradford Weather Station |
| Hardware | OTT Parsivel² + Vantage Pro2 |
| Resolution | 30-minute intervals |
| Coverage | One full calendar year |
| Size | ~18,044 rows × 38 variables ≈ 686,000 data points |
| Key variables | `Solar_Rad`, `UV_Index`, `Solar_Energy`, `UV_Dose`, `Temp_Out` |

---
