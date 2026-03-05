import os
import time
import logging
import warnings
from datetime import datetime

import pandas as pd
import numpy as np
from sqlalchemy import (
    create_engine, text, Column, Integer, Float, String,
    DateTime, Date, MetaData, Table, inspect
)
from sqlalchemy.exc import SQLAlchemyError

warnings.filterwarnings("ignore")



CSV_PATH     = os.getenv("CSV_PATH", "DATA2.csv")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///bradford_weather.db")
LOG_LEVEL    = os.getenv("LOG_LEVEL", "INFO")
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", "5000"))   # rows per DB write batch



logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl_pipeline.log", mode="a")
    ]
)
log = logging.getLogger("etl_pipeline")


def extract(csv_path: str) -> pd.DataFrame:
   
    log.info("━━━ STAGE 1: EXTRACT ━━━")
    log.info(f"Source file : {csv_path}")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"CSV not found at '{csv_path}'. "
            "Set CSV_PATH env variable or place DATA2.csv in the working directory."
        )

    start = time.time()
    df = pd.read_csv(csv_path, low_memory=False)
    elapsed = time.time() - start

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    log.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns in {elapsed:.1f}s")
    log.info(f"Columns detected: {list(df.columns)}")

    # Verify essential columns exist
    required = {"Date", "Time", "Solar_Rad", "Solar_Energy", "UV_Index",
                "UV_Dose", "Temp_Out"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    log.info("Extract stage complete ✓")
    return df



def transform(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Full data transformation pipeline:
      - Parse datetime
      - Cast numeric columns, handle sensor noise
      - Engineer temporal features (hour, season, day_of_year …)
      - Aggregate to daily and hourly summaries
      - Compute cumulative metrics
      - Classify UV risk categories
      - Flag anomalies / quality issues

    Returns a dict of DataFrames, each destined for its own DB table.
    """
    log.info("━━━ STAGE 2: TRANSFORM ━━━")

    
    log.info("Parsing datetime column …")
    df["DateTime"] = pd.to_datetime(
        df["Date"].astype(str).str.strip() + " " + df["Time"].astype(str).str.strip(),
        format="%d/%m/%Y %H:%M",
        errors="coerce"
    )
    original_len = len(df)
    df = df.dropna(subset=["DateTime"])
    dropped = original_len - len(df)
    if dropped:
        log.warning(f"Dropped {dropped:,} rows with unparseable DateTime")

    
    numeric_cols = [
        "Solar_Rad", "Solar_Energy", "Hi Solar_Rad",
        "UV_Index", "UV_Dose", "Hi_UV", "Temp_Out",
        "Hum_Out", "Bar", "Wind_Speed", "Wind_Dir",
        "Rain", "Rain_Rate"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    log.info("Numeric coercion complete")

    
    df.loc[df["Solar_Rad"] < 0, "Solar_Rad"] = 0
    df.loc[df["UV_Index"]  < 0, "UV_Index"]  = 0
    if "UV_Dose" in df.columns:
        df.loc[df["UV_Dose"] < 0, "UV_Dose"] = 0

    
    log.info("Engineering temporal features …")
    df["date"]        = df["DateTime"].dt.date
    df["hour"]        = df["DateTime"].dt.hour
    df["month"]       = df["DateTime"].dt.month
    df["month_name"]  = df["DateTime"].dt.strftime("%B")
    df["day_of_year"] = df["DateTime"].dt.dayofyear
    df["week"]        = df["DateTime"].dt.isocalendar().week.astype(int)
    df["year"]        = df["DateTime"].dt.year

    season_map = {
        12: "Winter", 1: "Winter",  2: "Winter",
         3: "Spring", 4: "Spring",  5: "Spring",
         6: "Summer", 7: "Summer",  8: "Summer",
         9: "Autumn", 10: "Autumn", 11: "Autumn"
    }
    df["season"] = df["month"].map(season_map)

   
    def uv_risk_label(uv):
        if pd.isna(uv):     return "Unknown"
        if uv < 3:          return "Low"
        elif uv < 6:        return "Moderate"
        elif uv < 8:        return "High"
        elif uv < 11:       return "Very High"
        else:               return "Extreme"

    def uv_risk_code(uv):
        if pd.isna(uv):     return 0
        if uv < 3:          return 1
        elif uv < 6:        return 2
        elif uv < 8:        return 3
        elif uv < 11:       return 4
        else:               return 5

    df["uv_risk_label"] = df["UV_Index"].apply(uv_risk_label)
    df["uv_risk_code"]  = df["UV_Index"].apply(uv_risk_code)

    
    solar_q3 = df["Solar_Rad"].quantile(0.99)
    df["quality_flag"] = "OK"
    df.loc[df["Solar_Rad"] > solar_q3 * 1.5, "quality_flag"] = "SUSPECT_HIGH_SOLAR"
    df.loc[df["Solar_Rad"].isna(),            "quality_flag"] = "MISSING_SOLAR"

    log.info(f"Quality flags: {df['quality_flag'].value_counts().to_dict()}")

    
    observations = df.rename(columns={
        "DateTime":    "datetime",
        "Date":        "_raw_date",
        "Time":        "_raw_time",
        "Solar_Rad":   "solar_rad",
        "Solar_Energy":"solar_energy",
        "UV_Index":    "uv_index",
        "UV_Dose":     "uv_dose",
        "Temp_Out":    "temp_out",
        "Hum_Out":     "humidity",
        "Bar":         "pressure",
        "Wind_Speed":  "wind_speed",
        "Wind_Dir":    "wind_dir",
        "Rain":        "rain",
        "Rain_Rate":   "rain_rate",
    }).copy()

    
    obs_keep = [
        "datetime", "date", "hour", "month", "month_name",
        "day_of_year", "week", "year", "season",
        "solar_rad", "solar_energy", "uv_index", "uv_dose",
        "temp_out", "uv_risk_label", "uv_risk_code", "quality_flag"
    ]
    for col in ["humidity", "pressure", "wind_speed", "wind_dir", "rain", "rain_rate"]:
        if col in observations.columns:
            obs_keep.append(col)

    observations = observations[[c for c in obs_keep if c in observations.columns]]

    log.info(f"Observations table: {len(observations):,} rows, {len(observations.columns)} cols")

    
    log.info("Computing daily aggregates …")
    daily = observations.groupby("date").agg(
        solar_rad_mean  = ("solar_rad",    "mean"),
        solar_rad_max   = ("solar_rad",    "max"),
        solar_energy_sum= ("solar_energy", "sum"),
        uv_index_mean   = ("uv_index",     "mean"),
        uv_index_max    = ("uv_index",     "max"),
        uv_dose_sum     = ("uv_dose",      "sum"),
        temp_mean       = ("temp_out",     "mean"),
        temp_max        = ("temp_out",     "max"),
        temp_min        = ("temp_out",     "min"),
        season          = ("season",       "first"),
        month           = ("month",        "first"),
        year            = ("year",         "first"),
        day_of_year     = ("day_of_year",  "first"),
        obs_count       = ("solar_rad",    "count"),
        high_uv_hours   = ("uv_risk_code", lambda x: (x >= 3).sum()),
    ).reset_index()

    daily = daily.sort_values("date").reset_index(drop=True)
    daily["cumulative_energy"] = daily["solar_energy_sum"].cumsum()
    daily["cumulative_uv"]     = daily["uv_dose_sum"].cumsum()
    daily["date"] = pd.to_datetime(daily["date"])

    log.info(f"Daily table: {len(daily):,} rows")

    
    log.info("Computing hourly averages …")
    hourly = observations.groupby("hour").agg(
        solar_rad_mean = ("solar_rad",  "mean"),
        solar_rad_std  = ("solar_rad",  "std"),
        uv_index_mean  = ("uv_index",   "mean"),
        temp_mean      = ("temp_out",   "mean"),
    ).reset_index()

    log.info(f"Hourly table: {len(hourly)} rows")

    
    log.info("Computing monthly summaries …")
    monthly = observations.groupby(["year", "month"]).agg(
        month_name      = ("month_name",  "first"),
        season          = ("season",      "first"),
        solar_rad_mean  = ("solar_rad",   "mean"),
        solar_rad_max   = ("solar_rad",   "max"),
        solar_energy_sum= ("solar_energy","sum"),
        uv_index_mean   = ("uv_index",    "mean"),
        uv_index_max    = ("uv_index",    "max"),
        temp_mean       = ("temp_out",    "mean"),
        high_uv_days    = ("uv_risk_code",lambda x: (x >= 3).any().astype(int) if len(x) > 0 else 0),
    ).reset_index()

    log.info(f"Monthly table: {len(monthly)} rows")

    log.info("Transform stage complete ✓")
    return {
        "observations": observations,
        "daily_stats":  daily,
        "hourly_avg":   hourly,
        "monthly_stats":monthly,
    }



def load(tables: dict[str, pd.DataFrame], database_url: str) -> None:
    """
    Load all transformed DataFrames into the target database.
    Uses batched writes for memory efficiency on large tables.
    Drops and recreates tables for idempotent full-refresh loads.
    """
    log.info("━━━ STAGE 3: LOAD ━━━")
    log.info(f"Target DB : {_mask_url(database_url)}")

    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}

    engine = create_engine(database_url, connect_args=connect_args, echo=False)

    # Write each table
    for table_name, df in tables.items():
        log.info(f"Writing '{table_name}' ({len(df):,} rows) …")
        start = time.time()

        # Convert date/datetime objects to strings for SQLite compatibility
        df_write = df.copy()
        for col in df_write.columns:
            if df_write[col].dtype == "object":
                pass  # already string
            elif hasattr(df_write[col], "dt"):
                try:
                    df_write[col] = df_write[col].astype(str)
                except Exception:
                    pass

        # Batched write — avoids memory spikes for 686k-row table
        df_write.to_sql(
            table_name,
            con=engine,
            if_exists="replace",   # full refresh; use "append" for incremental
            index=False,
            chunksize=BATCH_SIZE,
            method="multi"
        )

        elapsed = time.time() - start
        log.info(f"  ✓ '{table_name}' written in {elapsed:.1f}s")

    # Create lightweight indexes for fast dashboard queries
    _create_indexes(engine)

    log.info("Load stage complete ✓")
    log.info(f"Database ready → {database_url}")


def _create_indexes(engine) -> None:
    """Create indexes on frequently queried columns."""
    index_ddl = [
        "CREATE INDEX IF NOT EXISTS idx_obs_datetime ON observations(datetime);",
        "CREATE INDEX IF NOT EXISTS idx_obs_date     ON observations(date);",
        "CREATE INDEX IF NOT EXISTS idx_obs_season   ON observations(season);",
        "CREATE INDEX IF NOT EXISTS idx_daily_date   ON daily_stats(date);",
        "CREATE INDEX IF NOT EXISTS idx_daily_season ON daily_stats(season);",
    ]
    with engine.connect() as conn:
        for ddl in index_ddl:
            try:
                conn.execute(text(ddl))
            except SQLAlchemyError:
                pass   
        conn.commit()
    log.info("Indexes created ✓")


def _mask_url(url: str) -> str:
    """Hide credentials in logged URL."""
    if "@" in url:
        proto, rest = url.split("://", 1)
        rest = rest.split("@", 1)[1]
        return f"{proto}://***:***@{rest}"
    return url




def run_pipeline(csv_path: str = CSV_PATH, database_url: str = DATABASE_URL) -> None:
    """
    Execute the full Extract → Transform → Load pipeline.
    Logs timing and row counts at each stage.
    """
    pipeline_start = time.time()
    log.info("=" * 60)
    log.info("  Bradford Weather Station — ETL Pipeline")
    log.info(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    try:
        # Stage 1: Extract
        raw_df = extract(csv_path)

        # Stage 2: Transform
        tables = transform(raw_df)

        # Stage 3: Load
        load(tables, database_url)

        total = time.time() - pipeline_start
        log.info("=" * 60)
        log.info(f"  ✅ Pipeline complete in {total:.1f}s")
        log.info(f"  Tables written:")
        for name, df in tables.items():
            log.info(f"    • {name:<20} {len(df):>8,} rows")
        log.info("=" * 60)

    except FileNotFoundError as e:
        log.error(f"EXTRACT failed: {e}")
        raise
    except ValueError as e:
        log.error(f"TRANSFORM failed: {e}")
        raise
    except SQLAlchemyError as e:
        log.error(f"LOAD failed (database error): {e}")
        raise
    except Exception as e:
        log.error(f"Pipeline failed unexpectedly: {e}")
        raise


def get_engine(database_url: str = DATABASE_URL):
    """Return a SQLAlchemy engine for the configured database."""
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, connect_args=connect_args, echo=False)


def db_exists(database_url: str = DATABASE_URL) -> bool:
    """Check whether the database has been populated."""
    try:
        engine = get_engine(database_url)
        inspector = inspect(engine)
        return "daily_stats" in inspector.get_table_names()
    except Exception:
        return False


def load_table(table_name: str, database_url: str = DATABASE_URL) -> pd.DataFrame:
    """Read a named table from the database into a DataFrame."""
    engine = get_engine(database_url)
    return pd.read_sql_table(table_name, con=engine)




if __name__ == "__main__":
    run_pipeline()
