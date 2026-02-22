import os
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests
import psycopg2
from psycopg2.extras import execute_values
from pydantic import BaseModel, field_validator, ValidationError
from config import DB_DSN, API_BASE, INDICATOR, PAGE_SIZE, CHECKPOINT, REQUEST_DELAY

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

class lifebirthrecord(BaseModel):
    indicator_code: str
    spatial_dim: str          # Country ISO3 code
    parent_loc: Optional[str]          # Continent
    time_dim: int             # Year
    dim1: Optional[str]       # SEX dimension (MLE / FMLE / BTSX)
    numeric_value: Optional[float]
    low_value: Optional[float]
    high_value: Optional[float]
    date_modified: Optional[datetime]

# Validate attr
@field_validator("spatial_dim")
@classmethod
def valid_iso3(cls, v):
    if not (2 <= len(v) <= 3) or not v.isalpha():
        raise ValueError(f"Unexpected country code: {v}")
    return v.upper()

@field_validator("time_dim")
@classmethod
def valid_year(cls, v):
    if not (1900 <= v <= datetime.now().year):
        raise ValueError("Unrealistic Year")
    return v


@field_validator("numeric_value", "low_value", "high_value", mode="before")
@classmethod
def clamp_lifespan(cls, v):
    if v is None:
        return None
    v = float(v)
    if not (0 < v < 150):
        raise ValueError(f"Life expectancy out of plausible range: {v}")
    return v

def last_check() -> dict:
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT) as f:
            return json.load(f)
    return {"skip": 0, "loaded_rows": 0}


# ETL Data load check point
def save_check(skip: int, loaded_rows: int):
    with open(CHECKPOINT, "w") as f:
        json.dump({"skip":skip, "loaded_rows":loaded_rows }, f)

def clear_check ():
    if os.path.exists(CHECKPOINT):
        os.remove(CHECKPOINT)

def get_connection():
    return psycopg2.connect(DB_DSN)

# https://ghoapi.azureedge.net/api/WHOSIS_000001?$top=10&skip=1&$filter=SpatialDimType%20eq%20%27COUNTRY%27

#Extract data 
def url_data(skip: int) -> list [dict]:
    url_path = f"{API_BASE}/{INDICATOR}?top={PAGE_SIZE}&skip={skip}"
    try:
        resp = requests.get(url_path, timeout=30)
        resp.raise_for_status()
        return resp.json().get("value",[])
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch page at skip={skip}")
        
def fetch_data(start_skip: int = 0):
    skip = start_skip
    while True: 
        log.info(f"Fetching rows {skip}–{skip + PAGE_SIZE - 1}")
        page = url_data(skip)
        if not page: 
            print ("no data to fetch ")
            break 
        yield from page
        if len(page) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        time.sleep(REQUEST_DELAY)


def transform(raw: dict) -> Optional[lifebirthrecord]:
    """Parse and validate one raw API record. Returns None if invalid."""
    try:
        date_str = raw.get("Date")
        date_mod = datetime.fromisoformat(date_str.rstrip("Z")).replace(tzinfo=timezone.utc) if date_str else None
        numeric_value = float(raw["NumericValue"]) if raw.get("NumericValue") else None
        return lifebirthrecord(
            indicator_code = raw.get("IndicatorCode", INDICATOR),
            spatial_dim    = raw.get("SpatialDim", ""),
            parent_loc     = raw.get("ParentLocation", ""),
            time_dim       = int(raw.get("TimeDim", 0)),
            dim1           = raw.get("Dim1"),             # sex
            numeric_value  = numeric_value,
            low_value      = raw.get("Low"),
            high_value     = raw.get("High"),
            date_modified  = date_mod,
        )
    except (ValidationError, TypeError, ValueError) as e:
        log.debug(f"Skipping invalid record {raw.get('Id')}: {e}")
        print(e)
        return None


def save_data(conn, records: list[lifebirthrecord]):
    """Upsert a batch of records; returns count inserted/updated."""
    rows = [
        (
            r.indicator_code,
            r.spatial_dim,
            r.parent_loc,
            r.time_dim,
            r.dim1,
            r.numeric_value,
            r.low_value,
            r.high_value,
            r.date_modified,
        )
        for r in records
    ]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO who_life_expectancy
                (indicator_code, country_code, continent, year, sex, value, low_value, high_value, date_modified)
            VALUES %s
            ON CONFLICT (country_code, year, sex)
            DO UPDATE SET
                value         = EXCLUDED.value,
                low_value     = EXCLUDED.low_value,
                high_value    = EXCLUDED.high_value,
                date_modified = EXCLUDED.date_modified,
                loaded_at     = NOW()
            WHERE
                who_life_expectancy.date_modified IS DISTINCT FROM EXCLUDED.date_modified
                OR who_life_expectancy.value IS DISTINCT FROM EXCLUDED.value
        """, rows)
    conn.commit()
    return len(rows)


def log_run(conn, run_id: int, rows_loaded: int, rows_skipped: int, status: str):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE etl_runs
            SET finished_at = NOW(), rows_loaded = %s, rows_skipped = %s, status = %s
            WHERE id = %s
        """, (rows_loaded, rows_skipped, status, run_id))
    conn.commit()

def start_run(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO etl_runs DEFAULT VALUES RETURNING id")
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id

BATCH_SIZE = 100 # ROWS BUFFERED BEFORE DB SAVE 

def main(resume: bool = True):
    cp = last_check() if resume else {"skip": 0, "loaded_rows": 0}

    if not resume:
        clear_check()

    current_skip = cp["skip"]
    total_loaded = cp["loaded_rows"]
    total_skipped = 0
    
    conn = get_connection()
    run_id = start_run(conn)

    buffer: list[lifebirthrecord] = []

    try:
        
        for raw in fetch_data(current_skip):
            record = transform(raw)
            if record is None:
                total_skipped += 1
                continue

            buffer.append(record)
            total_loaded += 1

            #stop condition/ this code can be removed to load more data
            if total_loaded >= PAGE_SIZE:
                log.info("Reached DATA_BATCH limit")
                break

            # batch flush
            if len(buffer) >= BATCH_SIZE:
                save_data(conn, buffer)
                current_skip += len(buffer) + total_skipped
                total_skipped = 0

                save_check(current_skip, total_loaded)
                log.info(f"Loaded {total_loaded} rows so far | skip={current_skip}")

                buffer.clear()

        # flush remaining records
        if buffer:
            save_data(conn, buffer)
            current_skip += len(buffer) + total_skipped
            save_check(current_skip, total_loaded)

        # clear_check()
        log_run(conn, run_id, total_loaded, total_skipped, "success")
        log.info(f"✅ ETL complete: {total_loaded} loaded, {total_skipped} skipped.")
    
    except KeyboardInterrupt:
        save_check(current_skip, total_loaded)
        log_run(conn, run_id, total_loaded, total_skipped, "interrupted")
        log.warning("⏸  Pipeline interrupted – checkpoint saved. Re-run with resume=True.")
    except Exception as e:
        save_check(current_skip, total_loaded)
        log_run(conn, run_id, total_loaded, total_skipped, f"error: {e}")
        log.error(f"X Pipeline failed: {e}")
        raise
    finally:
        conn.close()

        
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="WHO GHO ETL Pipeline")
    parser.add_argument("--no-resume", action="store_true", help="Start fresh, ignore checkpoint")
    args = parser.parse_args()
    main(resume=not args.no_resume)