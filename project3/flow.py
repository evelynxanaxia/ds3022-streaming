"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python flow.py
"""
import os
import sys            

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))       
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"coinbase{TAG}.duckdb"                     
CHART = f"charts/usd_by_hour{TAG}.png"       

import duckdb
import matplotlib

matplotlib.use("Agg")  
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of the database
        con.execute(f"CREATE OR REPLACE TABLE raw_trades AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_trades").fetchone()[0]

@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_trades
            WHERE action <> 'malformed'
              AND msg_type = 'match'                -- not subscriptions or last_match
              AND product_id IS NOT NULL AND trade_id IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_trades WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")}
@task
def entities():
    """Kind 2: the same trade sent twice. One row per (product_id, trade_id)."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE trades AS
            SELECT product_id, trade_id, side, price, size,
                   CAST(event_time AS TIMESTAMP) AS event_time,  -- event time, UTC
                   CAST(raw::JSON ->> '$.data.sequence' AS BIGINT) AS sequence  -- parsed from raw
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY product_id, trade_id
                                       ORDER BY kafka_offset DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM trades").fetchone()[0]

@task
def marts():
    """Time window: average traded value (USD) per hour of day, per product."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE mart_usd_hour AS
            WITH hourly AS (
                SELECT time_bucket(INTERVAL '1 hour', event_time) AS hr,
                       CASE WHEN product_id IN ('DOGE-USD', 'ADA-USD')
                            THEN 'DOGE + ADA' ELSE product_id END AS product,
                       sum(price * size) AS usd
                FROM trades
                GROUP BY hr, product
            )
            SELECT CAST(extract(hour FROM hr) AS INTEGER) AS hour_utc,
                   product,
                   avg(usd) AS avg_usd_per_hour
            FROM hourly
            GROUP BY hour_utc, product
            ORDER BY hour_utc, product
        """)
            
@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute(
            "SELECT hour_utc, product, avg_usd_per_hour FROM mart_usd_hour").fetchall()
    os.makedirs("charts", exist_ok=True)
    hours = list(range(24))
    bottom = [0.0] * 24
    plt.figure(figsize=(10, 4.5))
    for product in sorted({r[1] for r in rows}):
        vals = [next((r[2] for r in rows if r[0] == h and r[1] == product), 0.0) / 1e6
                for h in hours]
        plt.bar(hours, vals, bottom=bottom, label=product)
        bottom = [b + v for b, v in zip(bottom, vals)]
    plt.title("Traded value by hour of day (Coinbase, UTC)")   
    plt.xlabel("hour of day (UTC, event time)")
    plt.ylabel("average traded value per hour (million USD)")
    plt.xticks(hours)
    plt.legend()
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    plt.close()
    return CHART

@flow(name="coinbase-pipeline")
def pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    things = entities()
    marts()
    path = chart()
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique trades:             %d", things)
    log.info("Chart: %s", path)

if __name__ == "__main__":
    pipeline()
