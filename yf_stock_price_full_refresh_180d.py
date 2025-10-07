# In Cloud Composer, add apache-airflow-providers-snowflake and yfinance to PYPI Packages
from __future__ import annotations

from airflow import DAG
from airflow.models import Variable     # ✅ Added
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

# -------------------
# Config
# -------------------
SNOWFLAKE_CONN_ID = "snowflake_conn"
TARGET_TABLE = "USER_DB_POODLE.RAW.STOCK_PRICES_180D"

# ✅ Read symbols list from Airflow Variable "stock_symbol"
# The Variable value in the UI should be: ["NVDA", "AAPL"]
SYMBOLS = Variable.get("stock_symbol", default_var='["NVDA","AAPL"]', deserialize_json=True)

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def return_snowflake_conn_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    return conn.cursor()

with DAG(
    dag_id="yf_stock_price_full_refresh_180d",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ETL", "yfinance", "snowflake", "full-refresh"],
    default_args=default_args,
    description="Fetch last 180d prices from yfinance (symbols from Airflow Variable) and full-refresh load into Snowflake",
) as dag:

    # ✅ Your existing extract and load tasks go below unchanged
    @task
    def extract(symbols: list[str]) -> dict[str, list[tuple]]:
        out: dict[str, list[tuple]] = {}
        for sym in symbols:
            df = yf.Ticker(sym).history(period="180d", interval="1d", auto_adjust=False)
            if df.empty:
                df = yf.download(sym, period="180d", interval="1d", auto_adjust=False, progress=False)
            if df.empty:
                raise ValueError(f"{sym}: yfinance returned no data")

            df = df.reset_index()
            if "Date" not in df.columns:
                df.rename(columns={df.columns[0]: "Date"}, inplace=True)
            df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

            rows = []
            for rec in df.itertuples(index=False):
                rows.append((
                    sym,
                    rec.Date,
                    float(rec.Open) if pd.notna(rec.Open) else None,
                    float(rec.Close) if pd.notna(rec.Close) else None,
                    float(rec.Low) if pd.notna(rec.Low) else None,
                    float(rec.High) if pd.notna(rec.High) else None,
                    int(rec.Volume) if pd.notna(rec.Volume) else None,
                ))
            out[sym] = rows
        return out

    @task
    def load(all_symbol_rows: dict[str, list[tuple]]):
        all_rows = []
        for rows in all_symbol_rows.values():
            all_rows.extend(rows)
        if not all_rows:
            raise ValueError("No rows to load; aborting to avoid truncating table to empty state.")

        cur = return_snowflake_conn_cursor()
        conn = cur.connection
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                    SYMBOL VARCHAR,
                    DATE DATE,
                    OPEN NUMBER,
                    CLOSE NUMBER,
                    MIN NUMBER,
                    MAX NUMBER,
                    VOLUME NUMBER,
                    PRIMARY KEY (SYMBOL, DATE)
                )
            """)
            conn.autocommit = False
            cur.execute("BEGIN")
            cur.execute(f"TRUNCATE TABLE {TARGET_TABLE}")
            insert_sql = f"""
                INSERT INTO {TARGET_TABLE} (SYMBOL, DATE, OPEN, CLOSE, MIN, MAX, VOLUME)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cur.executemany(insert_sql, all_rows)
            cur.execute("COMMIT")
            cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
            count = cur.fetchone()[0]
            print(f"Full refresh committed. Row count: {count}")
        except Exception as e:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()

    rows_by_symbol = extract(SYMBOLS)
    load_task = load(rows_by_symbol)
