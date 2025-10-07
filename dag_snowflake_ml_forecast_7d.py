from __future__ import annotations

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

SNOWFLAKE_CONN_ID = "snowflake_conn"
SRC_TABLE = "USER_DB_POODLE.RAW.STOCK_PRICES_180D"
FORECAST_MODEL = "USER_DB_POODLE.RAW.STOCK_CLOSE_FC_MODEL"
FORECAST_TABLE = "USER_DB_POODLE.RAW.STOCK_PRICES_FORECAST_7D"
FINAL_TABLE = "USER_DB_POODLE.RAW.STOCK_PRICES_FINAL"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def return_snowflake_conn_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    return conn.cursor()

with DAG(
    dag_id="snowflake_ml_forecast_7d",
    start_date=datetime(2025, 10, 5),
    schedule=None,
    catchup=False,
    tags=["ML", "forecast", "snowflake"],
    default_args=default_args,
    description="Train/run SQL ML forecast in Snowflake for next 7 days and union with ETL history",
) as dag:

    @task
    def ensure_objects():
        """
        Create forecast output table and (re)create the ML forecast object.
        Uses correct Snowflake ML Forecast syntax.
        """
        cur = return_snowflake_conn_cursor()
        try:
            # Output table for 7d forecast
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FORECAST_TABLE} (
                    SYMBOL VARCHAR,
                    DATE DATE,
                    OPEN NUMBER,
                    CLOSE NUMBER,
                    MIN NUMBER,
                    MAX NUMBER,
                    VOLUME NUMBER,
                    SOURCE VARCHAR
                )
            """)

            # Create the forecast model using correct syntax
            # Using CREATE OR REPLACE with proper parameter format
            cur.execute(f"""
                CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {FORECAST_MODEL}(
                    INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT SYMBOL, DATE, CLOSE FROM {SRC_TABLE}'),
                    TIMESTAMP_COLNAME => 'DATE',
                    TARGET_COLNAME => 'CLOSE',
                    SERIES_COLNAME => 'SYMBOL'
                )
            """)

            # Final table DDL
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FINAL_TABLE} (
                    SYMBOL VARCHAR,
                    DATE DATE,
                    OPEN NUMBER,
                    CLOSE NUMBER,
                    MIN NUMBER,
                    MAX NUMBER,
                    VOLUME NUMBER,
                    SOURCE VARCHAR
                )
            """)
            
            print(f"Successfully created/replaced forecast model: {FORECAST_MODEL}")
            
        except Exception as e:
            print(f"Error in ensure_objects: {e}")
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass

    @task
    def run_forecast_and_union():
        """
        Transaction 1: run forecast and (re)populate FORECAST_TABLE
        Transaction 2: rebuild FINAL_TABLE as union of history + forecast
        """
        cur = return_snowflake_conn_cursor()
        conn = cur.connection

        try:
            # -------- Transaction 1: produce 7-day forecasts --------
            conn.autocommit = False
            cur.execute("BEGIN")

            # Clear forecast table
            cur.execute(f"TRUNCATE TABLE {FORECAST_TABLE}")

            # Insert next-7-days forecasts
            # Note: Column names from FORECAST may vary - adjust if needed
            cur.execute(f"""
                INSERT INTO {FORECAST_TABLE} (SYMBOL, DATE, OPEN, CLOSE, MIN, MAX, VOLUME, SOURCE)
                SELECT
                    SERIES AS SYMBOL,
                    TS AS DATE,
                    NULL AS OPEN,
                    FORECAST AS CLOSE,
                    NULL AS MIN,
                    NULL AS MAX,
                    NULL AS VOLUME,
                    'FORECAST' AS SOURCE
                FROM TABLE({FORECAST_MODEL}!FORECAST(FORECASTING_PERIODS => 7))
            """)

            cur.execute("COMMIT")
            print("Forecast data committed successfully")

            # -------- Transaction 2: rebuild FINAL_TABLE as union --------
            cur.execute("BEGIN")

            cur.execute(f"TRUNCATE TABLE {FINAL_TABLE}")
            
            # Historical rows
            cur.execute(f"""
                INSERT INTO {FINAL_TABLE} (SYMBOL, DATE, OPEN, CLOSE, MIN, MAX, VOLUME, SOURCE)
                SELECT SYMBOL, DATE, OPEN, CLOSE, MIN, MAX, VOLUME, 'HIST' AS SOURCE
                FROM {SRC_TABLE}
            """)
            
            # Forecast rows
            cur.execute(f"""
                INSERT INTO {FINAL_TABLE} (SYMBOL, DATE, OPEN, CLOSE, MIN, MAX, VOLUME, SOURCE)
                SELECT SYMBOL, DATE, OPEN, CLOSE, MIN, MAX, VOLUME, SOURCE
                FROM {FORECAST_TABLE}
            """)

            cur.execute("COMMIT")
            
            # Verification
            cur.execute(f"SELECT COUNT(*), SOURCE FROM {FINAL_TABLE} GROUP BY SOURCE ORDER BY SOURCE")
            results = cur.fetchall()
            print(f"Final table populated. Counts by source: {results}")

        except Exception as e:
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
            print(f"Error while running forecast/union: {e}")
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass

    objs = ensure_objects()
    run = run_forecast_and_union()

    objs >> run