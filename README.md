# 📈 Stock Price Forecasting Pipeline using yfinance & Apache Airflow

## 🧩 Problem Statement
Develop an automated data pipeline that performs the following tasks:
1. **Extract** live stock price data using the [yfinance](https://pypi.org/project/yfinance/) API.  
2. **Load** and store the extracted data in a SQL database via an Airflow ETL process.  
3. **Train a forecasting model** to predict future stock prices using a separate Airflow ML pipeline.  
4. **Combine** the ETL and forecasted results into a final dataset using SQL transactions.

Both pipelines must be orchestrated and managed using **Apache Airflow** as two independent DAGs:

---

## ⚙️ Requirements & Specifications

- Use **yfinance** API to collect daily stock OHLCV data (Open, High, Low, Close, Volume).  
- Implement two **Airflow DAGs**:
  - `yf_stock_price_full_refresh_180d`: Extracts and loads raw stock data.
  - `dag_snowflake_ml_forecast_7d`: Trains forecasting model and merges final output.
- Use **Airflow Variables** to configure:
  - `stock_symbol` (e.g., “NVDA ,AAPL”)
  - `airflow_conn` (e.g., `postgres_default`)
- Use **Airflow Connections** to securely manage database credentials.
- Implement **SQL transactions** using `try/except` for safe table merging.
- Final table should **union** data from ETL and forecasting pipelines.
- Include a **IEEE-format report** (single column).
- Store **SQL & Airflow code in GitHub** and provide link in your final submission.

---

## 🧱 System Architecture Diagram

```
                +----------------+
                |  yfinance API  |
                +--------+-------+
                         |
                         v
                +----------------+
                | Airflow DAG 1: |
                | yf_stock_price
             _full_refresh_180d  |
                | (Extract,      |
                |  Transform,    |
                |   Load).       |
                +--------+-------+
                         |
                         v
                +----------------+
                |  Raw Data Table |
                +--------+-------+
                         |
                         v
                +----------------+
                | Airflow DAG 2: |
                | dag_snowflake_ml_forecast_7d |
                | (Forecast, Merge)|
                +--------+-------+
                         |
                         v
                +----------------+
                | Final Output Table |
                +----------------+
```

---

## 🪶 Airflow DAGs Overview

### 1️⃣ `yf_stock_price_full_refresh_180d`
- Fetches stock price data using the yfinance API.  
- Loads the extracted dataset into a SQL database.  
- Uses Airflow Variables (`stock_symbol`) and database connection (`snowflake_conn`).  
- Scheduled to run **daily** (`schedule_interval='@daily'`).  

### 2️⃣ `dag_snowflake_ml_forecast_7d`
- Reads historical data from the database.  
- Trains a machine learning model to forecast prices.  
- Saves predicted results to the forecast table.  
- Performs a **SQL transaction** to combine ETL and forecasted data into the final table.  
- Also scheduled to run daily, **after** `yfinance_etl`.


## 📊 Airflow Web UI

> Include a screenshot showing both DAGs running successfully:
> - `yf_stock_price_full_refresh_180d`
> - `dag_snowflake_ml_forecast_7d`

This confirms that both pipelines are correctly deployed and orchestrated within Airflow.

---

## 🔗 GitHub Repository

Include the following files in your repo:

```
📂 LAB-1/
│   ├── yf_stock_price_full_refresh_180d.py
│   ├── dag_snowflake_ml_forecast_7d.py
│   ├── Lab Report-Group 17.pdf
├── README.md
```

## 🧠 Best Practices Demonstrated

✅ Proper use of Airflow Variables & Connections  
✅ Implementation of SQL transactions with error handling  
✅ Two cleanly separated and orchestrated Airflow DAGs  
✅ Robust ETL and ML forecasting design  
✅ Clear IEEE-format reporting and documentation  
