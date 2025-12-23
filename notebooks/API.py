# Databricks notebook source
# MAGIC %run "/Workspace/Ingestão API/config"

# COMMAND ----------

import requests
import pandas as pd
from datetime import datetime

# COMMAND ----------

def fetch_global_quote(symbol: str) -> dict:
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': symbol,
        'apikey': API_KEY
    }

    r = requests.get(BASE_URL, params=params, timeout = 30)
    r.raise_for_status()
    data = r.json()
    return data

# COMMAND ----------

# Teste com 1 ticker primeiro, para não bater limite de API

test_symbol = TICKERS[0]
raw = fetch_global_quote(test_symbol)
raw

# COMMAND ----------

def normalize_global_quote (payload: dict) -> dict:
    gq = payload.get('Global Quote',{})

    return {
        'ticker': gq.get('01. symbol'),
        'price': float(gq.get('05. price')) if gq.get('05. price') else None,
        'open': float(gq.get('02. open')) if gq.get('02. open') else None,
        'high': float(gq.get('03. high')) if gq.get('03. high') else None,
        'low': float(gq.get('04. low')) if gq.get('04. low') else None,
        'volume': int(gq.get('06. volume')) if gq.get('06. volume') else None,
        'last_trading_day': gq.get('07. latest trading day'),
        'previous_close': float(gq.get('08. previous close')) if gq.get('08. previous close') else None,
        "change": float(gq.get("09. change")) if gq.get("09. change") else None,
        "change_percent": gq.get("10. change percent"),
        "ingestion_ts": datetime.utcnow().isoformat(),
        "raw_json": str(payload)
    }


# COMMAND ----------

payload = fetch_global_quote(TICKERS[0])
row = normalize_global_quote(payload)
row


# COMMAND ----------

rows = [row]
pdf = pd.DataFrame(rows)
pdf

# COMMAND ----------

display(pdf)

# COMMAND ----------

sdf = spark.createDataFrame(pdf)
display(sdf)

# COMMAND ----------

import time

rows = []
for sym in TICKERS:
    payload = fetch_global_quote(sym)
    rows.append(normalize_global_quote(payload))
    time.sleep(1)

pdf = pd.DataFrame(rows)
display(pdf)

# COMMAND ----------

full_table = f'{CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}'

spark_df = spark.createDataFrame(pdf)

(spark_df.write
 .format('delta')
 .mode('overwrite')
 .saveAsTable(full_table)
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.bronze_api.cotacao_alpha
# MAGIC ORDER BY ingestion_ts DESC
# MAGIC LIMIT 50;

# COMMAND ----------

pdf[["ticker", "price"]]
