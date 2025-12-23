# Databricks notebook source
# MAGIC %sql
# MAGIC show catalogs

# COMMAND ----------

CATALOG = 'workspace'

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

API_KEY = '<SET_VIA_SECRET_SCOPE>'

BASE_URL = 'https://www.alphavantage.co/query'

# COMMAND ----------

TICKERS = ['PETR4.SA', 'MXRF11.SA', 'XPML11.SA', 'ITUB4.SA', 'BBAS3.SA']

# COMMAND ----------

BRONZE_SCHEMA = 'bronze_api'
BRONZE_TABLE = 'cotacao_alpha'

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}')

# COMMAND ----------

print('=== CONFIG CARREGADA ===')
print(f'CATALOG: {CATALOG}')
print(f'SCHEMA BRONZE: {CATALOG}.{BRONZE_SCHEMA}')
print(f'TABLE BRONZE: {CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}')
print(f'TICKERS: {TICKERS}')
print('=========================================================')