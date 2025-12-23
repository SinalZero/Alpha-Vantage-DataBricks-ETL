# Databricks notebook source
# MAGIC %run "/Workspace/Ingestão API/config"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver_api;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.silver_api.cotacao_tratada AS
# MAGIC SELECT
# MAGIC   ticker,
# MAGIC   CAST(price AS DOUBLE) AS price,
# MAGIC   CAST(high AS DOUBLE) AS high,
# MAGIC   CAST(low AS DOUBLE) AS low,
# MAGIC   volume,
# MAGIC   last_trading_day,
# MAGIC   ingestion_ts
# MAGIC FROM workspace.bronze_api.cotacao_alpha
# MAGIC WHERE price IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT* FROM workspace.silver_api.cotacao_tratada
# MAGIC LIMIT 50;

# COMMAND ----------

