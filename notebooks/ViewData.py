# Databricks notebook source
# MAGIC %run "/Workspace/Ingestão API/config"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG workspace;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.analytics_api;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.analytics_api.vw_cotacoes_resumo AS
# MAGIC SELECT
# MAGIC   ticker,
# MAGIC   MAX(last_trading_day) AS ultima_data,
# MAGIC   MAX(high) AS maior_alta,
# MAGIC   MAX(low) AS menor_baixa,
# MAGIC   AVG(price) AS preco_medio,
# MAGIC   SUM(volume) AS volume_total,
# MAGIC   MAX(ingestion_ts) AS data
# MAGIC
# MAGIC
# MAGIC FROM workspace.silver_api.cotacao_tratada
# MAGIC GROUP BY ticker;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.analytics_api.vw_cotacoes_resumo
# MAGIC ORDER BY ticker;

# COMMAND ----------

