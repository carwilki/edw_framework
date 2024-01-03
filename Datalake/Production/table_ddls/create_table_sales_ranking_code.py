# Databricks notebook source
# MAGIC %md
# MAGIC ####DDL for sales_ranking_code, which is required for wf_sales_ranking_wk workflow

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE legacy.sales_ranking_code
# MAGIC (  
# MAGIC RANKING_CD STRING NOT NULL, 
# MAGIC RANKING_START_DT DATE NOT NULL, 
# MAGIC RANKING_END_DT DATE, 
# MAGIC RANKING_LEVEL SMALLINT,  
# MAGIC RANKING_DESC STRING, 
# MAGIC RANKING_START_PERCENT DECIMAL(7,4),  
# MAGIC RANKING_END_PERCENT DECIMAL(7,4), 
# MAGIC LOAD_TSTMP TIMESTAMP
# MAGIC )
# MAGIC USING delta LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/sales_ranking_code' 
# MAGIC
# MAGIC  
