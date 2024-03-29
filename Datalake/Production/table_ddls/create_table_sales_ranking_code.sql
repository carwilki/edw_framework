-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####DDL for sales_ranking_code, which is required for wf_sales_ranking_wk workflow

-- COMMAND ----------

CREATE TABLE legacy.sales_ranking_code
(  
RANKING_CD STRING NOT NULL, 
RANKING_START_DT DATE NOT NULL, 
RANKING_END_DT DATE, 
RANKING_LEVEL SMALLINT,  
RANKING_DESC STRING, 
RANKING_START_PERCENT DECIMAL(7,4),  
RANKING_END_PERCENT DECIMAL(7,4), 
LOAD_TSTMP TIMESTAMP
)
USING delta LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/sales_ranking_code' 

 
