-- Databricks notebook source
TRUNCATE TABLE legacy.INV_HOLES_COUNT 

-- COMMAND ----------

INSERT INTO legacy.INV_HOLES_COUNT 
select * except(bd_create_dt_tm, bd_update_dt_tm, source_file_name) from qa_refine.INV_HOLES_COUNT_history

-- COMMAND ----------

TRUNCATE TABLE legacy.inv_instock_price_day

-- COMMAND ----------

INSERT INTO legacy.inv_instock_price_day
select * except(bd_create_dt_tm, bd_update_dt_tm, source_file_name) from qa_refine.inv_instock_price_day_history
