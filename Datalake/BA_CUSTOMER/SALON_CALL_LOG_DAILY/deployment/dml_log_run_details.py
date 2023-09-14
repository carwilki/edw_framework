# Databricks notebook source

# MAGIC %sql
MAGIC INSERT INTO raw.log_run_details (job_id, run_id, task_name,  process, table_name, status, error, prev_run_date) VALUES
MAGIC (1,1,null,"SALON_BUNDLES_LAPSED_CUST_CALL_LOG","SALON_BUNDLES_LAPSED_CUST_CALL_LOG","Completed","N/A","2023-09-13"),
MAGIC (1,1,null,"SALON_LAPSED_CUST_CALL_LOG","SALON_LAPSED_CUST_CALL_LOG","Completed","N/A","2023-09-13"),
MAGIC (1,1,null,"SALON_LAPSED_SEASON_CUST_CALL_LOG","SALON_LAPSED_SEASON_CUST_CALL_LOG","Completed","N/A","2023-09-13")

# COMMAND ----------

MAGIC %sql
MAGIC describe history cust_sensitive.legacy_sds_order_item_rpt
