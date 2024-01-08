# Databricks notebook source
# DBTITLE 1,transferring data from qa to prod
#%sql
#insert into legacy.sales_ranking_code select * from qa_legacy.sales_ranking_code

# COMMAND ----------

# DBTITLE 1,truncating this table after its first run so that we can setup its delta window in rocky perfectly
#%sql
#truncate table legacy.sales_day_sku_store_rpt 

# COMMAND ----------

# DBTITLE 1,updating ingestion control of prod rocky sales_day_sku_store_rpt
# %sql
# update work.rocky_ingestion_control set delta_window_start='2010-03-17 00:00:00',delta_window_end='2023-12-31 00:00:00' where rocky_id= '<rocky_id of sales_day_sku_store_rpt>'

# COMMAND ----------

# DBTITLE 1,transferring history load to project 
#%sql
# insert into legacy.sales_day_sku_store_rpt 
# select
#   *,
#   bd_create_dt_tm as bd_update_dt_tm,
#   "historical_load" source_file_name
#   from dev_legacy.nz_sales_day_sku_store_rpt_hist
