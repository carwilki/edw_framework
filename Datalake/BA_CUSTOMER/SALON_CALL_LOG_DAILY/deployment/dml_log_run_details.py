# Databricks notebook source
# MAGIC %sql
# MAGIC select * from qa_raw.log_run_details

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO qa_raw.log_run_details (job_id, run_id, task_name,  process, table_name, status, error, prev_run_date) VALUES
# MAGIC (1,1,null,"SALON_BUNDLES_LAPSED_CUST_CALL_LOG","SALON_BUNDLES_LAPSED_CUST_CALL_LOG","Completed","N/A","2023-08-22"),
# MAGIC (1,1,null,"SALON_LAPSED_CUST_CALL_LOG","SALON_LAPSED_CUST_CALL_LOG","Completed","N/A","2023-08-22"),
# MAGIC (1,1,null,"SALON_LAPSED_SEASON_CUST_CALL_LOG","SALON_LAPSED_SEASON_CUST_CALL_LOG","Completed","N/A","2023-08-22")

# COMMAND ----------

# MAGIC %sql
# MAGIC select APPT_TSTMP, count(*) from qa_cust_sensitive.legacy_sds_order_item_rpt where APPT_TSTMP > current_date() group by  APPT_TSTMP order by APPT_TSTMP 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history qa_cust_sensitive.legacy_sds_order_item_rpt
