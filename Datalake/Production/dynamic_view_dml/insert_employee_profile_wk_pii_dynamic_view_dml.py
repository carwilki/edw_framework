# Databricks notebook source
# DBTITLE 1,making an entry in pii_dynamic_view_control table
# MAGIC %sql
# MAGIC INSERT INTO work.pii_dynamic_view_control (
# MAGIC secured_database, secured_table_name, view_database, view_name,
# MAGIC view_created_flg, load_user_name, load_dt_tm, update_dt_tm
# MAGIC )
# MAGIC VALUES (
# MAGIC "empl_protected", "legacy_employee_profile_wk", "legacy", "employee_profile_wk",
# MAGIC false, "dbodake", current_timestamp(), current_timestamp()
# MAGIC );

# COMMAND ----------

# DBTITLE 1,making an entry in pii_metadata_store table
# MAGIC %sql
# MAGIC insert into pii_metadata.pii_metadata_store select * from pii_metadata.pii_metadata_store_qa where table_name = 'legacy_employee_profile_wk'
