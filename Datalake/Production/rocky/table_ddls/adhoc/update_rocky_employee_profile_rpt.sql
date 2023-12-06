-- Databricks notebook source
delete from pii_metadata.pii_metadata_store where table_name = 'legacy_employee_profile_rpt'

-- COMMAND ----------

insert into pii_metadata.pii_metadata_store select * from pii_metadata.pii_metadata_store_qa where table_name = 'legacy_employee_profile_rpt'

-- COMMAND ----------

drop table empl_sensitive.legacy_employee_profile_rpt

-- COMMAND ----------

update work.pii_dynamic_view_control set secured_database= 'empl_protected', view_created_flg = false, update_user_name= 'rjalan', update_dt_tm = current_timestamp() where view_name='employee_profile_rpt'

-- COMMAND ----------

update work.rocky_ingestion_metadata set pii_type= 'employee_protected' where rocky_id = 763
