-- Databricks notebook source
-- MAGIC %sql
-- MAGIC INSERT INTO work.rocky_ingestion_metadata (
-- MAGIC table_group, table_group_desc, source_type, source_db, source_table, 
-- MAGIC table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
-- MAGIC target_db, target_schema, target_table_name, load_type, source_delta_column,
-- MAGIC primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
-- MAGIC expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
-- MAGIC job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql , snowflake_post_sql ,additional_config
-- MAGIC )
-- MAGIC VALUES 
-- MAGIC ("NZ_Migration", null, 'NZ_Export_Mako8', "EDW_PRD", "EMPLOYEE_PROFILE_DAY",
-- MAGIC null, true, "employee_protected", false, "delta",
-- MAGIC "legacy", null, "employee_profile_day", "append", "load_tstmp", 
-- MAGIC null, "load_tstmp > '2021-01-01 00:00:00'", "daily", null, array("DUMMY_TIDAL_JOB"), 
-- MAGIC null, array("sjaiswal@petsmart.com"), 3, null, false,
-- MAGIC null, null ,"ALL_MUST_BE_MET" ,false ,null , null, null)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC  insert into work.pii_dynamic_view_control (secured_database, secured_table_name,view_database,view_name,view_created_flg,load_user_name,load_dt_tm,update_dt_tm)
-- MAGIC  values('empl_protected','legacy_employee_profile_day','legacy','employee_profile_day',false,'sjaiswal',now(),now())

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC insert into pii_metadata.pii_metadata_store
-- MAGIC (
-- MAGIC   database_name,
-- MAGIC   table_name,
-- MAGIC   column_name,
-- MAGIC   data_type,
-- MAGIC   pii_metadata_type,
-- MAGIC   masking_policy,
-- MAGIC   load_ts,
-- MAGIC   updated_ts,
-- MAGIC   load_user,
-- MAGIC   update_user 
-- MAGIC )
-- MAGIC select 
-- MAGIC      database_name,
-- MAGIC      table_name,
-- MAGIC      column_name,
-- MAGIC      data_type,
-- MAGIC      pii_metadata_type,
-- MAGIC      masking_policy,
-- MAGIC      current_timestamp() load_ts,
-- MAGIC      current_timestamp() updated_ts,
-- MAGIC      split(current_user(),'[@]')[0] load_user,
-- MAGIC      split(current_user(),'[@]')[0] update_user 
-- MAGIC from pii_metadata.pii_metadata_store_qa 
-- MAGIC where table_name IN ("legacy_employee_profile_day")