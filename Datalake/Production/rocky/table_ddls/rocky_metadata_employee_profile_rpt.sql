-- Databricks notebook source
-- MAGIC %sql
-- MAGIC INSERT INTO work.rocky_ingestion_metadata (
-- MAGIC table_group, table_group_desc, source_type, source_db, source_table, 
-- MAGIC table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
-- MAGIC target_db, target_schema, target_table_name, load_type, source_delta_column,
-- MAGIC primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
-- MAGIC expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
-- MAGIC job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql , snowflake_post_sql, additional_config
-- MAGIC )
-- MAGIC VALUES (
-- MAGIC "NZ_Migration", null, "NZ_Mako8", "EDW_PRD", "EMPLOYEE_PROFILE_RPT",
-- MAGIC null, True, "employee_protected", false, "delta",
-- MAGIC "legacy", "public", "employee_profile_rpt", "full", null, 
-- MAGIC null, null, "daily", null, array("DUMMY_TIDAL_JOB"), 
-- MAGIC null, array("pkulkarni@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com"), 1, null, false,
-- MAGIC null, null ,"ALL_MUST_BE_MET" ,null ,null , null, null
-- MAGIC );

-- COMMAND ----------

INSERT INTO work.pii_dynamic_view_control (
secured_database, secured_table_name, view_database, view_name,
view_created_flg, load_user_name, load_dt_tm, update_dt_tm
)
VALUES (
"empl_protected", "legacy_employee_profile_rpt", "legacy", "employee_profile_rpt",
False, "pkulkarni", current_timestamp(), current_timestamp()
);
