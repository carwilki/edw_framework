-- Databricks notebook source
--JOB ID : 1043289735502408
-- TIDAL : ETL_BD-Rocky_sku_profile_rpt_load_DY

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT INTO work.rocky_ingestion_metadata (
-- MAGIC table_group, table_group_desc, source_type, source_db, source_table, 
-- MAGIC table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
-- MAGIC target_db, target_schema, target_table_name, load_type, source_delta_column,
-- MAGIC primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
-- MAGIC expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
-- MAGIC job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql , snowflake_post_sql , additional_config
-- MAGIC )
-- MAGIC VALUES (
-- MAGIC "NZ_Migration", null, "NZ_Mako8", "EDW_PRD", "SKU_PROFILE_RPT",
-- MAGIC null, false, null, True, "delta",
-- MAGIC "legacy", "public", "sku_profile_rpt", "full", "LOAD_DT", 
-- MAGIC "PRODUCT_ID", null, "daily", null, array("DUMMY_TIDAL_JOB"), 
-- MAGIC null, array("pkulkarni@petsmart.com"), 1, null, False,
-- MAGIC null, null ,"ALL_MUST_BE_MET" ,null ,null , null, null
-- MAGIC );
