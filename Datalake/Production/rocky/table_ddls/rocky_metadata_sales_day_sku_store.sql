-- Databricks notebook source
-- MAGIC %md
-- MAGIC rocky SALES_DAY_SKU_STORE as it is required source for bs_pog_agg

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group, table_group_desc, source_type, source_db, source_table, 
table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
target_db, target_schema, target_table_name, load_type, source_delta_column,
primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql , snowflake_post_sql ,additional_config
)
VALUES ("NZ_Migration", null, 'NZ_Mako8_legacy', "EDW_PRD", "SALES_DAY_SKU_STORE",
null, false, null, false, "delta",
"legacy", null, "sales_day_sku_store", "append", "DATE_LOADED", 
null, "DATE_LOADED > '2023-11-26'", "daily", null, array("DUMMY_TIDAL_JOB"), 
null, array("sjaiswal@petsmart.com"), 3, null, false,
null, null ,"ALL_MUST_BE_MET" ,true ,null , null, null)
