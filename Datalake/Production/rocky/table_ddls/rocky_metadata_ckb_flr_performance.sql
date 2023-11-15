-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####rocky table CKB_FLR_PERFORMANCE, as it is required source for bs_replenishment_profile

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group, table_group_desc, source_type, source_db, source_table, 
table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
target_db, target_schema, target_table_name, load_type, source_delta_column,
primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql , snowflake_post_sql ,additional_config
)
VALUES (
"NZ_Migration", null, 'NZ_Export_Mako8', "EDW_PRD", "CKB_FLR_PERFORMANCE",
null, false, null, false, "delta",
"legacy", null, "ckb_flr_performance", "upsert", "UPDATE_DT", 
"CKB_DB_FLOOR_PERFORMANCE_KEY", null, "daily", null, array("DUMMY_TIDAL_JOB"), 
null, array("dbodake@petsmart.com","pkulkarni@petsmart.com","sjaiswal@petsmart.com"), 3, '{"Department":"Netezza-Migration"}', false,
null, null ,"ALL_MUST_BE_MET" ,true ,null , null, null
);
