-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####rocky table CKB_SPC_FIXTURE, as it is required source for wf_ckb_spc_rpt

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
"NZ_Migration", null, 'NZ_Export_mako8', "EDW_PRD", "CKB_SPC_FIXTURE",
null, false, null, false, "delta",
"legacy", null, "ckb_spc_fixture", "upsert", "UPDATE_DT", 
"CKB_DB_SPC_FIXTURE_KEY,CKB_DB_PLANOGRAM_KEY", null, "daily", null, array("DUMMY_TIDAL_JOB"), 
null, array("dbodake@petsmart.com","pkulkarni@petsmart.com","sjaiswal@petsmart.com"), 3, '{"Department":"Netezza-Migration"}', false,
null, null ,"ALL_MUST_BE_MET" ,true ,null , null, null
);
