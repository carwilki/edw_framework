-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata (
table_group, table_group_desc, source_type, source_db, source_table, 
table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
target_db, target_schema, target_table_name, load_type, source_delta_column,
primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql , snowflake_post_sql ,additional_config
)
VALUES (
"NZ_Migration", null, "NZ_Mako8", "EDW_PRD", "TP_HISTORY",
null, false, null, false, "delta",
"refine", "public", "tp_history", "append", "load_dt", 
"HISTORY_DT,LOCATION_ID,TP_CUSTOMER_NBR,TP_PET_NBR,TP_HIST_ACTION,RESERVATION_ORIGIN_IND,HISTORY_DESC,USER_NAME,CHANGE_DT,LOAD_DT", null, "daily", null, array("DUMMY_TIDAL_JOB"), 
null, array("rjalan@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com"), 1, null, false,
null, null ,"ALL_MUST_BE_MET" ,null ,null , null, null
);
