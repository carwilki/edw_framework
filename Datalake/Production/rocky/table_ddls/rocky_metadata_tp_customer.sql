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
"NZ_Migration", null, "NZ_Mako8", "EDW_PRD", "TP_CUSTOMER",
null, true, "customer", false, "delta",
"refine", "public", "tp_customer", "upsert", "load_dt", 
"TP_CUSTOMER_NBR", null, "daily", null, array("DUMMY_TIDAL_JOB"), 
null, array("rjalan@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com"), 1, null, false,
null, null ,"ALL_MUST_BE_MET" ,null ,null , null, null
);

-- COMMAND ----------

INSERT INTO work.pii_dynamic_view_control (
secured_database, secured_table_name, view_database, view_name,
view_created_flg, load_user_name, load_dt_tm, update_dt_tm
)
VALUES (
"cust_sensitive", "refine_tp_customer", "refine", "tp_customer",
False, "rjalan", current_timestamp(), current_timestamp()
);

-- COMMAND ----------

UPDATE pii_metadata.pii_metadata_store SET database_name= "cust_sensitive", table_name= "refine_tp_customer", updated_ts= current_timestamp(), update_user = "rjalan" WHERE table_name = 'tp_customer'
