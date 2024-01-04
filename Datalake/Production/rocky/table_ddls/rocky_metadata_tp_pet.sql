-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Rocky table TP_PET, as it is a required source for wf_petshotel_email_extract  

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
"NZ_Migration", null, 'NZ_Export_Mako8', "EDW_PRD", "TP_PET",
null, true, "customer", false, "delta",
"refine", null, "tp_pet", "upsert", "LOAD_DT", 
"TP_PET_NBR", null, "daily", null, array("DUMMY_TIDAL_JOB"), 
null, array("tmarimuthu@petsmart.com","sjaiswal@petsmart.com"), 3, '{"Department":"Netezza-Migration"}', false,
null, null ,"ALL_MUST_BE_MET" ,true ,null , null, null
);

-- COMMAND ----------

-- DBTITLE 1,This step is required to create view with masking for sensitive details

INSERT INTO work.pii_dynamic_view_control (
secured_database, secured_table_name, view_database, view_name,
view_created_flg, load_user_name, load_dt_tm, update_dt_tm
)
VALUES (
"cust_sensitive", "refine_tp_pet", "refine", "tp_pet",
false, "tmarimuthu", current_timestamp(), current_timestamp()
);

-- COMMAND ----------

-- DBTITLE 1,This step is required to copy metadata details from QA table to PROD for view creation.

insert into pii_metadata.pii_metadata_store
 (
 database_name,
 table_name,
 column_name,
 data_type,
 pii_metadata_type,
 masking_policy,
 load_ts,
 updated_ts,
 load_user,
 update_user
 )
select
 database_name,
 table_name,
 column_name,
 data_type,
 pii_metadata_type,
 masking_policy,
 current_timestamp() load_ts,
 current_timestamp() updated_ts,
 split(current_user(),'[@]')[0] load_user,
 split(current_user(),'[@]')[0] update_user
from pii_metadata.pii_metadata_store_qa
where table_name IN
 (
  "refine_tp_pet"
 )
