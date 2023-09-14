-- Databricks notebook source
-- MAGIC %python
-- MAGIC if 'databricksprod' in spark.conf.get('spark.databricks.clusterUsageTags.gcpProjectId'):
-- MAGIC   print(spark.conf.get('spark.databricks.clusterUsageTags.gcpProjectId'))
-- MAGIC   print("Executing Run On Production Cluster...")
-- MAGIC   v_work_db = "work"
-- MAGIC   v_nz_db = "NZ_Export_Mako8"
-- MAGIC   v_cust_sensitive_db = "cust_sensitive"
-- MAGIC else:
-- MAGIC   print(spark.conf.get('spark.databricks.clusterUsageTags.gcpProjectId'))
-- MAGIC   print("Executing Run On Dev/QA Cluster...")
-- MAGIC   v_work_db = "qa_work"
-- MAGIC   v_nz_db = "NZ_Export_Mako4"
-- MAGIC   v_cust_sensitive_db = "qa_cust_sensitive"
-- MAGIC
-- MAGIC dbutils.widgets.text("work_db", v_work_db)
-- MAGIC dbutils.widgets.text("cust_sensitive_db", v_cust_sensitive_db)
-- MAGIC dbutils.widgets.text("nz_db", v_nz_db)

-- COMMAND ----------

INSERT INTO ${work_db}.rocky_ingestion_metadata (
table_group, table_group_desc, source_type, source_db, source_table, 
table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
target_db, target_schema, target_table_name, load_type, source_delta_column,
primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql , snowflake_post_sql ,additional_config
)
VALUES (
"NZ_Migration", null, '${nz_db}', "EDW_PRD", "sds_order_item_rpt",
null, true, "customer", false, "delta",
"legacy", "public", "sds_order_item_rpt", "full", null, 
null, null, "daily", null, array("DUMMY_TIDAL_JOB"), 
null, array("rjalan@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com"), 1, null, false,
null, null ,"ALL_MUST_BE_MET" ,True ,null , null, null
);

-- COMMAND ----------

INSERT INTO ${work_db}.pii_dynamic_view_control (
secured_database, secured_table_name, view_database, view_name,
view_created_flg, load_user_name, load_dt_tm, update_dt_tm
)
VALUES (
'${cust_sensitive_db}', "legacy_sds_order_item_rpt", "legacy", "sds_order_item_rpt",
False, "rjalan", current_timestamp(), current_timestamp()
);

-- COMMAND ----------

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
  "legacy_sds_order_item_rpt"
 )
