-- Databricks notebook source
-- MAGIC %python
-- MAGIC if 'databricksprod' in spark.conf.get('spark.databricks.clusterUsageTags.gcpProjectId'):
-- MAGIC   print(spark.conf.get('spark.databricks.clusterUsageTags.gcpProjectId'))
-- MAGIC   print("Executing Run On Production Cluster...")
-- MAGIC   v_work_db = "work"
-- MAGIC   v_nz_db = "NZ_Mako8_legacy"
-- MAGIC   v_legacy_db = "legacy"
-- MAGIC else:
-- MAGIC   print(spark.conf.get('spark.databricks.clusterUsageTags.gcpProjectId'))
-- MAGIC   print("Executing Run On Dev/QA Cluster...")
-- MAGIC   v_work_db = "qa_work"
-- MAGIC   v_nz_db = "NZ_Mako4_legacy"
-- MAGIC   v_legacy_db = "qa_legacy"
-- MAGIC
-- MAGIC dbutils.widgets.text("work_db", v_work_db)
-- MAGIC dbutils.widgets.text("nz_db", v_nz_db)
-- MAGIC dbutils.widgets.text("legacy_db", v_legacy_db)

-- COMMAND ----------

drop table if exists ${legacy_db}.VENDOR_PROFILE

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
"NZ_Migration", null, '${nz_db}', "EDW_PRD", "VENDOR_PROFILE",
null, false, null, false, "delta",
"legacy", "public", "vendor_profile", "full", null, 
null, null, "daily", "0 0 12 * * ?", null, 
null, array("dbodake@petsmart.com", "sjaiswal@petsmart.com"), 3, null, false,
null, null ,null ,true ,null , null, null
);
