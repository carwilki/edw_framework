# Databricks notebook source
# MAGIC %md 
# MAGIC ####rocky table svcs_service_category, as it is required source for wf_PRISM_Daily

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO work.rocky_ingestion_metadata (
# MAGIC table_group, table_group_desc, source_type, source_db, source_table, 
# MAGIC table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
# MAGIC target_db, target_schema, target_table_name, load_type, source_delta_column,
# MAGIC primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
# MAGIC expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
# MAGIC job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql , snowflake_post_sql ,additional_config
# MAGIC )
# MAGIC VALUES (
# MAGIC "NZ_Migration", null, 'NZ_Mako8_legacy', "EDW_PRD", "SVCS_SERVICE_CATEGORY",
# MAGIC null, false, null, false, "delta",
# MAGIC "legacy", null, "svcs_service_category", "upsert", "UPDATE_TSTMP", 
# MAGIC "SVCS_SERVICE_CATEGORY_GID",null, "daily", null, array("DUMMY_TIDAL_JOB"), 
# MAGIC null, array("abelsare@petsmart.com","pkulkarni@petsmart.com","sjaiswal@petsmart.com"), 3, '{"Department":"Netezza-Migration"}', false,
# MAGIC null, null ,"ALL_MUST_BE_MET" ,true ,null , null, null
# MAGIC );
