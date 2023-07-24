# Databricks notebook source
# MAGIC %sql
# MAGIC  INSERT INTO work.rocky_ingestion_metadata (
# MAGIC  table_group, table_group_desc, source_type, source_db, source_table, 
# MAGIC  table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
# MAGIC  target_db, target_schema, target_table_name, load_type, source_delta_column,
# MAGIC  primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
# MAGIC  expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
# MAGIC  job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql , snowflake_post_sql ,additional_config
# MAGIC  )
# MAGIC  VALUES (
# MAGIC  "NZ_Migration", null, "NZ_Mako8", "EDW_PRD", "LOCATION", 
# MAGIC  null, false, null, false, "delta",
# MAGIC  "legacy", "public", "location", "full", null, 
# MAGIC  'LOCATION_ID', null, "one-time", null, array('DUMMY_TIDAL_JOB'),
# MAGIC  null, array("abelsare@petsmart.com","DL_BIG_DATA_OPERATIONS@PetSmart.com"), 1, null, false,
# MAGIC  null, null ,'ANY_ONE_IS_MET' ,null ,null , null , null
# MAGIC  );
