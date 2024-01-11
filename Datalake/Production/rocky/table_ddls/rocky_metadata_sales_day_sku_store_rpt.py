# Databricks notebook source
# MAGIC %md
# MAGIC ####rocky table SALES_DAY_SKU_STORE_RPT, as it is required source for wf_sales_ranking_wk workflow

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
# MAGIC "NZ_Migration", null, 'NZ_Mako8_legacy', "EDW_PRD", "SALES_DAY_SKU_STORE_RPT",
# MAGIC null, false, null, false, "delta",
# MAGIC "legacy", null, "sales_day_sku_store_rpt", "upsert", "UPDATE_DT", 
# MAGIC "DAY_DT,PRODUCT_ID,LOCATION_ID,SALES_CUST_CAPTURE_CD", "UPDATE_DT>='2024-01-01' and UPDATE_DT<='2024-01-02'", "daily", null, array("DUMMY_TIDAL_JOB"), 
# MAGIC null, array("abelsare@petsmart.com","pkulkarni@petsmart.com","sjaiswal@petsmart.com"), 3, '{"Department":"Netezza-Migration"}', false,
# MAGIC null, null ,"ALL_MUST_BE_MET" ,true ,null , null, null
# MAGIC );
