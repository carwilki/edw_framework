# Databricks notebook source
# MAGIC %md
# MAGIC ####rocky table SALES_TRANS_SKU, as it is required source for wf_ShopperTrak_Sales

# COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group, table_group_desc, source_type, source_db, source_table, 
table_desc, is_pii, pii_type, has_hard_deletes, target_sink, 
target_db, target_schema, target_table_name, load_type, source_delta_column,
primary_key, initial_load_filter, load_frequency, load_cron_expr, tidal_dependencies,
expected_start_time, job_watchers, max_retry, job_tag, is_scheduled,
job_id, snowflake_ddl , tidal_trigger_condition , disable_no_record_failure , snowflake_pre_sql , snowflake_post_sql ,additional_config
)
VALUES (
"NZ_Migration", null, 'NZ_Export_Mako8', "EDW_PRD", "SALES_TRANS_SKU",
null, false, null, false, "delta",
"legacy", null, "sales_trans_sku", "upsert", "UPDATE_TSTMP", 
"DAY_DT,SALES_INSTANCE_ID_DIST_KEY,PRODUCT_ID", "UPDATE_TSTMP > '2024-01-17 00:00:00'", "daily", null, array("DUMMY_TIDAL_JOB"), 
null, array("sjaiswal@petsmart.com","dbodake@petsmart.com"), 3, '{"Department":"Netezza-Migration"}', false,
null, null ,"ALL_MUST_BE_MET" ,true ,null , null, null
);
