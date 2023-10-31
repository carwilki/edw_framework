-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8_legacy'
, "EDW_PRD"
, "LISTING_DAY"
, null
, false
, null
, false
, "delta"
, "legacy"
, null
, "LISTING_DAY"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8'
, "EDW_PRD"
, "OPEN_PO_PRE"
, null
, false
, null
, false
, "delta"
, "raw"
, null
, "OPEN_PO_PRE"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8'
, "EDW_PRD"
, "OPEN_STO_PRE"
, null
, false
, null
, false
, "delta"
, "raw"
, null
, "OPEN_STO_PRE"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8_legacy'
, "EDW_PRD"
, "PLANOGRAM_PRO"
, null
, false
, null
, false
, "delta"
, "legacy"
, null
, "PLANOGRAM_PRO"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8_legacy'
, "EDW_PRD"
, "POG_SKU_PRO"
, null
, false
, null
, false
, "delta"
, "legacy"
, null
, "POG_SKU_PRO"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8_legacy'
, "EDW_PRD"
, "POG_SKU_STORE"
, null
, false
, null
, false
, "delta"
, "legacy"
, null
, "POG_SKU_STORE"
, "upsert"
, "UPDATE_TSTMP"
, "PRODUCT_ID,LOCATION_ID,POG_NBR,POG_DBKEY,LISTING_START_DT"
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8_legacy'
, "EDW_PRD"
, "POG_SKU_STORE_PRO"
, null
, false
, null
, false
, "delta"
, "legacy"
, null
, "POG_SKU_STORE_PRO"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8_legacy'
, "EDW_PRD"
, "POG_STORE_PRO"
, null
, false
, null
, false
, "delta"
, "legacy"
, null
, "POG_STORE_PRO"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8_legacy'
, "EDW_PRD"
, "PRODUCT"
, null
, false
, null
, false
, "delta"
, "legacy"
, null
, "PRODUCT"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8_legacy'
, "EDW_PRD"
, "SKU_SITE_PROFILE"
, null
, false
, null
, false
, "delta"
, "legacy"
, null
, "SKU_SITE_PROFILE"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8'
, "EDW_PRD"
, "SKU_STORE_PRICE_COSTS_PRE"
, null
, false
, null
, false
, "delta"
, "raw"
, null
, "SKU_STORE_PRICE_COSTS_PRE"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);

-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata (
table_group
, table_group_desc
, source_type
, source_db
, source_table
, table_desc
, is_pii
, pii_type
, has_hard_deletes
, target_sink
, target_db
, target_schema
, target_table_name
, load_type
, source_delta_column
, primary_key
, initial_load_filter
, load_frequency
, load_cron_expr
, tidal_dependencies
, expected_start_time
, job_watchers
, max_retry
, job_tag
, is_scheduled
, job_id
, snowflake_ddl
, tidal_trigger_condition
, disable_no_record_failure
, snowflake_pre_sql
, snowflake_post_sql
,additional_config
)
VALUES (
"NZ_Migration"
, null
, 'NZ_Mako8_legacy'
, "EDW_PRD"
, "SUPPLY_CHAIN"
, null
, false
, null
, false
, "delta"
, "legacy"
, null
, "SUPPLY_CHAIN"
, "full"
, null
, null
, null
, "daily"
, null
, array("DUMMY_TIDAL_JOB")
, null
, array("ghuddar@petsmart.com", "sjaiswal@petsmart.com", "DL_BIG_DATA_OPERATIONS@PetSmart.com")
, 3
, '{"Department":"Netezza-Migration"}'
, false
, null
, null
, "ALL_MUST_BE_MET" 
, false
, null
, null
, null
);
