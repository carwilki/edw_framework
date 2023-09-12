-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_CATEGORY_TYPE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_CATEGORY_TYPE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_CATEGORY_TYPE_FOCUS_AREA' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_CATEGORY_TYPE_FOCUS_AREA_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_CLASS_PROMO' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_CLASS_PROMO_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_CLASS_TYPE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_CLASS_TYPE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_CLASS_TYPE_DETAIL' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_CLASS_TYPE_DETAIL_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_FOCUS_AREA' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_FOCUS_AREA_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_INVOICE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_INVOICE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_PACKAGE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_PACKAGE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_PACKAGE_OPTION' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_PACKAGE_OPTION_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_PACKAGE_OPTION_CLASS_TYPE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_PACKAGE_OPTION_CLASS_TYPE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_PACKAGE_OPTION_DETAIL' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_PACKAGE_OPTION_DETAIL_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_PACKAGE_PROMO' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_PACKAGE_PROMO_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_PET' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_PET_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_PET_FOCUS_AREA' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_PET_FOCUS_AREA_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_RESERVATION' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_RESERVATION_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_RESERVATION_HISTORY' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_RESERVATION_HISTORY_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_SCHED_CHANGE_CAPTURE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_SCHED_CHANGE_CAPTURE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_SCHED_CHANGE_STATE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_SCHED_CHANGE_STATE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_SCHED_CHANGE_TYPE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_SCHED_CHANGE_TYPE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);



-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_SCHED_CLASS_TYPE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_SCHED_CLASS_TYPE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);



-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_SCHED_ENTITY_TYPE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_SCHED_ENTITY_TYPE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_SCHED_STORE_CLASS' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_SCHED_STORE_CLASS_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_SCHED_STORE_CLASS_DETAIL' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_SCHED_STORE_CLASS_DETAIL_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_SCHED_TRAINER' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_SCHED_TRAINER_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_STORE_BLACKOUTS' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_STORE_BLACKOUTS_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_STORE_CLASS' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_STORE_CLASS_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_STORE_CLASS_DETAIL' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_STORE_CLASS_DETAIL_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_TRAINER' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_TRAINER_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_UNIT_OF_MEASURE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_UNIT_OF_MEASURE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_UNIT_OF_MEASURE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'TRAINING_UNIT_OF_MEASURE_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'SDS_PRD' --source_db
, 'TRAINING_CUSTOMER' --source_table
, null --table_desc
, 'TRUE' --is_pii
, 'customer' --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'legacy' --target_schema
, 'TRAINING_CUSTOMER_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);


-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'PET_TRAINING_RESERVATION' --source_table
, null --table_desc
, 'TRUE' --is_pii
, 'customer' --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'legacy' --target_schema
, 'PET_TRAINING_RESERVATION_history' --target_table_name
, 'full' --load_type
, null --source_delta_column
, null --primary_key
, null --initial_load_filter
, 'one-time' --load_frequency
, '0 0 6 ? * *' --load_cron_expr
, array('NULL') --tidal_dependencies
, null --tidal_trigger_condition
, null --expected_start_time
, array("rrajamani@petsmart.com", "pshekhar@petsmart.com", "APipewala@PetSmart.com") --job_watchers
, 0 --max_retry
, 'TRUE' --disable_no_record_failure
,'{"Department":"Netezza-Migration"}' --job_tag
, 'FALSE' --is_scheduled
, null --job_id
, null --snowflake_ddl
, null --snowflake_pre_sql
, null --snowflake_post_sql
, null --additional_config
);

