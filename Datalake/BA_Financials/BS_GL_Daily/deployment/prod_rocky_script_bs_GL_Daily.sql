-- Databricks notebook source
INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'GL_DOC_TYPE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'GL_DOC_TYPE_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'GL_CATEGORY' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'GL_CATEGORY_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'BAL_FILE_INTRFACE' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'BAL_FILE_INTRFACE_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'GL_PROFIT_CENTER' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'GL_PROFIT_CENTER_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'SAP_CATEGORY' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'SAP_CATEGORY_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'GL_PLAN_FORECAST_MONTH' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'GL_PLAN_FORECAST_MONTH_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'BAL_FILE_INTRFACE_TXT' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'BAL_FILE_INTRFACE_TXT_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'GL_ACTUAL_DAY_DETAIL' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'GL_ACTUAL_DAY_DETAIL_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'GL_ACCT_GRP' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'GL_ACCT_GRP_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'GL_ACTUAL_DAY' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'GL_ACTUAL_DAY_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'GL_ACTUAL_MONTH' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'GL_ACTUAL_MONTH_history' --target_table_name
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


-- COMMAND ----------

INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config) 
VALUES('NZ_Migration' --table_group
, null --table_group_desc
, 'NZ_Mako8' --source_type
, 'EDW_PRD' --source_db
, 'GL_ACCOUNT' --source_table
, null --table_desc
, 'FALSE' --is_pii
, null --pii_type
, 'FALSE' --has_hard_deletes
, 'delta' --target_sink
, 'refine' --target_db
, 'NULL' --target_schema
, 'GL_ACCOUNT_history' --target_table_name
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