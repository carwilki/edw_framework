-- Databricks notebook source
CREATE TABLE stranger_things.snowflake_cdc_log (
  dlSchema STRING,
  dlTable STRING,
  targetDatabase STRING,
  targetSchema STRING,
  targetTable STRING,
  version BIGINT,
  timestamp TIMESTAMP)
USING delta
LOCATION 'gs://petm-bdpl-prod-systemdb-p1-gcs-gbl/metadata/tables/snowflake_cdc_log';

-- COMMAND ----------

INSERT INTO stranger_things.snowflake_cdc_log values("legacy","WFA_TIME_SHEET_PUNCH","EDW_PROD","public","wfa_time_sheet_punch_lgcy",NULL,CURRENT_DATE);

update stranger_things.snowflake_cdc_log set version = (SELECT max(version) FROM (DESCRIBE HISTORY legacy.WFA_TIME_SHEET_PUNCH)) where dlTable = "WFA_TIME_SHEET_PUNCH";