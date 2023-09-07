-- Databricks notebook source
CREATE TABLE stranger_things.ingestion_metadata (
  table_name STRING,
  timestamp_columns STRING)
USING delta
LOCATION 'gs://petm-bdpl-prod-systemdb-p1-gcs-gbl/metdata/tables/ingestion_metadata'
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

insert into stranger_things.ingestion_metadata select * from refine.ingestion_metadata;
