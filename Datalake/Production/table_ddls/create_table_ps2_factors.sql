-- Databricks notebook source
CREATE TABLE legacy.ps2_factors (
  ROW_NO BIGINT NOT NULL,
  FACTOR_TYPE STRING NOT NULL,
  LOCATION_ID STRING NOT NULL,
  STORE_NBR STRING NOT NULL,
  START_DT TIMESTAMP NOT NULL,
  END_DT TIMESTAMP,
  FACTOR_DESC STRING,
  OTHER STRING,
  FACTOR_VALUE DECIMAL(16,9),
  UPDATE_DT TIMESTAMP,
  LOAD_DT TIMESTAMP)
USING delta
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/labor_mgmt/ps2_factors'
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')
