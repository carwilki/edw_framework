-- Databricks notebook source
CREATE TABLE spark_catalog.legacy.empl_labor_wk_psoft (
  WEEK_DT TIMESTAMP NOT NULL,
  LOCATION_ID INT NOT NULL,
  STORE_DEPT_NBR STRING NOT NULL,
  EARN_ID STRING NOT NULL,
  JOB_CODE INT NOT NULL,
  FULLPT_FLAG STRING NOT NULL,
  HOURS_WORKED DECIMAL(9,2),
  EARNINGS_AMT DECIMAL(9,2),
  EARNINGS_LOC_AMT DECIMAL(9,2),
  CURRENCY_NBR SMALLINT,
  LOAD_TSTMP TIMESTAMP)
USING delta
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Extracts/empl_labor_wk_psoft'
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')
