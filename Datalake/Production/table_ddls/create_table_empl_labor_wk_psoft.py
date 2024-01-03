# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE spark_catalog.legacy.empl_labor_wk_psoft (
# MAGIC   WEEK_DT TIMESTAMP NOT NULL,
# MAGIC   LOCATION_ID INT NOT NULL,
# MAGIC   STORE_DEPT_NBR STRING NOT NULL,
# MAGIC   EARN_ID STRING NOT NULL,
# MAGIC   JOB_CODE INT NOT NULL,
# MAGIC   FULLPT_FLAG STRING NOT NULL,
# MAGIC   HOURS_WORKED DECIMAL(9,2),
# MAGIC   EARNINGS_AMT DECIMAL(9,2),
# MAGIC   EARNINGS_LOC_AMT DECIMAL(9,2),
# MAGIC   CURRENCY_NBR SMALLINT,
# MAGIC   LOAD_TSTMP TIMESTAMP)
# MAGIC USING delta
# MAGIC LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Extracts/empl_labor_wk_psoft'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')
