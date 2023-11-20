-- Databricks notebook source
-- 1. Drop legacy.POG_SKU_STORE
-- 2. Drop refine.POG_SKU_STORE_history
-- 3. Recreate rocky table

-- COMMAND ----------

DROP TABLE legacy.POG_SKU_STORE;

-- COMMAND ----------

DROP TABLE refine.POG_SKU_STORE_history

-- COMMAND ----------

CREATE TABLE legacy.POG_SKU_STORE
USING delta
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/legacy/pog_sku_store';
