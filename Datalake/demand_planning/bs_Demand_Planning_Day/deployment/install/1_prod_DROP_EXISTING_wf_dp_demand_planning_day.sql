-- Databricks notebook source
DROP TABLE legacy.DP_SITE_VEND_PROFILE;

-- COMMAND ----------

CREATE TABLE refine.DP_SITE_VEND_PROFILE_history
USING delta
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/legacy/dp_site_vend_profile';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('gs://petm-bdpl-prod-refine-p1-gcs-gbl/legacy/dp_site_vend_profile', True)

-- COMMAND ----------

DROP TABLE legacy.DP_SKU_LINK;

-- COMMAND ----------

CREATE TABLE refine.DP_SKU_LINK_history
USING delta
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/legacy/dp_sku_link';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('gs://petm-bdpl-prod-refine-p1-gcs-gbl/legacy/dp_sku_link', True)
