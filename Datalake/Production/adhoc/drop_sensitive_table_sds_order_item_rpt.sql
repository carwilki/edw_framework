-- Databricks notebook source
drop table cust_sensitive.legacy_sds_order_item_rpt

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("gs://petm-bdpl-prod-cust-sensitive-refine-p1-gcs-gbl/IT/sds_order_item_rpt", True)

-- COMMAND ----------

update work.rocky_ingestion_metadata set source_type="NZ_Mako8" where rocky_id = 824
