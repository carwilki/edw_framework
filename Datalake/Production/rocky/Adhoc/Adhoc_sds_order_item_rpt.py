# Databricks notebook source
# MAGIC %sql
# MAGIC drop table legacy.sds_order_item_rpt

# COMMAND ----------

dbutils.fs.rm("gs://petm-bdpl-prod-refine-p1-gcs-gbl/IT/sds_order_item_rpt",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC update work.rocky_ingestion_metadata set is_pii=true,pii_type="customer" where rocky_id=824

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from work.rocky_ingestion_control where rocky_id=824

# COMMAND ----------

# MAGIC %sql
# MAGIC update work.pii_dynamic_view_control set view_created_flg=False where view_name="sds_order_item_rpt" and view_database="legacy"
