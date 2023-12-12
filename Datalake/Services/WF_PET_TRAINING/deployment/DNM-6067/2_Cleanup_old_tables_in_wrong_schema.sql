-- Databricks notebook source
DROP TABLE raw.TRAINING_PET_PRE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_pet_pre', True);

-- COMMAND ----------

DROP TABLE legacy.TRAINING_PET

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.rm('gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_pet', True);
