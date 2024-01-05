-- Databricks notebook source
-- MAGIC %md
-- MAGIC This table is used as a source for databricks workflow wf_cma_pet_training_sku as a part of NZ migration

-- COMMAND ----------

-- DBTITLE 1,Table DDL
CREATE TABLE legacy.cma_usr_pet_training_sku (
	PRODUCT_ID INT NOT NULL,
	UPC STRING,
	SKU_NBR INT,
	ACTIVE_FLAG SMALLINT,
	UPDATE_TSTMP TIMESTAMP,
	LOAD_TSTMP TIMESTAMP
)
USING delta
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Mobile/cma_usr_pet_training_sku'

-- COMMAND ----------

-- DBTITLE 1,One time load
insert into legacy.cma_usr_pet_training_sku select * from dev_legacy.nz_cma_usr_pet_training_sku
