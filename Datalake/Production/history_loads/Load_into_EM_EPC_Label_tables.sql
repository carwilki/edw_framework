-- Databricks notebook source
insert into legacy.EM_AMS_OFFER_TIERED_PRICE select * from dev_legacy.EM_AMS_OFFER_TIERED_PRICE

-- COMMAND ----------

insert into legacy.EM_AMS_OFFER_SKU select * from dev_legacy.EM_AMS_OFFER_SKU

-- COMMAND ----------

insert into legacy.EM_AMS_OFFER_COUNTRY select * from dev_legacy.EM_AMS_OFFER_COUNTRY

-- COMMAND ----------

insert into legacy.EM_AMS_OFFER_LABEL select * from dev_legacy.EM_AMS_OFFER_LABEL

-- COMMAND ----------

insert into legacy.EM_AMS_TEMPLATE_TYPE select * from dev_legacy.EM_AMS_TEMPLATE_TYPE

-- COMMAND ----------

insert into legacy.EM_DISCLAIMER_TYPE select * from dev_legacy.EM_DISCLAIMER_TYPE

-- COMMAND ----------

insert into legacy.EM_AMS_DISCOUNT_TYPE select * from dev_legacy.EM_AMS_DISCOUNT_TYPE

-- COMMAND ----------

insert into legacy.EM_AMS_OFFER_LIMIT_TYPE select * from dev_legacy.EM_AMS_OFFER_LIMIT_TYPE

-- COMMAND ----------

insert into legacy.EM_AMS_SKU_GROUP_TYPE select * from dev_legacy.EM_AMS_SKU_GROUP_TYPE
