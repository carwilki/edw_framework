# Databricks notebook source
# MAGIC %md
# MAGIC ####create view legacy.purch_group on top of refine.sap_t024_purch_group , as it is required as source for wf_POG_Weekly_Extracts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW legacy.purch_group AS 
# MAGIC SELECT PURCH_GROUP_ID, PURCH_GROUP_NAME FROM refine.sap_t024_purch_group 
