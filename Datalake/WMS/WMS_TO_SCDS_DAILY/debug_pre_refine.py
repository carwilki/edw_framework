# Databricks notebook source
from Datalake.WMS.WMS_TO_SCDS_DAILY.notebooks.m_WM_Yard_Zone_PRE import *

# COMMAND ----------

m_WM_Yard_Zone_PRE("dc10","qa")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
