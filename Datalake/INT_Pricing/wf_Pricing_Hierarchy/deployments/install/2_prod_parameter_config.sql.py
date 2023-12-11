# Databricks notebook source
from Datalake.utils.genericUtilities import *

# COMMAND ----------

raw='raw'
parameter_file_name='wf_Pricing_Hierarchy.prm'
parameter_section='INT_Pricing.WF:wf_Pricing_Hierarchy.M:m_SAP_Pricing_Hierarchy_Pre'
parameter_key='source_bucket'
parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/pricing/sap_pricing_hierarchy_pre/'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)
