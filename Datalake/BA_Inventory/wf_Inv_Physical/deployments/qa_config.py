# Databricks notebook source
from Datalake.utils.genericUtilities import *

# COMMAND ----------

raw='qa_raw'
parameter_file_name='wf_inv_physical.prm'
parameter_section='BA_Inventory.WF:wf_inv_physical.M:m_SAP_IKPF_Pre'
parameter_key='source_bucket'
parameter_value='gs://petm-bdpl-qa-raw-p1-gcs-gbl/sap/inventory/ikpf/'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# COMMAND ----------

parameter_file_name='wf_inv_physical.prm'
parameter_section='BA_Inventory.WF:wf_inv_physical.M:m_ZTB_RF_PHYINV_Pre'
parameter_key='source_bucket'
parameter_value='gs://petm-bdpl-qa-raw-p1-gcs-gbl/sap/inventory/ztb_rf_phyinv/'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# COMMAND ----------

parameter_file_name='wf_inv_physical.prm'
parameter_section='BA_Inventory.WF:wf_inv_physical.M:m_SAP_ISEG_Pre'
parameter_key='source_bucket'
parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/inventory/iseg/'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)
