# Databricks notebook source
from Datalake.utils.genericUtilities import *

# COMMAND ----------

raw='dev_raw'
parameter_file_name='wf_BPC' 
parameter_section='m_bpc_ff' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-qa-apps-p1-gcs-gbl/nas_outbound/BPC/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_BPC' 
parameter_section='m_bpc_ff' 
parameter_key='key' 
parameter_value='bpc.out' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)
