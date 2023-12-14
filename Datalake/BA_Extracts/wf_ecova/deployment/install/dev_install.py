# Databricks notebook source
from Datalake.utils.genericUtilities import *

# COMMAND ----------

raw='dev_raw'
parameter_file_name='wf_ecova' 
parameter_section='m_ecova_department' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-qa-apps-p1-gcs-gbl/nas_outbound/ECOVA/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_ecova' 
parameter_section='m_ecova_department' 
parameter_key='key' 
parameter_value='ecova_department.dat' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_ecova' 
parameter_section='m_ecova_employee' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-qa-apps-p1-gcs-gbl/nas_outbound/ECOVA/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_ecova' 
parameter_section='m_ecova_employee' 
parameter_key='key' 
parameter_value='ecova_employee.dat' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)
