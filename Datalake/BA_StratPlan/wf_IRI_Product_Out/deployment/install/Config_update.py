# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw='raw'

parameter_file_name='wf_IRI_Product_Out' 
parameter_section='m_IRI_Product_Out' 
parameter_key='Last_Run_date' 
parameter_value='1900-01-01' 
update_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_IRI_Product_Out' 
parameter_section='m_IRI_Product_Out' 
parameter_key='SKU_Date_Filter' 
parameter_value="SKU_PROFILE_RPT.UPDATE_DT>to_date('1900-01-01')"
update_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)
