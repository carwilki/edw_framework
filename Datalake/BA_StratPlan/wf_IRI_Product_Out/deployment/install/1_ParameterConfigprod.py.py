# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw='raw'

parameter_file_name='wf_IRI_Product_Out' 
parameter_section='m_IRI_Product_Out' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/IRI/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_IRI_Product_Out' 
parameter_section='m_IRI_Product_Out' 
parameter_key='key' 
parameter_value='Petsmart_Product.dat' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_IRI_Product_Out' 
parameter_section='m_IRI_Product_Out' 
parameter_key='Last_Run_date' 
parameter_value='01/01/1900' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_IRI_Product_Out' 
parameter_section='m_IRI_Product_Out' 
parameter_key='SKU_Date_Filter' 
parameter_value='SKU_PROFILE_RPT.UPDATE_DT>to_date("01/01/1900","MM/dd/yyyy")'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

