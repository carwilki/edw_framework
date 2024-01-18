# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw='dev_raw'

parameter_file_name='wf_RELEX_StoreUsage_Out' 
parameter_section='m_RELEX_StoreUsage_Out' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-qa-apps-p1-gcs-gbl/nas_outbound/Relex/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_StoreUsage_Out' 
parameter_section='m_RELEX_StoreUsage_Out' 
parameter_key='key' 
parameter_value='Write_Off_Sales_Transactions.csv' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_StoreUsage_Out' 
parameter_section='m_RELEX_StoreUsage_Out' 
parameter_key='Movement_Filter' 
parameter_value='MOVEMENT_INFO.MOVE_REASON_ID IN (36,37,38,40,46,50,52,53,63)' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_StoreUsage_Out' 
parameter_section='m_RELEX_StoreUsage_Out' 
parameter_key='Site_Filter' 
parameter_value='SITE_PROFILE_RPT.LOCATION_TYPE_ID = 8 AND SITE_PROFILE_RPT.SITE_SALES_FLAG=1' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_StoreUsage_Out' 
parameter_section='m_RELEX_StoreUsage_Out' 
parameter_key='LAST_RUN_DATE' 
parameter_value='09/17/2020' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_StoreUsage_Out' 
parameter_section='m_RELEX_StoreUsage_Out' 
parameter_key='Delta_Filter' 
parameter_value='MOVEMENT_DAY.UPDATE_DT>to_date("09/17/2020","MM/dd/yyyy")'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_StoreUsage_Out' 
parameter_section='m_RELEX_StoreUsage_Out' 
parameter_key='Exclude_Flag' 
parameter_value='if(MOVE_REASON_ID in (200,201,204) AND SAP_DEPT_ID NOT IN (50,59) ,1,0)' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)
