# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw='raw'

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_2018_DC_Shipments_Out' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/Relex/DC_Shipments/'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_2018_DC_Shipments_Out' 
parameter_key='key' 
parameter_value='Sales_Txn_DC_Shipments.csv' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_2018_DC_Shipments_Out' 
parameter_key='Delta_Filter1' 
parameter_value="WM_ORDERS.UPDATE_TSTMP>to_date('1900-01-01')"
update_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_2018_DC_Shipments_Out' 
parameter_key='Delta_Filter2' 
parameter_value="WM_ORDER_LINE_ITEM.UPDATE_TSTMP >to_date('1900-01-01')"
update_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_2018_DC_Shipments_Out' 
parameter_key='Last_Run_Date' 
parameter_value='1900-01-01' 
update_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# parameter_file_name='wf_RELEX_DC_Shipments_Out' 
# parameter_section='m_RELEX_2018_DC_Shipments_Out' 
# parameter_key='Last_Run_Date2' 
# parameter_value='01/01/1900' 
# insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_2018_DC_Shipments_Out' 
parameter_key='Filter' 
parameter_value='WM_ORDERS.CANCELLED_FLAG=0 AND WM_ORDERS.WM_DO_STATUS<>200 AND WM_ORDERS.WM_DO_STATUS>110 AND WM_ORDERS.WM_ACTUAL_SHIPPED_TSTMP IS NOT NULL AND WM_ORDERS.WM_EXT_PURCHASE_ORDER IS NOT NULL AND WM_LPN.WM_TC_ORDER_ID IS NULL' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)


parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_CAN_DC_Shipments_Out' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/Relex/DC_Shipments/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_CAN_DC_Shipments_Out' 
parameter_key='key' 
parameter_value='Sales_Txn_DC_Shipments.csv' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_CAN_DC_Shipments_Out' 
parameter_key='Delta_Filter' 
parameter_value="MOVEMENT_DAY.UPDATE_DT>to_date('1900-01-01')"
update_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_CAN_DC_Shipments_Out' 
parameter_key='Last_Run_Date' 
parameter_value='1900-01-01'
update_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_CAN_DC_Shipments_Out' 
parameter_key='Store_Filter' 
parameter_value='SITE_PROFILE_RPT.STORE_NBR IN (17,24)'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_DC_Shipments_Out' 
parameter_section='m_RELEX_CAN_DC_Shipments_Out' 
parameter_key='Move_Type_Filter' 
parameter_value='MOVEMENT_INFO.MOVE_TYPE_ID IN (641,642)'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# COMMAND ----------

 spark.sql(f""" select to_date("01/01/1900","MM/dd/yyyy") , to_date("1900-01-01")""").display()

# COMMAND ----------


