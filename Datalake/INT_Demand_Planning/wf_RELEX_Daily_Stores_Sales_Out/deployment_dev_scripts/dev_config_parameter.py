from Datalake.utils.genericUtilities import *

raw='dev_raw'


parameter_file_name='wf_RELEX_Daily_Stores_Sales_Out' 
parameter_section='m_RELEX_Daily_Stores_Sales_Out' 
parameter_key='nas_target' 
parameter_value='\\\\\\\\nas05\\\\edwshare\\\\DataLake\\\\Temp_NZ_Migration\\\\RELEX\\\\sales_txn_stores\\\\' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_Daily_Stores_Sales_Out' 
parameter_section='m_RELEX_Daily_Stores_Sales_Out' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-qa-apps-p1-gcs-gbl/nas_outbound/RELEX/sales_txn_stores/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_Daily_Stores_Sales_Out' 
parameter_section='m_RELEX_Daily_Stores_Sales_Out' 
parameter_key='key' 
parameter_value='Sales_Txn_Stores.csv' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_Daily_Stores_Sales_Out' 
parameter_section='m_RELEX_Daily_Stores_Sales_Out' 
parameter_key='DC_Filter' 
parameter_value='SITE_PROFILE_RPT.STORE_TYPE_ID != "100"' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_Daily_Stores_Sales_Out' 
parameter_section='m_RELEX_Daily_Stores_Sales_Out' 
parameter_key='Sku_Type_Filter' 
parameter_value='SKU_PROFILE_RPT.SKU_TYPE !="ZSPC"'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_RELEX_Daily_Stores_Sales_Out' 
parameter_section='m_RELEX_Daily_Stores_Sales_Out' 
parameter_key='Last_Run_Date' 
parameter_value='04/20/2021' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# parameter_file_name='wf_RELEX_Daily_Stores_Sales_Out' 
# parameter_section='m_RELEX_Daily_Stores_Sales_Out' 
# parameter_key='Delta_Filter' 
# parameter_value='DATE(SALES_DAY_SKU_STORE_RPT.UPDATE_DT)>to_date("04/20/2021","MM/dd/yyyy")'
# insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)