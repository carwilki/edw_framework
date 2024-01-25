from Datalake.utils.genericUtilities import *

raw='raw'

parameter_file_name='wf_ShopperTrak_Sales'
parameter_section='m_ST_SALES_FF'
parameter_key='target_bucket'
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/ShopperTrak/sales/'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)


parameter_file_name='wf_ShopperTrak_Sales'
parameter_section='m_ST_SALES_FF'
parameter_key='nas_target'
parameter_value='/mnt/nas05/edwshare/DataLake/Temp_NZ_Migration/ShopperTrak/sales/'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)