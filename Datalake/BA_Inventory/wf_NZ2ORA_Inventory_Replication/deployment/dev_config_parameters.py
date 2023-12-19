from Datalake.utils.genericUtilities import *

raw='dev_raw'

parameter_file_name='wf_NZ2ORA_Inventory_Replication'
parameter_section='m_NZ2ORA_INV_INSTOCK_PRICE_DAY_FLAT'
parameter_key='target_bucket'
parameter_value='gs://petm-bdpl-qa-apps-p1-gcs-gbl/nas_outbound/biwpload/extracts/'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_section='m_NZ2ORA_INV_INSTOCK_PRICE_DAY_FLAT'
parameter_key='target_file'
parameter_value='inv_instock_price_day.dat'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)