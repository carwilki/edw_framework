from Datalake.utils.genericUtilities import *

raw='raw'

parameter_file_name='wf_SMG_Extract' 
parameter_section='m_smg_site_extract' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/SMG/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_SMG_Extract' 
parameter_section='m_smg_site_extract' 
parameter_key='key' 
parameter_value='smg_site_extract.csv' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_SMG_Extract' 
parameter_section='m_smg_site_extract' 
parameter_key='p_sku_dept' 
parameter_value='SKU_PROFILE.SAP_DEPT_ID IN (3136,81)' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)


