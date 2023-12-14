from Datalake.utils.genericUtilities import *

raw='raw'
parameter_file_name='wf_ecova' 
parameter_section='m_ecova_department' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/ECOVA/' 

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_ecova' 
parameter_section='m_ecova_department' 
parameter_key='key' 
parameter_value='ecova_department.dat' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_ecova' 
parameter_section='m_ecova_employee' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/ECOVA/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_ecova' 
parameter_section='m_ecova_employee' 
parameter_key='key' 
parameter_value='ecova_employee.dat' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)




