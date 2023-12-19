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
parameter_value='pet_dept.txt' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_ecova' 
parameter_section='m_ecova_employee' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/ECOVA/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_ecova' 
parameter_section='m_ecova_employee' 
parameter_key='key' 
parameter_value='pet_empl.txt' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_ecova' 
parameter_section='m_ecova_employee' 
parameter_key='nas_target_path' 
parameter_value='\\\\\\\\nas05\\\\edwshare\\\\DataLake\\\\Temp_NZ_Migration\\\\ECOVA\\\\' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_ecova' 
parameter_section='m_ecova_department' 
parameter_key='nas_target_path' 
parameter_value='\\\\\\\\nas05\\\\edwshare\\\\DataLake\\\\Temp_NZ_Migration\\\\ECOVA\\\\'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)





