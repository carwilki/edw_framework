from Datalake.utils.genericUtilities import *

raw='raw'

parameter_file_name='wf_Nielsen_MasterData' 
parameter_section='m_Nielsen_Store_ff' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/Nielsen/petm_store/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_Nielsen_MasterData' 
parameter_section='m_Nielsen_Store_ff' 
parameter_key='key' 
parameter_value='PETM_Store.txt' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)


parameter_file_name='wf_Nielsen_MasterData' 
parameter_section='m_Nielsen_Article_ff' 
parameter_key='target_bucket' 
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/Nielsen/petm_article/' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_Nielsen_MasterData' 
parameter_section='m_Nielsen_Article_ff' 
parameter_key='key' 
parameter_value='PETM_Article.txt' 
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)


