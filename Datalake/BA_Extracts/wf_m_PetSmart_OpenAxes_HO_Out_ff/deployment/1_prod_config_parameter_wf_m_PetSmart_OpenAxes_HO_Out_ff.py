from Datalake.utils.genericUtilities import *
raw='raw'
parameter_file_name='wf_m_PetSmart_OpenAxes_HO_Out_ff'
parameter_section='m_PetSmart_OpenAxes_HO_Out_ff'
parameter_key='target_bucket'
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/Ricoh/openaxes/'

insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_m_PetSmart_OpenAxes_HO_Out_ff'
parameter_section='m_PetSmart_OpenAxes_HO_Out_ff'
parameter_key='key'
parameter_value='PetSmart_OpenAxes_HOOut.TXT'

insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_file_name='wf_m_PetSmart_OpenAxes_HO_Out_ff'
parameter_section='m_PetSmart_OpenAxes_HO_Out_ff'
parameter_key='nas_target_path'
parameter_value='/mnt/nas05/edwshare/DataLake/Temp_NZ_Migration/Ricoh/openaxes/'

insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)