
from Datalake.utils.genericUtilities import *
raw='dev_raw'
parameter_file_name='wf_m_PetSmart_OpenAxes_HO_Out_ff'
parameter_section='m_PetSmart_OpenAxes_HO_Out_ff'
parameter_key='target_bucket'
parameter_value='gs://petm-bdpl-qa-apps-p1-gcs-gbl/nas_outbound/B2BShare/Ricoh/Outbound/'

insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)
