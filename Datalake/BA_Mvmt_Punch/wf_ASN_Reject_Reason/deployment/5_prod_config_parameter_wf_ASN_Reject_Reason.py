from Datalake.utils.genericUtilities import *

raw='raw'
parameter_file_name='wf_ASN_Reject_Reason'
parameter_section='m_ASN_Reject_Reason'
parameter_key='source_bucket'
parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/movement/asn_n_cplt_rpt/'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_key='key'
parameter_value='ASN_N_CPLT_RPT'

insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)
