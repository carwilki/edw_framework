from Datalake.utils.genericUtilities import *

raw='raw'

parameter_file_name='INT_Merchandising_Parameter.prm'
parameter_section='INT_Merchandising.WF:wf_IKB_ODS_Daily'
parameter_key='source_bucket'

parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/merchandising/ztb_pog_grp_rls/'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_key='key'
parameter_value='ZTB_POG_GRP_RLS'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)