from Datalake.utils.genericUtilities import *

raw='raw'

parameter_file_name='BA_Dimension_Parameter.prm'
parameter_section='BA_Dimension.WF:wf_Label_Price_ADVANCE'
parameter_key='source_bucket'

parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/masterdata/ztb_adv_lbl_chgs/'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_key='key'
parameter_value='ZTB_ADV_LBL_CHGS'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)