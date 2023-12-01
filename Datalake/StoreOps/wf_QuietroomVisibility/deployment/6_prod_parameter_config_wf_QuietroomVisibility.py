
from Datalake.utils.genericUtilities import *

raw = "raw"
parameter_file_name = "BA_Store_Ops.prm"
parameter_section = "BA_Store_Ops.WF:wf_QuietRoom_Visibility"
parameter_key = "source_bucket"
parameter_value = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/storeops/"

# DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key, parameter_value)  #

