from Datalake.utils.genericUtilities import insert_param_config

raw = "raw"
parameter_file_name = "wf_store_data"
parameter_section = "m_store_data"
parameter_key = "source_bucket"
parameter_value = "gs://petm-bdpl-qa-prep-p1-gcs-gbl/nas/masterdata/store_data"

# DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key, parameter_value
)

parameter_key = "archive_bucket"
parameter_value = "gs://petm-bdpl-qa-raw-p1-gcs-gbl/nas/masterdata/store_data/"

insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key, parameter_value
)

parameter_key = "key"
parameter_value = "store_data"

insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key, parameter_value
)
