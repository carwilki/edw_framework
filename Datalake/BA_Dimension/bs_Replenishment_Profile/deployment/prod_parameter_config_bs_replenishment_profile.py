from Datalake.utils.genericUtilities import *
 
raw = "raw"
parameter_file_name = "BA_Dimension_Parameter.prm"
parameter_section_1 = "BA_Dimension.WF:bs_Replenishment_Profile.M:m_site_group_pre"
parameter_key = "source_bucket"
parameter_value_1 = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/replenishment_profile/"
 
 
# DO NOT Execute this script twice which will create the duplicates and the workflow will fail
 
insert_param_config(
    raw, parameter_file_name, parameter_section_1, parameter_key, parameter_value_1
)

parameter_section_2="BA_Dimension.WF:bs_Replenishment_Profile.M:m_replenishment_pre"
 insert_param_config(
    raw, parameter_file_name, parameter_section_2, parameter_key, parameter_value_1
)
 
parameter_section_3="BA_Dimension.WF:bs_Replenishment_Profile.M:m_ZTB_ART_LOC_SITE_PRE"
parameter_value_3 = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/masterdata/ztb_art_loc_site/"
insert_param_config(
    raw, parameter_file_name, parameter_section_3, parameter_key, parameter_value_3
)