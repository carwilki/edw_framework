# Databricks notebook source
from Datalake.utils.genericUtilities import *

# COMMAND ----------

raw='raw'
parameter_file_name='bs_Demand_Planning_Day.prm'
parameter_section='Demand_Planning.WF:bs_Demand_Planning_Day.M:m_dp_site_vend_pre'
parameter_key='source_bucket'
parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/demand_planning/ztb_relate_mast/'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# COMMAND ----------


parameter_key='key'
parameter_value='ZTB_RELATE_MAST'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)


# COMMAND ----------

parameter_file_name='bs_Demand_Planning_Day.prm'
parameter_section='Demand_Planning.WF:bs_Demand_Planning_Day.M:m_SAP_Sku_Link_Type'
parameter_key='source_bucket'
parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/demand_planning/wrf_folup_typt/'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# COMMAND ----------


parameter_key='key'
parameter_value='WRF_FOLUP_TYPT'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# COMMAND ----------


parameter_file_name='bs_Demand_Planning_Day.prm'
parameter_section='Demand_Planning.WF:bs_Demand_Planning_Day.M:m_dp_sku_link_pre'
parameter_key='source_bucket'
parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/demand_planning/wrf_folup_typ_a/'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)


# COMMAND ----------


parameter_key='key'
parameter_value='WRF_FOLUP_TYP_A'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)


# COMMAND ----------


parameter_file_name='wf_PMSourceFileDir_RH01.prm'
parameter_section='BA_Demand_Planning.WF:bs_Demand_Planning_Day.ST:s_dp_sku_link'
parameter_key='SKU_LINK_TYPE_CD'
parameter_value='SUB'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)


# COMMAND ----------


parameter_file_name='wf_PMSourceFileDir_RH01.prm'
parameter_section='BA_Demand_Planning.WF:bs_Demand_Planning_Day.ST:s_SAP_Sku_Link_Type'
parameter_key='EDW_SKU_LINK_TYPE_CD'
parameter_value='IF(UPPER(FOLLOWUP_TYP_NR) = "01","SUB","FOLLOWUP_TYP_NR")'

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)
