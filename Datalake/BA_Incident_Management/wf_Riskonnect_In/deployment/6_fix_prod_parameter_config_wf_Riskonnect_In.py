# Databricks notebook source
# MAGIC %sql select * from raw.parameter_config 
# MAGIC where id=23;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- incident_investigation. Update the already existing entry for Riskonnect
# MAGIC update raw.parameter_config 
# MAGIC set parameter_section='BA_Incident_Management.WF:wf_Riskonnect_In:incident_investigation',parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/incident_management/riskonnect_incident_investigation/'
# MAGIC where id=23;

# COMMAND ----------

# Add another entry for incident_claim
from Datalake.utils.genericUtilities import *

raw = "raw"
parameter_file_name = "BA_Incident_Management_Parameter.prm"
parameter_section = "BA_Incident_Management.WF:wf_Riskonnect_In:incident_claim"
parameter_key = "source_bucket"
parameter_value = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/incident_management/riskonnect_claim/"

# DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key, parameter_value
)  # Assumption is, this table already exist in Prod
