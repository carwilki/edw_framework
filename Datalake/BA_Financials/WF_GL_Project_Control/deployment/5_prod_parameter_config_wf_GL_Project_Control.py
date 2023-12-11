# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw = "raw"
parameter_file_name = "BA_Financials_Parameter.prm"
parameter_section = "BA_BA_Financials.WF:wf_GL_Project_Control"

parameter_key_prps = "source_bucket_prps"
parameter_value_prps = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/financials/prps/"

# DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key_prps, parameter_value_prps
)  # Assumption is, this table already exist in Prod


parameter_key_cosp = "source_bucket_cosp"
parameter_value_cosp = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/financials/cosp/"

# DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key_cosp, parameter_value_cosp
)  # Assumption is, this table already exist in Prod


parameter_key_bpge = "source_bucket_bpge"
parameter_value_bpge = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/financials/bpge/"

# DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key_bpge, parameter_value_bpge
)  # Assumption is, this table already exist in Prod