# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw = "raw"
parameter_file_name = "INT_Labor_Mgmt_Parameter.prm"
parameter_section = "INT_Labor_Mgmt.WF:wf_LMS_Holiday_Freight"
parameter_key = "source_bucket"
parameter_value = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/masterdata/employee/"

# DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key, parameter_value
)  # Assumption is, this table already exist in Prod