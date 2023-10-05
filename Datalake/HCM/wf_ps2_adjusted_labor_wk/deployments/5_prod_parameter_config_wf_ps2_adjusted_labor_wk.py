# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw = "raw"
parameter_file_name = "BA_HCM.prm"
parameter_section = "BA_HCM.WF:wf_ps2_adjusted_labor_wk"
parameter_key = "source_bucket"
parameter_value = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/employee/ps2adjlbr/"

# DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key, parameter_value
)  # Assumption is, this table already exist in Prod
