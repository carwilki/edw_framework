# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw = "raw"
parameter_file_name = "BA_Demand_Planning_Parameter.prm"
parameter_section = "BA_Demand_Planning.WF:wf_relex_to_EDW_Daily"
parameter_key = "source_bucket"
parameter_value = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/demand_planing/"

# DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(
    raw, parameter_file_name, parameter_section, parameter_key, parameter_value
)  # Assumption is, this table already exist in Prod