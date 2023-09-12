# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw='raw'
parameter_file_name='BA_Dimension_Parameter.prm'
parameter_section='BA_Dimension.WF:wf_site_hours_day'
parameter_key='source_url'
#parameter_value='https://petm-qa-facility-svc.cloudhub.io/facility-svc/v1/store/all/hours'  #qa
parameter_value='https://petm-prd-facility-svc.cloudhub.io/facility-svc/v1/store/all/hours'  #prod

#DO NOT Execute this script twice which will create the duplicates and the workflow will fail
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value) #Assumption is, this table already exist in Prod
