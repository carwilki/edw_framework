# Databricks notebook source
# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw='raw'
parameter_file_name='wf_NZ2ORA_SC_RP_Replication'
parameter_section='m_NZ2ORA_Replenishment_profile_flat_file'
parameter_key='target_bucket'
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/SupplyChain/replenishment_profile'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_section='m_NZ2ORA_Replenishment_profile_flat_file'
parameter_key='target_file'
parameter_value='replenishment_profile.dat'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)
