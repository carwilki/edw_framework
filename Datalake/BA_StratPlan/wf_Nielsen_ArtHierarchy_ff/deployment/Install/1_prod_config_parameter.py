# Databricks notebook source
from Datalake.utils.genericUtilities import *

raw='raw'

parameter_file_name='wf_Nielsen_ArtHierarchy_ff'
parameter_section='m_Nielsen_ArtHierarchy_ff'
parameter_key='target_bucket'
parameter_value='gs://petm-bdpl-prod-apps-p1-gcs-gbl/nas_outbound/Nielsen/petm_hierarchy/'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

parameter_section='m_Nielsen_ArtHierarchy_ff'
parameter_key='target_file'
parameter_value='PETM-Hierarchy.txt'
insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

