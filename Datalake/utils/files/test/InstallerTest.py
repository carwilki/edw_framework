# Databricks notebook source

from Datalake.utils.files.driver_installer import get_file_driver_payload,get_file_driver_cluster_payload

job_json = "prod_wf_store_data.json"
env = "prod"
pf = "wf_store_data"
name = f"{pf}_driver"
dc = "FileDriverCluster"
rau = "gcpdatajobs-shared@petsmart.com"
to = "2h"
job_id = "12345"
print(get_file_driver_cluster_payload())
print(get_file_driver_payload(name, env, job_id, pf, dc, rau, to))
