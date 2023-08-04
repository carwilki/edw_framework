-- Databricks notebook source
CREATE TABLE raw.parameter_config(
id BIGINT generated by default as identity
, parameter_file_name STRING
, parameter_section STRING
, parameter_key STRING
, parameter_value STRING
) 
USING delta

-- COMMAND ----------

insert into raw.parameter_config(parameter_file_name, parameter_section, parameter_key, parameter_value) 
values ('Merch_Parameter.prm', 'BA_TouchPoint.WF:wf_Petshotel_Accrual.ST:s_petshotel_accrual_SQL', 'p_accrual_sql', 'd.sap_dept_id IN (3502,3508,82)')
