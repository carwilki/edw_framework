-- Databricks notebook source
INSERT INTO cust_sensitive.legacy_TRAINING_PET
select * from legacy.TRAINING_PET

-- COMMAND ----------

insert into work.pii_dynamic_view_control
select 
"cust_sensitive",
"legacy_TRAINING_PET",
"legacy",
"TRAINING_PET",
"false",
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
lower(substr(current_user(), 1, instr(current_user(), '@')-1)),
from_utc_timestamp(current_timestamp(), 'America/Phoenix'),
from_utc_timestamp(current_timestamp(), 'America/Phoenix');
