-- Databricks notebook source
insert into pii_metadata.pii_metadata_store
select * from pii_metadata.pii_metadata_store_qa
where table_name like '%legacy_pet%' and column_name != 'pet_name'

-- COMMAND ----------

select * from pii_metadata.pii_metadata_store
where table_name like '%legacy_pet%'

-- COMMAND ----------

update work.pii_dynamic_view_control 
set view_created_flg=false,
update_user_name='apipewala'
where secured_table_name like '%pet_training%'

-- COMMAND ----------

DROP VIEW legacy.pet_training_reservation
