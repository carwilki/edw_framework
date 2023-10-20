-- Databricks notebook source
insert into pii_metadata.pii_metadata_store 
select * from pii_metadata.pii_metadata_store_qa where table_name in ("legacy_e_res_pets","legacy_e_res_request","legacy_ic_wc_claims")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC update work.pii_dynamic_view_control set secured_table_name = 'legacy_e_res_requests', view_name = 'e_res_requests' where secured_table_name like '%res_req%'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from pii_metadata.pii_metadata_store_qa where table_name in ("legacy_e_res_requests")
