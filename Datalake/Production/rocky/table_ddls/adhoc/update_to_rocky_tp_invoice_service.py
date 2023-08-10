# Databricks notebook source
# MAGIC %sql
# MAGIC update work.rocky_ingestion_metadata set load_type='full',source_delta_column=null,primary_key=null where source_table='TP_INVOICE_SERVICE' and table_group='NZ_Migration' and rocky_id=780
