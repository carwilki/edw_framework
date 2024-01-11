-- Databricks notebook source
update work.rocky_ingestion_metadata
set source_type = "NZ_Mako8"
where upper(source_table) = 'SALES_INSTANCE_SKEY'
