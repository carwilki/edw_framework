-- Databricks notebook source
--Create table "SKU_SUBSTITUTION"
USE legacy;

CREATE TABLE SKU_SUBSTITUTION (
	PRODUCT_ID INTEGER NOT NULL
	,SUBS_EFF_DT TIMESTAMP NOT NULL
	,SUBS_PRODUCT_ID INTEGER NOT NULL
	,SUBS_END_DT TIMESTAMP
	,EXPIRY_FLAG STRING
	,ADD_DT TIMESTAMP
	,LOAD_DT TIMESTAMP
	) 
       USING delta 
       LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/sku_substitution' ;
