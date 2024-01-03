# Databricks notebook source
%sql
DROP TABLE legacy.pog_sku_store_pro;
 
CREATE TABLE refine.pog_sku_store_pro_history
USING delta
LOCATION 'ggs://petm-bdpl-prod-refine-p1-gcs-gbl/legacy/pog_sku_store_pro';
 

# COMMAND ----------
%sql
 --*****  Creating table:  "pog_sku_store_pro" , ***** Creating table: "pog_sku_store_pro"

use legacy;
CREATE TABLE pog_sku_store_pro (
  PRODUCT_ID BIGINT NOT NULL COMMENT 'Primary Key',
  LOCATION_ID BIGINT NOT NULL COMMENT 'Primary Key',
  POG_ID INT NOT NULL COMMENT 'Primary Key',
  SKU_CAPACITY_QTY INT,
  SKU_FACINGS_QTY INT,
  SKU_HEIGHT_IN DECIMAL(7,2),
  SKU_DEPTH_IN DECIMAL(7,2),
  SKU_WIDTH_IN DECIMAL(7,2),
  UNIT_OF_MEASURE STRING,
  TRAY_PACK_NBR INT,
  POG_SKU_STATUS STRING,
  POG_STORE_STATUS STRING,
  SKU_CAPACITY_COST DECIMAL(12,2))
USING delta
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/pog_sku_store_pro';