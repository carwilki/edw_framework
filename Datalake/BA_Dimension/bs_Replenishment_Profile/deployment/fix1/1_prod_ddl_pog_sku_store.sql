-- Databricks notebook source
DROP TABLE legacy.POG_SKU_STORE;

CREATE TABLE refine.POG_SKU_STORE_history (
  product_id INT,
  location_id INT,
  pog_nbr STRING,
  pog_dbkey INT,
  listing_start_dt DATE,
  listing_end_dt DATE,
  positions_cnt INT,
  facings_cnt INT,
  capacity_cnt INT,
  presentation_qty INT,
  pog_type_cd STRING,
  pog_sku_position_status_id DECIMAL(1,0),
  delete_flag DECIMAL(1,0),
  sap_last_change_tstmp TIMESTAMP,
  update_tstmp TIMESTAMP,
  load_tstmp TIMESTAMP,
  bd_create_dt_tm TIMESTAMP,
  bd_update_dt_tm TIMESTAMP,
  source_file_name STRING)
USING delta
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/legacy/pog_sku_store';

-- COMMAND ----------

use legacy;

CREATE TABLE  POG_SKU_STORE
(
 PRODUCT_ID INT not null

, LOCATION_ID INT not null

, POG_NBR       STRING                  not null

, POG_DBKEY INT not null

, LISTING_START_DT                                 DATE                                 not null

, LISTING_END_DT DATE

, POSITIONS_CNT INT

, FACINGS_CNT INT

, CAPACITY_CNT INT

, PRESENTATION_QTY INT

, POG_TYPE_CD                                       STRING 

, POG_SKU_POSITION_STATUS_ID                        TINYINT 

, DELETE_FLAG                                       TINYINT 

, SAP_LAST_CHANGE_TSTMP                             TIMESTAMP 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/pog_sku_store' ;
