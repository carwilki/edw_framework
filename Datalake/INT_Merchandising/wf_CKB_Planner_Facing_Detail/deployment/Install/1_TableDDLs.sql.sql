-- Databricks notebook source
--*****  Creating table:  "CKB_PLANNER_DETAIL_HIST" , ***** Creating table: "CKB_PLANNER_DETAIL_HIST"


use legacy;
 CREATE TABLE  CKB_PLANNER_DETAIL_HIST 
(
 SNAPSHOT_DT                           DATE                                not null

, LOCATION_ID INT not null

, POG_DBKEY INT not null

, POG_VERSION_KEY INT not null

, PRODUCT_ID INT not null

, EFF_START_DT                          DATE                                not null

, EFF_END_DT                            DATE                                not null

, POG_GROUP                              STRING 

, EOT_FLAG TINYINT

, MIN_FILL INT

, CAPACITY INT

, CKB_LOAD_TSTMP                         TIMESTAMP 

, LOAD_TSTMP                             TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/INT_Merchandising/ckb_planner_detail_hist';

--DISTRIBUTE ON (LOCATION_ID, PRODUCT_ID, POG_DBKEY)

--ORGANIZE   ON (SNAPSHOT_DT)


