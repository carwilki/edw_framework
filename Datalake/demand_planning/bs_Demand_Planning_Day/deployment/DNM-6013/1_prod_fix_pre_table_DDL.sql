-- Databricks notebook source
DROP TABLE  raw.DP_SITE_VEND_PRE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Demand_Planning/dp_site_vend_pre', True)

-- COMMAND ----------


use raw;
 CREATE TABLE  DP_SITE_VEND_PRE 
(  STORE_NBR INT not null

, VENDOR_ID BIGINT not null

, VENDOR_SUBGROUP_ID                          STRING                 not null

, DP_PURCH_GROUP_ID                           STRING 

, DUE_BASED_ON_CD                             STRING 

, ORDER_DAY_OF_WK_ARRAY                       STRING 

, WEEK_FREQ_CD                                STRING 

, QUOTED_LT_DAY_CNT SMALLINT

, LT_DAY_CNT                                  DECIMAL(5,2) 

, LOAD_DT                                     TIMESTAMP                            not null

, MANDT                                       STRING 

, DPR_ORDER_ARRAY                             STRING 

, PROD_LEADTIME INT

, TRANSIT_LEADTIME INT

, ADJUST_TIME_SUN                             DECIMAL(5,2) 

, TRANSIT_TIME_SUN INT

, ADJUST_TIME_MON                             DECIMAL(5,2) 

, TRANSIT_TIME_MON INT

, ADJUST_TIME_TUE                             DECIMAL(5,2) 

, TRANSIT_TIME_TUE INT

, ADJUST_TIME_WED                             DECIMAL(5,2) 

, TRANSIT_TIME_WED INT

, ADJUST_TIME_THU                             DECIMAL(5,2) 

, TRANSIT_TIME_THU INT

, ADJUST_TIME_FRI                             DECIMAL(5,2) 

, TRANSIT_TIME_FRI INT

, ADJUST_TIME_SAT                             DECIMAL(5,2) 

, TRANSIT_TIME_SAT INT

, ALT_ORIGIN                                  STRING 

, DEL_START_TIME_SUN STRING

, DEL_END_TIME_SUN STRING

, DEL_START_TIME_MON STRING

, DEL_END_TIME_MON STRING

, DEL_START_TIME_TUE STRING

, DEL_END_TIME_TUE STRING

, DEL_START_TIME_WED STRING

, DEL_END_TIME_WED STRING

, DEL_START_TIME_THU STRING

, DEL_END_TIME_THU STRING

, DEL_START_TIME_FRI STRING

, DEL_END_TIME_FRI STRING

, DEL_START_TIME_SAT STRING

, DEL_END_TIME_SAT STRING

, PICK_DAY1                                   STRING 

, PICK_DAY2                                   STRING 

, PICK_DAY3                                   STRING 

, PICK_DAY4                                   STRING 

, PICK_DAY5                                   STRING 

, PICK_DAY6                                   STRING 

, PICK_DAY7                                   STRING 

, DEL_DAY1                                    STRING 

, DEL_DAY2                                    STRING 

, DEL_DAY3                                    STRING 

, DEL_DAY4                                    STRING 

, DEL_DAY5                                    STRING 

, DEL_DAY6                                    STRING 

, DEL_DAY7                                    STRING 

, BLUE_GREEN_FLAG                             STRING 

, PROTECTION_LEVEL                            STRING 

, OVERRIDE_ORDER_WEIGHT INT

, OVERRIDE_ORDER_VOLUME INT

, CHANGE_DATE                                 TIMESTAMP 

, CHANGE_TIME STRING

, NOTES                                       STRING 

, OVERRIDE_FLAG                               STRING 

, CROSSDOCK_ID                                STRING 

, ALT_ORIGIN1                                 STRING 

, ALT_ORIGIN2                                 STRING 

, ALT_ORIGIN3                                 STRING 

, ALT_ORIGIN4                                 STRING 

, ALT_ORIGIN5                                 STRING 

, ALT_ORIGIN6                                 STRING 

, ALT_ORIGIN7                                 STRING 

, CROSSDOCK_ID2                               STRING 

, CROSSDOCK_ID3                               STRING 

, CROSSDOCK_ID4                               STRING 

, CROSSDOCK_ID5                               STRING 

, CROSSDOCK_ID6                               STRING 

, CROSSDOCK_ID7                               STRING 

, ADJUST_LEADTIME                             DECIMAL(5,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Demand_Planning/dp_site_vend_pre';
