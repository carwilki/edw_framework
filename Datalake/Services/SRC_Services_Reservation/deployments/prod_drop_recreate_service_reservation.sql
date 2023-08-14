-- Databricks notebook source
DROP table cust_sensitive.legacy_SRC_SERVICES_RESERVATION;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('gs://petm-bdpl-prod-cust-sensitive-nzlegacy-p1-gcs-gbl/services/src_services_reservation', True)

-- COMMAND ----------

use cust_sensitive;
 CREATE TABLE legacy_SRC_SERVICES_RESERVATION 
( CREATE_DT    DATE                                 not null

, TP_CUSTOMER_NBR BIGINT not null

, TP_INVOICE_NBR BIGINT

, SKU_DESC      STRING 

, SAP_CLASS_DESC                                    STRING 

, LOCATION_ID INT not null

, STORE_NBR INT

, COUNTRY_CD                                        STRING 

, CUST_FIRST_NAME                                   STRING 

, CUST_LAST_NAME                                    STRING 

, CUST_EMAIL_ADDRESS                                STRING 

, SRC_CC_ID     STRING 

, COMMUNICATION_CHANNEL                             STRING 

, EMAIL_OPT_OUT_FLAG INT

, SENT_FLAG INT

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-cust-sensitive-nzlegacy-p1-gcs-gbl/services/src_services_reservation' 
 TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
