-- Databricks notebook source
--*****  Creating table:  "EM_AMS_OFFER_TIERED_PRICE" , ***** Creating table: "EM_AMS_OFFER_TIERED_PRICE"


use legacy;
 CREATE TABLE EM_AMS_OFFER_TIERED_PRICE 
(
 EM_TIER_ID INT not null
, EM_OFFER_ID INT
, TIER_NBR INT
, MIN_QTY INT
, MAX_QTY INT
, DISCOUNT_AMT                                DECIMAL(18,2) 
, DELETE_IND TINYINT
, DELETE_TSTMP                                TIMESTAMP 
, UPDATE_TSTMP                                TIMESTAMP 
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_EMS/em_ams_offer_tiered_price';

--DISTRIBUTE ON (EM_TIER_ID)

-- COMMAND ----------

--*****  Creating table:  "EM_AMS_OFFER_SKU" , ***** Creating table: "EM_AMS_OFFER_SKU"


use legacy;
 CREATE TABLE EM_AMS_OFFER_SKU 
(
 EM_OFFER_ID INT not null
, EM_AMS_SKU_GROUP_TYPE_ID INT not null
, PRODUCT_ID INT not null
, SKU_NBR INT not null
, DELETE_IND TINYINT
, DELETE_TSTMP                                TIMESTAMP 
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_EMS/em_ams_offer_sku';

--DISTRIBUTE ON (EM_OFFER_ID)

-- COMMAND ----------

--*****  Creating table:  "EM_AMS_OFFER_COUNTRY" , ***** Creating table: "EM_AMS_OFFER_COUNTRY"


use legacy;
 CREATE TABLE EM_AMS_OFFER_COUNTRY 
(
 EM_OFFER_ID INT not null
, COUNTRY_CD                                  STRING                 not null
, COUNTRY_NAME                                STRING 
, DELETE_IND TINYINT
, DELETE_TSTMP                                TIMESTAMP 
, UPDATE_TSTMP                                TIMESTAMP 
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_EMS/em_ams_offer_country';

--DISTRIBUTE ON (EM_OFFER_ID)

-- COMMAND ----------

--*****  Creating table:  "EM_AMS_OFFER_LABEL" , ***** Creating table: "EM_AMS_OFFER_LABEL"


use legacy;
 CREATE TABLE EM_AMS_OFFER_LABEL 
(
 EM_OFFER_ID INT not null
, EM_EVENT_ID INT
, AMS_OFFER_SUBMISSION_ID BIGINT
, EM_OFFER_NAME                               STRING 
, EM_AMS_TEMPLATE_TYPE_ID INT
, EM_AMS_OFFER_LIMIT_QTY INT
, EM_AMS_OFFER_LIMIT_TYPE_ID INT
, EM_AMS_DISCOUNT_TYPE_ID INT
, DISCOUNT_AMT                                DECIMAL(18,2) 
, TOTAL_DISCOUNT_AMT                          DECIMAL(18,2) 
, MIN_SPEND_AMT                               DECIMAL(18,2) 
, BUY_QTY INT
, BUY_DESC                                    STRING 
, GET_QTY INT
, GET_DESC                                    STRING 
, BB_BUY_OR_SPEND_IND                         STRING 
, BB_SPEND_AMT                                DECIMAL(18,2) 
, BTGT_GRP_A_BUY_QTY INT
, BTGT_GRP_A_BUY_DESC                         STRING 
, BTGT_GRP_A_MIN_SPEND_AMT                    DECIMAL(18,2) 
, BTGT_GRP_B_BUY_QTY INT
, BTGT_GRP_B_BUY_DESC                         STRING 
, BTGT_GRP_B_MIN_SPEND_AMT                    DECIMAL(18,2) 
, SS_SPEND_AMT                                DECIMAL(18,2) 
, SS_SPEND_DESC                               STRING 
, SS_SAVE_DESC                                STRING 
, SS_SAME_SPEND_ITEM_IND TINYINT
, BOGO_SAME_BUY_ITEM_IND TINYINT
, GET_LABEL_SUPPRESSED_IND TINYINT
, DISCLAIMER_1_TYPE_ID INT
, DISCLAIMER_1_TYPE_DESC                      STRING 
, DISCLAIMER_1_OTHER_DESC                     STRING 
, DISCLAIMER_2_TYPE_ID INT
, DISCLAIMER_2_TYPE_DESC                      STRING 
, DISCLAIMER_2_OTHER_DESC                     STRING 
, BTGT_GRP_A_DISCLAIMER_TYPE_ID INT
, BTGT_GRP_A_DISCLAIMER_TYPE_DESC             STRING 
, BTGT_GRP_A_DISCLAIMER_OTHER_DESC            STRING 
, BTGT_GRP_B_DISCLAIMER_TYPE_ID INT
, BTGT_GRP_B_DISCLAIMER_TYPE_DESC             STRING 
, BTGT_GRP_B_DISCLAIMER_OTHER_DESC            STRING 
, EM_OFFER_UNLOCK_TSTMP                       TIMESTAMP 
, EM_CREATED_BY                               STRING 
, EM_CREATED_TSTMP                            TIMESTAMP 
, EM_MODIFIED_BY                              STRING 
, EM_MODIFIED_TSTMP                           TIMESTAMP 
, UPDATE_TSTMP                                TIMESTAMP 
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_EMS/em_ams_offer_label';

--DISTRIBUTE ON (EM_OFFER_ID)

-- COMMAND ----------

--*****  Creating table:  "EM_AMS_TEMPLATE_TYPE" , ***** Creating table: "EM_AMS_TEMPLATE_TYPE"


use legacy;
 CREATE TABLE EM_AMS_TEMPLATE_TYPE 
(
 EM_AMS_TEMPLATE_TYPE_ID INT not null
, EM_AMS_TEMPLATE_TYPE_NAME                   STRING 
, EM_AMS_TEMPLATE_TYPE_DESC                   STRING 
, EM_SAP_TEMPLATE_TYPE_ID                     STRING 
, UPDATE_TSTMP                                TIMESTAMP 
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_EMS/em_ams_template_type';

--DISTRIBUTE ON RANDOM

-- COMMAND ----------

--*****  Creating table:  "EM_DISCLAIMER_TYPE" , ***** Creating table: "EM_DISCLAIMER_TYPE"


use legacy;
 CREATE TABLE EM_DISCLAIMER_TYPE 
(
 DISCLAIMER_TYPE_ID INT not null
, DISCLAIMER_TYPE_NAME                        STRING 
, DISCLAIMER_TYPE_DESC                        STRING 
, DISCLAIMER_NBR SMALLINT
, EM_SAP_DISCLAIMER_TYPE_ID                   STRING 
, EM_AMS_TEMPLATE_TYPE_ID INT
, CUSTOM_TXT_IND TINYINT
, UPDATE_TSTMP                                TIMESTAMP 
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_EMS/em_disclaimer_type';

--DISTRIBUTE ON RANDOM

-- COMMAND ----------

--*****  Creating table:  "EM_AMS_DISCOUNT_TYPE" , ***** Creating table: "EM_AMS_DISCOUNT_TYPE"


use legacy;
 CREATE TABLE EM_AMS_DISCOUNT_TYPE 
(
 EM_AMS_DISCOUNT_TYPE_ID INT not null
, EM_AMS_DISCOUNT_TYPE_NAME                   STRING 
, EM_AMS_DISCOUNT_TYPE_DESC                   STRING 
, EM_AMS_TEMPLATE_TYPE_ID INT
, EM_SAP_DISCOUNT_TYPE_ID                     STRING 
, REQUIRES_AMT_IND TINYINT
, AMT_DESC                                    STRING 
, UPDATE_TSTMP                                TIMESTAMP 
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_EMS/em_ams_discount_type';

--DISTRIBUTE ON RANDOM

-- COMMAND ----------

--*****  Creating table:  "EM_AMS_OFFER_LIMIT_TYPE" , ***** Creating table: "EM_AMS_OFFER_LIMIT_TYPE"


use legacy;
 CREATE TABLE EM_AMS_OFFER_LIMIT_TYPE 
(
 EM_AMS_OFFER_LIMIT_TYPE_ID INT not null
, EM_AMS_OFFER_LIMIT_TYPE_NAME                STRING 
, EM_AMS_OFFER_LIMIT_TYPE_DESC                STRING 
, EM_SAP_LIMIT_TYPE_ID                        STRING 
, UPDATE_TSTMP                                TIMESTAMP 
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_EMS/em_ams_offer_limit_type';

--DISTRIBUTE ON RANDOM

-- COMMAND ----------

--*****  Creating table:  "EM_AMS_SKU_GROUP_TYPE" , ***** Creating table: "EM_AMS_SKU_GROUP_TYPE"


use legacy;
 CREATE TABLE EM_AMS_SKU_GROUP_TYPE 
(
 EM_AMS_SKU_GROUP_TYPE_ID INT not null
, EM_AMS_SKU_GROUP_TYPE_NAME                  STRING 
, EM_AMS_SKU_GROUP_TYPE_DESC                  STRING 
, EM_SAP_SKU_GROUP_TYPE_ID                    STRING 
, BUY_GET_IND                                 STRING 
, UPDATE_TSTMP                                TIMESTAMP 
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_EMS/em_ams_sku_group_type';

--DISTRIBUTE ON RANDOM
