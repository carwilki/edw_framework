-- Databricks notebook source





--*****  Creating table:  "SITE_PROFILE" , ***** Creating table: "SITE_PROFILE"


use dev_legacy;
 CREATE TABLE  SITE_PROFILE 
(
 LOCATION_ID INT not null

, LOCATION_TYPE_ID TINYINT

, STORE_NBR INT

, STORE_NAME                                        STRING 

, STORE_TYPE_ID                                     STRING 

, STORE_OPEN_CLOSE_FLAG                             STRING 

, COMPANY_ID INT

, REGION_ID BIGINT

, DISTRICT_ID BIGINT

, PRICE_ZONE_ID                                     STRING 

, PRICE_AD_ZONE_ID                                  STRING 

, REPL_DC_NBR INT

, REPL_FISH_DC_NBR INT

, REPL_FWD_DC_NBR INT

, SQ_FEET_RETAIL                                    DOUBLE 

, SQ_FEET_TOTAL                                     DOUBLE 

, SITE_ADDRESS                                      STRING 

, SITE_CITY     STRING 

, STATE_CD      STRING 

, COUNTRY_CD                                        STRING 

, POSTAL_CD     STRING 

, SITE_MAIN_TELE_NO                                 STRING 

, SITE_GROOM_TELE_NO                                STRING 

, SITE_EMAIL_ADDRESS                                STRING 

, SITE_SALES_FLAG                                   STRING 

, EQUINE_MERCH_ID TINYINT

, EQUINE_SITE_ID TINYINT

, EQUINE_SITE_OPEN_DT                               TIMESTAMP 

, GEO_LATITUDE_NBR                                  DECIMAL(12,6) 

, GEO_LONGITUDE_NBR                                 DECIMAL(12,6) 

, PETSMART_DMA_CD                                   STRING 

, LOYALTY_PGM_TYPE_ID TINYINT

, LOYALTY_PGM_STATUS_ID TINYINT

, LOYALTY_PGM_START_DT                              TIMESTAMP 

, LOYALTY_PGM_CHANGE_DT                             TIMESTAMP 

, BP_COMPANY_NBR SMALLINT

, BP_GL_ACCT SMALLINT

, TP_LOC_FLAG                                       STRING 

, TP_ACTIVE_CNT TINYINT

, PROMO_LABEL_CD                                    STRING 

, PARENT_LOCATION_ID INT

, LOCATION_NBR                                      STRING 

, TIME_ZONE_ID                                      STRING 

, DELV_SERVICE_CLASS_ID                             STRING 

, PICK_SERVICE_CLASS_ID                             STRING 

, SITE_LOGIN_ID                                     STRING 

, SITE_MANAGER_ID INT

, SITE_OPEN_YRS_AMT                                 DECIMAL(5,2) 

, HOTEL_FLAG TINYINT

, DAYCAMP_FLAG TINYINT

, VET_FLAG TINYINT

, DIST_MGR_NAME                                     STRING 

, DIST_SVC_MGR_NAME                                 STRING 

, REGION_VP_NAME                                    STRING 

, REGION_TRAINER_NAME                               STRING 

, ASSET_PROTECT_NAME                                STRING 

, SITE_COUNTY                                       STRING 

, SITE_FAX_NO                                       STRING 

, SFT_OPEN_DT                                       TIMESTAMP 

, DM_EMAIL_ADDRESS                                  STRING 

, DSM_EMAIL_ADDRESS                                 STRING 

, RVP_EMAIL_ADDRESS                                 STRING 

, TRADE_AREA                                        STRING 

, FDLPS_NAME                                        STRING 

, FDLPS_EMAIL                                       STRING 

, OVERSITE_MGR_NAME                                 STRING 

, OVERSITE_MGR_EMAIL                                STRING 

, SAFETY_DIRECTOR_NAME                              STRING 

, SAFETY_DIRECTOR_EMAIL                             STRING 

, RETAIL_MANAGER_SAFETY_NAME                        STRING 

, RETAIL_MANAGER_SAFETY_EMAIL                       STRING 

, AREA_DIRECTOR_NAME                                STRING 

, AREA_DIRECTOR_EMAIL                               STRING 

, DC_GENERAL_MANAGER_NAME                           STRING 

, DC_GENERAL_MANAGER_EMAIL                          STRING 

, ASST_DC_GENERAL_MANAGER_NAME1                     STRING 

, ASST_DC_GENERAL_MANAGER_EMAIL1                    STRING 

, ASST_DC_GENERAL_MANAGER_NAME2                     STRING 

, ASST_DC_GENERAL_MANAGER_EMAIL2                    STRING 

, REGIONAL_DC_SAFETY_MGR_NAME                       STRING 

, REGIONAL_DC_SAFETY_MGR_EMAIL                      STRING 

, DC_PEOPLE_SUPERVISOR_NAME                         STRING 

, DC_PEOPLE_SUPERVISOR_EMAIL                        STRING 

, PEOPLE_MANAGER_NAME                               STRING 

, PEOPLE_MANAGER_EMAIL                              STRING 

, ASSET_PROT_DIR_NAME                               STRING 

, ASSET_PROT_DIR_EMAIL                              STRING 

, SR_REG_ASSET_PROT_MGR_NAME                        STRING 

, SR_REG_ASSET_PROT_MGR_EMAIL                       STRING 

, REG_ASSET_PROT_MGR_NAME                           STRING 

, REG_ASSET_PROT_MGR_EMAIL                          STRING 

, ASSET_PROTECT_EMAIL                               STRING 

, TP_START_DT                                       TIMESTAMP 

, OPEN_DT       TIMESTAMP 

, GR_OPEN_DT                                        TIMESTAMP 

, CLOSE_DT      TIMESTAMP 

, HOTEL_OPEN_DT                                     TIMESTAMP 

, ADD_DT        TIMESTAMP 

, DELETE_DT     TIMESTAMP 

, UPDATE_DT     TIMESTAMP 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-dev-nzlegacy-p1-gcs-gbl/INT_Merchandising/site_profile';

--DISTRIBUTE ON (LOCATION_ID)







--*****  Creating table:  "SKU_PROFILE" , ***** Creating table: "SKU_PROFILE"


use dev_legacy;
 CREATE TABLE  SKU_PROFILE 
(
 PRODUCT_ID INT not null

, SKU_NBR INT

, SKU_TYPE      STRING 

, PRIMARY_UPC_ID BIGINT

, STATUS_ID     STRING 

, SUBS_HIST_FLAG                                    STRING 

, SUBS_CURR_FLAG                                    STRING 

, SKU_DESC      STRING 

, ALT_DESC      STRING 

, SAP_CATEGORY_ID INT

, SAP_CLASS_ID INT

, SAP_DEPT_ID INT

, SAP_DIVISION_ID INT

, PRIMARY_VENDOR_ID BIGINT

, PARENT_VENDOR_ID BIGINT

, COUNTRY_CD                                        STRING 

, IMPORT_FLAG                                       STRING 

, HTS_CODE_ID BIGINT

, CONTENTS      DECIMAL(13,3) 

, CONTENTS_UNITS                                    STRING 

, WEIGHT_NET_AMT                                    DECIMAL(9,3) 

, WEIGHT_UOM_CD                                     STRING 

, SIZE_DESC     STRING 

, BUM_QTY       DOUBLE 

, UOM_CD        STRING 

, UNIT_NUMERATOR                                    DOUBLE 

, UNIT_DENOMINATOR                                  DOUBLE 

, BUYER_ID      STRING 

, PURCH_GROUP_ID INT

, PURCH_COST_AMT                                    DECIMAL(8,2) 

, NAT_PRICE_US_AMT                                  DECIMAL(8,2) 

, TAX_CLASS_ID                                      STRING 

, VALUATION_CLASS_CD                                STRING 

, BRAND_CD      STRING 

, BRAND_CLASSIFICATION_ID SMALLINT

, OWNBRAND_FLAG                                     STRING 

, STATELINE_FLAG                                    STRING 

, SIGN_TYPE_CD                                      STRING 

, OLD_ARTICLE_NBR                                   STRING 

, VENDOR_ARTICLE_NBR                                STRING 

, INIT_MKDN_DT                                      TIMESTAMP 

, DISC_START_DT                                     TIMESTAMP 

, ADD_DT        TIMESTAMP 

, DELETE_DT     TIMESTAMP 

, UPDATE_DT     TIMESTAMP 

, FIRST_SALE_DT                                     TIMESTAMP 

, LAST_SALE_DT                                      TIMESTAMP 

, FIRST_INV_DT                                      TIMESTAMP 

, LAST_INV_DT                                       TIMESTAMP 

, LOAD_DT       TIMESTAMP 

, BASE_NBR      STRING 

, BP_COLOR_ID                                       STRING 

, BP_SIZE_ID                                        STRING 

, BP_BREED_ID                                       STRING 

, BP_ITEM_CONCATENATED                              STRING 

, BP_AEROSOL_FLAG TINYINT

, BP_HAZMAT_FLAG TINYINT

, CANADIAN_HTS_CD                                   STRING 

, NAT_PRICE_CA_AMT                                  DECIMAL(8,2) 

, NAT_PRICE_PR_AMT                                  DECIMAL(8,2) 

, RTV_DEPT_CD                                       STRING 

, GL_ACCT_NBR INT

, ARTICLE_CATEGORY_ID SMALLINT

, COMPONENT_FLAG                                    STRING 

, ZDISCO_SCHED_TYPE_ID                              STRING                 not null  

, ZDISCO_MKDN_SCHED_ID                              STRING                  not null  

, ZDISCO_PID_DT                                     TIMESTAMP 

, ZDISCO_START_DT                                   TIMESTAMP 

, ZDISCO_INIT_MKDN_DT                               TIMESTAMP 

, ZDISCO_DC_DT                                      TIMESTAMP 

, ZDISCO_STR_DT                                     TIMESTAMP 

, ZDISCO_STR_OWNRSHP_DT                             TIMESTAMP 

, ZDISCO_STR_WRT_OFF_DT                             TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-dev-nzlegacy-p1-gcs-gbl/INT_Merchandising/sku_profile';

--DISTRIBUTE ON (PRODUCT_ID)







--*****  Creating table:  "CKB_PLANNER_DETAIL_HIST" , ***** Creating table: "CKB_PLANNER_DETAIL_HIST"


use dev_legacy;
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
LOCATION 'gs://petm-bdpl-dev-nzlegacy-p1-gcs-gbl/INT_Merchandising/ckb_planner_detail_hist';

--DISTRIBUTE ON (LOCATION_ID, PRODUCT_ID, POG_DBKEY)

--ORGANIZE   ON (SNAPSHOT_DT)


