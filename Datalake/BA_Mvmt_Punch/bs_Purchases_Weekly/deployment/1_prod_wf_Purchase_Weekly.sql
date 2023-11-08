




--*****  Creating table:  "PURCHASES" , ***** Creating table: "PURCHASES"


use legacy;
 CREATE TABLE  PURCHASES 
(  DAY_DT        TIMESTAMP                             not null

, PRODUCT_ID INT not null

, VENDOR_ID BIGINT not null

, PO_NBR BIGINT not null

, PO_LINE_NBR INT not null

, LOCATION_ID INT not null

, PO_TYPE_ID                                        STRING 

, ORIG_DATE_DUE                                     TIMESTAMP 

, DATE_DUE      TIMESTAMP 

, DELETE_FLAG                                       STRING 

, DELIVERY_FLAG                                     STRING 

, ORDER_COST_ACTUAL                                 DECIMAL(11,3) 

, ORDER_COST_GROSS                                  DECIMAL(11,3) 

, ORDER_COST_NET                                    DECIMAL(11,3) 

, ORIG_ORDER_QTY                                    INT 

, ORDER_QTY     INT 

, SCHED_QTY     INT 

, RECEIPT_FLAG                                      STRING 

, DATE_RECEIVED                                     TIMESTAMP 

, RECEIPTS_QTY                                      INT 

, FREIGHT_COST                                      DECIMAL(9,2) 

, UPDT_CNT TINYINT

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Mvmt_Purch/purchases';

--DISTRIBUTE ON (PO_NBR)







--*****  Creating table:  "SITE_PROFILE" , ***** Creating table: "SITE_PROFILE"


use legacy;
 CREATE TABLE  SITE_PROFILE 
(  LOCATION_ID INT not null

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
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Mvmt_Purch/site_profile';

--DISTRIBUTE ON (LOCATION_ID)







--*****  Creating table:  "SKU_PROFILE" , ***** Creating table: "SKU_PROFILE"


use legacy;
 CREATE TABLE  SKU_PROFILE 
(  PRODUCT_ID INT not null

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
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Mvmt_Purch/sku_profile';

--DISTRIBUTE ON (PRODUCT_ID)







--*****  Creating table:  "DAYS" , ***** Creating table: "DAYS"


use legacy;
 CREATE TABLE  DAYS 
(  DAY_DT                                    TIMESTAMP                            not null

, BUSINESS_DAY_FLAG                         STRING 

, HOLIDAY_FLAG                              STRING 

, DAY_OF_WK_NAME                            STRING 

, DAY_OF_WK_NAME_ABBR                       STRING 

, DAY_OF_WK_NBR TINYINT

, CAL_DAY_OF_MO_NBR TINYINT

, CAL_DAY_OF_YR_NBR SMALLINT

, CAL_WK INT

, CAL_WK_NBR TINYINT

, CAL_MO INT

, CAL_MO_NBR TINYINT

, CAL_MO_NAME                               STRING 

, CAL_MO_NAME_ABBR                          STRING 

, CAL_QTR INT

, CAL_QTR_NBR TINYINT

, CAL_HALF INT

, CAL_YR SMALLINT

, FISCAL_DAY_OF_MO_NBR TINYINT

, FISCAL_DAY_OF_YR_NBR SMALLINT

, FISCAL_WK INT

, FISCAL_WK_NBR TINYINT

, FISCAL_MO INT

, FISCAL_MO_NBR TINYINT

, FISCAL_MO_NAME                            STRING 

, FISCAL_MO_NAME_ABBR                       STRING 

, FISCAL_QTR INT

, FISCAL_QTR_NBR TINYINT

, FISCAL_HALF INT

, FISCAL_YR SMALLINT

, LYR_WEEK_DT                               TIMESTAMP                            not null

, LWK_WEEK_DT                               TIMESTAMP                            not null

, WEEK_DT                                   TIMESTAMP                            not null

, EST_TIME_CONV_AMT                         DECIMAL(6,6)                         not null  

, EST_TIME_CONV_HRS TINYINT not null 

, ES0_TIME_CONV_AMT                         DECIMAL(6,6)                         not null  

, ES0_TIME_CONV_HRS TINYINT not null 

, CST_TIME_CONV_AMT                         DECIMAL(6,6)                         not null  

, CST_TIME_CONV_HRS TINYINT not null 

, CS0_TIME_CONV_AMT                         DECIMAL(6,6)                         not null  

, CS0_TIME_CONV_HRS TINYINT not null 

, MST_TIME_CONV_AMT                         DECIMAL(6,6)                         not null  

, MST_TIME_CONV_HRS TINYINT not null 

, MS0_TIME_CONV_AMT                         DECIMAL(6,6)                         not null  

, MS0_TIME_CONV_HRS TINYINT not null 

, PST_TIME_CONV_AMT                         DECIMAL(6,6)                         not null  

, PST_TIME_CONV_HRS TINYINT not null 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Mvmt_Purch/days';

--DISTRIBUTE ON (DAY_DT)







--*****  Creating table:  "PURCH_PERF" , ***** Creating table: "PURCH_PERF"


use legacy;
 CREATE TABLE  PURCH_PERF 
(  WEEK_DT       TIMESTAMP                             not null

, SAP_CATEGORY_ID INT not null

, VENDOR_ID BIGINT not null

, PO_TYPE_ID                                        STRING                  not null

, DISTRICT_ID BIGINT not null

, STORE_TYPE_ID                                     STRING                          not null

, RECVD_AFTER_DUE_DAYS_CNT INT

, RECVD_BEFORE_DUE_DAYS_CNT INT

, ACTUAL_LEADTIME_DAYS_CNT INT

, PO_DUE_CNT INT

, PO_RECVD_CNT INT

, PO_LATE_CNT INT

, LINE_ITEM_DUE_CNT INT

, LINE_ITEM_RECVD_CNT INT

, LINE_ITEMS_LATE_CNT INT

, LINE_ITEM_OOB_CNT INT

, DUE_QTY       INT 

, RECVD_QTY     INT 

, LATE_QTY      INT 

, EXCESS_QTY                                        INT 

, SHORT_QTY     INT 

, ORDER_COST_GROSS                                  DECIMAL(12,2) 

, ORDER_COST_ACTUAL                                 DECIMAL(12,2) 

, ORDER_COST_NET                                    DECIMAL(12,2) 

, FREIGHT_COST                                      DECIMAL(12,2) 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Mvmt_Purch/purch_perf';

--DISTRIBUTE ON (SAP_CATEGORY_ID, DISTRICT_ID)







--*****  Creating table:  "PURCH_PO_NBR_PRE" , ***** Creating table: "PURCH_PO_NBR_PRE"


use raw;
 CREATE TABLE  PURCH_PO_NBR_PRE 
(  WEEK_DT       TIMESTAMP                             not null

, SAP_CATEGORY_ID INT not null

, VENDOR_ID BIGINT not null

, PO_TYPE_ID                                        STRING                  not null

, PO_NBR BIGINT not null

, DISTRICT_ID BIGINT not null

, STORE_TYPE_ID                                     STRING                          not null

, PO_DUE_CNT INT 

, PO_RECVD_CNT INT 

, PO_LATE_CNT INT 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Mvmt_Purch/purch_po_nbr_pre';

--DISTRIBUTE ON (PO_NBR)







--*****  Creating table:  "PURCH_PO_TYPE_PRE" , ***** Creating table: "PURCH_PO_TYPE_PRE"


use raw;
 CREATE TABLE  PURCH_PO_TYPE_PRE 
(  WEEK_DT       TIMESTAMP                             not null

, SAP_CATEGORY_ID INT not null

, VENDOR_ID BIGINT not null

, PO_TYPE_ID                                        STRING                  not null

, DISTRICT_ID BIGINT not null

, STORE_TYPE_ID                                     STRING                          not null

, RECVD_AFTER_DUE_DAYS_CNT INT

, RECVD_BEFORE_DUE_DAYS_CNT INT

, ACTUAL_LEADTIME_DAYS_CNT INT

, LINE_ITEM_DUE_CNT INT

, LINE_ITEM_RECVD_CNT INT

, LINE_ITEMS_LATE_CNT INT

, LINE_ITEM_OOB_CNT INT

, DUE_QTY       INT 

, RECVD_QTY     INT 

, LATE_QTY      INT 

, EXCESS_QTY                                        INT 

, SHORT_QTY     INT 

, ORDER_COST_GROSS                                  DECIMAL(12,2) 

, ORDER_COST_ACTUAL                                 DECIMAL(12,2) 

, ORDER_COST_NET                                    DECIMAL(12,2) 

, FREIGHT_COST                                      DECIMAL(12,2) 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Mvmt_Purch/purch_po_type_pre';

--DISTRIBUTE ON (SAP_CATEGORY_ID, DISTRICT_ID)