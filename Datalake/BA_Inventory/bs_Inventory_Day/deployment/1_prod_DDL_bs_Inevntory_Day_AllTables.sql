




--*****  Creating table:  "INVENTORY_EVENT_STR_SKU" , ***** Creating table: "INVENTORY_EVENT_STR_SKU"


use legacy;
CREATE TABLE INVENTORY_EVENT_STR_SKU
(
 LOCATION_ID INT not null COMMENT "Primary Key"

, STO_TYPE_ID INT not null COMMENT "Primary Key"

, PRODUCT_ID INT not null COMMENT "Primary Key"

, QUANTITY      DECIMAL(13,3) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/inventory_event_str_sku';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "INVENTORY_PRE" , ***** Creating table: "INVENTORY_PRE"


use RAW;
CREATE TABLE INVENTORY_PRE
(
 DAY_DT        TIMESTAMP                            not null COMMENT "Primary Key"

, SKU_NBR INT not null COMMENT "Primary Key"

, STORE_NBR INT not null COMMENT "Primary Key"

, ON_HAND_QTY                                       INT 

, XFER_IN_TRANS_QTY                                 INT 

, MAP_AMT       DECIMAL(9,3) 

, PRICE_CHANGE_DT                                   TIMESTAMP 

, VALUATED_STOCK_QTY INT

, VALUATED_STOCK_AMT                                DECIMAL(11,3) 

, PREV_PRICE_AMT                                    DECIMAL(9,3) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/inventory_pre' ;

--DISTRIBUTE ON (SKU_NBR, STORE_NBR)







--*****  Creating table:  "LISTING_DAY" , ***** Creating table: "LISTING_DAY"


use legacy;
CREATE TABLE LISTING_DAY
(
 PRODUCT_ID INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, SKU_NBR INT

, STORE_NBR INT

, LISTING_END_DT                                    TIMESTAMP 

, LISTING_SEQ_NBR SMALLINT

, LISTING_EFF_DT                                    TIMESTAMP 

, LISTING_MODULE_ID BIGINT

, LISTING_SOURCE_ID                                 STRING 

, NEGATE_FLAG                                       STRING 

, STRUCT_COMP_CD                                    STRING 

, STRUCT_ARTICLE_NBR INT

, DELETE_IND                                        STRING 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/listing_day' ;

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "OPEN_STO_PRE" , ***** Creating table: "OPEN_STO_PRE"


use raw;
CREATE TABLE OPEN_STO_PRE
(
 DAY_DT        TIMESTAMP                             not null COMMENT "Primary Key"

, PRODUCT_ID INT not null COMMENT "Primary Key"

, PO_NBR BIGINT not null COMMENT "Primary Key"

, PO_LINE_NBR INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, SUPPLY_LOCATION_ID INT not null

, PO_TYPE_ID                                        STRING 

, DATE_DUE      TIMESTAMP 

, DELETE_FLAG                                       STRING 

, DELIVERY_FLAG                                     STRING 

, ORDER_COST_ACTUAL                                 DECIMAL(9,3) 

, ORDER_COST_GROSS                                  DECIMAL(9,3) 

, ORDER_COST_NET                                    DECIMAL(9,3) 

, ORDER_QTY     INT 

, ISSUED_QTY                                        INT 

, DELIVERED_QTY                                     INT 

, DATE_RECEIVED                                     TIMESTAMP 

, RECEIPTS_QTY                                      INT 

, FREIGHT_COST BIGINT

, SOURCE_VENDOR_ID BIGINT

, PLAN_DELIV_DAYS SMALLINT

, ROUND_VALUE_QTY                                   INT 

, ROUND_PROFILE_CD                                  STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl//inventory/open_sto_pre' ;

--DISTRIBUTE ON (PO_NBR)







--*****  Creating table:  "OPEN_PO_PRE" , ***** Creating table: "OPEN_PO_PRE"


use raw;
CREATE TABLE OPEN_PO_PRE
(
 DAY_DT        TIMESTAMP                             not null COMMENT "Primary Key"

, PRODUCT_ID INT not null COMMENT "Primary Key"

, VENDOR_ID BIGINT not null COMMENT "Primary Key"

, PO_NBR BIGINT not null COMMENT "Primary Key"

, PO_LINE_NBR INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, PO_TYPE_ID                                        STRING 

, DATE_DUE      TIMESTAMP 

, DELETE_FLAG                                       STRING 

, DELIVERY_FLAG                                     STRING 

, ORDER_COST_ACTUAL                                 DECIMAL(9,3) 

, ORDER_COST_GROSS                                  DECIMAL(9,3) 

, ORDER_COST_NET                                    DECIMAL(9,3) 

, ORDER_QTY     INT 

, SCHED_QTY     INT 

, DATE_RECEIVED                                     TIMESTAMP 

, RECEIPTS_QTY                                      INT 

, FREIGHT_COST BIGINT

, PLAN_DELIV_DAYS SMALLINT

, ROUND_VALUE_QTY                                   INT 

, ROUND_PROFILE_CD                                  STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/open_po_pre' ;

--DISTRIBUTE ON (PO_NBR)







--*****  Creating table:  "PLANOGRAM_PRO" , ***** Creating table: "PLANOGRAM_PRO"


use legacy;
CREATE TABLE PLANOGRAM_PRO
(
 POG_ID INT not null COMMENT "Primary Key"

, POG_DBKEY     DECIMAL(38,0) 

, PROJECT_ID                                        STRING 

, FIXTURE_TYPE INT

, POG_NBR       STRING 

, PROJECT_NAME                                      STRING 

, POG_DESC      STRING 

, POG_IMPLEMENT_DT                                  TIMESTAMP 

, POG_REVIEW_YM                                     TIMESTAMP 

, POG_REV_DT                                        TIMESTAMP 

, FIX_LINEAR_IN                                     DECIMAL(9,2) 

, FIX_CUBIC_IN                                      DECIMAL(12,2) 

, FIX_FLOOR_FT                                      DECIMAL(9,2) 

, MIX_FLAG      STRING 

, US_FLAG       STRING 

, CA_FLAG       STRING 

, POG_STATUS                                        STRING 

, LAST_CHNG_DT                                      TIMESTAMP 

, LISTING_FLAG                                      STRING 

, EVENT_ID INT not null

, POG_DISPLAY_FLAG                                  STRING 

, EFF_START_DT                                      TIMESTAMP 

, EFF_END_DT                                        TIMESTAMP 

, POG_TYPE_CD                                       STRING 

, POG_DIVISION                                      STRING 

, POG_DEPT      STRING 

, DATE_POG_ADDED                                    TIMESTAMP 

, DATE_POG_REFRESHED                                TIMESTAMP 

, DATE_POG_DELETED                                  TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/planogram_pro'; 

--DISTRIBUTE ON (POG_ID)







--*****  Creating table:  "POG_SKU_PRO" , ***** Creating table: "POG_SKU_PRO"


use legacy;
CREATE TABLE POG_SKU_PRO
(
 POG_ID INT not null

, PRODUCT_ID INT not null

, SKU_CAPACITY_QTY                                  INT 

, SKU_FACINGS_QTY                                   INT 

, SKU_HEIGHT_IN                                     DECIMAL(7,2) 

, SKU_DEPTH_IN                                      DECIMAL(7,2) 

, SKU_WIDTH_IN                                      DECIMAL(7,2) 

, UNIT_OF_MEASURE                                   STRING 

, TRAY_PACK_NBR INT

, POG_STATUS                                        STRING 

, LAST_CHNG_DT                                      TIMESTAMP 

, PQ_CHNG_DT                                        TIMESTAMP 

, LIST_START_DT                                     TIMESTAMP 

, LIST_END_DT                                       TIMESTAMP 

, PROMO_START_DT                                    TIMESTAMP 

, PROMO_END_DT                                      TIMESTAMP 

, POG_PROMO_QTY                                     INT 

, DATE_POG_ADDED                                    TIMESTAMP 

, DATE_POG_REFRESHED                                TIMESTAMP 

, DATE_POG_DELETED                                  TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/pog_sku_pro'; 

--DISTRIBUTE ON (PRODUCT_ID, POG_ID)







--*****  Creating table:  "POG_SKU_STORE" , ***** Creating table: "POG_SKU_STORE"


use legacy;
CREATE TABLE POG_SKU_STORE
(
 PRODUCT_ID INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, POG_NBR       STRING                  not null COMMENT "Primary Key"

, POG_DBKEY INT not null COMMENT "Primary Key"

, LISTING_START_DT                                 DATE                                 not null COMMENT "Primary Key"

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
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/pog_sku_store'; 

--DISTRIBUTE ON (POG_DBKEY)







--*****  Creating table:  "POG_SKU_STORE_PRO" , ***** Creating table: "POG_SKU_STORE_PRO"


use legacy;
CREATE TABLE POG_SKU_STORE_PRO
(
 PRODUCT_ID BIGINT not null COMMENT "Primary Key"

, LOCATION_ID BIGINT not null COMMENT "Primary Key"

, POG_ID INT not null COMMENT "Primary Key"

, SKU_CAPACITY_QTY                                  INT 

, SKU_FACINGS_QTY                                   INT 

, SKU_HEIGHT_IN                                     DECIMAL(7,2) 

, SKU_DEPTH_IN                                      DECIMAL(7,2) 

, SKU_WIDTH_IN                                      DECIMAL(7,2) 

, UNIT_OF_MEASURE                                   STRING 

, TRAY_PACK_NBR INT

, POG_SKU_STATUS                                    STRING 

, POG_STORE_STATUS                                  STRING 

, SKU_CAPACITY_COST                                 DECIMAL(12,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/pog_sku_store_pro'; 

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "POG_STORE_PRO" , ***** Creating table: "POG_STORE_PRO"


use legacy;
CREATE TABLE POG_STORE_PRO
(
 POG_ID INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, POG_STATUS                                        STRING 

, LAST_CHNG_DT                                      TIMESTAMP 

, POG_CANADIAN                                      STRING 

, POG_EQUINE                                        STRING 

, POG_REGISTER                                      STRING 

, POG_GENERIC_MAP                                   STRING 

, DATE_POG_ADDED                                    TIMESTAMP 

, DATE_POG_REFRESHED                                TIMESTAMP 

, DATE_POG_DELETED                                  TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/pog_store_pro'; 

--DISTRIBUTE ON (POG_ID, LOCATION_ID)







--*****  Creating table:  "SITE_PROFILE" , ***** Creating table: "SITE_PROFILE"


use legacy;
CREATE TABLE SITE_PROFILE
(
 LOCATION_ID INT not null COMMENT "Primary Key"

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
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/site_profile' ;

--DISTRIBUTE ON (LOCATION_ID)







--*****  Creating table:  "SKU_PROFILE" , ***** Creating table: "SKU_PROFILE"


use legacy;
CREATE TABLE SKU_PROFILE
(
 PRODUCT_ID INT not null COMMENT "Primary Key"

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
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/sku_profile'; 

--DISTRIBUTE ON (PRODUCT_ID)







--*****  Creating table:  "SKU_SITE_PROFILE" , ***** Creating table: "SKU_SITE_PROFILE"


use legacy;
CREATE TABLE SKU_SITE_PROFILE
(
 PRODUCT_ID INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, COUNTRY_CD                                        STRING 

, LAST_SALE_DT                                      TIMESTAMP 

, COND_UNIT     STRING 

, HIST_SALE_FLAG TINYINT

, HIST_INV_FLAG TINYINT

, CURR_ORDER_FLAG TINYINT

, CURR_SAP_LISTED_FLAG TINYINT

, CURR_POG_LISTED_FLAG TINYINT

, CURR_CATALOG_FLAG TINYINT

, CURR_DOTCOM_FLAG TINYINT

, PROJ_ORDER_FLAG TINYINT

, AD_PRICE_AMT                                      DECIMAL(8,2) 

, REGULAR_PRICE_AMT                                 DECIMAL(8,2) 

, NAT_PRICE_AMT                                     DECIMAL(8,2) 

, PETPERKS_PRICE_AMT                                DECIMAL(8,2) 

, LOC_AD_PRICE_AMT                                  DECIMAL(8,2) 

, LOC_REGULAR_PRICE_AMT                             DECIMAL(8,2) 

, LOC_NAT_PRICE_AMT                                 DECIMAL(8,2) 

, LOC_PETPERKS_PRICE_AMT                            DECIMAL(8,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/sku_site_profile'; 

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "SKU_STORE_PRICE_COSTS_PRE" , ***** Creating table: "SKU_STORE_PRICE_COSTS_PRE"


use raw;
CREATE TABLE SKU_STORE_PRICE_COSTS_PRE
(
 SKU_NBR INT not null COMMENT "Primary Key"

, STORE_NBR INT not  COMMENT "Primary Key"

, RETAIL_PRICE_AMT                                  DECIMAL(9,2) 

, PETPERKS_AMT                                      DECIMAL(9,2) 

, SUM_COST      DECIMAL(9,2) 

, BUM_COST      DECIMAL(9,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/sku_store_price_costs_pre' ;

--DISTRIBUTE ON (SKU_NBR, STORE_NBR)







--*****  Creating table:  "PRODUCT" , ***** Creating table: "PRODUCT"


use legacy;
CREATE TABLE PRODUCT
(
 PRODUCT_ID INT not null

, ALT_DESC      STRING 

, ANIMAL        STRING 

, ANIMALSIZE                                        STRING 

, BRAND_NAME                                        STRING 

, BUM_QTY       INT 

, BUYER_ID      STRING 

, BUYER_NAME                                        STRING 

, CTRY_ORIGIN                                       STRING 

, DATE_DISC_START                                   TIMESTAMP 

, DATE_DISC_END                                     TIMESTAMP 

, DATE_FIRST_INV                                    TIMESTAMP 

, DATE_FIRST_SALE                                   TIMESTAMP 

, DATE_LAST_INV                                     TIMESTAMP 

, DATE_LAST_SALE                                    TIMESTAMP 

, DATE_PROD_ADDED                                   TIMESTAMP 

, DATE_PROD_DELETED                                 TIMESTAMP 

, DATE_PROD_REFRESHED                               TIMESTAMP 

, DEPTH         DECIMAL(9,3) 

, DIM_UNITS     STRING 

, FLAVOR        STRING 

, HEIGHT        DECIMAL(9,3) 

, HTS_CODE BIGINT

, HTS_DESC      STRING 

, IMPORT_FLAG                                       STRING 

, MFGREP_NAME                                       STRING 

, OWNBRAND_FLAG                                     STRING 

, PRIMARY_UPC BIGINT

, PRIMARY_VENDOR_ID BIGINT

, PURCH_GROUP_ID INT

, PURCH_GROUP_NAME                                  STRING 

, SAP_CATEGORY_DESC                                 STRING 

, SAP_CLASS_ID INT

, SAP_DEPT_ID INT

, SAP_DIVISION_ID INT

, CONTENTS      DECIMAL(9,3) 

, CONTENTS_UNITS                                    STRING 

, SKU_DESC      STRING 

, SKU_NBR INT

, SKU_NBR_REF INT

, STATELINE_FLAG                                    STRING 

, SUM_COST_NTL                                      DECIMAL(9,3) 

, SUM_RETAIL_NTL                                    DECIMAL(9,3) 

, TUM_QTY       INT 

, VENDOR_SUB_RANGE                                  INT 

, VOLUME        DECIMAL(9,3) 

, VOLUME_UNITS                                      STRING 

, WEIGHT_GROSS                                      DECIMAL(9,3) 

, WEIGHT_NET                                        DECIMAL(9,3) 

, WEIGHT_UNITS                                      STRING 

, WIDTH         DECIMAL(9,3) 

, CATEGORY_DESC                                     STRING 

, CATEGORY_ID INT not null

, VENDOR_STYLE_NBR                                  STRING 

, PRIMARY_VENDOR_NAME                               STRING 

, COLOR         STRING 

, FLAVOR2       STRING 

, POND_FLAG     STRING 

, PRODUCT_SIZE                                      STRING 

, DATE_INIT_MKDN                                    TIMESTAMP 

, ARTICLE_TYPE                                      STRING 

, SAP_CLASS_DESC                                    STRING 

, SAP_DEPT_DESC                                     STRING 

, SAP_DIVISION_DESC                                 STRING 

, SEASON_DESC                                       STRING 

, SEASON_ID     STRING 

, SAP_CATEGORY_ID INT not null

, PLAN_GROUP_DESC                                   STRING 

, PLAN_GROUP_ID INT not null

, STATUS_ID     STRING 

, STATUS_NAME                                       STRING 

, OLD_ARTICLE_NBR                                   STRING 

, TAX_CLASS_ID                                      STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/product'; 

--DISTRIBUTE ON (PRODUCT_ID)







--*****  Creating table:  "CURRENCY_DAY" , ***** Creating table: "CURRENCY_DAY"


use legacy;
CREATE TABLE CURRENCY_DAY
(
 DAY_DT                                    TIMESTAMP                            not null COMMENT "Primary Key"

, CURRENCY_ID                               STRING                         not null

, DATE_RATE_START                           TIMESTAMP                            not null

, CURRENCY_TYPE                             STRING                        not null

, DATE_RATE_ENDED                           TIMESTAMP 

, EXCHANGE_RATE_PCNT                        DECIMAL(9,6) 

, RATIO_TO                                  DOUBLE 

, RATIO_FROM                                DOUBLE 

, STORE_CTRY_ABBR                           STRING 

, CURRENCY_NBR SMALLINT not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/currency_day'; 

--DISTRIBUTE ON (DAY_DT)







--*****  Creating table:  "SUPPLY_CHAIN" , ***** Creating table: "SUPPLY_CHAIN"


use legacy;
CREATE TABLE SUPPLY_CHAIN
(
 PRODUCT_ID INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, DIRECT_VENDOR_ID BIGINT

, SOURCE_VENDOR_ID BIGINT

, PRIMARY_VENDOR_ID BIGINT

, FROM_LOCATION_ID INT

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/supply_chain'; 

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "INV_SKU_STORE" , ***** Creating table: "INV_SKU_STORE"


use legacy;
CREATE TABLE INV_SKU_STORE
(
 PRODUCT_ID INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, FIRST_ON_HAND_DT                                  TIMESTAMP 

, LAST_ON_HAND_DT                                   TIMESTAMP 

, LOAD_DT       TIMESTAMP                            not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/inv_sku_store'; 

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "INV_HOLES_COUNT" , ***** Creating table: "INV_HOLES_COUNT"


use legacy;
CREATE TABLE INV_HOLES_COUNT
(
 DAY_DT        TIMESTAMP                            not null

, PRODUCT_ID INT not null

, LOCATION_ID INT not null

, SKU_STATUS_ID                                     STRING 

, COUNTED_OOS_IND TINYINT

, OUT_OF_STOCK_IND TINYINT

, INLINE_IND TINYINT

, POG_LISTED_IND TINYINT

, ON_HAND_QTY                                       INT 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/inv_holes_count'; 

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "INV_INSTOCK_PRICE_DAY" , ***** Creating table: "INV_INSTOCK_PRICE_DAY"


use legacy;
CREATE TABLE INV_INSTOCK_PRICE_DAY
(
 DAY_DT        TIMESTAMP                            not null

, PRODUCT_ID INT not null

, LOCATION_ID INT not null

, FROM_LOCATION_ID INT

, SOURCE_VENDOR_ID BIGINT

, SKU_STATUS_ID                                     STRING 

, STORE_OPEN_IND TINYINT

, OUT_OF_STOCK_CNT TINYINT

, POG_LISTED_IND TINYINT

, SAP_LISTED_IND TINYINT

, INLINE_CNT TINYINT

, PLANNER_IND TINYINT

, SUBS_IND TINYINT

, MAP_AMT       DECIMAL(9,3) 

, EXCH_RATE_PCT                                     DECIMAL(9,6) 

, ON_HAND_QTY                                       INT 

, COMMITTED_QTY                                     INT 

, XFER_IN_TRANS_QTY                                 INT 

, ON_ORDER_QTY                                      INT 

, SUM_COST_AMT                                      DECIMAL(9,3) 

, BUM_COST_AMT                                      DECIMAL(9,3) 

, ON_ORDER_CD                                       STRING 

, RETAIL_PRICE_AMT                                  DECIMAL(8,2) 

, SKU_FACINGS_QTY                                   INT 

, SKU_CAPACITY_QTY                                  INT 

, PETPERKS_AMT                                      DECIMAL(8,2) 

, PETPERKS_IND TINYINT

, LOCAL_PRICE_AMT                                   DECIMAL(8,2) 

, LOC_PETPERKS_PRICE_AMT                            DECIMAL(8,2) 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/inv_instock_price_day'; 

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "INV_SKU_STORE" , ***** Creating table: "INV_SKU_STORE"


use legacy;
CREATE TABLE INV_SKU_STORE
(
 PRODUCT_ID INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, FIRST_ON_HAND_DT                                  TIMESTAMP 

, LAST_ON_HAND_DT                                   TIMESTAMP 

, LOAD_DT       TIMESTAMP                            not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/inv_sku_store'; 

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "INV_HOLES_SITE_PRE" , ***** Creating table: "INV_HOLES_SITE_PRE"


use raw;
CREATE TABLE INV_HOLES_SITE_PRE
(
 DAY_DT        TIMESTAMP                            not null COMMENT "Primary Key"

, CREATE_TIME                                       STRING 

, SKU_NBR INT not null COMMENT "Primary Key"

, STORE_NBR INT not null COMMENT "Primary Key"

, ON_HAND_QTY                                       INT 

, USER_NAME     STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/inv_holes_site_pre'; 

--DISTRIBUTE ON (SKU_NBR, STORE_NBR)







--*****  Creating table:  "INV_INSTOCK_PRE" , ***** Creating table: "INV_INSTOCK_PRE"


use raw;
CREATE TABLE INV_INSTOCK_PRE
(
 DAY_DT        TIMESTAMP                            not null COMMENT "Primary Key"

, PRODUCT_ID BIGINT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, AVG_QTY BIGINT

, OUT_OF_STOCK_IND BIGINT

, POG_LISTED_IND BIGINT

, SAP_LISTED_IND BIGINT

, INLINE_IND BIGINT

, PLANNER_IND BIGINT

, SUBS_IND BIGINT

, SKU_CAPACITY_QTY BIGINT

, LOAD_DT       TIMESTAMP 

, SKU_FACINGS_QTY BIGINT

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/inv_instock_pre' ;

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "INV_SKU_STORE_PRE" , ***** Creating table: "INV_SKU_STORE_PRE"


use raw;
CREATE TABLE INV_SKU_STORE_PRE
(
 PRODUCT_ID INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, FIRST_ON_HAND_DT                                  TIMESTAMP 

, LAST_ON_HAND_DT                                   TIMESTAMP 

, LOAD_DT       TIMESTAMP                            not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/inv_sku_store_pre' ;

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "INVENTORY_DAY_PRE" , ***** Creating table: "INVENTORY_DAY_PRE"


use raw;
CREATE TABLE INVENTORY_DAY_PRE
(
 DAY_DT        TIMESTAMP                            not null COMMENT "Primary Key"

, PRODUCT_ID INT not null COMMENT "Primary Key"

, LOCATION_ID INT not null COMMENT "Primary Key"

, MAP_AMT       DECIMAL(9,3) 

, ON_HAND_QTY INT

, COMMITTED_QTY INT

, XFER_IN_TRANS_QTY INT

, ON_ORDER_QTY INT

, SUM_COST      INT 

, BUM_COST      INT 

, RETAIL_PRICE_AMT                                  DECIMAL(9,2) 

, PETSPERK_AMT                                      DECIMAL(9,2) 

, ON_ORDER_CD                                       STRING 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/inventory_day_pre' ;

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)



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

USE legacy;

CREATE VIEW SKU_SUBS_VIEW (
	PRODUCT_ID
	,SKU_NBR
	,SUBS_PRODUCT_ID
	,SUBS_SKU_NBR
	,SOURCE_CD
	,SUBS_IND
	) AS

SELECT SSL.PRODUCT_ID
	,SP1.SKU_NBR
	,SSL.SUBS_PRODUCT_ID
	,SP2.SKU_NBR AS SUBS_SKU_NBR
	,CASE 
		WHEN (
				(MAX(SSL.SS_CD) = 'S')
				AND (MAX(SSL.SL_CD) = 'D')
				)
			THEN 'B'
		WHEN (
				(MAX(SSL.SS_CD) = 'S')
				AND (MAX(SSL.SL_CD) = ' ')
				)
			THEN 'S'
		WHEN (
				(MAX(SSL.SS_CD) = ' ')
				AND (MAX(SSL.SL_CD) = 'D')
				)
			THEN 'D'
		ELSE NULL
		END AS SOURCE_CD
	,CASE 
		WHEN (
				(now() >= MAX(SSL.SUBS_EFF_DT))
				AND (now() <= MAX(SSL.SUBS_END_DT))
				)
			THEN 1
		ELSE 0
		END AS SUBS_IND
FROM (
	(
		(
			(
				SELECT SS.PRODUCT_ID
					,SS.SUBS_PRODUCT_ID
					,('S') AS SS_CD
					,(' ') AS SL_CD
					,SS.SUBS_EFF_DT
					,SS.SUBS_END_DT
				FROM DEV_LEGACY.SKU_SUBSTITUTION SS
				)
			
			UNION ALL
			
			(
				SELECT SL.PRODUCT_ID
					,SL.LINK_PRODUCT_ID
					,(' ') AS SS_CD
					,('D') AS SL_CD
					,SL.SKU_LINK_EFF_DT
					,SL.SKU_LINK_END_DT
				FROM DEV_LEGACY.DP_SKU_LINK SL
				GROUP BY SL.PRODUCT_ID
					,SL.LINK_PRODUCT_ID
					,SL.SKU_LINK_EFF_DT
					,SL.SKU_LINK_END_DT
				)
			)
		
		UNION ALL
		
		(
			SELECT SS.SUBS_PRODUCT_ID
				,SS.PRODUCT_ID
				,('S') AS SS_CD
				,(' ') AS SL_CD
				,SS.SUBS_EFF_DT
				,SS.SUBS_END_DT
			FROM DEV_LEGACY.SKU_SUBSTITUTION SS
			)
		)
	
	UNION ALL
	
	(
		SELECT SL.LINK_PRODUCT_ID
			,SL.PRODUCT_ID
			,(' ') AS SS_CD
			,('D') AS SL_CD
			,SL.SKU_LINK_EFF_DT
			,SL.SKU_LINK_END_DT
		FROM DEV_LEGACY.DP_SKU_LINK SL
		GROUP BY SL.LINK_PRODUCT_ID
			,SL.PRODUCT_ID
			,SL.SKU_LINK_EFF_DT
			,SL.SKU_LINK_END_DT
		)
	) SSL
	,DEV_LEGACY.SKU_PROFILE SP1
	,DEV_LEGACY.SKU_PROFILE SP2
WHERE (
		(SSL.PRODUCT_ID = SP1.PRODUCT_ID)
		AND (SSL.SUBS_PRODUCT_ID = SP2.PRODUCT_ID)
		)
GROUP BY SSL.PRODUCT_ID
	,SP1.SKU_NBR
	,SSL.SUBS_PRODUCT_ID
	,SP2.SKU_NBR;