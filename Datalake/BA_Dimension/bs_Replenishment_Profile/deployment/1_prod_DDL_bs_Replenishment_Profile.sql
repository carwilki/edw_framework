
--*****  Creating table:  "REPLENISHMENT_DAY" , ***** Creating table: "REPLENISHMENT_DAY"


use legacy;
CREATE TABLE  REPLENISHMENT_DAY
(
 SKU_NBR INT not null

, STORE_NBR INT not null

, DELETE_IND                                        STRING                

, SAFETY_QTY                                        INT 

, SERVICE_LVL_RT                                    DECIMAL(3,1) 

, REORDER_POINT_QTY                                 INT 

, PLAN_DELIV_DAYS SMALLINT

, TARGET_STOCK_QTY                                  INT 

, PRESENT_QTY                                       INT 

, PROMO_QTY     INT 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/replenishment_day'; 

--DISTRIBUTE ON (SKU_NBR, STORE_NBR)




--*****  Creating table:  "SITE_GROUP_DAY" , ***** Creating table: "SITE_GROUP_DAY"


use legacy;
CREATE TABLE  SITE_GROUP_DAY
(
 STORE_NBR INT not null

, SITE_GROUP_CD                                     STRING                 not null

, DELETE_IND                                        STRING 

, SITE_GROUP_DESC                                   STRING 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/site_group_day' ;

--DISTRIBUTE ON (STORE_NBR)






--*****  Creating table:  "REPLENISHMENT_PROFILE" , ***** Creating table: "REPLENISHMENT_PROFILE"


use legacy;
CREATE TABLE  REPLENISHMENT_PROFILE
(
 PRODUCT_ID INT not null

, LOCATION_ID INT not null

, ROUND_VALUE_QTY                                   INT 

, SAFETY_QTY                                        INT 

, SERVICE_LVL_RT                                    DECIMAL(3,1) 

, REORDER_POINT_QTY                                 INT 

, PLAN_DELIV_DAYS SMALLINT

, ROUND_PROFILE_CD                                  STRING 

, TARGET_STOCK_QTY                                  INT 

, PRESENT_QTY                                       INT 

, POG_CAPACITY_QTY                                  INT 

, POG_FACINGS_QTY                                   INT 

, PROMO_QTY     INT 

, BASIC_VALUE_QTY                                   DECIMAL(13,3) 

, LAST_FC_DT                                        TIMESTAMP 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/replenishment_profile'; 

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "REPLENISHMENT_PRE" , ***** Creating table: "REPLENISHMENT_PRE"


use raw;
CREATE TABLE  REPLENISHMENT_PRE
(
 SKU_NBR INT not null

, STORE_NBR INT not null

, DELETE_IND                                        STRING                 

, SAFETY_QTY                                        INT 

, SERVICE_LVL_RT                                    DECIMAL(3,1) 

, REORDER_POINT_QTY                                 INT 

, PLAN_DELIV_DAYS SMALLINT

, TARGET_STOCK_QTY                                  INT 

, PRESENT_QTY                                       INT 

, PROMO_QTY     INT 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/dimension/replenishment_pre' ;

--DISTRIBUTE ON (SKU_NBR, STORE_NBR)







--*****  Creating table:  "SITE_GROUP_PRE" , ***** Creating table: "SITE_GROUP_PRE"


use raw;
CREATE TABLE  SITE_GROUP_PRE
(
 STORE_NBR INT not null

, SITE_GROUP_CD                                     STRING                 not null

, DELETE_IND                                        STRING                  not null

, SITE_GROUP_DESC                                   STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/dimension/site_group_pre'; 

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "ZTB_ART_LOC_SITE_PRE" , ***** Creating table: "ZTB_ART_LOC_SITE_PRE"


use raw;
CREATE TABLE  ZTB_ART_LOC_SITE_PRE
(
 ARTICLE INT not null

, SITE INT not null

, POG_ID        STRING                  not null

, POG_DBKEY INT not null

, EFFECTIVE_DT                                     DATE                                 not null

, EFFECTIVE_ENDT DATE

, NUMBER_OF_POSITIONS INT

, NUMBER_OF_FACINGS INT

, CAPACITY INT

, PRESENTATION_QUANTITY INT

, POSITION_STATUS                                   TINYINT 

, POG_TYPE      STRING 

, LAST_CHG_DATE DATE

, LAST_CHG_TIME                                     STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/dimension/ztb_art_loc_site_pre' ;

--DISTRIBUTE ON (POG_DBKEY)





--*****  Creating table:  "ROUNDING_PROFILE_PRE" , ***** Creating table: "ROUNDING_PROFILE_PRE"


use raw;
CREATE TABLE  ROUNDING_PROFILE_PRE
(
 ROUNDING_PROFILE_CD                               STRING                          not null

, ROUNDING_RULE_CD                                  STRING 

, ROUNDING_PROFILE_DESC                             STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/dimension/rounding_profile_pre' ;

--DISTRIBUTE ON RANDOM



--*****  Creating table:  "UOM_ROUNDING_RULE_PRE" , ***** Creating table: "UOM_ROUNDING_RULE_PRE"


use raw;
CREATE TABLE  UOM_ROUNDING_RULE_PRE
(
 ROUNDING_RULE_CD                                  STRING                          not null

, UOM_CD        STRING                          not null

, ROUNDING_UP_RT                                    DECIMAL(4,1) 

, ROUNDING_DOWN_RT                                  DECIMAL(4,1) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/dimension/uom_rounding_rule_pre'; 

--DISTRIBUTE ON RANDOM






--*****  Creating table:  "DC_FCST_DAY" , ***** Creating table: "DC_FCST_DAY"


use legacy;
CREATE TABLE  DC_FCST_DAY
(
 LOCATION_ID INT not null

, VENDOR_ID BIGINT not null

, PRODUCT_ID INT not null

, BASIC_VALUE_QTY                           DECIMAL(13,3) 

, LAST_FC_DT                                TIMESTAMP 

, LOAD_DT                                   TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/dc_fcst_day';

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







-- --*****  Creating table:  "SKU_STORE_VENDOR_DAY" , ***** Creating table: "SKU_STORE_VENDOR_DAY"


-- use legacy;
-- CREATE TABLE  SKU_STORE_VENDOR_DAY
-- (
--  SKU_NBR INT not null

-- , STORE_NBR INT not null

-- , VENDOR_ID BIGINT not null

-- , DELETE_IND                                        STRING 

-- , SOURCE_LIST_EFF_DT                                TIMESTAMP                             not null

-- , SOURCE_LIST_END_DT                                TIMESTAMP                             not null

-- , FIXED_VENDOR_IND                                  STRING 

-- , PROCURE_STORE_NBR INT

-- , LOAD_DT       TIMESTAMP                             not null

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/sku_store_vendor_day';

-- --DISTRIBUTE ON (SKU_NBR, STORE_NBR)







-- --*****  Creating table:  "SKU_VENDOR_DAY" , ***** Creating table: "SKU_VENDOR_DAY"


-- use legacy;
-- CREATE TABLE  SKU_VENDOR_DAY
-- (
--  SKU_NBR INT not null

-- , VENDOR_ID BIGINT not null

-- , VENDOR_SUBRANGE_CD                                STRING 

-- , DELETE_IND                                        STRING 

-- , UNIT_NUMERATOR                                    INT 

-- , UNIT_DENOMINATOR                                  INT 

-- , DELIV_EFF_DT                                      TIMESTAMP                             not null

-- , DELIV_END_DT                                      TIMESTAMP                             not null

-- , REGULAR_VENDOR_CD                                 STRING 

-- , ROUNDING_PROFILE_CD                               STRING 

-- , COUNTRY_CD                                        STRING 

-- , VENDOR_ARTICLE_NBR                                STRING 

-- , LOAD_DT       TIMESTAMP                             not null

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/sku_vendor_day' ;

-- --DISTRIBUTE ON (SKU_NBR)











-- --*****  Creating table:  "SKU_UOM" , ***** Creating table: "SKU_UOM"


-- use legacy;
-- CREATE TABLE  SKU_UOM
-- (
--  PRODUCT_ID INT not null

-- , UOM_CD        STRING                          not null

-- , UOM_NUMERATOR                                     DOUBLE 

-- , UOM_DENOMINATOR                                   DOUBLE 

-- , LENGTH_AMT                                        DECIMAL(13,3) 

-- , WIDTH_AMT     DECIMAL(13,3) 

-- , HEIGHT_AMT                                        DECIMAL(13,3) 

-- , DIMENSION_UNIT_DESC                               STRING 

-- , VOLUME_AMT                                        DECIMAL(13,3) 

-- , VOLUME_UOM_CD                                     STRING 

-- , WEIGHT_GROSS_AMT                                  DECIMAL(13,3) 

-- , WEIGHT_UOM_CD                                     STRING 

-- , WEIGHT_NET_AMT                                    DECIMAL(13,3) 

-- , SCM_VOLUME_UOM_CD                                 STRING 

-- , SCM_VOLUME_AMT                                    DECIMAL(13,3) 

-- , SCM_WEIGHT_UOM_CD                                 STRING 

-- , SCM_WEIGHT_NET_AMT                                DECIMAL(13,3) 

-- , DELETE_DT     TIMESTAMP 

-- , LOAD_DT       TIMESTAMP 

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/sku_uom' ;

-- --DISTRIBUTE ON (PRODUCT_ID)







-- --*****  Creating table:  "POG_SKU_PRO" , ***** Creating table: "POG_SKU_PRO"


-- use legacy;
-- CREATE TABLE  POG_SKU_PRO
-- (
--  POG_ID INT not null

-- , PRODUCT_ID INT not null

-- , SKU_CAPACITY_QTY                                  INT 

-- , SKU_FACINGS_QTY                                   INT 

-- , SKU_HEIGHT_IN                                     DECIMAL(7,2) 

-- , SKU_DEPTH_IN                                      DECIMAL(7,2) 

-- , SKU_WIDTH_IN                                      DECIMAL(7,2) 

-- , UNIT_OF_MEASURE                                   STRING 

-- , TRAY_PACK_NBR INT

-- , POG_STATUS                                        STRING 

-- , LAST_CHNG_DT                                      TIMESTAMP 

-- , PQ_CHNG_DT                                        TIMESTAMP 

-- , LIST_START_DT                                     TIMESTAMP 

-- , LIST_END_DT                                       TIMESTAMP 

-- , PROMO_START_DT                                    TIMESTAMP 

-- , PROMO_END_DT                                      TIMESTAMP 

-- , POG_PROMO_QTY                                     INT 

-- , DATE_POG_ADDED                                    TIMESTAMP 

-- , DATE_POG_REFRESHED                                TIMESTAMP 

-- , DATE_POG_DELETED                                  TIMESTAMP 

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/pog_sku_pro'; 

-- --DISTRIBUTE ON (PRODUCT_ID, POG_ID)







-- --*****  Creating table:  "POG_STORE_PRO" , ***** Creating table: "POG_STORE_PRO"


-- use legacy;
-- CREATE TABLE  POG_STORE_PRO
-- (
--  POG_ID INT not null

-- , LOCATION_ID INT not null

-- , POG_STATUS                                        STRING 

-- , LAST_CHNG_DT                                      TIMESTAMP 

-- , POG_CANADIAN                                      STRING 

-- , POG_EQUINE                                        STRING 

-- , POG_REGISTER                                      STRING 

-- , POG_GENERIC_MAP                                   STRING 

-- , DATE_POG_ADDED                                    TIMESTAMP 

-- , DATE_POG_REFRESHED                                TIMESTAMP 

-- , DATE_POG_DELETED                                  TIMESTAMP 

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/pog_store_pro'; 

-- --DISTRIBUTE ON (POG_ID, LOCATION_ID)











-- --*****  Creating table:  "SUPPLY_CHAIN" , ***** Creating table: "SUPPLY_CHAIN"


-- use legacy;
-- CREATE TABLE  SUPPLY_CHAIN
-- (
--  PRODUCT_ID INT not null

-- , LOCATION_ID INT not null

-- , DIRECT_VENDOR_ID BIGINT

-- , SOURCE_VENDOR_ID BIGINT

-- , PRIMARY_VENDOR_ID BIGINT

-- , FROM_LOCATION_ID INT

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/supply_chain'; 

-- --DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







-- --*****  Creating table:  "DP_SITE_VEND_PROFILE" , ***** Creating table: "DP_SITE_VEND_PROFILE"


-- use legacy;
-- CREATE TABLE  DP_SITE_VEND_PROFILE
-- (
--  LOCATION_ID INT not null

-- , VENDOR_ID BIGINT not null

-- , VENDOR_SUBGROUP_ID                          STRING                 not null

-- , DPR_ORDER_ARRAY                             STRING 

-- , DP_PURCH_GROUP_ID                           STRING 

-- , PROD_LEADTIME                               SMALLINT 

-- , ADJUST_LEADTIME                             DECIMAL(5,2) 

-- , TRANSIT_LEADTIME                            SMALLINT 

-- , TOTAL_LEADTIME                              SMALLINT 

-- , ADJUST_TIME_SUN                             DECIMAL(5,2) 

-- , TRANSIT_TIME_SUN                            SMALLINT 

-- , ADJUST_TIME_MON                             DECIMAL(5,2) 

-- , TRANSIT_TIME_MON                            SMALLINT 

-- , ADJUST_TIME_TUE                             DECIMAL(5,2) 

-- , TRANSIT_TIME_TUE                            SMALLINT 

-- , ADJUST_TIME_WED                             DECIMAL(5,2) 

-- , TRANSIT_TIME_WED                            SMALLINT 

-- , ADJUST_TIME_THU                             DECIMAL(5,2) 

-- , TRANSIT_TIME_THU                            SMALLINT 

-- , ADJUST_TIME_FRI                             DECIMAL(5,2) 

-- , TRANSIT_TIME_FRI                            SMALLINT 

-- , ADJUST_TIME_SAT                             DECIMAL(5,2) 

-- , TRANSIT_TIME_SAT                            SMALLINT 

-- , ALT_ORIGIN                                  STRING 

-- , DEL_START_TIME_SUN TIMESTAMP

-- , DEL_END_TIME_SUN TIMESTAMP

-- , DEL_START_TIME_MON TIMESTAMP

-- , DEL_END_TIME_MON TIMESTAMP

-- , DEL_START_TIME_TUE TIMESTAMP

-- , DEL_END_TIME_TUE TIMESTAMP

-- , DEL_START_TIME_WED TIMESTAMP

-- , DEL_END_TIME_WED TIMESTAMP

-- , DEL_START_TIME_THU TIMESTAMP

-- , DEL_END_TIME_THU TIMESTAMP

-- , DEL_START_TIME_FRI TIMESTAMP

-- , DEL_END_TIME_FRI TIMESTAMP

-- , DEL_START_TIME_SAT TIMESTAMP

-- , DEL_END_TIME_SAT TIMESTAMP

-- , PICK_DAY1                                   STRING 

-- , PICK_DAY2                                   STRING 

-- , PICK_DAY3                                   STRING 

-- , PICK_DAY4                                   STRING 

-- , PICK_DAY5                                   STRING 

-- , PICK_DAY6                                   STRING 

-- , PICK_DAY7                                   STRING 

-- , DEL_DAY1                                    STRING 

-- , DEL_DAY2                                    STRING 

-- , DEL_DAY3                                    STRING 

-- , DEL_DAY4                                    STRING 

-- , DEL_DAY5                                    STRING 

-- , DEL_DAY6                                    STRING 

-- , DEL_DAY7                                    STRING 

-- , BLUE_GREEN_FLAG                             STRING 

-- , PROTECTION_LEVEL                            STRING 

-- , OVERRIDE_ORDER_WEIGHT                       DECIMAL(13,3) 

-- , OVERRIDE_ORDER_VOLUME                       DECIMAL(13,3) 

-- , CHANGE_DATE                                 TIMESTAMP 

-- , CHANGE_TIME TIMESTAMP

-- , NOTES                                       STRING 

-- , OVERRIDE_FLAG                               STRING 

-- , CROSSDOCK_ID                                STRING 

-- , ALT_ORIGIN1                                 STRING 

-- , ALT_ORIGIN2                                 STRING 

-- , ALT_ORIGIN3                                 STRING 

-- , ALT_ORIGIN4                                 STRING 

-- , ALT_ORIGIN5                                 STRING 

-- , ALT_ORIGIN6                                 STRING 

-- , ALT_ORIGIN7                                 STRING 

-- , CROSSDOCK_ID2                               STRING 

-- , CROSSDOCK_ID3                               STRING 

-- , CROSSDOCK_ID4                               STRING 

-- , CROSSDOCK_ID5                               STRING 

-- , CROSSDOCK_ID6                               STRING 

-- , CROSSDOCK_ID7                               STRING 

-- , DUE_BASED_ON_CD                             STRING 

-- , ORDER_DAY_OF_WK_ARRAY                       STRING 

-- , WEEK_FREQ_CD                                STRING 

-- , QUOTED_LT_DAY_CNT SMALLINT

-- , LT_DAY_CNT                                  DECIMAL(5,2) 

-- , LOAD_DT                                     TIMESTAMP                            not null

-- , MANDT                                       STRING 

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/dp_site_vend_profile'; 

-- --DISTRIBUTE ON (VENDOR_ID, LOCATION_ID)







-- --*****  Creating table:  "POG_PROMO_HOLD" , ***** Creating table: "POG_PROMO_HOLD"


-- use legacy;
-- CREATE TABLE  POG_PROMO_HOLD
-- (
--  PRODUCT_ID INT not null

-- , LOCATION_ID INT not null

-- , POG_NBR       STRING                  not null

-- , REPL_START_DT                                     TIMESTAMP                             not null

-- , REPL_END_DT                                       TIMESTAMP 

-- , LIST_START_DT                                     TIMESTAMP 

-- , LIST_END_DT                                       TIMESTAMP 

-- , PROMO_HOLD_QTY                                    INT 

-- , LAST_CHNG_DT                                      TIMESTAMP 

-- , POG_STATUS_CD                                     STRING 

-- , UPDATE_DT     TIMESTAMP 

-- , LOAD_DT       TIMESTAMP 

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/pog_promo_hold'; 

-- --DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







-- --*****  Creating table:  "CKB_SPC_PERFORMANCE" , ***** Creating table: "CKB_SPC_PERFORMANCE"


-- use legacy;
-- CREATE TABLE  CKB_SPC_PERFORMANCE
-- (
--  CKB_DB_PLANOGRAM_KEY INT not null

-- , CKB_DB_PRODUCT_KEY INT not null

-- , CKB_DB_PERFORMANCE_KEY INT not null

-- , PRICE                                  DECIMAL(19,4) 

-- , CASE_COST                              DECIMAL(19,4) 

-- , FACINGS INT

-- , UNITS SMALLINT

-- , CAPACITY INT

-- , LINEAR                                 DECIMAL(17,8) 

-- , SQUARE                                 DECIMAL(17,8) 

-- , CUBIC                                  DECIMAL(17,8) 

-- , SALES                                  DECIMAL(19,4) 

-- , UNIT_COST                              DECIMAL(19,4) 

-- , UNIT_PROFIT                            DECIMAL(19,4) 

-- , PROFIT                                 DECIMAL(19,4) 

-- , LINEAR_PCT                             DECIMAL(17,8) 

-- , SQUARE_PCT                             DECIMAL(17,8) 

-- , CUBIC_PCT                              DECIMAL(17,8) 

-- , LINEAR_PCT_USED                        DECIMAL(17,8) 

-- , SQUARE_PCT_USED                        DECIMAL(17,8) 

-- , CUBIC_PCT_USED                         DECIMAL(17,8) 

-- , SPC_PERF_CHANGE_DESC                   STRING 

-- , AVG_SALES_DLRS                         DECIMAL(17,8) 

-- , AVG_MARGIN_DLRS                        DECIMAL(17,8) 

-- , SUM_SALES_DLRS                         DECIMAL(17,8) 

-- , SUM_MARGIN_DLRS                        DECIMAL(17,8) 

-- , SUM_UNITS INT

-- , NUMBER_OF_STORES SMALLINT

-- , NUMBER_OF_WEEKS_SOLD SMALLINT

-- , AO_CLUSTER_DBKEY                       DECIMAL(17,8) 

-- , AO_AVG_SALES_DLRS                      DECIMAL(19,4) 

-- , AO_AVG_MARGIN_DLRS                     DECIMAL(19,4) 

-- , AO_AVG_UNITS                           DECIMAL(19,4) 

-- , AO_SUM_SALES_DLRS                      DECIMAL(19,4) 

-- , AO_SUM_MARGIN_DLRS                     DECIMAL(19,4) 

-- , AO_SUM_UNITS INT

-- , PREVIOUS_LOCATION_ID SMALLINT

-- , PREVIOUS_POSITIONS SMALLINT

-- , PREVIOUS_FACINGS SMALLINT

-- , PREVIOUS_X                             DECIMAL(17,8) 

-- , PREVIOUS_Y                             DECIMAL(17,8) 

-- , PREVIOUS_CAPACITY SMALLINT

-- , CPI_RANK SMALLINT

-- , RECOMMENDED_FACINGS SMALLINT

-- , ASSORTMENT_STRATEGY                    STRING 

-- , ASSORTMENT_TACTIC                      STRING 

-- , ASSORTMENT_REASON                      STRING 

-- , ASSORTMENT_ACTION                      STRING 

-- , NUMBER_OF_POSITIONS SMALLINT

-- , CLUSTER_NAME                           STRING 

-- , ASSORTMENT_NOTE                        STRING 

-- , RECOMMENDED_ORIENTATION SMALLINT

-- , RECOMMENDED_MERCH_STYLE SMALLINT

-- , IGNORE_RECOMMENDATIONS SMALLINT

-- , PRIORITY SMALLINT

-- , PRIORITY_DESC                          STRING 

-- , DEL_FLAG SMALLINT

-- , LOAD_DT DATE

-- , UPDATE_DT DATE

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/ckb_spc_performance' ;

-- --DISTRIBUTE ON (CKB_DB_PLANOGRAM_KEY)

-- --ORGANIZE   ON (CKB_DB_PRODUCT_KEY)







-- --*****  Creating table:  "CKB_SPC_PRODUCT" , ***** Creating table: "CKB_SPC_PRODUCT"


-- use legacy;
-- CREATE TABLE  CKB_SPC_PRODUCT
-- (
--  CKB_DB_PRODUCT_KEY INT not null

-- , SKU_NBR INT

-- , PRODUCT_ID INT

-- , DB_STATUS INT

-- , MANUFACTURER                           STRING 

-- , INNER_PACK INT

-- , PACKAGE_STYLE INT

-- , PRODUCT_PRICE                          DECIMAL(19,4) 

-- , CASE_COST                              DECIMAL(19,4) 

-- , TAX_CODE INT

-- , UNIT_MOVEMENT                          DECIMAL(15,8) 

-- , PRODUCT_SHARE                          DECIMAL(15,8) 

-- , CASE_MULTIPLE                          DECIMAL(15,8) 

-- , COMBINED_PERFORMANCE_INDEX             DECIMAL(15,8) 

-- , TRAY_NUMBER_WIDE INT

-- , TRAY_NUMBER_HIGH INT

-- , TRAY_NUMBER_DEEP INT

-- , CASE_NUMBER_WIDE INT

-- , CASE_NUMBER_HIGH INT

-- , CASE_NUMBER_DEEP INT

-- , DISPLAY_WIDTH                          DECIMAL(15,8) 

-- , DISPLAY_HEIGHT                         DECIMAL(15,8) 

-- , DISPLAY_DEPTH                          DECIMAL(15,8) 

-- , DISPLAY_TOTAL_NBR INT

-- , ALTERNATE_WIDTH                        DECIMAL(15,8) 

-- , ALTERNATE_HEIGHT                       DECIMAL(15,8) 

-- , ALTERNATE_DEPTH                        DECIMAL(15,8) 

-- , ALTERNATE_TOTAL_NBR                   INT

-- , LOOSE_WIDTH                            DECIMAL(15,8) 

-- , LOOSE_HEIGHT                           DECIMAL(15,8) 

-- , LOOSE_DEPTH                            DECIMAL(15,8) 

-- , LOOSE_TOTAL_NBR INT

-- , NUMBER_OF_POSITIONS INT

-- , USR_PRODUCT_FLD_1                      STRING 

-- , USR_PRODUCT_FLD_2                      STRING 

-- , USR_PRODUCT_FLD_3                      STRING 

-- , USR_PRODUCT_FLD_4                      STRING 

-- , USR_PRODUCT_FLD_5                      STRING 

-- , STRIP_DESC                             STRING 

-- , CA_USR_PRODUCT_FLD_1                   STRING 

-- , CA_USR_PRODUCT_FLD_2                   STRING 

-- , CA_USR_PRODUCT_FLD_3                   STRING 

-- , CA_USR_PRODUCT_FLD_4                   STRING 

-- , CA_USR_PRODUCT_FLD_5                   STRING 

-- , AO_FLD_1                               STRING 

-- , AO_FLD_2                               STRING 

-- , AO_FLD_3                               STRING 

-- , AO_FLD_4                               STRING 

-- , AO_FLD_5                               STRING 

-- , PRODUCT_SIZE                           DECIMAL(15,8) 

-- , SALES_AMT_52WK                         DECIMAL(15,8) 

-- , MARGIN_AMT_52WK                        DECIMAL(15,8) 

-- , SALES_QTY_52WK                         DECIMAL(15,8) 

-- , SALES_AMT_26WK                         DECIMAL(15,8) 

-- , MARGIN_AMT_26WK                        DECIMAL(15,8) 

-- , SALES_QTY_26WK                         DECIMAL(15,8) 

-- , SALES_AMT_13WK                         DECIMAL(15,8) 

-- , MARGIN_AMT_13WK                        DECIMAL(15,8) 

-- , SALES_QTY_13WK                         DECIMAL(15,8) 

-- , NUMBER_OF_STORES_CHAIN                 DECIMAL(15,8) 

-- , ARTICLE_REPL INT

-- , PRODUCT_WITH_NO_RETAIL INT

-- , DISCONTINUED INT

-- , PRODUCT_STATUS                         STRING 

-- , LOAD_DT DATE

-- , UPDATE_DT DATE

-- , CASE_TOTAL_NBR INT

-- , TRAY_TOTAL_NBR INT

-- , MADE_IN_CANADA_FLAG INT

-- , DEL_FLAG SMALLINT

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/ckb_spc_product'; 

-- --DISTRIBUTE ON (CKB_DB_PRODUCT_KEY)







-- --*****  Creating table:  "CKB_SPC_PLANOGRAM" , ***** Creating table: "CKB_SPC_PLANOGRAM"


-- use legacy;
-- CREATE TABLE  CKB_SPC_PLANOGRAM
-- (
--  CKB_DB_PLANOGRAM_KEY INT not null

-- , DB_STATUS INT

-- , NAME                                   STRING 

-- , WIDTH                                  DECIMAL(15,2) 

-- , HEIGHT                                 DECIMAL(15,2) 

-- , DEPTH                                  DECIMAL(15,2) 

-- , TRAFFIC_FLOW INT

-- , NUMBER_OF_FIXTURES INT

-- , NUMBER_OF_SEGMENTS INT

-- , NUMBER_OF_STORES INT

-- , SALES                                  DECIMAL(17,8) 

-- , COST                                   DECIMAL(17,8) 

-- , MARGIN                                 DECIMAL(17,8) 

-- , CAPACITY_COST                          DECIMAL(17,8) 

-- , CAPACITY_RETAIL                        DECIMAL(17,8) 

-- , ANNUAL_PROFIT                          DECIMAL(17,8) 

-- , ROLL_COST                              DECIMAL(17,8) 

-- , ROLL_RETAIL                            DECIMAL(17,8) 

-- , POG_TYPE_CD                            STRING 

-- , POG_DIVISION                           STRING 

-- , POG_DEPARTMENT                         STRING 

-- , POG_SUB_DIVISION                       STRING 

-- , POG_GROUP                              STRING 

-- , POG_GROUP_ID INT

-- , FIXTURE_TYPE_NM                        STRING 

-- , CLUSTER_NM                             STRING 

-- , PRESENTATION                           STRING 

-- , CONFIGURATION                          STRING 

-- , VERSION_COMMENTS                       STRING 

-- , POG_CHANGE_TYPE                        STRING 

-- , STRIP_TYPE                             STRING 

-- , CATEGORY_ROLE                          STRING 

-- , ANALYST                                STRING 

-- , VERSION_REASON                         STRING 

-- , DRIVE_AISLE                            STRING 

-- , CALCULATED_PERCENT_CHANGE              DECIMAL(17,8) 

-- , PLANNED_PERCENT_CHANGE                 DECIMAL(17,8) 

-- , FLAG_US INT

-- , FLAG_CA INT

-- , FLAG_PR INT

-- , CAPACITY INT

-- , NBR_PRD_ALLOC INT

-- , SALES_ALLOC                            DECIMAL(17,8) 

-- , COST_ALLOC                             DECIMAL(17,8) 

-- , MOVEMENT_ALLOC                         DECIMAL(17,8) 

-- , MARGIN_ALLOC                           DECIMAL(17,8) 

-- , ANNUAL_PROFIT_ALLOC                    DECIMAL(17,8) 

-- , POG_STATUS                             STRING 

-- , DATE_CREATED                           TIMESTAMP 

-- , DATE_MODIFIED                          TIMESTAMP 

-- , DATE_PENDING                           TIMESTAMP 

-- , DATE_EFFECTIVE                         TIMESTAMP 

-- , DATE_FINISHED                          TIMESTAMP 

-- , PLANNER_LISTING_END_DT DATE

-- , APPROVED_DT DATE

-- , PLANNING_DT DATE

-- , DB_DATE_EFFECTIVE_FROM                 TIMESTAMP 

-- , DB_DATE_EFFECTIVE_TO                   TIMESTAMP 

-- , DB_VERSION_KEY INT

-- , DEPATMENT                              STRING 

-- , ROLL_COST_ALLOC                        DECIMAL(17,8) 

-- , ROLL_RETAIL_ALLOC                      DECIMAL(17,8) 

-- , PROFIT                                 DECIMAL(17,8) 

-- , PROFIT_ALLOC                           DECIMAL(17,8) 

-- , PG_STATUS INT

-- , ABBREV_NM                              STRING 

-- , CATEGORY_NM                            STRING 

-- , NUMBER_OF_SECTIONS INT

-- , LINEAR                                 DECIMAL(17,8) 

-- , SQUARE                                 DECIMAL(17,8) 

-- , CUBIC                                  DECIMAL(17,8) 

-- , SUB_CATEGORY_NM                        STRING 

-- , FLOORPLANS_CNT INT

-- , PENDING_DATE                           TIMESTAMP 

-- , LIVE_DATE                              TIMESTAMP 

-- , FINISHED_DATE                          TIMESTAMP 

-- , PG_TYPE INT

-- , DEL_FLAG SMALLINT

-- , LOAD_DT DATE

-- , UPDATE_DT DATE

-- , FULL_NM                                STRING 

-- , PLANNER_DESC                           STRING 

-- , RECENT_MO_YR                           STRING 

-- , LAYOUT_FILE_NM                         STRING 

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/ckb_spc_planogram' ;

-- --DISTRIBUTE ON (CKB_DB_PLANOGRAM_KEY)







-- --*****  Creating table:  "CKB_FLR_PERFORMANCE" , ***** Creating table: "CKB_FLR_PERFORMANCE"


-- use legacy;
-- CREATE TABLE  CKB_FLR_PERFORMANCE
-- (
--  CKB_DB_FLOOR_PLAN_KEY INT not null

-- , CKB_DB_PLANOGRAM_KEY INT not null

-- , CKB_DB_FLOOR_PERFORMANCE_KEY INT not null

-- , LINEAR                                 DECIMAL(17,8) 

-- , SQUARE                                 DECIMAL(17,8) 

-- , CUBIC                                  DECIMAL(17,8) 

-- , LINEAR_PCT                             DECIMAL(17,8) 

-- , SQUARE_PCT                             DECIMAL(17,8) 

-- , LINEAR_PCT_USED                        DECIMAL(17,8) 

-- , SQUARE_PCT_USED                        DECIMAL(17,8) 

-- , TRAFIC_FLOW INT

-- , NUMBER_OF_FIXTURES INT

-- , NUMBER_OF_SECTIONS INT

-- , NUMBER_OF_PRODUCTS_ALLOC INT

-- , DB_FROM_EFF_DT                         TIMESTAMP 

-- , DB_TO_EFF_DT                           TIMESTAMP 

-- , DB_STATUS INT

-- , ACTIVE INT

-- , DEL_FLAG SMALLINT

-- , LOAD_DT DATE

-- , UPDATE_DT DATE

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/ckb_flr_performance'; 

-- --DISTRIBUTE ON (CKB_DB_FLOOR_PERFORMANCE_KEY)

-- --ORGANIZE   ON (CKB_DB_PLANOGRAM_KEY)







-- --*****  Creating table:  "CKB_FLR_FLOORPLAN" , ***** Creating table: "CKB_FLR_FLOORPLAN"


-- use legacy;
-- CREATE TABLE  CKB_FLR_FLOORPLAN
-- (
--  CKB_DB_FLOOR_PLAN_KEY INT not null

-- , DB_STATUS INT

-- , NAME                                   STRING 

-- , STORE_NBR INT

-- , LOCATION_ID INT

-- , WIDTH                                  DECIMAL(14,7) 

-- , DEPTH                                  DECIMAL(14,7) 

-- , BANNER                                 STRING 

-- , COUNTRY                                STRING 

-- , STATE                                  STRING 

-- , FDOOR_LOC                              STRING 

-- , RDOCK_LOC                              STRING 

-- , HOTEL_LOC                              STRING 

-- , VET_LOC                                STRING 

-- , GROOM_LOC                              STRING 

-- , TRAIN_LOC                              STRING 

-- , REGISTER_LOC                           STRING 

-- , FISH_SYS_LOC                           STRING 

-- , VERSION_REASON                         STRING 

-- , VERSION_COMMENTS                       STRING 

-- , FP_TYPE                                STRING 

-- , ADOPTION_CENTER                        STRING 

-- , FPSLM_NAME                             STRING 

-- , STORE_SOFT_OPEN_DT                     STRING 

-- , BUILDING_PROTOTYPE                     STRING 

-- , MERCH_PROTOTYPE                        STRING 

-- , PERF_LOAD_STATUS                       STRING 

-- , CITY                                   STRING 

-- , BIRD_SA_HABITAT_TYPE                   STRING 

-- , FRONT_DRIVE                            DECIMAL(15,6) 

-- , REAR_DRIVE                             DECIMAL(15,6) 

-- , DRIVE_AISLE_LENGTH                     DECIMAL(15,6) 

-- , MERCH_WIDTH                            DECIMAL(15,6) 

-- , MERCH_DEPTH                            DECIMAL(15,6) 

-- , STORE_CNT                              DECIMAL(15,6) 

-- , UNIQUE_FP INT

-- , HORIZONTAL_LAYOUT INT

-- , REGULAR_FLOW INT

-- , NUMBER_OF_FIXTURES INT

-- , NUMBER_OF_DRAWINGS INT

-- , HORIZONTAL_SPACING INT

-- , VERTICAL_SPACING INT

-- , PENDING_DT                             TIMESTAMP 

-- , LIVE_DT                                TIMESTAMP 

-- , FINISHED_DT                            TIMESTAMP 

-- , WARNINGS_CNT INT

-- , WARNING                                STRING 

-- , DB_FROM_EFF_DT                         TIMESTAMP 

-- , DB_TO_EFF_DT                           TIMESTAMP 

-- , DB_VERSION_KEY INT

-- , STATUS                                 STRING 

-- , DEL_FLAG SMALLINT

-- , LOAD_DT DATE

-- , UPDATE_DT DATE

-- )
-- USING delta 
-- LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/ckb_flr_floorplan' ;

-- --DISTRIBUTE ON (CKB_DB_FLOOR_PLAN_KEY)



