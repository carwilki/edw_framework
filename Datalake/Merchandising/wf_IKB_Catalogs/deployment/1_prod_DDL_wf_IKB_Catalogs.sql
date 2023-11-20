




--*****  Creating table:  "POG_GROUP" , ***** Creating table: "POG_GROUP"


use legacy;
CREATE TABLE  POG_GROUP
(
 POG_GROUP_ID INT not null

, POG_GROUP_DESC                                    STRING                not null

, UPDATE_DT     TIMESTAMP 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/int_merchandising/pog_group' 
;

--DISTRIBUTE ON (POG_GROUP_ID)









--*****  Creating table:  "POG_SKU_VERSION" , ***** Creating table: "POG_SKU_VERSION"


use legacy;
CREATE TABLE  POG_SKU_VERSION
(
 PRODUCT_ID INT not null

, POG_DBKEY INT not null

, SKU_NBR INT

, LINK_SUB_PRODUCT_ID INT

, LINK_SUB_SKU_NBR INT

, SKU_CAPACITY_QTY                                  INT 

, SKU_FACINGS_QTY                                   INT 

, SKU_HEIGHT_IN                                     DECIMAL(7,2) 

, SKU_DEPTH_IN                                      DECIMAL(7,2) 

, SKU_WIDTH_IN                                      DECIMAL(7,2) 

, UNIT_OF_MEASURE                                   STRING 

, LAST_CHNG_DT DATE

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/int_merchandising/pog_sku_version' 
;

--DISTRIBUTE ON (PRODUCT_ID)







--*****  Creating table:  "POG_STATUS" , ***** Creating table: "POG_STATUS"


use legacy;
CREATE TABLE  POG_STATUS
(
 STATUS_ID     STRING                          not null

, STATUS_NAME                                       STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/int_merchandising/pog_status' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "POG_VERSION" , ***** Creating table: "POG_VERSION"


use legacy;
CREATE TABLE  POG_VERSION
(
 POG_DBKEY INT not null

, POG_NM        STRING 

, POG_EFFECTIVE_FROM_DT                             TIMESTAMP 

, POG_EFFECTIVE_TO_DT                               TIMESTAMP 

, DB_STATUS INT

, DB_TIME       TIMESTAMP 

, DB_USER       STRING 

, POG_IMPLEMENT_DT                                  TIMESTAMP 

, POG_REVIEW_DT                                     TIMESTAMP 

, US_FLAG       STRING 

, CA_FLAG       STRING 

, PR_FLAG       STRING 

, POG_TYPE_CD                                       STRING 

, POG_DIVISION                                      STRING 

, POG_DEPT      STRING 

, WIDTH FLOAT

, HEIGHT FLOAT

, DEPTH FLOAT

, ABBREV_NM     STRING 

, DATE_CREATED                                      TIMESTAMP 

, DATE_MODIFIED                                     TIMESTAMP 

, DATE_PENDING                                      TIMESTAMP 

, DATE_EFFECTIVE                                    TIMESTAMP 

, POG_GROUP_ID                                      DOUBLE 

, POG_GROUP_DESC                                    STRING 

, PG_STATUS INT

, PG_SCORE_PERCENT INT

, PG_SCORE_NOTE                                     STRING 

, DB_VERSION_KEY INT

, PG_WARNINGS_COUNT INT

, PG_ERRORS_COUNT INT

, PG_ACTION_LIST                                    STRING 

, ALLOCATION_GROUP                                  STRING 

, ALLOCATION_SEQUENCE INT

, DBPARENT_PGTEMPLATE_KEY INT

, LISTING_ONLY_FLAG INT

, NEW_STORE_FLAG INT

, NOT_GOING_FOWARD_FLAG INT

, PLANNER_PRINT_FLAG INT

, OBSOLETE_FLAG INT

, FIXTURE_TYPE_ID INT

, FIXTURE_TYPE_DESC                                 STRING 

, DISPLAY_LOCATION                                  STRING 

, CATEGORY_ROLE                                     STRING 

, POG_CHANGE_TYPE                                   STRING 

, CLUSTER_NBR                                       DOUBLE 

, ASO           DOUBLE 

, POG_VERSION_REASON                                STRING 

, POG_VERSION_COMMENTS                              STRING 

, DRIVE_AISLE                                       STRING 

, CLUSTER_NM                                        STRING 

, PRESENTATION_MGR_NM                               STRING 

, PLANNER_DESC                                      STRING 

, PLANNER_YEAR                                      STRING 

, COMBO1        DOUBLE 

, COMBO2        DOUBLE 

, COMBO3        DOUBLE 

, PG_SCENARIO_NM                                    STRING 

, PG_EQUIPEMENT_SOURCE                              STRING 

, PG_ASSORTMENT_SOURCE                              STRING 

, PG_PERFORMANCE_SOURCE                             STRING 

, UPDATE_DT     TIMESTAMP 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/int_merchandising/pog_version' 
;

--DISTRIBUTE ON (POG_DBKEY)





--*****  Creating table:  "IKB_SKU_ATTR_PRE" , ***** Creating table: "IKB_SKU_ATTR_PRE"


use raw;
CREATE TABLE  IKB_SKU_ATTR_PRE
(
 SKU_NBR INT not null

, SKU_CAPACITY_QTY                            INT 

, SKU_FACINGS_QTY                             INT 

, SKU_HEIGHT_IN                               DECIMAL(7,2) 

, SKU_DEPTH_IN                                DECIMAL(7,2) 

, SKU_WIDTH_IN                                DECIMAL(7,2) 

, UNIT_OF_MEASURE                             STRING 

, LAST_CHNG_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/int_merchandising/ikb_sku_attr_pre' 
;

--DISTRIBUTE ON (SKU_NBR)

