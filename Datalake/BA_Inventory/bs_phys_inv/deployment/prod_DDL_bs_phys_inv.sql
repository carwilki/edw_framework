--*****  Creating table:  "PHYS_INV_HISTORY" , ***** Creating table: "PHYS_INV_HISTORY"


use legacy;
CREATE TABLE 
     PHYS_INV_HISTORY(
        DAY_DT        TIMESTAMP                             not null
        , LOCATION_ID INT not null
        , PHYS_INV_TYPE_ID TINYINT not null
        , CURR_PLANNED_CNT_DT                               TIMESTAMP 
        , PREV_PLANNED_CNT_DT                               TIMESTAMP 
        , CURR_ACTUAL_CNT_DT                                TIMESTAMP 
        , PREV_ACTUAL_CNT_DT                                TIMESTAMP 
        , PHYS_INV_DOC_NBR BIGINT
        , LOAD_DT       TIMESTAMP 
        )
        USING delta 
        LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/phys_inv_history' 
        ;

--DISTRIBUTE ON (LOCATION_ID)


--*****  Creating table:  "PHYS_INV_CURRENT" , ***** Creating table: "PHYS_INV_CURRENT"


use legacy;
CREATE TABLE
    PHYS_INV_CURRENT(
        LOCATION_ID INT not null
        , PHYS_INV_TYPE_ID TINYINT not null
        , CURR_PLANNED_CNT_DT                               TIMESTAMP 
        , PREV_PLANNED_CNT_DT                               TIMESTAMP 
        , CURR_ACTUAL_CNT_DT                                TIMESTAMP 
        , PREV_ACTUAL_CNT_DT                                TIMESTAMP 
        , LOAD_DT       TIMESTAMP 
        )
        USING delta 
        LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/phys_inv_current' 
        ;

--DISTRIBUTE ON (LOCATION_ID)



--*****  Creating table:  "PHYS_INV_HDR_PRE" , ***** Creating table: "PHYS_INV_HDR_PRE"

use raw;
CREATE TABLE 
    PHYS_INV_HDR_PRE(
        PHYS_INV_DOC_NBR BIGINT not null
        , FISCAL_YR SMALLINT not null
        , STORE_NBR INT
        , PLANNED_COUNT_DT  TIMESTAMP 
        , LAST_COUNT_DT  TIMESTAMP 
        , DOC_POSTING_DT TIMESTAMP 
        , USER_NAME     STRING 
        , PHYS_INV_DESC STRING 
        )
        USING delta 
        LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/inventory/phys_inv_hdr_pre' 
        ;

--DISTRIBUTE ON RANDOM


use legacy;
CREATE TABLE  
    PHYS_INV_TYPE(
        PHYS_INV_TYPE_ID TINYINT 
        , PHYS_INV_TYPE_DESC      STRING 
        , LOAD_DT       TIMESTAMP 
        )
        USING delta 
        LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/inventory/phys_inv_type';