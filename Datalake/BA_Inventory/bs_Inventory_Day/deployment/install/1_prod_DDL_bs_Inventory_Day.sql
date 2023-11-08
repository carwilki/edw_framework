
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

