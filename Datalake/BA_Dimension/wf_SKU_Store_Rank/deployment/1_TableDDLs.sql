--*****  Creating table:  "STORE_RANK" , ***** Creating table: "STORE_RANK"


use legacy;
 CREATE TABLE STORE_RANK 
(
 LOCATION_ID INT not null

, STORE_NBR INT not null

, HIERARCHY_SUB_LVL                                 STRING                 not null

, HIERARCHY_NAME                                    STRING                 not null

, RANK_NAME     STRING                 not null

, RANK_CRITERIA                                     STRING 

, RANK_VALUE_TYPE                                   STRING 

, RANK_MIN_VALUE                                    DECIMAL(20,6) 

, RANK_MAX_VALUE                                    DECIMAL(20,6) 

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/store_rank';

--DISTRIBUTE ON (LOCATION_ID, HIERARCHY_NAME)

--*****  Creating table:  "SKU_RANK" , ***** Creating table: "SKU_RANK"


use legacy;
 CREATE TABLE SKU_RANK 
(
 PRODUCT_ID INT not null

, SKU_NBR INT not null

, HIERARCHY_SUB_LVL                                 STRING                 not null

, HIERARCHY_NAME                                    STRING                 not null

, RANK_NAME     STRING                 not null

, RANK_CRITERIA                                     STRING 

, RANK_VALUE_TYPE                                   STRING 

, RANK_MIN_VALUE                                    DECIMAL(20,6) 

, RANK_MAX_VALUE                                    DECIMAL(20,6) 

, LOAD_DT DATE

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/sku_rank';

--DISTRIBUTE ON (PRODUCT_ID, HIERARCHY_NAME)

