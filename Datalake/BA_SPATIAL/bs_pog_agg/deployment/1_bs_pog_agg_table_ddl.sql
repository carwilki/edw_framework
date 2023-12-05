



--*****  Creating table:  "SALES_INV_WEEK_POG" , ***** Creating table: "SALES_INV_WEEK_POG"


use legacy;
 CREATE TABLE  SALES_INV_WEEK_POG 
(  WEEK_DT       TIMESTAMP                             not null

, POG_ID INT not null

, COUNTRY_CD                                        STRING                  not null

, NET_SALES_AMT                                     DECIMAL(10,2) 

, NET_SALES_COST                                    DECIMAL(10,2) 

, NET_SALES_QTY                                     DECIMAL(12,3) 

, MERCH_SALES_AMT                                   DECIMAL(10,2) 

, ON_HAND_QTY                                       DECIMAL(12,3) 

, ON_HAND_COST                                      DECIMAL(10,2) 

, EXCH_RATE_PCT                                     DECIMAL(9,6) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Spatial/sales_inv_week_pog';

--DISTRIBUTE ON (POG_ID)


--*****  Creating table:  "SALES_INV_WEEK_SKU_STORE_POG" , ***** Creating table: "SALES_INV_WEEK_SKU_STORE_POG"


use legacy;
 CREATE TABLE  SALES_INV_WEEK_SKU_STORE_POG 
(  WEEK_DT       TIMESTAMP                             not null

, PRODUCT_ID INT not null

, LOCATION_ID INT not null

, POG_ID INT not null

, NET_SALES_AMT                                     DECIMAL(8,2) 

, NET_SALES_COST                                    DECIMAL(8,2) 

, NET_SALES_QTY                                     DECIMAL(10,3) 

, MERCH_SALES_AMT                                   DECIMAL(8,2) 

, ON_HAND_QTY                                       DECIMAL(10,3) 

, ON_HAND_COST                                      DECIMAL(8,2) 

, POG_INLINE_PLANNER_FLAG                           STRING 

, POG_MULTI_INLINE_FLAG                             STRING 

, POG_MULTI_PLANNER_FLAG                            STRING 

, EXCH_RATE_PCT                                     DECIMAL(9,6) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Spatial/sales_inv_week_sku_store_pog';

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)


--*****  Creating table:  "SALES_INV_WEEK_STORE_POG" , ***** Creating table: "SALES_INV_WEEK_STORE_POG"


use legacy;
 CREATE TABLE  SALES_INV_WEEK_STORE_POG 
(  WEEK_DT       TIMESTAMP                             not null

, LOCATION_ID INT not null

, POG_ID INT not null

, NET_SALES_AMT                                     DECIMAL(10,2) 

, NET_SALES_COST                                    DECIMAL(10,2) 

, NET_SALES_QTY                                     DECIMAL(12,3) 

, MERCH_SALES_AMT                                   DECIMAL(10,2) 

, ON_HAND_QTY                                       DECIMAL(12,3) 

, ON_HAND_COST                                      DECIMAL(10,2) 

, EXCH_RATE_PCT                                     DECIMAL(9,6) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Spatial/sales_inv_week_store_pog';

--DISTRIBUTE ON (LOCATION_ID, POG_ID)


--*****  Creating table:  "POG_AVG_WEEK_SALES_PRE" , ***** Creating table: "POG_AVG_WEEK_SALES_PRE"


use raw;
 CREATE TABLE  POG_AVG_WEEK_SALES_PRE 
(  PRODUCT_ID INT not null

, LOCATION_ID INT not null

, POG_ID INT not null

, AVG_WK_SALES_QTY                                  INT 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Spatial/pog_avg_week_sales_pre';

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "POG_DISTRIB_PRE" , ***** Creating table: "POG_DISTRIB_PRE"


use raw;
 CREATE TABLE  POG_DISTRIB_PRE 
(  PRODUCT_ID INT not null

, LOCATION_ID INT not null

, POG_ID INT not null

, POG_TYPE_CD                                       STRING                          not null

, INLINE_CNT INT

, PLANNER_CNT INT

, PRODLOC_CNT INT

, POG_PROMO_QTY                                     INT 

, PROMO_START_DT                                    TIMESTAMP 

, PROMO_END_DT                                      TIMESTAMP 

, PRODLOC_PROMO_QTY                                 INT 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Spatial/pog_distrib_pre';

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "POG_MERCH_DISCOUNT_WEEK_PRE" , ***** Creating table: "POG_MERCH_DISCOUNT_WEEK_PRE"


use raw;
 CREATE TABLE  POG_MERCH_DISCOUNT_WEEK_PRE 
(  WEEK_DT       TIMESTAMP                             not null

, PRODUCT_ID INT not null

, LOCATION_ID INT not null

, MERCH_DISCOUNT_AMT                                DECIMAL(8,2) 

, MERCH_DISCOUNT_QTY                                INT 

, MERCH_DISCOUNT_RETURN_AMT                         DECIMAL(8,2) 

, MERCH_DISCOUNT_RETURN_QTY                         INT 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Spatial/pog_merch_discount_week_pre';

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)







--*****  Creating table:  "POG_SALES_WEEK_SKU_STORE_PRE" , ***** Creating table: "POG_SALES_WEEK_SKU_STORE_PRE"


use raw;
 CREATE TABLE  POG_SALES_WEEK_SKU_STORE_PRE 
(  WEEK_DT       TIMESTAMP                             not null

, PRODUCT_ID INT not null

, LOCATION_ID INT not null

, POG_ID INT not null

, NET_SALES_AMT                                     DECIMAL(8,2) 

, NET_SALES_COST                                    DECIMAL(8,2) 

, NET_SALES_QTY                                     DECIMAL(10,3) 

, MERCH_SALES_AMT                                   DECIMAL(8,2) 

, POG_TYPE_CD                                       STRING 

, POG_MULTI_INLINE_FLAG                             STRING 

, POG_MULTI_PLANNER_FLAG                            STRING 

, PRODLOC_PROMO_QTY                                 INT 

, INLINE_CNT INT

, PLANNER_CNT INT

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Spatial/pog_sales_week_sku_store_pre';

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)
