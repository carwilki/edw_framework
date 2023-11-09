




--*****  Creating table:  "DP_FORECAST_WEEK_HIST" , ***** Creating table: "DP_FORECAST_WEEK_HIST"


use legacy;
CREATE TABLE  DP_FORECAST_WEEK_HIST
(
 SNAPSHOT_DT                                DATE                                not null

, WEEK_DT                                    DATE                                not null

, LOCATION_ID INT not null

, PRODUCT_ID INT not null

, FROM_LOCATION_ID INT not null

, SOURCE_VENDOR_ID INT

, SOURCE_VENDOR_SUB_GROUP                     STRING 

, DP_PURCH_GROUP_ID                           STRING 

, BASELINE_FORECAST_QTY                       DECIMAL(12,4) 

, CAMPAIGN_INCREASE_QTY                       DECIMAL(12,4) 

, CALCULATED_FORECAST_QTY                     DECIMAL(12,4) 

, EFFECTIVE_FORECAST_QTY                      DECIMAL(12,4) 

, CORRECTED_EFFECTIVE_FORECAST_QTY            DECIMAL(12,4) 

, LOAD_TSTMP                                  TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/demand_planning/dp_forecast_week_hist' 
 ;

--DISTRIBUTE ON (LOCATION_ID, PRODUCT_ID)

--ORGANIZE   ON (SNAPSHOT_DT, WEEK_DT)







--*****  Creating table:  "DP_ACCURACY_DAY" , ***** Creating table: "DP_ACCURACY_DAY"


use legacy;
CREATE TABLE  DP_ACCURACY_DAY
(
 DAY_DT                                     DATE                                not null

, LOCATION_ID INT not null

, PRODUCT_ID INT not null

, SOURCE_VENDOR_ID INT not null

, SOURCE_VENDOR_SUB_GROUP                     STRING 

, DP_PURCH_GROUP_ID                           STRING 

, EFFECTIVE_FORECAST_QTY                      DECIMAL(12,4) 

, OUT_OF_STOCK_FLAG                           TINYINT 

, ESTIMATED_LOST_SALES_QTY                    DECIMAL(12,4) 

, ESTIMATED_LOST_SALES_AMT                    DECIMAL(12,4) 

, LOAD_TSTMP                                  TIMESTAMP                           

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/demand_planning/dp_accuracy_day' 
 ;

--DISTRIBUTE ON (LOCATION_ID, PRODUCT_ID)

--ORGANIZE   ON (DAY_DT)







--*****  Creating table:  "DP_PRODUCT_LOCATION_SETTINGS_HIST" , ***** Creating table: "DP_PRODUCT_LOCATION_SETTINGS_HIST"


use legacy;
CREATE TABLE  DP_PRODUCT_LOCATION_SETTINGS_HIST
(
 SNAPSHOT_DT                                DATE                                not null

, LOCATION_ID INT not null

, PRODUCT_ID INT not null

, SOURCE_VENDOR_ID INT not null

, SOURCE_VENDOR_SUB_GROUP                     STRING 

, DP_PURCH_GROUP_ID                           STRING 

, PRODUCT_DP_START_DT DATE

, PRODUCT_DP_END_DT DATE

, OLD_PRODUCT_ID INT

, NEW_PRODUCT_ID INT

, REFERENCE_PRODUCT_ID INT

, REFERENCE_SCALING_FACTOR                    DECIMAL(8,4) 

, ORDER_MODEL                                 STRING 

, FORECAST_MODEL                              STRING 

, PRODUCT_LOCATION_ABC_CLASS                  STRING 

, PRODUCT_ABC_CLASS                           STRING 

, PRODUCT_LOCATION_XYZ_CLASS                  STRING 

, PRODUCT_XYZ_CLASS                           STRING 

, DEMAND_SATISFIED_PCT                        DECIMAL(8,4) 

, END_BALANCE_QTY                             DECIMAL(12,4) 

, ORDER_PARAMETER_QTY                         DECIMAL(12,4) 

, MIN_FILL_QTY                                DECIMAL(12,4) 

, ORDER_BATCH_SIZE                            DECIMAL(12,4) 

, ECONOMIC_ORDER_QTY                          DECIMAL(12,4) 

, MIN_DELIVERY_QTY                            DECIMAL(12,4) 

, LOAD_TSTMP                                  TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/demand_planning/dp_product_location_settings_hist' 
 ;

--DISTRIBUTE ON (LOCATION_ID, PRODUCT_ID)

--ORGANIZE   ON (SNAPSHOT_DT)







--*****  Creating table:  "DP_DEMAND_DAY" , ***** Creating table: "DP_DEMAND_DAY"


use legacy;
CREATE TABLE  DP_DEMAND_DAY
(
 DAY_DT                                     DATE                                not null

, LOCATION_ID INT not null

, PRODUCT_ID INT not null

, FROM_LOCATION_ID INT not null

, SOURCE_VENDOR_ID INT

, SOURCE_VENDOR_SUB_GROUP                     STRING 

, ORDERING_NEED_QTY                           DECIMAL(12,4) 

, NEEDED_QTY                                  DECIMAL(12,4) 

, EFFECTIVE_QTY                               DECIMAL(12,4) 

, PROPOSED_QTY                                DECIMAL(12,4) 

, UPDATE_TSTMP                                TIMESTAMP 

, LOAD_TSTMP                                  TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/demand_planning/dp_demand_day' 
 ;

--DISTRIBUTE ON (LOCATION_ID, PRODUCT_ID)

--ORGANIZE   ON (DAY_DT)







--*****  Creating table:  "DP_ORDER_PROJECTION_DAY_HIST" , ***** Creating table: "DP_ORDER_PROJECTION_DAY_HIST"


use legacy;
CREATE TABLE  DP_ORDER_PROJECTION_DAY_HIST
(
 SNAPSHOT_DT                                DATE                                not null

, DAY_DT                                     DATE                                not null

, LOCATION_ID INT not null

, PRODUCT_ID INT not null

, FROM_LOCATION_ID INT not null

, SOURCE_VENDOR_ID INT

, SOURCE_VENDOR_SUB_GROUP                     STRING 

, PROJECTED_ORDER_PROPOSAL_QTY                DECIMAL(12,4) 

, PROJECTED_DELIVERY_QTY                      DECIMAL(12,4) 

, LOAD_TSTMP                                  TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/demand_planning/dp_order_projection_day_hist' 
 ;

--DISTRIBUTE ON (LOCATION_ID, PRODUCT_ID)

--ORGANIZE   ON (SNAPSHOT_DT, DAY_DT)







--*****  Creating table:  "DP_ORDER_PROJECTION_WEEK_HIST" , ***** Creating table: "DP_ORDER_PROJECTION_WEEK_HIST"


use legacy;
CREATE TABLE  DP_ORDER_PROJECTION_WEEK_HIST
(
 SNAPSHOT_DT                                DATE                                not null

, WEEK_DT                                    DATE                                not null

, LOCATION_ID INT not null

, PRODUCT_ID INT not null

, FROM_LOCATION_ID INT not null

, SOURCE_VENDOR_ID INT

, SOURCE_VENDOR_SUB_GROUP                     STRING 

, PROJECTED_ORDER_PROPOSAL_QTY                DECIMAL(12,4) 

, PROJECTED_DELIVERY_QTY                      DECIMAL(12,4) 

, LOAD_TSTMP                                  TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/demand_planning/dp_order_projection_week_hist' 
 ;

--DISTRIBUTE ON (LOCATION_ID, PRODUCT_ID)

--ORGANIZE   ON (SNAPSHOT_DT, WEEK_DT)







--*****  Creating table:  "RELEX_PAST_ACCURACY_PRE" , ***** Creating table: "RELEX_PAST_ACCURACY_PRE"


use raw;
CREATE TABLE  RELEX_PAST_ACCURACY_PRE
(
 DAY_DATE     DATE                                 not null

, LOCATION_CODE INT not null

, PRODUCT_CODE INT not null

, SUPPLIER_CODE                                     STRING                 not null

, PURCHASE_GROUP                                    STRING 

, EFFECTIVE_FORECAST                                DECIMAL(12,4) 

, ESTIMATED_LOST_SALES                              DECIMAL(12,4) 

, ESTIMATED_LOST_SALES_VALUE                        DECIMAL(12,4) 

, COUNT_OF_STOCK_OUTS                               TINYINT 

, LOAD_TSTMP                                        TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/demand_planning/relex_past_accuracy_pre' 
 ;

--DISTRIBUTE ON (LOCATION_CODE, PRODUCT_CODE)

--ORGANIZE   ON (DAY_DATE)







--*****  Creating table:  "RELEX_PRODUCT_LOCATION_SETTINGS_PRE" , ***** Creating table: "RELEX_PRODUCT_LOCATION_SETTINGS_PRE"


use raw;
CREATE TABLE  RELEX_PRODUCT_LOCATION_SETTINGS_PRE
(
 LOCATION_CODE INT not null

, PRODUCT_CODE INT not null

, SUPPLIER_CODE                                     STRING                 not null

, PURCHASE_GROUP                                    STRING 

, INTRODUCTION_DATE DATE

, TERMINATION_DATE DATE

, REPLACES_PRODUCT INT

, REPLACING_PRODUCT INT

, REFERENCE_PRODUCT INT

, REFERENCE_SCALING_FACTOR                          DECIMAL(8,4) 

, ORDER_MODEL                                       STRING 

, FORECAST_MODEL                                    STRING 

, ABC_CLASS     STRING 

, PRODUCT_ABC_CLASS                                 STRING 

, XYZ_CLASS     STRING 

, PRODUCT_XYZ_CLASS                                 STRING 

, DEMAND_SATISFIED_PCT                              DECIMAL(8,4) 

, LOAD_TSTMP                                        TIMESTAMP                            

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/demand_planning/relex_product_location_settings_pre' 
 ;

--DISTRIBUTE ON (LOCATION_CODE, PRODUCT_CODE)







--*****  Creating table:  "RELEX_INVENTORY_LAYER_PRE" , ***** Creating table: "RELEX_INVENTORY_LAYER_PRE"


use raw;
CREATE TABLE  RELEX_INVENTORY_LAYER_PRE
(
 LOCATION_CODE INT not null

, PRODUCT_CODE INT not null

, END_BALANCE                                       STRING 

, ORDER_PARAMETER_QTY                               DECIMAL(12,4) 

, MIN_FILL      DECIMAL(12,4) 

, ORDER_BATCH_SIZE                                  DECIMAL(12,4) 

, ECONOMIC_ORDER_QTY                                DECIMAL(12,4) 

, MIN_DELIVERY_QTY                                  DECIMAL(12,4) 

, LOAD_TSTMP                                        TIMESTAMP                             

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/demand_planning/relex_inventory_layer_pre' 
 ;

--DISTRIBUTE ON (LOCATION_CODE, PRODUCT_CODE)







--*****  Creating table:  "RELEX_DAILY_ORDER_PROJECTION_PRE" , ***** Creating table: "RELEX_DAILY_ORDER_PROJECTION_PRE"


use raw;
CREATE TABLE  RELEX_DAILY_ORDER_PROJECTION_PRE
(
 DAY_DATE     DATE                                 not null

, LOCATION_CODE INT not null

, PRODUCT_CODE INT not null

, SUPPLIER_CODE                                     STRING                 not null

, PROJECTED_ORDER_PROPOSALS                         DECIMAL(12,4) 

, PROJECTED_DELIVERIES                              DECIMAL(12,4) 

, LOAD_TSTMP                                        TIMESTAMP                             

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/demand_planning/relex_daily_order_projection_pre' 
 ;

--DISTRIBUTE ON (LOCATION_CODE, PRODUCT_CODE)

--ORGANIZE   ON (DAY_DATE)







--*****  Creating table:  "RELEX_FUTURE_FORECAST_PRE" , ***** Creating table: "RELEX_FUTURE_FORECAST_PRE"


use raw;
CREATE TABLE  RELEX_FUTURE_FORECAST_PRE
(
 WEEK_DATE    DATE                                 not null

, LOCATION_CODE INT not null

, PRODUCT_CODE INT not null

, SUPPLIER_CODE                                     STRING                 not null

, PURCHASE_GROUP                                    STRING 

, BASELINE_FORECAST                                 DECIMAL(12,4) 

, CAMPAIGN_INCREASE                                 DECIMAL(12,4) 

, CALCULATED_FORECAST                               DECIMAL(12,4) 

, EFFECTIVE_FORECAST                                DECIMAL(12,4) 

, CORRECTED_EFFECTIVE_FORECAST                      DECIMAL(12,4) 

, LOAD_TSTMP                                        TIMESTAMP                             

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/demand_planning/relex_future_forecast_pre' 
 ;

--DISTRIBUTE ON (LOCATION_CODE, PRODUCT_CODE)

--ORGANIZE   ON (WEEK_DATE)







--*****  Creating table:  "RELEX_DAILY_DEMAND_PRE" , ***** Creating table: "RELEX_DAILY_DEMAND_PRE"


use raw;
CREATE TABLE  RELEX_DAILY_DEMAND_PRE
(
 DAY_DATE     DATE                                 not null

, LOCATION_CODE INT not null

, PRODUCT_CODE INT not null

, SUPPLIER_CODE                                     STRING                 not null

, ORDERING_NEED                                     DECIMAL(12,4) 

, NEEDED_QUANTITY                                   DECIMAL(12,4) 

, EFFECTIVE_QUANTITY                                DECIMAL(12,4) 

, PROPOSED_QUANTITY                                 DECIMAL(12,4) 

, LOAD_TSTMP                                        TIMESTAMP                             

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/demand_planning/relex_daily_demand_pre' 
 ;

--DISTRIBUTE ON (LOCATION_CODE, PRODUCT_CODE)

--ORGANIZE   ON (DAY_DATE)







--*****  Creating table:  "RELEX_WEEKLY_ORDER_PROJECTION_PRE" , ***** Creating table: "RELEX_WEEKLY_ORDER_PROJECTION_PRE"


use raw;
CREATE TABLE  RELEX_WEEKLY_ORDER_PROJECTION_PRE
(
 WEEK_DATE    DATE                                 not null

, LOCATION_CODE INT not null

, PRODUCT_CODE INT not null

, SUPPLIER_CODE                                     STRING                 not null

, PROJECTED_ORDER_PROPOSALS                         DECIMAL(12,4) 

, PROJECTED_DELIVERIES                              DECIMAL(12,4) 

, LOAD_TSTMP                                        TIMESTAMP                             

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/demand_planning/relex_weekly_order_projection_pre' 
 ;

--DISTRIBUTE ON (LOCATION_CODE, PRODUCT_CODE)

--ORGANIZE   ON (WEEK_DATE)


