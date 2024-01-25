use raw;

CREATE TABLE SALES_RANKING_DATE_PRE 

(

WEEK_DT      DATE                                 not null
 
, RANKING_WEEK_DT DATE
 
)

USING delta 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Dimension/sales_ranking_date_pre';
 
--DISTRIBUTE ON (WEEK_DT)
 
 
 
 
--*****  Creating table:  "SALES_RANKING_SALES_PRE" , ***** Creating table: "SALES_RANKING_SALES_PRE"
 
 
use raw;

CREATE TABLE SALES_RANKING_SALES_PRE 

(

WEEK_DT      DATE                                 not null
 
, LOCATION_ID INT not null
 
, TOTAL_52WK_SALES_AMT                              DECIMAL(18,2)
 
, MERCH_52WK_SALES_AMT                              DECIMAL(18,2)
 
, SERVICES_52WK_SALES_AMT                           DECIMAL(18,2)
 
, SALON_52WK_SALES_AMT                              DECIMAL(18,2)
 
, TRAINING_52WK_SALES_AMT                           DECIMAL(18,2)
 
, HOTEL_DDC_52WK_SALES_AMT                          DECIMAL(18,2)
 
, CONSUMABLES_52WK_SALES_AMT                        DECIMAL(18,2)
 
, HARDGOODS_52WK_SALES_AMT                          DECIMAL(18,2)
 
, SPECIALTY_52WK_SALES_AMT                          DECIMAL(18,2)
 
, COMP_CURR_FLAG SMALLINT
 
, SALES_CURR_FLAG SMALLINT
 
, LOCATION_TYPE_ID SMALLINT
 
)

USING delta 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Dimension/sales_ranking_sales_pre';
 
--DISTRIBUTE ON (LOCATION_ID)
 
 
 
 
--*****  Creating table:  "SALES_RANKING_TOTALS_PRE" , ***** Creating table: "SALES_RANKING_TOTALS_PRE"
 
 
use raw;

CREATE TABLE SALES_RANKING_TOTALS_PRE 

(

WEEK_DT      DATE                                 not null
 
, TOTAL_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, MERCH_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, SERVICES_52WK_COMP_STORES_AMT                     DECIMAL(18,2)
 
, SALON_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, TRAINING_52WK_COMP_STORES_AMT                     DECIMAL(18,2)
 
, HOTEL_DDC_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, CONSUMABLES_52WK_COMP_STORES_AMT                  DECIMAL(18,2)
 
, HARDGOODS_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, SPECIALTY_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
)

USING delta 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Dimension/sales_ranking_totals_pre';
 
--DISTRIBUTE ON (WEEK_DT)
 
 
 
 
--*****  Creating table:  "SALES_RANKING_RUNNING_SUM_PRE" , ***** Creating table: "SALES_RANKING_RUNNING_SUM_PRE"
 
 
use raw;

CREATE TABLE SALES_RANKING_RUNNING_SUM_PRE 

(

WEEK_DT      DATE                                 not null
 
, LOCATION_ID INT not null
 
, COMP_CURR_FLAG TINYINT
 
, SALES_CURR_FLAG TINYINT
 
, TOTAL_52WK_SALES_AMT                              DECIMAL(18,2)
 
, TOTAL_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, STORE_TOTAL_SALES_PCT                             DECIMAL(14,10)
 
, RUNNING_SUM_TOTAL_SALES_PCT                       DECIMAL(14,10)
 
, MERCH_52WK_SALES_AMT                              DECIMAL(18,2)
 
, MERCH_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, STORE_MERCH_SALES_PCT                             DECIMAL(14,10)
 
, RUNNING_SUM_MERCH_SALES_PCT                       DECIMAL(14,10)
 
, SERVICES_52WK_SALES_AMT                           DECIMAL(18,2)
 
, SERVICES_52WK_COMP_STORES_AMT                     DECIMAL(18,2)
 
, STORE_SERVICES_SALES_PCT                          DECIMAL(14,10)
 
, RUNNING_SUM_SERVICES_SALES_PCT                    DECIMAL(14,10)
 
, SALON_52WK_SALES_AMT                              DECIMAL(18,2)
 
, SALON_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, STORE_SALON_SALES_PCT                             DECIMAL(14,10)
 
, RUNNING_SUM_SALON_SALES_PCT                       DECIMAL(14,10)
 
, TRAINING_52WK_SALES_AMT                           DECIMAL(18,2)
 
, TRAINING_52WK_COMP_STORES_AMT                     DECIMAL(18,2)
 
, STORE_TRAINING_SALES_PCT                          DECIMAL(14,10)
 
, RUNNING_SUM_TRAINING_SALES_PCT                    DECIMAL(14,10)
 
, HOTEL_DDC_52WK_SALES_AMT                          DECIMAL(18,2)
 
, HOTEL_DDC_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, STORE_HOTEL_DDC_SALES_PCT                         DECIMAL(14,10)
 
, RUNNING_SUM_HOTEL_DDC_SALES_PCT                   DECIMAL(14,10)
 
, CONSUMABLES_52WK_SALES_AMT                        DECIMAL(18,2)
 
, CONSUMABLES_52WK_COMP_STORES_AMT                  DECIMAL(18,2)
 
, STORE_CONSUMABLES_SALES_PCT                       DECIMAL(14,10)
 
, RUNNING_SUM_CONSUMABLES_SALES_PCT                 DECIMAL(14,10)
 
, HARDGOODS_52WK_SALES_AMT                          DECIMAL(18,2)
 
, HARDGOODS_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, STORE_HARDGOODS_SALES_PCT                         DECIMAL(14,10)
 
, RUNNING_SUM_HARDGOODS_SALES_PCT                   DECIMAL(14,10)
 
, SPECIALTY_52WK_SALES_AMT                          DECIMAL(18,2)
 
, SPECIALTY_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, STORE_SPECIALTY_SALES_PCT                         DECIMAL(14,10)
 
, RUNNING_SUM_SPECIALTY_SALES_PCT                   DECIMAL(14,10)
 
)

USING delta 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Dimension/sales_ranking_running_sum_pre';
 
--DISTRIBUTE ON (LOCATION_ID)
 
 
 
 
--*****  Creating table:  "SALES_RANKING_WK_PRE" , ***** Creating table: "SALES_RANKING_WK_PRE"
 
 
use raw;

CREATE TABLE SALES_RANKING_WK_PRE 

(

WEEK_DT      DATE                                 not null
 
, LOCATION_ID INT not null
 
, COMP_CURR_FLAG TINYINT
 
, SALES_CURR_FLAG TINYINT
 
, TOTAL_52WK_SALES_AMT                              DECIMAL(18,2)
 
, TOTAL_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, STORE_TOTAL_SALES_PCT                             DECIMAL(14,10)
 
, RUNNING_SUM_TOTAL_SALES_PCT                       DECIMAL(14,10)
 
, TOTAL_SALES_RANKING_CD                            STRING
 
, TOTAL_SALES_RANKING_LEVEL SMALLINT
 
, MERCH_52WK_SALES_AMT                              DECIMAL(18,2)
 
, MERCH_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, STORE_MERCH_SALES_PCT                             DECIMAL(14,10)
 
, RUNNING_SUM_MERCH_SALES_PCT                       DECIMAL(14,10)
 
, MERCH_SALES_RANKING_CD                            STRING
 
, MERCH_SALES_RANKING_LEVEL SMALLINT
 
, SERVICES_52WK_SALES_AMT                           DECIMAL(18,2)
 
, SERVICES_52WK_COMP_STORES_AMT                     DECIMAL(18,2)
 
, STORE_SERVICES_SALES_PCT                          DECIMAL(14,10)
 
, RUNNING_SUM_SERVICES_SALES_PCT                    DECIMAL(14,10)
 
, SERVICES_SALES_RANKING_CD                         STRING
 
, SERVICES_SALES_RANKING_LEVEL SMALLINT
 
, SALON_52WK_SALES_AMT                              DECIMAL(18,2)
 
, SALON_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, STORE_SALON_SALES_PCT                             DECIMAL(14,10)
 
, RUNNING_SUM_SALON_SALES_PCT                       DECIMAL(14,10)
 
, SALON_SALES_RANKING_CD                            STRING
 
, SALON_SALES_RANKING_LEVEL SMALLINT
 
, TRAINING_52WK_SALES_AMT                           DECIMAL(18,2)
 
, TRAINING_52WK_COMP_STORES_AMT                     DECIMAL(18,2)
 
, STORE_TRAINING_SALES_PCT                          DECIMAL(14,10)
 
, RUNNING_SUM_TRAINING_SALES_PCT                    DECIMAL(14,10)
 
, TRAINING_SALES_RANKING_CD                         STRING
 
, TRAINING_SALES_RANKING_LEVEL SMALLINT
 
, HOTEL_DDC_52WK_SALES_AMT                          DECIMAL(18,2)
 
, HOTEL_DDC_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, STORE_HOTEL_DDC_SALES_PCT                         DECIMAL(14,10)
 
, RUNNING_SUM_HOTEL_DDC_SALES_PCT                   DECIMAL(14,10)
 
, HOTEL_DDC_SALES_RANKING_CD                        STRING
 
, HOTEL_DDC_SALES_RANKING_LEVEL SMALLINT
 
, CONSUMABLES_52WK_SALES_AMT                        DECIMAL(18,2)
 
, CONSUMABLES_52WK_COMP_STORES_AMT                  DECIMAL(18,2)
 
, STORE_CONSUMABLES_SALES_PCT                       DECIMAL(14,10)
 
, RUNNING_SUM_CONSUMABLES_SALES_PCT                 DECIMAL(14,10)
 
, CONSUMABLES_SALES_RANKING_CD                      STRING
 
, CONSUMABLES_SALES_RANKING_LEVEL SMALLINT
 
, HARDGOODS_52WK_SALES_AMT                          DECIMAL(18,2)
 
, HARDGOODS_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, STORE_HARDGOODS_SALES_PCT                         DECIMAL(14,10)
 
, RUNNING_SUM_HARDGOODS_SALES_PCT                   DECIMAL(14,10)
 
, HARDGOODS_SALES_RANKING_CD                        STRING
 
, HARDGOODS_SALES_RANKING_LEVEL SMALLINT
 
, SPECIALTY_52WK_SALES_AMT                          DECIMAL(18,2)
 
, SPECIALTY_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, STORE_SPECIALTY_SALES_PCT                         DECIMAL(14,10)
 
, RUNNING_SUM_SPECIALTY_SALES_PCT                   DECIMAL(14,10)
 
, SPECIALTY_SALES_RANKING_CD                        STRING
 
, SPECIALTY_SALES_RANKING_LEVEL SMALLINT
 
)

USING delta 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Dimension/sales_ranking_wk_pre';
 
--DISTRIBUTE ON (LOCATION_ID)



use legacy;

CREATE TABLE SALES_RANKING_WK 

(

WEEK_DT      DATE                                 not null
 
, LOCATION_ID INT not null
 
, COMP_CURR_FLAG TINYINT
 
, SALES_CURR_FLAG TINYINT
 
, TOTAL_52WK_SALES_AMT                              DECIMAL(18,2)
 
, TOTAL_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, STORE_TOTAL_SALES_PCT                             DECIMAL(14,10)
 
, RUNNING_SUM_TOTAL_SALES_PCT                       DECIMAL(14,10)
 
, TOTAL_SALES_RANKING_CD                            STRING
 
, TOTAL_SALES_RANKING_LEVEL SMALLINT
 
, MERCH_52WK_SALES_AMT                              DECIMAL(18,2)
 
, MERCH_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, STORE_MERCH_SALES_PCT                             DECIMAL(14,10)
 
, RUNNING_SUM_MERCH_SALES_PCT                       DECIMAL(14,10)
 
, MERCH_SALES_RANKING_CD                            STRING
 
, MERCH_SALES_RANKING_LEVEL SMALLINT
 
, SERVICES_52WK_SALES_AMT                           DECIMAL(18,2)
 
, SERVICES_52WK_COMP_STORES_AMT                     DECIMAL(18,2)
 
, STORE_SERVICES_SALES_PCT                          DECIMAL(14,10)
 
, RUNNING_SUM_SERVICES_SALES_PCT                    DECIMAL(14,10)
 
, SERVICES_SALES_RANKING_CD                         STRING
 
, SERVICES_SALES_RANKING_LEVEL SMALLINT
 
, SALON_52WK_SALES_AMT                              DECIMAL(18,2)
 
, SALON_52WK_COMP_STORES_AMT                        DECIMAL(18,2)
 
, STORE_SALON_SALES_PCT                             DECIMAL(14,10)
 
, RUNNING_SUM_SALON_SALES_PCT                       DECIMAL(14,10)
 
, SALON_SALES_RANKING_CD                            STRING
 
, SALON_SALES_RANKING_LEVEL SMALLINT
 
, TRAINING_52WK_SALES_AMT                           DECIMAL(18,2)
 
, TRAINING_52WK_COMP_STORES_AMT                     DECIMAL(18,2)
 
, STORE_TRAINING_SALES_PCT                          DECIMAL(14,10)
 
, RUNNING_SUM_TRAINING_SALES_PCT                    DECIMAL(14,10)
 
, TRAINING_SALES_RANKING_CD                         STRING
 
, TRAINING_SALES_RANKING_LEVEL SMALLINT
 
, HOTEL_DDC_52WK_SALES_AMT                          DECIMAL(18,2)
 
, HOTEL_DDC_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, STORE_HOTEL_DDC_SALES_PCT                         DECIMAL(14,10)
 
, RUNNING_SUM_HOTEL_DDC_SALES_PCT                   DECIMAL(14,10)
 
, HOTEL_DDC_SALES_RANKING_CD                        STRING
 
, HOTEL_DDC_SALES_RANKING_LEVEL SMALLINT
 
, CONSUMABLES_52WK_SALES_AMT                        DECIMAL(18,2)
 
, CONSUMABLES_52WK_COMP_STORES_AMT                  DECIMAL(18,2)
 
, STORE_CONSUMABLES_SALES_PCT                       DECIMAL(14,10)
 
, RUNNING_SUM_CONSUMABLES_SALES_PCT                 DECIMAL(14,10)
 
, CONSUMABLES_SALES_RANKING_CD                      STRING
 
, CONSUMABLES_SALES_RANKING_LEVEL SMALLINT
 
, HARDGOODS_52WK_SALES_AMT                          DECIMAL(18,2)
 
, HARDGOODS_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, STORE_HARDGOODS_SALES_PCT                         DECIMAL(14,10)
 
, RUNNING_SUM_HARDGOODS_SALES_PCT                   DECIMAL(14,10)
 
, HARDGOODS_SALES_RANKING_CD                        STRING
 
, HARDGOODS_SALES_RANKING_LEVEL SMALLINT
 
, SPECIALTY_52WK_SALES_AMT                          DECIMAL(18,2)
 
, SPECIALTY_52WK_COMP_STORES_AMT                    DECIMAL(18,2)
 
, STORE_SPECIALTY_SALES_PCT                         DECIMAL(14,10)
 
, RUNNING_SUM_SPECIALTY_SALES_PCT                   DECIMAL(14,10)
 
, SPECIALTY_SALES_RANKING_CD                        STRING
 
, SPECIALTY_SALES_RANKING_LEVEL SMALLINT
 
, UPDATE_TSTMP                                      TIMESTAMP
 
, LOAD_TSTMP                                        TIMESTAMP
 
)

USING delta 

LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/sales_ranking_wk';
 
--DISTRIBUTE ON (LOCATION_ID)
