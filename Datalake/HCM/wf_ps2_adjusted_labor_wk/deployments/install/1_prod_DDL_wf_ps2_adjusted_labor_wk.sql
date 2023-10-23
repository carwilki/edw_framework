
--*****  Creating table:  "PS2_ADJUSTED_LABOR_WK" , ***** Creating table: "PS2_ADJUSTED_LABOR_WK"
use legacy;
 CREATE TABLE PS2_ADJUSTED_LABOR_WK 
(
 WEEK_DT       TIMESTAMP                             not null
, LOCATION_ID INT not null
, EARN_ID       STRING                  not null
, STORE_DEPT_NBR                                    STRING                  not null
, JOB_CODE INT not null
, ACT_HOURS_WORKED                                  DECIMAL(9,2) 
, ACT_EARNINGS_LOC_AMT                              DECIMAL(9,2) 
, EARNED_HOURS                                      DECIMAL(9,2) 
, EARNED_LOC_AMT                                    DECIMAL(15,2) 
, FORECAST_HRS                                      DECIMAL(9,2) 
, FORECAST_LOC_AMT                                  DECIMAL(15,4) 
, EXCHANGE_RATE                                     DECIMAL(9,6) 
, UPDATE_DT     TIMESTAMP 
, LOAD_DT       TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/hcm/ps2_adjusted_labor_wk' ;
--DISTRIBUTE ON (LOCATION_ID)


--*****  Creating table:  "PS2_ADJUSTED_LABOR_WK_PRE" , ***** Creating table: "PS2_ADJUSTED_LABOR_WK_PRE"
use raw;
 CREATE TABLE PS2_ADJUSTED_LABOR_WK_PRE 
(
 WEEK_DT       TIMESTAMP                             not null
, STORE_NBR INT not null
, EARN_ID       STRING                  not null
, STORE_DEPT_NBR SMALLINT not null
, JOB_CODE INT not null
, ACT_HOURS_WORKED                                  DECIMAL(9,2) 
, ACT_EARNINGS_LOC_AMT                              DECIMAL(9,4) 
, EARNED_HOURS                                      DECIMAL(9,2) 
, EARNED_LOC_AMT                                    DECIMAL(15,4) 
, FORECAST_HRS                                      DECIMAL(9,2) 
, FORECAST_LOC_AMT                                  DECIMAL(9,4) 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/hcm/ps2_adjusted_labor_wk_pre' ;
--DISTRIBUTE ON (STORE_NBR)
