
--*****  Creating table:  "PS2_ACCRUED_LABOR_WK_PRE" , ***** Creating table: "PS2_ACCRUED_LABOR_WK_PRE"
use empl_protected;
 CREATE TABLE raw_PS2_ACCRUED_LABOR_WK_PRE 
(
 WEEK_DT       TIMESTAMP                             not null
, LOCATION_ID INT not null
, EMPLOYEE_ID INT not null
, EARN_ID       STRING                  not null
, STORE_DEPT_NBR                                    STRING                  not null
, JOB_CODE INT not null
, FULLPT_FLAG                                       STRING                  not null
, PAY_FREQ_CD                                       STRING                  not null
, HOURS_WORKED                                      DECIMAL(11,2) 
, EARNINGS_AMT                                      DECIMAL(11,2) 
, EARNINGS_LOC_AMT                                  DECIMAL(11,2) 
, CURRENCY_NBR SMALLINT
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-empl-protected-raw-p1-gcs-gbl/hcm/ps2_accrued_labor_wk_pre';
--DISTRIBUTE ON (EMPLOYEE_ID)

--*****  Creating table:  "PS2_EMPL_EMPL_LOC_WK_PRE" , ***** Creating table: "PS2_EMPL_EMPL_LOC_WK_PRE"
use empl_protected;
 CREATE TABLE  raw_PS2_EMPL_EMPL_LOC_WK_PRE 
(
 WEEK_DT       TIMESTAMP                             not null
, LOCATION_ID INT not null
, EMPLOYEE_ID INT not null
, EARN_ID       STRING                  not null
, STORE_DEPT_NBR                                    STRING                  not null
, JOB_CODE INT
, FULLPT_FLAG                                       STRING 
, HOURS_WORKED                                      DECIMAL(9,2) 
, EARNINGS_AMT                                      DECIMAL(9,2) 
, EARNINGS_LOC_AMT                                  DECIMAL(9,2) 
, PAY_FREQ_CD                                       STRING 
, CURRENCY_NBR SMALLINT
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-empl-protected-raw-p1-gcs-gbl/hcm/ps2_empl_empl_loc_wk_pre';
--DISTRIBUTE ON (LOCATION_ID)


--*****  Creating table:  "PS2_ACCRUED_LABOR_WK" , ***** Creating table: "PS2_ACCRUED_LABOR_WK"
use legacy;
 CREATE TABLE PS2_ACCRUED_LABOR_WK 
(
 WEEK_DT       TIMESTAMP                             not null
, LOCATION_ID INT not null
, STORE_DEPT_NBR                                    STRING                  not null
, EARN_ID       STRING                  not null
, JOB_CODE INT not null
, FULLPT_FLAG                                       STRING                  not null
, PAY_FREQ_CD                                       STRING                  not null
, HOURS_WORKED                                      DECIMAL(9,2) 
, EARNINGS_AMT                                      DECIMAL(9,2) 
, EARNINGS_LOC_AMT                                  DECIMAL(9,2) 
, CURRENCY_NBR SMALLINT
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/hcm/ps2_accrued_labor_wk';
--DISTRIBUTE ON (LOCATION_ID)
