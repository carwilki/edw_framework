
--*****  Creating table:  "PS2_ACCRUED_LABOR_WK_PRE" , ***** Creating table: "PS2_ACCRUED_LABOR_WK_PRE"
use raw;
 CREATE TABLE PS2_ACCRUED_LABOR_WK_PRE 
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
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/hcm/ps2_accrued_labor_wk_pre';
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

--*****  Creating table:  "EMPL_EARNINGS_WK" , ***** Creating table: "EMPL_EARNINGS_WK"
use empl_protected;
 CREATE TABLE legacy_EMPL_EARNINGS_WK 
(
 WEEK_DT                                     TIMESTAMP                            not null
, EMPLOYEE_ID INT not null
, WORK_COST_CENTER_CD                         STRING                not null
, EARN_ID                                     STRING                 not null
, HOME_COST_CENTER_CD                         STRING                not null
, PS_WORK_DEPT_CD                             STRING                not null
, PS_WAGE_TYPE_GID SMALLINT
, HOURS_WORKED                                DECIMAL(9,2) 
, EARNINGS_LOC_AMT                            DECIMAL(9,2) 
, CURRENCY_ID                                 STRING 
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-empl-protected-nzlegacy-p1-gcs-gbl/hcm/empl_earnings_wk';
--DISTRIBUTE ON (EMPLOYEE_ID)

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

--*****  Creating table:  "EMPLOYEE_PROFILE_WK" , ***** Creating table: "EMPLOYEE_PROFILE_WK"
use empl_protected;
 CREATE TABLE legacy_EMPLOYEE_PROFILE_WK 
(
 WEEK_DT                                     TIMESTAMP                            not null
, EMPLOYEE_ID INT not null
, EMPL_FIRST_NAME                             STRING 
, EMPL_MIDDLE_NAME                            STRING 
, EMPL_LAST_NAME                              STRING 
, EMPL_BIRTH_DT                               TIMESTAMP 
, GENDER_CD                                   STRING 
, PS_MARITAL_STATUS_CD                        STRING 
, ETHNIC_GROUP_ID                             STRING 
, EMPL_ADDR_1                                 STRING 
, EMPL_ADDR_2                                 STRING 
, EMPL_CITY                                   STRING 
, EMPL_STATE                                  STRING 
, EMPL_PROVINCE                               STRING 
, EMPL_ZIPCODE                                STRING 
, COUNTRY_CD                                  STRING 
, EMPL_HOME_PHONE                             STRING 
, EMPL_EMAIL_ADDR                             STRING 
, EMPL_LOGIN_ID                               STRING 
, BADGE_NBR                                   STRING 
, EMPL_STATUS_CD                              STRING 
, STATUS_CHG_DT                               TIMESTAMP 
, FULLPT_FLAG                                 STRING 
, FULLPT_CHG_DT                               TIMESTAMP 
, EMPL_TYPE_CD                                STRING 
, PS_REG_TEMP_CD                              STRING 
, EMPL_CATEGORY_CD                            STRING 
, EMPL_GROUP_CD                               STRING 
, EMPL_SUBGROUP_CD                            STRING 
, EMPL_HIRE_DT                                TIMESTAMP 
, EMPL_REHIRE_DT                              TIMESTAMP 
, EMPL_TERM_DT                                TIMESTAMP 
, TERM_REASON_CD                              STRING 
, EMPL_SENORITY_DT                            TIMESTAMP 
, PS_ACTION_DT                                TIMESTAMP 
, PS_ACTION_CD                                STRING 
, PS_ACTION_REASON_CD                         STRING 
, LOCATION_ID INT
, LOCATION_CHG_DT                             TIMESTAMP 
, STORE_NBR INT
, STORE_DEPT_NBR                              STRING 
, COMPANY_ID INT
, PS_PERSONNEL_AREA_ID                        STRING 
, PS_PERSONNEL_SUBAREA_ID                     STRING 
, PS_DEPT_CD                                  STRING 
, PS_DEPT_CHG_DT                              TIMESTAMP 
, PS_POSITION_ID INT
, POSITION_CHG_DT                             TIMESTAMP 
, PS_SUPERVISOR_ID INT
, JOB_CODE INT
, JOB_CODE_CHG_DT                             TIMESTAMP 
, EMPL_JOB_ENTRY_DT                           TIMESTAMP 
, PS_GRADE_ID SMALLINT
, EMPL_STD_BONUS_PCT                          DECIMAL(5,2) 
, EMPL_OVR_BONUS_PCT                          DECIMAL(5,2) 
, EMPL_RATING                                 DECIMAL(5,2) 
, PAY_RATE_CHG_DT                             TIMESTAMP 
, PS_PAYROLL_AREA_CD                          STRING 
, PS_TAX_COMPANY_CD                           STRING 
, PS_COMP_FREQ_CD                             STRING 
, COMP_RATE_AMT                               DECIMAL(15,2) 
, ANNUAL_RATE_LOC_AMT                         DECIMAL(15,2) 
, HOURLY_RATE_LOC_AMT                         DECIMAL(12,2) 
, CURRENCY_ID                                 STRING 
, EXCH_RATE_PCT                               DECIMAL(9,6) 
, LOAD_TSTMP                                  TIMESTAMP                          
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-empl-protected-nzlegacy-p1-gcs-gbl/hcm/employee_profile_wk';
--DISTRIBUTE ON (WEEK_DT, EMPLOYEE_ID)

--*****  Creating table:  "PS_PAYROLL_CALENDAR" , ***** Creating table: "PS_PAYROLL_CALENDAR"
use legacy;
CREATE TABLE  PS_PAYROLL_CALENDAR
(
 CHECK_DT      TIMESTAMP                             not null
, PS_TAX_COMPANY_CD                                 STRING                  not null
, PAY_PERIOD_PARAMETER                              STRING                  not null
, PERIOD_START_DT                                   TIMESTAMP 
, PERIOD_END_DT                                     TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/hcm/ps_payroll_calendar';
--DISTRIBUTE ON RANDOM

--*****  Creating table:  "PS_DEPARTMENT" , ***** Creating table: "PS_DEPARTMENT"
use legacy;
CREATE TABLE  PS_DEPARTMENT
(
 PS_DEPT_CD                                        STRING                 not null
, PS_DEPT_DESC                                      STRING 
, PS_PERSONNEL_AREA_ID                              STRING 
, PS_PERSONNEL_SUBAREA_ID                           STRING 
, LOCATION_ID INT
, STORE_NBR INT
, STORE_DEPT_NBR                                    STRING 
, STORE_NAME                                        STRING 
, COST_CENTER_CD                                    STRING 
, UPDATE_TSTMP                                      TIMESTAMP 
, LOAD_TSTMP                                        TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/hcm/ps_department';
--DISTRIBUTE ON (PS_DEPT_CD)

--*****  Creating table:  "PS_PAYROLL_AREA" , ***** Creating table: "PS_PAYROLL_AREA"
use legacy;
CREATE TABLE PS_PAYROLL_AREA
(
 PS_PAYROLL_AREA_CD                                STRING                  not null
, PS_PAYROLL_AREA_DESC                              STRING 
, PAY_PERIOD_PARAMETER                              STRING 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl//hcm/ps_payroll_area';
--DISTRIBUTE ON RANDOM
