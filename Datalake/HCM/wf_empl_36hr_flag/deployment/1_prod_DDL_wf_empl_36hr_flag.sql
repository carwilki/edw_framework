

--*****  Creating table:  "EARNINGS_ID" , ***** Creating table: "EARNINGS_ID"
use legacy;
CREATE TABLE EARNINGS_ID
(
 EARN_ID                                     STRING                 not null
, PS_TAX_COMPANY_CD                           STRING 
, PS_WAGE_TYPE_GID SMALLINT
, PS_WAGE_TYPE_CD                             STRING 
, PS_COUNTRY_GROUP_CD                         STRING 
, PS_WAGE_TYPE_DESC                           STRING 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/hcm/earnings_id' ;
--DISTRIBUTE ON RANDOM
--*****  Creating table:  "EMPL_PT_36HR_FLAG" , ***** Creating table: "EMPL_PT_36HR_FLAG"

use legacy;
CREATE TABLE EMPL_PT_36HR_FLAG
(
 WEEK_DT                                     TIMESTAMP                            not null
, EMPLOYEE_ID INT not null
, LOCATION_ID INT not null
, EMPL_13WK_AVG_HRS_WORKED                    DECIMAL(9,2) 
, FLAG TINYINT
, LOAD_TSTMP                                  TIMESTAMP                         
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/hcm/empl_pt_36hr_flag' ;
--DISTRIBUTE ON (WEEK_DT)
--*****  Creating table:  "EMPL_36HR_FLAG" , ***** Creating table: "EMPL_36HR_FLAG"
use legacy;
CREATE TABLE EMPL_36HR_FLAG
(
 WEEK_DT                                     TIMESTAMP                            not null
, EMPLOYEE_ID INT not null
, LOCATION_ID INT not null
, EMPL_13WK_AVG_HRS_WORKED                    DECIMAL(9,2) 
, FLAG TINYINT
, LOAD_TSTMP                                  TIMESTAMP                         
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/hcm/empl_36hr_flag' ;
--DISTRIBUTE ON (EMPLOYEE_ID)
--*****  Creating table:  "EMPL_EMPL_LOC_WK" , ***** Creating table: "EMPL_EMPL_LOC_WK"

use empl_protected;
 CREATE TABLE legacy_EMPL_EMPL_LOC_WK 
(
 WEEK_DT                                     TIMESTAMP                            not null
, LOCATION_ID INT not null
, EMPLOYEE_ID INT not null
, EARN_ID                                     STRING                 not null
, STORE_DEPT_NBR                              STRING                 not null
, JOB_CODE INT
, FULLPT_FLAG                                 STRING 
, HOURS_WORKED                                DECIMAL(9,2) 
, EARNINGS_AMT                                DECIMAL(9,2) 
, EARNINGS_LOC_AMT                            DECIMAL(9,2) 
, PAY_FREQ_CD                                 STRING 
, CURRENCY_NBR SMALLINT
, LOAD_TSTMP                                  TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-empl-protected-nzlegacy-p1-gcs-gbl/hcm/empl_empl_loc_wk' ;
--DISTRIBUTE ON (EMPLOYEE_ID)
--ORGANIZE   ON (WEEK_DT)
--*****  Creating table:  "EMPLOYEE_PROFILE_DAY" , ***** Creating table: "EMPLOYEE_PROFILE_DAY"

use empl_protected;
 CREATE TABLE legacy_EMPLOYEE_PROFILE_DAY 
(
 DAY_DT                                      TIMESTAMP                            not null
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
LOCATION 'gs://petm-bdpl-prod-empl-protected-nzlegacy-p1-gcs-gbl/hcm/employee_profile_day' ;
--DISTRIBUTE ON (DAY_DT, EMPLOYEE_ID)