--*****  Creating table:  "LP_TENDER_VAR_DAY" , ***** Creating table: "LP_TENDER_VAR_DAY"


use prod_legacy;
 CREATE TABLE LP_TENDER_VAR_DAY 
(  DAY_DT        TIMESTAMP                            not null

, LOCATION_ID INT not null

, TENDER_TYPE_ID TINYINT not null

, CASHIER_NBR INT not null

, TENDER_VARIANCE_AMT                               DECIMAL(10,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Financials/lp_tender_var_day';

--DISTRIBUTE ON (LOCATION_ID)


--*****  Creating table:  "LP_TENDER_DAY" , ***** Creating table: "LP_TENDER_DAY"


use prod_legacy;
 CREATE TABLE LP_TENDER_DAY 
(  DAY_DT        TIMESTAMP                            not null

, LOCATION_ID INT not null

, TENDER_TYPE_ID TINYINT not null

, SYS_TENDER_AMT                                    DECIMAL(10,2) 

, ASSOC_TENDER_AMT                                  DECIMAL(10,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Financials/lp_tender_day';

--DISTRIBUTE ON (LOCATION_ID)


--*****  Creating table:  "LP_EMPL_UNKNOWN" , ***** Creating table: "LP_EMPL_UNKNOWN"


use prod_legacy;
 CREATE TABLE LP_EMPL_UNKNOWN 
(  EMPLOYEE_ID INT not null

, LOCATION_ID INT

, EMPL_FIRST_NAME                                   STRING 

, EMPL_LAST_NAME                                    STRING 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Financials/lp_empl_unknown';

--DISTRIBUTE ON (EMPLOYEE_ID)


