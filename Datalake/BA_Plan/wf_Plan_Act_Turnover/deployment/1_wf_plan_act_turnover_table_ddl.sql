

--*****  Creating table:  "PLAN_ACT_TO_DATE_PRE" , ***** Creating table: "PLAN_ACT_TO_DATE_PRE"


use raw;
 CREATE TABLE PLAN_ACT_TO_DATE_PRE 
(
 WEEK_DT       TIMESTAMP                             not null

, FISCAL_WK INT

, FISCAL_WK_NBR TINYINT

, FISCAL_YR SMALLINT

, MAX_FISCAL_WK_NBR TINYINT

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Dimension/plan_act_to_date_pre';

--DISTRIBUTE ON (WEEK_DT)







--*****  Creating table:  "PLAN_ACT_TURNOVER" , ***** Creating table: "PLAN_ACT_TURNOVER"


use legacy;
 CREATE TABLE PLAN_ACT_TURNOVER 
(
 WEEK_DT       TIMESTAMP                             not null

, LOCATION_ID INT not null

, FISCAL_YR SMALLINT

, FISCAL_WK_NBR TINYINT

, STORE_NBR INT

, STORE_NAME                                        STRING 

, OPEN_DT       TIMESTAMP 

, TO_PLAN_TERM_CNT INT

, TO_AVG_EMPL_CNT                                   DECIMAL(18,2) 

, TO_DEANNL_PLAN                                    DECIMAL(9,6) 

, TO_EMPL_90DAY_CNT INT

, TO_EMPL_365DAY_CNT INT

, WC_PLAN_CLAIMS INT

, WC_DEANNL_PLAN                                    DECIMAL(9,6) 

, PET_SAFETY_PLAN_INC_CNT INT

, PET_SAFETY_DEANNL_PLAN_CNT                        DECIMAL(9,6) 

, LOAD_TSTMP                                        TIMESTAMP                             

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/plan_act_turnover';

--DISTRIBUTE ON (LOCATION_ID)
