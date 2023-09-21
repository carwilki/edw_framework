







--*****  Creating table:  "GL_PROFIT_CENTER" , ***** Creating table: "GL_PROFIT_CENTER"


use legacy;
CREATE TABLE  GL_PROFIT_CENTER
(
 GL_PROFIT_CTR_CD                            STRING                not null

, GL_PROFIT_CTR_DESC                          STRING 

, LOCATION_ID INT

, VALID_FROM_DT                               TIMESTAMP 

, EXP_DT                                      TIMESTAMP 

, CURRENCY_ID                                 STRING 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_profit_center';

--DISTRIBUTE ON (GL_PROFIT_CTR_CD)







--*****  Creating table:  "GL_PLAN_FORECAST_MONTH" , ***** Creating table: "GL_PLAN_FORECAST_MONTH"


use legacy;
CREATE TABLE  GL_PLAN_FORECAST_MONTH
(
 FISCAL_MO INT not null

, GL_ACCT_NBR INT not null

, GL_CATEGORY_CD                              STRING                 not null

, GL_PROFIT_CTR_CD                            STRING                not null

, LOCATION_ID INT not null

, LOC_CURRENCY_ID                             STRING 

, GL_PLAN_AMT_LOC                             DECIMAL(15,2) 

, GL_PLAN_AMT_US                              DECIMAL(15,2) 

, GL_FORECAST_AMT_LOC                         DECIMAL(15,2) 

, GL_FORECAST_AMT_US                          DECIMAL(15,2) 

, GL_F1_ADJ_AMT_LOC                           DECIMAL(15,2) 

, GL_F1_ADJ_AMT_US                            DECIMAL(15,2) 

, UPDATE_DT                                   TIMESTAMP 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_plan_forecast_month';

--DISTRIBUTE ON (GL_ACCT_NBR, FISCAL_MO)







--*****  Creating table:  "GL_EXCHANGE_RATE" , ***** Creating table: "GL_EXCHANGE_RATE"


use legacy;
CREATE TABLE  GL_EXCHANGE_RATE
(
 COMPANY INT not null

, PERIOD INT not null

, FORECAST_EXCH_RATE                          DECIMAL(9,6) 

, F1_ADJ_EXCH_RATE                            DECIMAL(9,6) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_exchange_rate';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GL_PLAN_FORECAST_DAY" , ***** Creating table: "GL_PLAN_FORECAST_DAY"


use legacy;
CREATE TABLE  GL_PLAN_FORECAST_DAY
(
 DAY_DT                                      TIMESTAMP                            not null

, GL_ACCT_NBR INT not null

, GL_CATEGORY_CD                              STRING                 not null

, GL_PROFIT_CTR_CD                            STRING                not null

, LOCATION_ID INT not null

, LOC_CURRENCY_ID                             STRING 

, GL_PLAN_AMT_LOC                             DECIMAL(15,2) 

, GL_PLAN_AMT_US                              DECIMAL(15,2) 

, GL_FORECAST_AMT_LOC                         DECIMAL(15,2) 

, GL_FORECAST_AMT_US                          DECIMAL(15,2) 

, GL_F1_ADJ_AMT_LOC                           DECIMAL(15,2) 

, GL_F1_ADJ_AMT_US                            DECIMAL(15,2) 

, UPDATE_DT                                   TIMESTAMP 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_plan_forecast_day';

--DISTRIBUTE ON (GL_ACCT_NBR, DAY_DT)







--*****  Creating table:  "GL_PLAN_MONTH_PRE" , ***** Creating table: "GL_PLAN_MONTH_PRE"


use raw;
CREATE TABLE  GL_PLAN_MONTH_PRE
(
 FISCAL_MO INT not null

, GL_ACCT_NBR INT not null

, GL_CATEGORY_CD                              STRING                 not null

, PROFIT_CTR                                  STRING                not null

, LOCATION_ID INT

, CURR_CD                                     STRING 

, GL_AMT_US                                   DECIMAL(15,2) 

, GL_AMT_LOC                                  DECIMAL(15,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_plan_month_pre';

--DISTRIBUTE ON (GL_ACCT_NBR, FISCAL_MO)







--*****  Creating table:  "GL_FORECAST_MONTH_PRE" , ***** Creating table: "GL_FORECAST_MONTH_PRE"


use raw;
CREATE TABLE  GL_FORECAST_MONTH_PRE
(
 FISCAL_MO INT not null

, GL_ACCT_NBR INT not null

, GL_CATEGORY_CD                              STRING                 not null

, PROFIT_CTR                                  STRING                not null

, LOCATION_ID INT

, CURR_CD                                     STRING 

, GL_FORECAST_AMT_US                          DECIMAL(15,2) 

, GL_FORECAST_AMT_LOC                         DECIMAL(15,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_forecast_month_pre';

--DISTRIBUTE ON (GL_ACCT_NBR, FISCAL_MO)







--*****  Creating table:  "GL_PLAN_PRE" , ***** Creating table: "GL_PLAN_PRE"


use raw;
CREATE TABLE  GL_PLAN_PRE
(
 FORECAST_NAME                               STRING                not null

, FISCAL_MO INT not null

, GL_ACCT_NBR INT not null

, GL_CAT_CD                                   STRING                 not null

, STORE_NBR SMALLINT not null

, CURRENCY_ID                                 STRING                 not null

, COMPANY_NBR SMALLINT not null

, GL_AMT                                      DECIMAL(15,4) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_plan_pre';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GL_FORECAST_PRE" , ***** Creating table: "GL_FORECAST_PRE"


use raw;
CREATE TABLE  GL_FORECAST_PRE
(
 FISCAL_MO INT not null

, GL_ACCT_NBR INT not null

, GL_CAT_CD                                   STRING                 not null

, STORE_NBR SMALLINT not null

, CURRENCY_ID                                 STRING                 not null

, COMPANY_NBR SMALLINT not null

, GL_FORECAST_AMT                             DECIMAL(15,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_forecast_pre';

--DISTRIBUTE ON RANDOM


