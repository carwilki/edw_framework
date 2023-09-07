




--*****  Creating table:  "GL_DOC_TYPE" , ***** Creating table: "GL_DOC_TYPE"


use legacy;
CREATE TABLE  IF NOT EXISTS GL_DOC_TYPE
(
 GL_DOC_TYPE_CD                              STRING                 not null

, GL_DOC_TYPE_DESC                            STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_doc_type' 
;

--DISTRIBUTE ON (GL_DOC_TYPE_CD)







--*****  Creating table:  "GL_CATEGORY" , ***** Creating table: "GL_CATEGORY"


use legacy;
CREATE TABLE  IF NOT EXISTS GL_CATEGORY
(
 GL_CATEGORY_CD                              STRING                 not null

, GL_CATEGORY_DESC                            STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_category' 
;

--DISTRIBUTE ON (GL_CATEGORY_CD)







--*****  Creating table:  "BAL_FILE_INTRFACE" , ***** Creating table: "BAL_FILE_INTRFACE"


use legacy;
CREATE TABLE  IF NOT EXISTS BAL_FILE_INTRFACE
(
 DAY_DT                                DATE                              not null

, BAL_FILE_ID                            INT                       not null

, BAL_SEQ_NBR                            INT                       not null

, BAL_FAIL_FLAG                          TINYINT                       not null

, TRAIL_REC_CNT                          BIGINT 

, INTRFACE_REC_CNT                       BIGINT 

, BAL_FILENAME_TX                        STRING 

, LOAD_DT                                TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/bal_file_intrface' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GL_PROFIT_CENTER" , ***** Creating table: "GL_PROFIT_CENTER"


use legacy;
CREATE TABLE  IF NOT EXISTS GL_PROFIT_CENTER
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







--*****  Creating table:  "SAP_CATEGORY" , ***** Creating table: "SAP_CATEGORY"


use legacy;
CREATE TABLE  IF NOT EXISTS SAP_CATEGORY
(
 SAP_CATEGORY_ID INT not null

, SAP_CATEGORY_DESC                                 STRING 

, SAP_CLASS_ID INT

, GL_CATEGORY_CD                                    STRING 

, SAP_PRICING_CATEGORY_ID INT

, UPD_TSTMP     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/sap_category' 
;

--DISTRIBUTE ON (SAP_CATEGORY_ID)







--*****  Creating table:  "GL_PLAN_FORECAST_MONTH" , ***** Creating table: "GL_PLAN_FORECAST_MONTH"


use legacy;
CREATE TABLE  IF NOT EXISTS GL_PLAN_FORECAST_MONTH
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







--*****  Creating table:  "GL_ACTUAL_DAY_DETAIL" , ***** Creating table: "GL_ACTUAL_DAY_DETAIL"


use legacy;
CREATE TABLE  IF NOT EXISTS GL_ACTUAL_DAY_DETAIL
(
 DAY_DT                                      TIMESTAMP                            not null

, COMPANY_CD SMALLINT not null

, GL_ACCT_NBR INT not null

, GL_CATEGORY_CD                              STRING                 not null

, GL_PROFIT_CTR_CD                            STRING                not null

, GL_DOC_TYPE_CD                              STRING                 not null

, GL_ACCT_DOC_NBR BIGINT not null

, GL_ACCT_LINE_NBR SMALLINT not null

, VENDOR_ID BIGINT not null

, FISCAL_MO INT not null

, LOCATION_ID INT

, LOC_CURRENCY_ID                             STRING                 not null

, GL_DOC_DT                                   TIMESTAMP 

, GL_PURCH_DOC_NBR                            STRING 

, GL_DOC_AMT                                  DECIMAL(15,2) 

, GL_LOC_AMT                                  DECIMAL(15,2) 

, GL_GROUP_AMT                                DECIMAL(15,2) 

, BAL_SHEET_FLAG TINYINT

, P_L_FLAG TINYINT

, CONTROL_AREA                                STRING 

, EXCH_RATE_PCT                               DECIMAL(9,4) 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_actual_day_detail';

--DISTRIBUTE ON (GL_ACCT_NBR, DAY_DT)







--*****  Creating table:  "GL_ACCT_GRP" , ***** Creating table: "GL_ACCT_GRP"


use legacy;
CREATE TABLE  IF NOT EXISTS GL_ACCT_GRP
(
 GL_ACCT_GRP_CD                              STRING                 not null

, GL_ACCT_GRP_DESC                            STRING 

, GL_BAL_SHEET_IND                            STRING 

, GL_PL_IND                                   STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_acct_grp' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GL_ACTUAL_DAY" , ***** Creating table: "GL_ACTUAL_DAY"


use legacy;
CREATE TABLE  IF NOT EXISTS GL_ACTUAL_DAY
(
 DAY_DT                                      TIMESTAMP                            not null

, COMPANY_CD SMALLINT not null

, GL_ACCT_NBR INT not null

, GL_CATEGORY_CD                              STRING                 not null

, GL_PROFIT_CTR_CD                            STRING                not null

, GL_DOC_TYPE_CD                              STRING                 not null

, VENDOR_ID BIGINT not null

, LOC_CURRENCY_ID                             STRING                 not null

, FISCAL_MO INT not null

, LOCATION_ID INT

, GL_DOC_AMT                                  DECIMAL(15,2) 

, GL_LOC_AMT                                  DECIMAL(15,2) 

, GL_GROUP_AMT                                DECIMAL(15,2) 

, EXCH_RATE_PCT                               DECIMAL(9,4) 

, UPDATE_DT                                   TIMESTAMP 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_actual_day' 
;

--DISTRIBUTE ON (GL_ACCT_NBR, DAY_DT)







--*****  Creating table:  "GL_ACTUAL_MONTH" , ***** Creating table: "GL_ACTUAL_MONTH"


use legacy;
CREATE TABLE  IF NOT EXISTS GL_ACTUAL_MONTH
(
 FISCAL_MO INT not null

, COMPANY_CD SMALLINT not null

, GL_ACCT_NBR INT not null

, GL_CATEGORY_CD                              STRING                 not null

, GL_PROFIT_CTR_CD                            STRING                not null

, GL_DOC_TYPE_CD                              STRING                 not null

, VENDOR_ID BIGINT not null

, LOC_CURRENCY_ID                             STRING                 not null

, LOCATION_ID INT not null

, GL_DOC_AMT                                  DECIMAL(15,2) 

, GL_LOC_AMT                                  DECIMAL(15,2) 

, GL_GROUP_AMT                                DECIMAL(15,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_actual_month' 
;

--DISTRIBUTE ON (GL_ACCT_NBR, FISCAL_MO)







--*****  Creating table:  "GL_ACCOUNT" , ***** Creating table: "GL_ACCOUNT"


use legacy;
CREATE TABLE  IF NOT EXISTS GL_ACCOUNT
(
 GL_ACCT_NBR INT not null

, GL_ACCT_GRP_CD                              STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/gl_account' 
;

--DISTRIBUTE ON (GL_ACCT_NBR)







--*****  Creating table:  "GL_CSKS_COST_CTR_PRE" , ***** Creating table: "GL_CSKS_COST_CTR_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS GL_CSKS_COST_CTR_PRE
(
 COST_CENTER_ID                              STRING                not null

, VALID_FROM_DT                               TIMESTAMP                            not null

, EXP_DT                                      TIMESTAMP                            not null

, CO_AREA_ID                                  STRING 

, COMPANY_CODE SMALLINT

, CCTR_CATEGORY_CD                            STRING 

, PERSON_RESP                                 STRING 

, CURRENCY_CD                                 STRING 

, COSTING_SHEET                               STRING 

, TAX_JURISDICTION_CD                         STRING 

, PROFIT_CENTER_ID                            STRING 

, STORE_NBR                                   STRING 

, CREATED_ON_DT                               TIMESTAMP 

, CREATED_BY                                  STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_csks_cost_ctr_pre' 
;

--DISTRIBUTE ON (COST_CENTER_ID)







--*****  Creating table:  "GL_T023_PRE" , ***** Creating table: "GL_T023_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS GL_T023_PRE
(
 SAP_CATEGORY_ID INT not null

, GL_DIVISION_ID                              STRING                 not null

, GL_CATEGORY_CD                              STRING                 not null

, ACTIVE_IND                                  STRING 

, MERCH_CAT_DESC                              STRING 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_t023_pre' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GL_SKAT_PRE" , ***** Creating table: "GL_SKAT_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS GL_SKAT_PRE
(
 CHART_OF_ACCTS_CD                           STRING                 not null

, GL_ACCT_NBR INT not null

, DESC_FLD                                    STRING 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_skat_pre' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GL_TSAB_GL_CAT_PRE" , ***** Creating table: "GL_TSAB_GL_CAT_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS GL_TSAB_GL_CAT_PRE
(
 GL_CATEGORY_CD                              STRING                 not null

, DESCRIPTION                                 STRING 

, LOAD_DT                                     STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_tsab_gl_cat_pre' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GL_GLPCT_PLAN_PRE" , ***** Creating table: "GL_GLPCT_PLAN_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS GL_GLPCT_PLAN_PRE
(
 SEQ_NBR BIGINT not null

, FISCAL_YR SMALLINT

, CUR     STRING 

, DEBIT_CR_IND                                STRING 

, COMPANY_CD SMALLINT

, PROFIT_CTR                                  STRING 

, GL_CATEGORY_CD                              STRING 

, GL_ACCT_NBR INT

, PER1_COMP_AMT                               DECIMAL(15,2) 

, PER2_COMP_AMT                               DECIMAL(15,2) 

, PER3_COMP_AMT                               DECIMAL(15,2) 

, PER4_COMP_AMT                               DECIMAL(15,2) 

, PER5_COMP_AMT                               DECIMAL(15,2) 

, PER6_COMP_AMT                               DECIMAL(15,2) 

, PER7_COMP_AMT                               DECIMAL(15,2) 

, PER8_COMP_AMT                               DECIMAL(15,2) 

, PER9_COMP_AMT                               DECIMAL(15,2) 

, PER10_COMP_AMT                              DECIMAL(15,2) 

, PER11_COMP_AMT                              DECIMAL(15,2) 

, PER12_COMP_AMT                              DECIMAL(15,2) 

, LOC1_COMP_AMT                               DECIMAL(15,2) 

, LOC2_COMP_AMT                               DECIMAL(15,2) 

, LOC3_COMP_AMT                               DECIMAL(15,2) 

, LOC4_COMP_AMT                               DECIMAL(15,2) 

, LOC5_COMP_AMT                               DECIMAL(15,2) 

, LOC6_COMP_AMT                               DECIMAL(15,2) 

, LOC7_COMP_AMT                               DECIMAL(15,2) 

, LOC8_COMP_AMT                               DECIMAL(15,2) 

, LOC9_COMP_AMT                               DECIMAL(15,2) 

, LOC10_COMP_AMT                              DECIMAL(15,2) 

, LOC11_COMP_AMT                              DECIMAL(15,2) 

, LOC12_COMP_AMT                              DECIMAL(15,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_glpct_plan_pre' 
;

--DISTRIBUTE ON (SEQ_NBR)







--*****  Creating table:  "GL_BSEG_PRE" , ***** Creating table: "GL_BSEG_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS GL_BSEG_PRE
(
 FISCAL_YR SMALLINT not null

, FISCAL_PERIOD TINYINT not null

, DOC_TYPE_CD                                 STRING                 not null

, GL_ACCT_NBR INT not null

, ACCT_DOC_NBR BIGINT not null

, LINE_NBR SMALLINT not null

, PROFIT_CTR                                  STRING                not null

, COMPANY_CD SMALLINT

, DOC_DT                                      TIMESTAMP 

, POSTING_DT                                  TIMESTAMP 

, CURR_KEY                                    STRING 

, XRATE                                       DECIMAL(5,4) 

, XRATE_L2                                    DECIMAL(5,4) 

, LOC_CURR                                    STRING 

, DB_CR_IND                                   STRING 

, LOC_CURR_AMT                                DECIMAL(15,2) 

, DOC_CURR_AMT                                DECIMAL(15,2) 

, LOC2_CURR_AMT                               DECIMAL(15,2) 

, TRANS_TYPE_CD                               STRING 

, CONTROL_AREA                                STRING 

, VENDOR_ID                                   STRING 

, BAL_SHEET_IND                               STRING 

, P_L_IND                                     STRING 

, PURCH_DOC_NBR                               STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_bseg_pre' 
;

--DISTRIBUTE ON (POSTING_DT, COMPANY_CD, GL_ACCT_NBR, PROFIT_CTR)







--*****  Creating table:  "GL_GLPCT_PLAN_MONTH_PRE" , ***** Creating table: "GL_GLPCT_PLAN_MONTH_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS GL_GLPCT_PLAN_MONTH_PRE
(
 PROFIT_CTR                                  STRING                not null

, FISCAL_MO INT not null

, GL_CATEGORY_CD                              STRING                 not null

, GL_ACCT_NBR INT not null

, LOCATION_ID INT

, CURR_CD                                     STRING 

, LOC_PLAN_AMT                                DECIMAL(15,2) 

, US_PLAN_AMT                                 DECIMAL(15,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_glpct_plan_month_pre' 
;

--DISTRIBUTE ON (GL_ACCT_NBR, FISCAL_MO)







--*****  Creating table:  "GL_T077S_PRE" , ***** Creating table: "GL_T077S_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS GL_T077S_PRE
(
 CHART_OF_ACCTS_CD                           STRING                 not null

, ACCT_GRP_CD                                 STRING                 not null

, FROM_GL_NBR INT

, TO_GL_NBR INT

, ACCT_GRP_TYPE_CD                            STRING 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/gl_t077s_pre' 
;

--DISTRIBUTE ON RANDOM


