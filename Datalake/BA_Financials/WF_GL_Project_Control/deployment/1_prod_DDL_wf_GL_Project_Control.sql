--*****  Creating table:  GL_PROJECT_DETAIL_DIFF_PRE , ***** Creating table: GL_PROJECT_DETAIL_DIFF_PRE
 
 
use raw;
CREATE TABLE GL_PROJECT_DETAIL_DIFF_PRE 
(
OBJECT_CD                                   STRING                not null
 
, PLAN_AMT                                    DECIMAL(15,2)
 
, BUDGET_AMT                                  DECIMAL(15,2)
 
, ACTUAL_AMT                                  DECIMAL(15,2)
 
, COMMITMENT_AMT                              DECIMAL(15,2)
 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Financials/gl_project_detail_diff_pre';
 
--DISTRIBUTE ON (OBJECT_CD)
 
 
 
 
--*****  Creating table:  GL_PROJECT_DETAIL_PREV_PRE , ***** Creating table: GL_PROJECT_DETAIL_PREV_PRE
 
 
use raw;
CREATE TABLE GL_PROJECT_DETAIL_PREV_PRE 
(
OBJECT_CD                                   STRING                not null
 
, PLAN_AMT                                    DECIMAL(15,2)
 
, BUDGET_AMT                                  DECIMAL(15,2)
 
, ACTUAL_AMT                                  DECIMAL(15,2)
 
, COMMITMENT_AMT                              DECIMAL(15,2)
 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Financials/gl_project_detail_prev_pre';
 
--DISTRIBUTE ON (OBJECT_CD)
 
 
 
 
--*****  Creating table:  GL_BPGE_PRE , ***** Creating table: GL_BPGE_PRE
 
 
use raw;
CREATE TABLE GL_BPGE_PRE 
(
OBJNR                                       STRING
 
, WRTTP                                       STRING
 
, VORGA                                       STRING
 
, TWAER                                       STRING
 
, WTGES                                       DECIMAL(15,2)
 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Financials/gl_bpge_pre';
 
--DISTRIBUTE ON (OBJNR)
 
 
 
 
--*****  Creating table:  GL_COSP_PRE , ***** Creating table: GL_COSP_PRE
 
 
use raw;
CREATE TABLE GL_COSP_PRE 
(
OBJNR                                       STRING
 
, GJAHR                                       STRING
 
, WRTTP                                       STRING
 
, KSTAR                                       STRING
 
, TWAER                                       STRING
 
, WTG001                                      DECIMAL(15,2)
 
, WTG002                                      DECIMAL(15,2)
 
, WTG003                                      DECIMAL(15,2)
 
, WTG004                                      DECIMAL(15,2)
 
, WTG005                                      DECIMAL(15,2)
 
, WTG006                                      DECIMAL(15,2)
 
, WTG007                                      DECIMAL(15,2)
 
, WTG008                                      DECIMAL(15,2)
 
, WTG009                                      DECIMAL(15,2)
 
, WTG010                                      DECIMAL(15,2)
 
, WTG011                                      DECIMAL(15,2)
 
, WTG012                                      DECIMAL(15,2)
 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Financials/gl_cosp_pre';
 
--DISTRIBUTE ON (OBJNR)
 
 
 
 
--*****  Creating table:  GL_PRPS_PRE , ***** Creating table: GL_PRPS_PRE
 
 
use raw;
CREATE TABLE GL_PRPS_PRE 
(
PSPNR                                       STRING                 not null
 
, OBJNR                                       STRING
 
, POSID                                       STRING
 
, POST1                                       STRING
 
, ERDAT                                       STRING
 
, AEDAT                                       STRING
 
, PWPOS                                       STRING
 
, VERNR                                       STRING
 
, VERNA                                       STRING
 
, PBUKR                                       STRING
 
, PKOKR                                       STRING
 
, WERKS                                       STRING
 
, PRCTR                                       STRING
 
, FKSTL                                       STRING
 
, ASTNR                                       STRING
 
, ASTNA                                       STRING
 
, ZZMATKL                                     STRING
 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Financials/gl_prps_pre';
 
--DISTRIBUTE ON (PSPNR)
 
 
 
 
--*****  Creating table:  GL_PROJECT_DETAIL_PRE , ***** Creating table: GL_PROJECT_DETAIL_PRE
 
 
use raw;
CREATE TABLE GL_PROJECT_DETAIL_PRE 
(
OBJECT_CD                                   STRING                not null
 
, PLAN_AMT                                    DECIMAL(15,2)
 
, BUDGET_AMT                                  DECIMAL(15,2)
 
, ACTUAL_AMT                                  DECIMAL(15,2)
 
, COMMITMENT_AMT                              DECIMAL(15,2)
 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Financials/gl_project_detail_pre';
 
--DISTRIBUTE ON (OBJECT_CD)
 
 
 
 
--*****  Creating table:  GL_PROJECT_COST_DETAIL , ***** Creating table: GL_PROJECT_COST_DETAIL
 
 
use legacy;
CREATE TABLE GL_PROJECT_COST_DETAIL 
(
GL_PROJECT_GID INT not null
 
, FISCAL_MO                                   INT                         not null
 
, GL_COST_ELEMENT_CD                          STRING                not null
 
, CURRENCY_CD                                 STRING                 not null
 
, GL_PROJ_PLAN_AMT                            DECIMAL(15,2)
 
, GL_PROJ_COMMITMENT_AMT                      DECIMAL(15,2)
 
, GL_PROJ_ACTUAL_AMT                          DECIMAL(15,2)
 
, UPDATE_DT                                   TIMESTAMP
 
, LOAD_DT                                     TIMESTAMP
 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Financials/gl_project_cost_detail';
 
--DISTRIBUTE ON (GL_PROJECT_GID)
 
 
 
 
--*****  Creating table:  GL_PROJECT_DETAIL , ***** Creating table: GL_PROJECT_DETAIL
 
 
use legacy;
CREATE TABLE GL_PROJECT_DETAIL 
(
GL_PROJECT_GID INT not null
 
, GL_PROJ_PLAN_AMT                            DECIMAL(15,2)
 
, GL_PROJ_BUDGET_AMT                          DECIMAL(15,2)
 
, GL_PROJ_ACTUAL_AMT                          DECIMAL(15,2)
 
, GL_PROJ_COMMITMENT_AMT                      DECIMAL(15,2)
 
, UPDATE_DT                                   TIMESTAMP
 
, LOAD_DT                                     TIMESTAMP
 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Financials/gl_project_detail';
 
--DISTRIBUTE ON (GL_PROJECT_GID)
 
 
 
 
--*****  Creating table:  GL_PROJECT , ***** Creating table: GL_PROJECT
 
 
use legacy;
CREATE TABLE GL_PROJECT 
(
GL_PROJECT_GID INT not null
 
, WBS_ELEMENT_CD                              STRING
 
, WBS_OBJECT_CD                               STRING
 
, WBS_ELEMENT_ID                              STRING
 
, GL_PROJ_DESC                                STRING
 
, GL_PROJ_CURRENCY_CD                         STRING
 
, GL_PROJ_RESPONSIBLE_ID INT
 
, GL_PROJ_RESPONSIBLE_NAME                    STRING
 
, LOCATION_ID                                 INT
 
, STORE_NBR INT
 
, GL_PROFIT_CENTER_GID INT
 
, RESP_GL_PROFIT_CENTER_GID INT
 
, GL_PROJ_APPLICANT_ID INT
 
, GL_PROJ_APPLICANT_NAME                      STRING
 
, GL_PROJ_MERCH_CATEGORY                      STRING
 
, CREATE_DT                                   TIMESTAMP
 
, CHANGE_DT                                   TIMESTAMP
 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Financials/gl_project';
 
--DISTRIBUTE ON (GL_PROJECT_GID)
 
 
 
 
