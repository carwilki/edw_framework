






--*****  Creating table:  "RFX_SWA_STORE_WALK" , ***** Creating table: "RFX_SWA_STORE_WALK"


use legacy;
CREATE TABLE  RFX_SWA_STORE_WALK
(
 RFX_WALK_ID BIGINT not null

, RFX_WALK_TSTMP                                    TIMESTAMP 

, RFX_SUBMISSION_TSTMP                              TIMESTAMP 

, LOCATION_ID INT

, RFX_STORE_ID                                      STRING 

, STORE_NAME                                        STRING 

, RFX_WALK_PROJECT_ID                               STRING 

, RFX_WALK_TYPE_ID INT not null

, RFX_WALK_TYPE_NAME                                STRING 

, RFX_WALK_PARTICIPANTS_NAME                        STRING 

, RFX_WALK_OWNER_ID                                 STRING 

, RFX_WALK_OWNER_NAME                               STRING 

, SITE_MANAGER_ID INT

, SITE_MANAGER_NAME                                 STRING 

, RFX_TASK_CNT SMALLINT

, RFX_PASS_STATUS_CD                                STRING 

, POINTS_SCORED                                     DECIMAL(7,2) 

, TOTAL_POINTS                                      DECIMAL(7,2) 

, TARGET_POINTS                                     DECIMAL(7,2) 

, SCORE_PCT     DECIMAL(5,2) 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_swa_store_walk' 
 ;

--DISTRIBUTE ON (RFX_WALK_ID)







--*****  Creating table:  "RFX_SWA_WALK_RESPONSE" , ***** Creating table: "RFX_SWA_WALK_RESPONSE"


use legacy;
CREATE TABLE RFX_SWA_WALK_RESPONSE
(
 RFX_WALK_ID BIGINT not null

, LOCATION_ID INT not null

, RFX_WALK_GROUP_HANDLE                             STRING                not null

, RFX_WALK_QUESTION_HANDLE                          STRING                not null

, RFX_WALK_SUB_QUESTION_NBR SMALLINT not null

, RFX_WALK_RESPONSE_TSTMP                           TIMESTAMP                             not null

, RFX_STORE_ID                                      STRING 

, RFX_WALK_TYPE_ID INT

, RFX_WALK_QUESTION_TEXT                            STRING 

, RFX_WALK_SUB_QUESTION_TEXT                        STRING 

, RFX_WALK_RESPONSE_TYPE_CD                         STRING 

, OPTION_SELECTED SMALLINT not null

, OPTION_TEXT                                       STRING 

, NUMERIC_VALUE                                     DECIMAL(22,4) 

, TEXT_VALUE                                        STRING 

, DATE_VALUE                                        TIMESTAMP 

, ATTACHMENT_COUNT SMALLINT

, COMMENTS      STRING 

, POINTS_SCORED                                     DECIMAL(7,2) 

, TOTAL_POINTS                                      DECIMAL(7,2) 

, TARGET_POINTS                                     DECIMAL(7,2) 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_swa_walk_response' 
 ;

--DISTRIBUTE ON (RFX_WALK_ID, LOCATION_ID)

--ORGANIZE   ON (RFX_WALK_RESPONSE_TSTMP)







--*****  Creating table:  "RFX_STORE_WALK_PRE" , ***** Creating table: "RFX_STORE_WALK_PRE"


use raw;
CREATE TABLE RFX_STORE_WALK_PRE
(
 WALK_ID BIGINT not null

, STORE_ID      STRING 

, STORE_NAME                                        STRING 

, WALK_DATE_TIME                                    TIMESTAMP 

, SUBMISSION_DATE_TIME                              TIMESTAMP 

, WALK_TYPE_ID INT not null

, WALK_TYPE     STRING 

, WALK_PARTICIPANTS                                 STRING 

, PASS_STATUS                                       STRING 

, POINTS_SCORED                                     DECIMAL(7,2) 

, TOTAL_POINTS                                      DECIMAL(7,2) 

, TARGET_POINTS                                     DECIMAL(7,2) 

, SCORE_PCT     DECIMAL(5,2) 

, TASK_COUNT SMALLINT

, WALK_OWNER_ID                                     STRING 

, WALK_OWNER                                        STRING 

, WALK_PROJECT_ID                                   STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/storeops/rfx_store_walk_pre' 
 ;

--DISTRIBUTE ON (WALK_ID)







--*****  Creating table:  "RFX_WALK_RESPONSE_PRE" , ***** Creating table: "RFX_WALK_RESPONSE_PRE"


use raw;
CREATE TABLE  RFX_WALK_RESPONSE_PRE
(
 WALK_ID BIGINT not null

, WALK_TYPE_ID INT

, STORE_ID      STRING 

, GROUP_HANDLE                                      STRING                not null

, QUESTION_HANDLE                                   STRING                not null

, SUB_QUESTION_NO SMALLINT not null

, RESPONSE_DATE                                     TIMESTAMP                             not null

, QUESTION_TEXT                                     STRING 

, SUB_QUESTION_TEXT                                 STRING 

, RESPONSE_TYPE                                     STRING 

, OPTION_SELECTED SMALLINT not null

, OPTION_TEXT                                       STRING 

, NUMERIC_VALUE                                     DECIMAL(22,4) 

, TEXT_VALUE                                        STRING 

, DATE_VALUE                                        TIMESTAMP 

, ATTACHMENT_COUNT SMALLINT

, COMMENTS      STRING 

, POINTS_SCORED                                     DECIMAL(7,2) 

, TOTAL_POINTS                                      DECIMAL(7,2) 

, TARGET_POINTS                                     DECIMAL(7,2) 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/storeops/rfx_walk_response_pre' 
 ;

--DISTRIBUTE ON (WALK_ID, GROUP_HANDLE)


