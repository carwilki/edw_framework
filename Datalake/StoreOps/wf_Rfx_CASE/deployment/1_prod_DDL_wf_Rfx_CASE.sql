




--*****  Creating table:  "RFX_RTM_PROJECT" , ***** Creating table: "RFX_RTM_PROJECT"


use legacy;
CREATE TABLE  RFX_RTM_PROJECT
(
 RFX_PROJECT_ID                                    STRING                 not null

, LOCATION_ID INT not null

, RFX_UNIT_ID                                       STRING 

, RFX_PROJECT_TYPE_CD                               STRING 

, RFX_PROJECT_TITLE                                 STRING 

, RFX_PROJECT_PRIORITY_CD SMALLINT

, RFX_EXECUTION_ASSIGNED_ROLE_CD                    STRING 

, RFX_EXECUTION_ASSIGNED_DEPT_CD                    STRING 

, RFX_EXECUTION_ASSIGNED_USER_ID                    STRING 

, RFX_EXECUTION_ASSIGNED_USER_NAME                  STRING 

, RFX_PROJECT_EXECUTION_START_TSTMP                 TIMESTAMP 

, RFX_PROJECT_EXECUTION_END_TSTMP                   TIMESTAMP 

, RFX_PROJECT_EXECUTION_COMPLETION_TSTMP            TIMESTAMP 

, EST_EFFORT_IN_HOURS                               DECIMAL(7,2) 

, RFX_PROJECT_EXECUTION_STATUS_CD                   STRING 

, ON_TIME_COMPLETION_FLAG                           TINYINT 

, RFX_CREATOR_USER_ID                               STRING 

, RFX_CREATOR_NAME                                  STRING 

, RFX_CREATOR_UNIT                                  STRING 

, RFX_CREATOR_DEPT_CD                               STRING 

, RFX_RELEASE_USER_ID                               STRING 

, RFX_RELEASE_USER_NAME                             STRING 

, RFX_PROJECT_ASSIGNED_ROLE_CD                      STRING 

, RFX_PROJECT_ASSIGNED_DEPT_CD                      STRING 

, RFX_UNIT_ORG_LEVEL                                STRING 

, RFX_TASK_CNT SMALLINT

, WORKLOAD_FLAG SMALLINT

, CONFIDENTIAL_FLAG SMALLINT

, RFX_PROJECT_CREATION_TSTMP                        TIMESTAMP 

, RFX_PROJECT_LAUNCH_TSTMP                          TIMESTAMP 

, RFX_PROJECT_START_TSTMP                           TIMESTAMP 

, RFX_PROJECT_END_TSTMP                             TIMESTAMP 

, RFX_PROJECT_COMPLETION_TSTMP                      TIMESTAMP 

, RFX_PROJECT_STATUS_CD                             STRING 

, RFX_LAST_UPDATED_USER_ID                          STRING 

, RFX_LAST_UPDATED_USER_NAME                        STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_project' 
 ;

--DISTRIBUTE ON (RFX_PROJECT_ID, LOCATION_ID)







--*****  Creating table:  "RFX_RTM_TASK" , ***** Creating table: "RFX_RTM_TASK"


use legacy;
CREATE TABLE  RFX_RTM_TASK
(
 RFX_PROJECT_ID                                    STRING                 not null

, LOCATION_ID INT not null

, RFX_TASK_ID                                       STRING                 not null

, RFX_TASK_SEQ_NBR SMALLINT not null

, RFX_UNIT_ID                                       STRING 

, RFX_TASK_DESC                                     STRING 

, RFX_EXECUTION_ASSIGNED_DEPT_CD                    STRING 

, RFX_EXECUTION_ASSIGNED_ROLE_CD                    STRING 

, RFX_ASSIGNED_USER_ID                              STRING                 not null

, RFX_ASSIGNED_USER_NAME                            STRING                not null

, RFX_TASK_EXECUTION_START_TSTMP                    TIMESTAMP 

, RFX_TASK_EXECUTION_END_TSTMP                      TIMESTAMP 

, RFX_TASK_STATUS                                   STRING 

, EST_EFFORT_HOURS                                  DECIMAL(7,2) 

, RFX_TASK_EXECUTION_COMPLETION_TSTMP               TIMESTAMP 

, ON_TIME_COMPLETION_FLAG                           TINYINT 

, RFX_TASK_ASSIGNED_DEPT_CD                         STRING 

, RFX_TASK_ASSIGNED_ROLE_CD                         STRING 

, RFX_LAST_USER_ID                                  STRING 

, RFX_LAST_UPDATED_USER_NAME                        STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_task' 
 ;

--DISTRIBUTE ON (RFX_PROJECT_ID, LOCATION_ID)







--*****  Creating table:  "RFX_RTM_UNIT_ATTR" , ***** Creating table: "RFX_RTM_UNIT_ATTR"


use legacy;
CREATE TABLE  RFX_RTM_UNIT_ATTR
(
 RFX_UNIT_ID                                       STRING                 not null

, RFX_UNIT_ORG_LEVEL SMALLINT not null

, RFX_ATTR_ID                                       STRING                 not null

, RFX_ATTR_DESC                                     STRING 

, ATTR_TEXT_VALUE                                   STRING 

, ATTR_NUMERIC_VALUE                                DECIMAL(22,4) 

, ATTR_DATE_VALUE                                   TIMESTAMP 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_unit_attr' 
 ;

--DISTRIBUTE ON (RFX_UNIT_ID)







--*****  Creating table:  "RFX_RTM_PROJECT_MESSAGE" , ***** Creating table: "RFX_RTM_PROJECT_MESSAGE"


use legacy;
CREATE TABLE  RFX_RTM_PROJECT_MESSAGE
(
 RFX_PROJECT_ID                                    STRING                 not null

, LOCATION_ID INT not null

, RFX_MESSAGE_TSTMP                                 TIMESTAMP                             not null

, RFX_UNIT_ID                                       STRING 

, RFX_UNIT_ORG_LEVEL SMALLINT

, RFX_USER_ID                                       STRING 

, RFX_USER_NAME                                     STRING 

, RFX_MESSAGE_TEXT                                  STRING 

, RFX_MESSAGE_TYPE_CD                               STRING                 not null

, RFX_MESSAGE_TYPE_DESC                             STRING 

, RFX_MESSAGE_STATUS_CD                             STRING                 not null

, RFX_MESSAGE_RECORD_TYPE_CD                        STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_project_message' 
 ;

--DISTRIBUTE ON (RFX_PROJECT_ID, LOCATION_ID)







--*****  Creating table:  "RFX_RTM_PROJECT_RESPONSE" , ***** Creating table: "RFX_RTM_PROJECT_RESPONSE"


use legacy;
CREATE TABLE  RFX_RTM_PROJECT_RESPONSE
(
 RFX_PROJECT_ID                                    STRING                 not null

, LOCATION_ID INT not null

, RFX_TASK_ID                                       STRING                 not null

, RFX_QUESTION_ID                                   STRING                 not null

, RFX_SUB_QUESTION_NBR INT not null

, RFX_SURVEY_TYPE_CD                                STRING 

, RFX_UNIT_ID                                       STRING 

, RFX_RESPONSE_TSTMP                                TIMESTAMP 

, RFX_QUESTION_TEXT                                 STRING 

, RFX_SUB_QUESTION_TEXT                             STRING 

, RFX_RESPONSE_TYPE_CD                              STRING 

, OPTION_SELECTED SMALLINT not null

, OPTION_TEXT                                       STRING 

, NUMERIC_VALUE                                     DECIMAL(22,4) 

, TEXT_VALUE                                        STRING 

, DATE_VALUE                                        TIMESTAMP 

, ATTACHMENT_CNT INT

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_project_response' 
 ;

--DISTRIBUTE ON (RFX_PROJECT_ID, LOCATION_ID)







--*****  Creating table:  "RFX_RTM_LOOK_UP" , ***** Creating table: "RFX_RTM_LOOK_UP"


use legacy;
CREATE TABLE  RFX_RTM_LOOK_UP
(
 RFX_LOOKUP_TYPE_CD                                STRING                 not null

, RFX_KEY_CD                                        STRING                 not null

, RFX_KEY_DESC                                      STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_look_up' 
 ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "RFX_RTM_PROJECT_STATUS" , ***** Creating table: "RFX_RTM_PROJECT_STATUS"


use legacy;
CREATE TABLE  RFX_RTM_PROJECT_STATUS
(
 RFX_PROJECT_STATUS_CD                             STRING                 not null

, RFX_PROJECT_STATUS_DESC                           STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_project_status' 
 ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "RFX_RTM_PROJECT_TYPE" , ***** Creating table: "RFX_RTM_PROJECT_TYPE"


use legacy;
CREATE TABLE  RFX_RTM_PROJECT_TYPE
(
 RFX_PROJECT_TYPE_CD                               STRING                 not null

, RFX_PROJECT_TYPE_DESC                             STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_project_type' 
 ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "RFX_RTM_PROJECT_PRIORITY" , ***** Creating table: "RFX_RTM_PROJECT_PRIORITY"


use legacy;
CREATE TABLE  RFX_RTM_PROJECT_PRIORITY
(
 RFX_PROJECT_PRIORITY_CD                           STRING                 not null

, RFX_PROJECT_PRIORITY_DESC                         STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_project_priority' 
 ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "RFX_RTM_ROLE" , ***** Creating table: "RFX_RTM_ROLE"


use legacy;
CREATE TABLE  RFX_RTM_ROLE
(
 RFX_ROLE_CD                                       STRING                 not null

, RFX_ROLE_DESC                                     STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_role' 
 ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "RFX_RTM_DEPT" , ***** Creating table: "RFX_RTM_DEPT"


use legacy;
CREATE TABLE  RFX_RTM_DEPT
(
 RFX_DEPT_CD                                       STRING                 not null

, RFX_DEPT_DESC                                     STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_dept' 
 ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "RFX_RTM_PROJECT_EXECUTION_STATUS" , ***** Creating table: "RFX_RTM_PROJECT_EXECUTION_STATUS"


use legacy;
CREATE TABLE  RFX_RTM_PROJECT_EXECUTION_STATUS
(
 RFX_PROJECT_EXECUTION_STATUS_CD                   STRING                 not null

, RFX_PROJECT_EXECUTION_STATUS_DESC                 STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_project_execution_status' 
 ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "RFX_PROJECT_PRE" , ***** Creating table: "RFX_PROJECT_PRE"


use raw;
CREATE TABLE  RFX_PROJECT_PRE
(
 PROJECT_ID                                        STRING                 not null

, UNIT_ID       STRING                 not null

, PROJECT_TYPE                                      STRING 

, PROJECT_TITLE                                     STRING 

, PROJECT_PRIORITY_CODE SMALLINT

, CREATOR_USER_ID                                   STRING 

, CREATOR       STRING 

, EXECUTION_ASSIGNED_ROLE                           STRING 

, EXECUTION_ASSIGNED_DEPT                           STRING 

, EXECUTION_ASSIGNED_USER_ID                        STRING 

, EXECUTION_ASSIGNED_USER                           STRING 

, PROJECT_EXECUTION_START_DATE                      TIMESTAMP 

, PROJECT_EXECUTION_END_DATE                        TIMESTAMP 

, STORE_PROJECT_STATUS                              STRING 

, ESTIMATED_EFFORT_IN_HOURS                         DECIMAL(7,2) 

, PROJECT_EXECUTION_COMPLETION_DATE                 TIMESTAMP 

, ON_TIME_COMPLETION_FLAG                           STRING 

, PROJECT_STATUS                                    STRING 

, PROJECT_LAUNCH_DATE                               TIMESTAMP 

, PROJECT_START_DATE                                TIMESTAMP 

, PROJECT_END_DATE                                  TIMESTAMP 

, PROJECT_CREATION_DATE                             TIMESTAMP 

, CREATOR_UNIT                                      STRING 

, CREATOR_DEPT                                      STRING 

, UNIT_ORG_LEVEL                                    STRING 

, CONFIDENTIAL_FLAG SMALLINT

, TASK_COUNT SMALLINT

, PROJECT_COMPLETION_DATE                           TIMESTAMP 

, RELEASE_USER_ID                                   STRING 

, RELEASE_USER                                      STRING 

, WORKLOAD_FLAG SMALLINT

, PROJECT_ASSIGNED_ROLE                             STRING 

, PROJECT_ASSIGNED_DEPT                             STRING 

, LAST_UPDATED_USER_ID                              STRING 

, LAST_UPDATED_USER                                 STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/storeops/rfx_project_pre' 
 ;

--DISTRIBUTE ON (PROJECT_ID, UNIT_ID)







--*****  Creating table:  "RFX_TASK_PRE" , ***** Creating table: "RFX_TASK_PRE"


use raw;
CREATE TABLE  RFX_TASK_PRE
(
 PROJECT_ID                                        STRING                 not null

, UNIT_ID       STRING                 not null

, TASK_ID       STRING                 not null

, SEQUENCE_NO SMALLINT not null

, TASK_TITLE                                        STRING 

, EXECUTION_ASSIGNED_DEPT                           STRING 

, EXECUTION_ASSIGNED_ROLE                           STRING 

, ASSIGNED_USER_ID                                  STRING                 not null

, ASSIGNED_USER                                     STRING                not null

, TASK_EXECUTION_START_DATE                         TIMESTAMP 

, TASK_EXECUTION_END_DATE                           TIMESTAMP 

, STATUS        STRING 

, ESTIMATED_EFFORT_HOURS                            DECIMAL(7,2) 

, TASK_EXECUTION_COMPLETION_DATE                    TIMESTAMP 

, ON_TIME_COMPLETION_INDICATOR                      STRING 

, TASK_ASSIGNED_DEPT                                STRING 

, TASK_ASSIGNED_ROLE                                STRING 

, LAST_USER_ID                                      STRING 

, LAST_UPDATED_BY_USER                              STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/storeops/rfx_task_pre' 
 ;

--DISTRIBUTE ON (PROJECT_ID, UNIT_ID)







--*****  Creating table:  "RFX_LOOK_UP_PRE" , ***** Creating table: "RFX_LOOK_UP_PRE"


use raw;
CREATE TABLE  RFX_LOOK_UP_PRE
(
 LOOKUP_TYPE                                       STRING                 not null

, RFX_KEY       STRING                 not null

, RFX_DESCRIPTION                                   STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/storeops/rfx_look_up_pre' 
 ;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "RFX_UNIT_ATTRIBUTE_PRE" , ***** Creating table: "RFX_UNIT_ATTRIBUTE_PRE"


use raw;
CREATE TABLE  RFX_UNIT_ATTRIBUTE_PRE
(
 UNIT_ID       STRING                 not null

, UNIT_ORG_LEVEL SMALLINT not null

, ATTRIBUTE_ID                                      STRING                 not null

, ATTRIBUTE_DESCRIPTION                             STRING 

, ATTRIBUTE_TEXT_VALUE                              STRING 

, ATTRIBUTE_NUMERIC_VALUE                           DECIMAL(22,4) 

, ATTRIBUTE_DATE_VALUE                              TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/storeops/rfx_unit_attribute_pre' 
 ;

--DISTRIBUTE ON (UNIT_ID)







--*****  Creating table:  "RFX_PROJ_MESSAGE_PRE" , ***** Creating table: "RFX_PROJ_MESSAGE_PRE"


use raw;
CREATE TABLE  RFX_PROJ_MESSAGE_PRE
(
 PROJECT_ID                                        STRING                 not null

, UNIT_ID       STRING                 not null

, UNIT_ORG_LEVEL SMALLINT not null

, MESSAGE_DATE                                      TIMESTAMP                             not null

, USER_ID       STRING 

, USER_NAME     STRING 

, MESSAGE_TEXT                                      STRING 

, MESSAGE_TYPE                                      STRING                 not null

, MESSAGE_TYPE_DESC                                 STRING 

, MESSAGE_STATUS                                    STRING                 not null

, RECORD_TYPE                                       STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/storeops/rfx_proj_message_pre' 
 ;

--DISTRIBUTE ON (PROJECT_ID, UNIT_ID)







--*****  Creating table:  "RFX_PRJ_RESPONSE_PRE" , ***** Creating table: "RFX_PRJ_RESPONSE_PRE"


use raw;
CREATE TABLE  RFX_PRJ_RESPONSE_PRE
(
 PROJECT_ID                                        STRING                 not null

, TASK_ID       STRING                 not null

, SURVEY_TYPE                                       STRING                not null

, UNIT_ID       STRING                 not null

, QUESTION_ID                                       STRING                 not null

, QUESTION_TEXT                                     STRING 

, SUB_QUESTION_NO INT not null

, SUB_QUESTION_TEXT                                 STRING 

, RESPONSE_TYPE                                     STRING 

, OPTION_SELECTED SMALLINT not null

, OPTION_TEXT                                       STRING 

, RESPONSE_DATE                                     TIMESTAMP 

, NUMERIC_VALUE                                     DECIMAL(22,4) 

, TEXT_VALUE                                        STRING 

, DATE_VALUE                                        TIMESTAMP 

, ATTACHMENT_COUNT INT

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/storeops/rfx_prj_response_pre' 
 ;

--DISTRIBUTE ON (PROJECT_ID, TASK_ID, UNIT_ID)





--*****  Creating table:  "RFX_RTM_UNIT_HIERARCHY" , ***** Creating table: "RFX_RTM_UNIT_HIERARCHY"


use legacy;
CREATE TABLE  RFX_RTM_UNIT_HIERARCHY
(
 RFX_UNIT_ID                                       STRING                 not null

, RFX_UNIT_NAME                                     STRING 

, LOCATION_ID INT

, STORE_NBR INT

, STORE_NAME                                        STRING 

, RFX_UNIT_ORG_LEVEL SMALLINT

, RFX_UNIT_STATUS_CD                                STRING 

, STATE_CD      STRING 

, ZIP_CD        STRING 

, CORPORATE_ID                                      STRING 

, CORPORATE_NAME                                    STRING 

, LEVEL_2_ID                                        STRING 

, LEVEL_2_NAME                                      STRING 

, LEVEL_3_ID                                        STRING 

, LEVEL_3_NAME                                      STRING 

, LEVEL_4_ID                                        STRING 

, LEVEL_4_NAME                                      STRING 

, LEVEL_5_ID                                        STRING 

, LEVEL_5_NAME                                      STRING 

, LEVEL_6_ID                                        STRING 

, LEVEL_6_NAME                                      STRING 

, LEVEL_7_ID                                        STRING 

, LEVEL_7_NAME                                      STRING 

, DISTRICT_ID                                       STRING 

, DISTRICT_NAME                                     STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/storeops/rfx_rtm_unit_hierarchy';

--DISTRIBUTE ON (RFX_UNIT_ID)







--*****  Creating table:  "RFX_UNIT_HRCY_PRE" , ***** Creating table: "RFX_UNIT_HRCY_PRE"


use raw;
CREATE TABLE  RFX_UNIT_HRCY_PRE
(
 UNIT_ID       STRING                 not null

, UNIT_NAME     STRING 

, STORE_ID      STRING 

, STORE_NAME                                        STRING 

, UNIT_ORG_LEVEL SMALLINT

, UNIT_STATUS                                       STRING 

, STATE_CODE                                        STRING 

, ZIP_CODE      STRING 

, CORPORATE_ID                                      STRING 

, CORPORATE_NAME                                    STRING 

, LEVEL_2_ID                                        STRING 

, LEVEL_2_NAME                                      STRING 

, LEVEL_3_ID                                        STRING 

, LEVEL_3_NAME                                      STRING 

, LEVEL_4_ID                                        STRING 

, LEVEL_4_NAME                                      STRING 

, LEVEL_5_ID                                        STRING 

, LEVEL_5_NAME                                      STRING 

, LEVEL_6_ID                                        STRING 

, LEVEL_6_NAME                                      STRING 

, LEVEL_7_ID                                        STRING 

, LEVEL_7_NAME                                      STRING 

, DISTRICT_ID                                       STRING 

, DISTRICT_NAME                                     STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/storeops/rfx_unit_hrcy_pre';

--DISTRIBUTE ON (UNIT_ID)