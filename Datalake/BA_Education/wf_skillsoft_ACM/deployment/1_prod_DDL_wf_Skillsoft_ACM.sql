
--*****  Creating table:  "SKILLSOFT_ACM_LEARNING_PROGRAM" , ***** Creating table: "SKILLSOFT_ACM_LEARNING_PROGRAM"


use legacy;
 CREATE TABLE  SKILLSOFT_ACM_LEARNING_PROGRAM 
( USERID INT not null

, USER_NAME     STRING 

, REHIRE_DATE                                       TIMESTAMP 

, ACCOUNT_NUMBER                                    STRING 

, COURSEID      STRING 

, COURSE_TITLE                                      STRING 

, SKILLSOFT_COURSE_NUMBER                           STRING 

, COURSE_STATUS                                     STRING 

, SCORE         STRING 

, TOTAL_TIME_IN_COURSE                              STRING 

, TOTAL_TIME_IN_COURSE_TO_COMPLETION                STRING 

, COURSE_ORDER                                      STRING 

, COURSE_BEGIN_DATE                                 TIMESTAMP 

, COURSE_LAST_VISIT_DATE                            TIMESTAMP 

, COURSE_REQOPT                                     STRING 

, COURSE_DATE_COMPLETED                             TIMESTAMP 

, DELIVERY_METHOD                                   STRING 

, LEARNING_PROGRAM_ID                               STRING 

, LEARNING_PROGRAM                                  STRING 

, LEARNING_PROGRAM_STATUS                           STRING 

, REQOPT        STRING 

, DUE_DATE      TIMESTAMP 

, VALIDITY_IN_DAYS INT

, LEARNING_PROGRAM_DATE_COMPLETED                   TIMESTAMP 

, TOTAL_TIME_IN_LEARNING_PROGRAM                    STRING 

, TOTAL_TIME_IN_LEARNING_PROGRAM_TO_COMPLETION      STRING 

, REQUIRED_TIME_IN_LEARNING_PROGRAM                 STRING 

, EXEMPTION_EXPIRATION_DATE                         TIMESTAMP 

, USER_STATUS                                       STRING 

, USER_EMAIL                                        STRING 

, SUPERVISOR_EMAIL                                  STRING 

, COURSE_COMMENTS                                   STRING 

, LEARNING_PROGRAM_COMMENTS                         STRING 

, SUPERVISOR_LEVEL_2_EMAIL                          STRING 

, LOAD_DT       TIMESTAMP                             not null  

, UPDATE_DT     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/skillsoft_acm_learning_program';

--DISTRIBUTE ON (USERID)







--*****  Creating table:  "SKILLSOFT_ACM_ASSET_ACTIVITY" , ***** Creating table: "SKILLSOFT_ACM_ASSET_ACTIVITY"


use legacy;
 CREATE TABLE  SKILLSOFT_ACM_ASSET_ACTIVITY 
( STUDENT_REC_ID INT not null

, USERID INT not null

, USER_NAME     STRING 

, REHIRE_DATE                                       TIMESTAMP 

, ACCOUNT_NUMBER                                    STRING 

, COURSEID      STRING 

, COURSE        STRING 

, SKILLSOFT_COURSE_NUMBER                           STRING 

, METHOD        STRING 

, ESTIMATED_COST                                    STRING 

, ESTIMATED_DURATION                                STRING 

, CEU           STRING 

, STATUS        STRING 

, REQOPT        STRING 

, DUE_DATE      TIMESTAMP 

, VALIDITYINDAYS INT

, BEGIN_DATE                                        TIMESTAMP 

, LAST_VISIT_DATE                                   TIMESTAMP 

, DATE_COMPLETED                                    TIMESTAMP 

, SCORE         STRING 

, TIME_IN_COURSE_TO_COMPLETION                      STRING 

, TOTAL_TIME_IN_COURSE                              STRING 

, DATE_OPEN_FOR_TRAINING                            TIMESTAMP 

, EXEMPTION_EXPIRATION_DATE                         TIMESTAMP 

, USER_STATUS                                       STRING 

, USER_EMAIL                                        STRING 

, SUPERVISOR_EMAIL                                  STRING 

, SUPERVISOR_LEVEL_2_EMAIL                          STRING 

, PRIORCOURSECOMPLETED                              STRING 

, PRIORCOURSETITLE                                  STRING 

, COMMENTS      STRING 

, MAXIMUM_ASSESSMENT_ATTEMPTS                       STRING 

, ACTUAL_ASSESSMENT_ATTEMPTS                        STRING 

, GROUP_FROM                                        STRING 

, DELETE_FLAG INT 

, LOAD_DT       TIMESTAMP                             not null  

, UPDATE_DT     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/skillsoft_acm_asset_activity';

--DISTRIBUTE ON (USERID)







--*****  Creating table:  "EDU_CERT_SUMMARY_CONS" , ***** Creating table: "EDU_CERT_SUMMARY_CONS"


use legacy;
 CREATE TABLE  EDU_CERT_SUMMARY_CONS 
( DAY_DT                                      TIMESTAMP                            not null

, EMPLOYEE_ID BIGINT not null

, MISSED_ASSESS_MID BIGINT not null

, MISSED_ASSESS_LID BIGINT not null

, MISSED_ASSESS_NAME                          STRING 

, JOB_CD INT

, LOCATION_ID INT

, CURR_COMPLIANCE_FLAG TINYINT

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/edu_cert_summary_cons';

--DISTRIBUTE ON (EMPLOYEE_ID)







--*****  Creating table:  "EDU_CERT_DAILY_CONS" , ***** Creating table: "EDU_CERT_DAILY_CONS"


use legacy;
 CREATE TABLE  EDU_CERT_DAILY_CONS 
( DAY_DT                                      TIMESTAMP                            not null

, EMPLOYEE_ID BIGINT not null

, ASSESSMENT_MID BIGINT not null

, ASSESSMENT_LID BIGINT not null

, TEST_TAKEN_DT                               TIMESTAMP 

, ASSESSMENT_NAME                             STRING 

, JOB_CD INT

, LOCATION_ID INT

, LAST_TEST_SCORE_NBR BIGINT

, LAST_TEST_PASSED_FLAG TINYINT

, COMPLIANT_START_DT                          TIMESTAMP 

, COMPLIANT_EXPIRATION_DT                     TIMESTAMP 

, CURR_COMPLIANCE_FLAG TINYINT

, CURR_MISSING_FLAG TINYINT

, CURR_PERIOD_ATTEMPTS_NBR SMALLINT

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/edu_cert_daily_cons';

--DISTRIBUTE ON (EMPLOYEE_ID)







--*****  Creating table:  "EDU_RESULT_CONS" , ***** Creating table: "EDU_RESULT_CONS"


use legacy;
 CREATE TABLE  EDU_RESULT_CONS 
( RESULT_ID BIGINT not null

, TEST_TAKEN_DT                               TIMESTAMP                            not null

, TEST_TAKEN_START_TSTMP                      TIMESTAMP 

, ASSESSMENT_MID BIGINT not null

, ASSESSMENT_LID BIGINT not null

, LAST_MODIFIED_TSTMP                         TIMESTAMP 

, WRITE_ANSWER_FLAG TINYINT

, EMPLOYEE_ID BIGINT not null

, MEMBER_GROUP                                STRING 

, PARTICIPANT_DETAILS                         STRING 

, HOSTNAME                                    STRING 

, IP_ADDRESS                                  STRING 

, SIGNATURE                                   STRING 

, STILL_GOING_FLAG TINYINT

, STATUS_ID INT

, SECTIONS_CNT INT

, MAX_SCORE_NBR BIGINT

, TOTAL_SCORE_NBR BIGINT

, SPECIAL_1                                   STRING 

, SPECIAL_2                                   STRING 

, SPECIAL_3                                   STRING 

, SPECIAL_4                                   STRING 

, SPECIAL_5                                   STRING 

, SPECIAL_6                                   STRING 

, SPECIAL_7                                   STRING 

, SPECIAL_8                                   STRING 

, SPECIAL_9                                   STRING 

, SPECIAL_10                                  STRING 

, TIME_TAKEN_NBR BIGINT

, SCORE_RESULT                                STRING 

, SCORE_RESULT_NBR BIGINT

, PASSED_FLAG TINYINT

, PERCENTAGE_SCORE_NBR INT

, SCHEDULE_NAME                               STRING 

, MONITORED_FLAG TINYINT

, MONITOR_NAME                                STRING 

, TIME_LIMIT_DISABLED_FLAG TINYINT

, DISABLED_BY                                 STRING 

, IMAGE_REF                                   STRING 

, SCOREBAND_ID BIGINT not null

, FIRST_NAME                                  STRING 

, LAST_NAME                                   STRING 

, PRIMARY_EMAIL                               STRING 

, RESTRICT_PART_FLAG TINYINT

, RESTRICT_ADMIN_FLAG TINYINT

, R_PART_FROM_DT                              TIMESTAMP 

, R_PART_TO_DT                                TIMESTAMP 

, R_ADMIN_FROM_DT                             TIMESTAMP 

, R_ADMIN_TO_DT                               TIMESTAMP 

, COURSE_NAME                                 STRING 

, MEMBER_SUB_GROUP_1                          STRING 

, MEMBER_SUB_GROUP_2                          STRING 

, MEMBER_SUB_GROUP_3                          STRING 

, MEMBER_SUB_GROUP_4                          STRING 

, MEMBER_SUB_GROUP_5                          STRING 

, MEMBER_SUB_GROUP_6                          STRING 

, MEMBER_SUB_GROUP_7                          STRING 

, MEMBER_SUB_GROUP_8                          STRING 

, MEMBER_SUB_GROUP_9                          STRING 

, TEST_CENTER                                 STRING 

, LOAD_DT                                     TIMESTAMP 

, LOCATION_ID INT

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/edu_result_cons';

--DISTRIBUTE ON (RESULT_ID)







--*****  Creating table:  "EDU_ASSESSMENTS_CONS" , ***** Creating table: "EDU_ASSESSMENTS_CONS"


use legacy;
 CREATE TABLE  EDU_ASSESSMENTS_CONS 
( ASSESSMENT_MID BIGINT not null

, ASSESSMENT_LID BIGINT not null

, REVISION_NBR INT

, ASSESSMENT_NAME                             STRING 

, ASSESSMENT_AUTHOR                           STRING 

, MODIFY_TSTMP                                TIMESTAMP 

, TIME_LIMIT_FLAG TINYINT

, TIME_LIMIT_NBR INT

, SECTIONS_CNT INT

, LAST_UPDATE_TSTMP                           TIMESTAMP 

, ASSESSMENT_TYPE_ID INT

, COURSE_NAME                                 STRING 

, ASSESSMENT_DESC                             STRING 

, PETSHOTEL_ASSESSMENT_FLAG TINYINT

, SALON_ASSESSMENT_FLAG TINYINT

, LAST_UPDT_USER                              STRING 

, LAST_UPDT_TSTMP                             TIMESTAMP 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/edu_assessments_cons';

--DISTRIBUTE ON RANDOM
