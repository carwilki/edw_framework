-- Databricks notebook source

DROP TABLE legacy.EDU_ASSESSMENTS;


CREATE TABLE refine.EDU_ASSESSMENTS_history
USING delta
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/IT/edu_assessments';

-- COMMAND ----------

--*****  Creating table:  "EDU_ASSESSMENTS" , ***** Creating table: "EDU_ASSESSMENTS"


use legacy;
CREATE TABLE  EDU_ASSESSMENTS 
(  ASSESSMENT_MID BIGINT not null

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
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/edu_assessments';

--DISTRIBUTE ON RANDOM


-- COMMAND ----------


DROP TABLE legacy.EDU_RESULT;


CREATE TABLE refine.EDU_RESULT_history
USING delta
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/IT/edu_result';


--*****  Creating table:  "EDU_RESULT" , ***** Creating table: "EDU_RESULT"


use legacy;
CREATE TABLE  EDU_RESULT 
(  RESULT_ID BIGINT not null

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

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/edu_result';

--DISTRIBUTE ON (RESULT_ID)


-- COMMAND ----------



DROP TABLE legacy.EDU_CERT_SUMMARY;


CREATE TABLE refine.EDU_CERT_SUMMARY_history
USING delta
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/IT/edu_cert_summary';



--*****  Creating table:  "EDU_CERT_SUMMARY" , ***** Creating table: "EDU_CERT_SUMMARY"


use legacy;
CREATE TABLE  EDU_CERT_SUMMARY 
(  DAY_DT                                      TIMESTAMP                            not null

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
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/edu_cert_summary';

--DISTRIBUTE ON (EMPLOYEE_ID)


-- COMMAND ----------



DROP TABLE legacy.EDU_CERT_DAILY;


CREATE TABLE refine.EDU_CERT_DAILY_history
USING delta
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/IT/edu_cert_daily';



--*****  Creating table:  "EDU_CERT_DAILY" , ***** Creating table: "EDU_CERT_DAILY"


use legacy;
CREATE TABLE  EDU_CERT_DAILY 
(  DAY_DT                                      TIMESTAMP                            not null

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
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/edu_cert_daily';

--DISTRIBUTE ON (EMPLOYEE_ID)



