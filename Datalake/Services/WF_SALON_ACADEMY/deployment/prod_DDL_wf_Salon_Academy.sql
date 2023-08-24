-- Databricks notebook source
--*****  Creating table:  "SALON_ACADEMY" , ***** Creating table: "SALON_ACADEMY"


use legacy;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY
(
 SALON_ACADEMY_ID BIGINT not null

, LOCATION_ID INT not null

, START_DT DATE

, END_DT DATE

, ACTIVE_FLAG                                       TINYINT                          not null

, SEAT_LIMIT INT

, CANCELLED_FLAG                                    TINYINT                          not null

, TRAINER_EMPLOYEE_ID INT

, TRAINER_NAME                                      STRING 

, SALON_ACADEMY_TYPE_ID INT

, SALON_ACADEMY_TYPE_DESC                           STRING 

, APPROVED_BY_DISTRICT_LEADER_FLAG TINYINT

, UPDATE_TSTMP                                      TIMESTAMP                             not null

, LOAD_TSTMP                                        TIMESTAMP                             not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/salon_academy';

--DISTRIBUTE ON (SALON_ACADEMY_ID)







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT"


use legacy;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT
(
 SALON_ACADEMY_ASSESSMENT_ID BIGINT not null

, ASSESSMENT_REQUEST_TSTMP                          TIMESTAMP 

, SCHEDULED_ASSESSMENT_DT DATE

, SCHEDULED_ASSESSMENT_TIME                         TIMESTAMP 

, PRE_ASSESSMENT_COMPLETION_TSTMP                   TIMESTAMP 

, ASSESSMENT_PASS_TSTMP                             TIMESTAMP 

, ASSESSMENT_FAIL_TSTMP                             TIMESTAMP 

, ACADEMY_ENROLLMENT_TSTMP                          TIMESTAMP 

, ASSESSMENT_STATUS                                 STRING 

, ASSESSMENT_COMPLETION_TSTMP                       TIMESTAMP 

, REASON_COMMENT                                    STRING 

, SALON_ACADEMY_ASSESSMENT_DENY_REASON_ID BIGINT

, SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID BIGINT

, ACADEMY_ENROLLMENT_CHANGED_TSTMP                  TIMESTAMP 

, ACADEMY_COMPLETED_TSTMP                           TIMESTAMP 

, SCHEDULE_ASSESSMENT_CHANGED_TSTMP                 TIMESTAMP 

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/salon_academy_assessment';

--DISTRIBUTE ON (SALON_ACADEMY_ASSESSMENT_ID)







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER"


use legacy;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER
(
 SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_ID BIGINT not null

, SALON_ACADEMY_ASSESSMENT_ID BIGINT

, SALON_ACADEMY_ASSESSMENT_TRAINER_ID BIGINT

, ACTIVITY      STRING 

, ACTIVITY_STATUS                                   STRING 

, ACTIVITY_TSTMP                                    TIMESTAMP 

, ACTIVITY_NOTE                                     STRING 

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/salon_academy_assessment_activity_tracker';

--DISTRIBUTE ON (SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_ID)







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_DENY_REASON" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_DENY_REASON"


use legacy;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_DENY_REASON
(
 SALON_ACADEMY_ASSESSMENT_DENY_REASON_ID INT not null

, SALON_ACADEMY_ASSESSMENT_DENY_REASON_DESC         STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/salon_academy_assessment_deny_reason';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_FAIL_REASON" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_FAIL_REASON"


use legacy;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_FAIL_REASON
(
 SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID INT not null

, SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC         STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/salon_academy_assessment_fail_reason';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_REASON" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_REASON"


use legacy;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_REASON
(
 SALON_ACADEMY_ASSESSMENT_REASON_ID BIGINT not null

, SALON_ACADEMY_ASSESSMENT_ID BIGINT

, REASON_TEXT                                       STRING 

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/salon_academy_assessment_reason';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_TRAINER" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_TRAINER"


use legacy;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_TRAINER
(
 SALON_ACADEMY_ASSESSMENT_TRAINER_ID BIGINT not null

, SALON_ACADEMY_ASSESSMENT_ID BIGINT

, SALON_ACADEMY_INSTRUCTOR_ID                       STRING 

, SALON_ACADEMY_INSTRUCTOR_NAME                     STRING 

, ASSESSMENT_TRAINER_TEXT                           STRING 

, REGION_ID BIGINT

, REGION_NAME                                       STRING 

, DISTRICT_ID BIGINT

, DISTRICT_NAME                                     STRING 

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/salon_academy_assessment_trainer';

--DISTRIBUTE ON (SALON_ACADEMY_ASSESSMENT_TRAINER_ID)







--*****  Creating table:  "SALON_ACADEMY_TRAINING" , ***** Creating table: "SALON_ACADEMY_TRAINING"


use legacy;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_TRAINING
(
 SALON_ACADEMY_TRAINING_ID BIGINT not null

, SALON_ACADEMY_ID BIGINT

, SALON_ACADEMY_TYPE_ID INT

, SALON_ACADEMY_TYPE_DESC                           STRING 

, SALON_ACADEMY_CLASS                               STRING 

, SALON_ACADEMY_TRAVEL_ID BIGINT

, SALON_ACADEMY_ASSESSMENT_ID BIGINT

, LOCATION_ID INT not null

, EMPLOYEE_ID INT

, DISTRICT_LEADER_NAME                              STRING                 not null

, SALON_SAFETY_CERTIFICATE_DT DATE

, SPLASH_DT DATE

, SPLASH_EXAM_PTS_DT DATE

, SPLASH_OBSERV_PTS_DT DATE

, GROOMING_TOOL_KIT_ID INT

, TRAINING_STATUS_ID INT not null

, TRAINING_STATUS_DESC                              STRING 

, TRAINING_TYPE_ID INT not null

, ENROLLED_TSTMP                                    TIMESTAMP                             not null

, DOG_ACADEMY_COMPLETED_DT DATE

, SAFETY_ALL_E_LEARNING_DT DATE

, SAFETY_ALL_PTS_DT DATE

, SAFETY_SALON_E_LEARNING_DT DATE

, SAFETY_SALON_PTS_DT DATE

, CANCELLED_FLAG INT

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, UPDATE_TSTMP                                      TIMESTAMP                             not null

, LOAD_TSTMP                                        TIMESTAMP                             not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/salon_academy_training';

--DISTRIBUTE ON (SALON_ACADEMY_TRAINING_ID)







--*****  Creating table:  "SALON_ACADEMY_TRAVEL" , ***** Creating table: "SALON_ACADEMY_TRAVEL"


use legacy;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_TRAVEL
(
 SALON_ACADEMY_TRAVEL_ID BIGINT not null

, AIRLINE_DEPART_DT DATE

, AIRLINE_DEPART_TIME_TX                            STRING 

, AIRLINE_RETURN_DT DATE

, AIRLINE_RETURN_TIME_TX                            STRING 

, AIRLINE_SEAT_PREFERENCE                           STRING 

, HOTEL_CHECK_IN_DT DATE

, HOTEL_CHECK_OUT_DT DATE

, LEGAL_NAME                                        STRING 

, PREFERRED_HOTEL                                   STRING 

, PREFERRED_HOTEL_ADDR                              STRING 

, PREFERRED_HOTEL_PHONE_NBR                         STRING 

, PREFERRED_HOTEL_CONTACT                           STRING 

, PREFERRED_HOTEL_RATE                              STRING 

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/salon_academy_travel';

--DISTRIBUTE ON (SALON_ACADEMY_TRAVEL_ID)







--*****  Creating table:  "SALON_ACADEMY_TYPE" , ***** Creating table: "SALON_ACADEMY_TYPE"


use legacy;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_TYPE
(
 SALON_ACADEMY_TYPE_ID INT not null

, SALON_ACADEMY_TYPE_DESC                           STRING 

, UPDATE_TSTMP                                      TIMESTAMP                             not null

, LOAD_TSTMP                                        TIMESTAMP                             not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/salon_academy_type';

--DISTRIBUTE ON (SALON_ACADEMY_TYPE_ID)







--*****  Creating table:  "SALON_ACADEMY_PRE" , ***** Creating table: "SALON_ACADEMY_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_PRE
(
 SALON_ACADEMY_ID BIGINT not null

, LOCATION_ID INT not null

, START_DT DATE

, END_DT DATE

, ACTIVE_FLAG                                       TINYINT                          not null

, SEAT_LIMIT INT

, CANCELLED_FLAG                                    TINYINT                          not null

, TRAINER_EMPLOYEE_ID INT

, TRAINER_NAME                                      STRING 

, SALON_ACADEMY_TYPE_ID INT

, SALON_ACADEMY_TYPE_DESC                           STRING 

, APPROVED_BY_DISTRICT_LEADER_FLAG TINYINT

, LOAD_TSTMP                                        TIMESTAMP                             not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/salon_academy_pre';

--DISTRIBUTE ON (SALON_ACADEMY_ID)







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_PRE" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_PRE
(
 SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_ID BIGINT not null

, SALON_ACADEMY_ASSESSMENT_ID BIGINT

, SALON_ACADEMY_ASSESSMENT_TRAINER_ID BIGINT

, ACTIVITY      STRING 

, ACTIVITY_STATUS                                   STRING 

, ACTIVITY_TSTMP                                    TIMESTAMP 

, ACTIVITY_NOTE                                     STRING 

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/salon_academy_assessment_activity_tracker_pre';

--DISTRIBUTE ON (SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_ID)







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_DENY_REASON_PRE" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_DENY_REASON_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_DENY_REASON_PRE
(
 SALON_ACADEMY_ASSESSMENT_DENY_REASON_ID INT not null

, SALON_ACADEMY_ASSESSMENT_DENY_REASON_DESC         STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/salon_academy_assessment_deny_reason_pre';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE
(
 SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID INT not null

, SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC         STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/salon_academy_assessment_fail_reason_pre';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_PRE" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_PRE
(
 SALON_ACADEMY_ASSESSMENT_ID BIGINT not null

, ASSESSMENT_REQUEST_TSTMP                          TIMESTAMP 

, SCHEDULED_ASSESSMENT_DT DATE

, SCHEDULED_ASSESSMENT_TIME                         TIMESTAMP 

, PRE_ASSESSMENT_COMPLETION_TSTMP                   TIMESTAMP 

, ASSESSMENT_PASS_TSTMP                             TIMESTAMP 

, ASSESSMENT_FAIL_TSTMP                             TIMESTAMP 

, ACADEMY_ENROLLMENT_TSTMP                          TIMESTAMP 

, ASSESSMENT_STATUS                                 STRING 

, ASSESSMENT_COMPLETION_TSTMP                       TIMESTAMP 

, REASON_COMMENT                                    STRING 

, SALON_ACADEMY_ASSESSMENT_DENY_REASON_ID BIGINT

, SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID BIGINT

, ACADEMY_ENROLLMENT_CHANGED_TSTMP                  TIMESTAMP 

, ACADEMY_COMPLETED_TSTMP                           TIMESTAMP 

, SCHEDULE_ASSESSMENT_CHANGED_TSTMP                 TIMESTAMP 

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/salon_academy_assessment_pre';

--DISTRIBUTE ON (SALON_ACADEMY_ASSESSMENT_ID)







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_REASON_PRE" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_REASON_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_REASON_PRE
(
 SALON_ACADEMY_ASSESSMENT_REASON_ID BIGINT not null

, SALON_ACADEMY_ASSESSMENT_ID BIGINT

, REASON_TEXT                                       STRING 

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/salon_academy_assessment_reason_pre';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "SALON_ACADEMY_ASSESSMENT_TRAINER_PRE" , ***** Creating table: "SALON_ACADEMY_ASSESSMENT_TRAINER_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_ASSESSMENT_TRAINER_PRE
(
 SALON_ACADEMY_ASSESSMENT_TRAINER_ID BIGINT not null

, SALON_ACADEMY_ASSESSMENT_ID BIGINT

, SALON_ACADEMY_INSTRUCTOR_ID                       STRING 

, SALON_ACADEMY_INSTRUCTOR_NAME                     STRING 

, ASSESSMENT_TRAINER_TEXT                           STRING 

, REGION_ID BIGINT

, REGION_NAME                                       STRING 

, DISTRICT_ID BIGINT

, DISTRICT_NAME                                     STRING 

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/salon_academy_assessment_trainer_pre';

--DISTRIBUTE ON (SALON_ACADEMY_ASSESSMENT_TRAINER_ID)







--*****  Creating table:  "SALON_ACADEMY_TRAINING_PRE" , ***** Creating table: "SALON_ACADEMY_TRAINING_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_TRAINING_PRE
(
 SALON_ACADEMY_TRAINING_ID BIGINT not null

, SALON_ACADEMY_ID BIGINT

, SALON_ACADEMY_TYPE_ID INT

, SALON_ACADEMY_TYPE_DESC                           STRING 

, SALON_ACADEMY_CLASS                               STRING 

, SALON_ACADEMY_TRAVEL_ID BIGINT

, SALON_ACADEMY_ASSESSMENT_ID BIGINT

, LOCATION_ID INT not null

, EMPLOYEE_ID INT

, DISTRICT_LEADER_NAME                              STRING                 not null

, SALON_SAFETY_CERTIFICATE_DT DATE

, SPLASH_DT DATE

, SPLASH_EXAM_PTS_DT DATE

, SPLASH_OBSERV_PTS_DT DATE

, GROOMING_TOOL_KIT_ID INT

, TRAINING_STATUS_ID INT not null

, TRAINING_STATUS_DESC                              STRING 

, TRAINING_TYPE_ID INT not null

, ENROLLED_TSTMP                                    TIMESTAMP                             not null

, DOG_ACADEMY_COMPLETED_DT DATE

, SAFETY_ALL_E_LEARNING_DT DATE

, SAFETY_ALL_PTS_DT DATE

, SAFETY_SALON_E_LEARNING_DT DATE

, SAFETY_SALON_PTS_DT DATE

, CANCELLED_FLAG INT

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, LOAD_TSTMP                                        TIMESTAMP                             not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/salon_academy_training_pre';

--DISTRIBUTE ON (SALON_ACADEMY_TRAINING_ID)







--*****  Creating table:  "SALON_ACADEMY_TRAVEL_PRE" , ***** Creating table: "SALON_ACADEMY_TRAVEL_PRE"


use raw;
CREATE TABLE  IF NOT EXISTS  SALON_ACADEMY_TRAVEL_PRE
(
 SALON_ACADEMY_TRAVEL_ID BIGINT not null

, AIRLINE_DEPART_DT DATE

, AIRLINE_DEPART_TIME_TX                            STRING 

, AIRLINE_RETURN_DT DATE

, AIRLINE_RETURN_TIME_TX                            STRING 

, AIRLINE_SEAT_PREFERENCE                           STRING 

, HOTEL_CHECK_IN_DT DATE

, HOTEL_CHECK_OUT_DT DATE

, LEGAL_NAME                                        STRING 

, PREFERRED_HOTEL                                   STRING 

, PREFERRED_HOTEL_ADDR                              STRING 

, PREFERRED_HOTEL_PHONE_NBR                         STRING 

, PREFERRED_HOTEL_CONTACT                           STRING 

, PREFERRED_HOTEL_RATE                              STRING 

, CREATED_TSTMP                                     TIMESTAMP 

, CREATED_BY                                        STRING 

, MODIFIED_TSTMP                                    TIMESTAMP 

, MODIFIED_BY                                       STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/salon_academy_travel_pre';

--DISTRIBUTE ON (SALON_ACADEMY_TRAVEL_ID)



