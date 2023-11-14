
--*****  Creating table:  "GS_TRAINING" , ***** Creating table: "GS_TRAINING"


use legacy;
 CREATE TABLE  GS_TRAINING 
(  GS_TRAINING_ID BIGINT not null

, GS_TRAVEL_ID BIGINT

, HOME_LOCATION_ID INT

, HOME_STORE_NBR INT

, EMPLOYEE_ID INT

, EMPLOYEE_FIRST_NAME                         STRING 

, EMPLOYEE_LAST_NAME                          STRING 

, NEW_GS_POSITION_ID INT

, PREV_GS_POSITION_ID INT

, GS_SALON_ACADEMY_ID BIGINT

, DISTRICT_MANAGER_NAME                       STRING 

, SALON_SAFETY_CERT_DT                        TIMESTAMP 

, SPLASH_DT                                   TIMESTAMP 

, TRAINING_LOCATION_ID INT

, TRAINING_STORE_NBR INT

, HS_TRAINING_START_DT                        TIMESTAMP 

, HS_TRAINING_END_DT                          TIMESTAMP 

, TRAINING_START_DT                           TIMESTAMP 

, TRAINING_END_DT                             TIMESTAMP 

, GROOMING_TOOL_KIT INT

, HOTEL_PARTNER_LOCATION_ID INT

, HOTEL_PARTNER_STORE_NBR INT

, HOTEL_PARTNER_TRAINING_START_DT             TIMESTAMP 

, HOTEL_PARTNER_TRAINING_END_DT               TIMESTAMP 

, GS_TRAINING_STATUS_ID INT

, GS_TRAINING_TYPE_ID INT

, SYS_CREATED_TSTMP                           TIMESTAMP 

, SYS_CREATED_BY                              STRING 

, SYS_MODIFIED_TSTMP                          TIMESTAMP 

, SYS_MODIFIED_BY                             STRING 

, UPDATE_DT                                   TIMESTAMP 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/gs_training';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GS_TRAVEL" , ***** Creating table: "GS_TRAVEL"


use empl_sensitive;
 CREATE TABLE  legacy_GS_TRAVEL 
(  GS_TRAVEL_ID BIGINT not null

, ASSOC_CONTACT_PHN                           STRING 

, HOTEL_PARTNER_STORE_CITY_TO                 STRING 

, AIRLINE_DEPART_DT                           TIMESTAMP 

, ARILINE_DEPART_TM                           STRING 

, AIRLINE_RETURN_DT                           TIMESTAMP 

, ARILINE_RETURN_TM                           STRING 

, AIRLINE_SEAT_PREFERENCE                     STRING 

, HOTEL_CHECKIN_DT                            TIMESTAMP 

, HOTEL_CHECKOUT_DT                           TIMESTAMP 

, HOTEL_SMOKING_TEXT                          STRING 

, CAR_PICKUP_DT                               TIMESTAMP 

, CAR_PICKUP_TM                               TIMESTAMP 

, CAR_RETURN_DT                               TIMESTAMP 

, DRIVERS_LICENSE                             STRING 

, LEGAL_NAME                                  STRING 

, BIRTH_DT                                    TIMESTAMP 

, GENDER TINYINT

, PREFERRED_HOTEL                             STRING 

, PREFERRED_HOTEL_ADDRESS                     STRING 

, PREFERRED_HOTEL_PHONE                       STRING 

, PREFERRED_HOTEL_CONTACT                     STRING 

, PREFERRED_HOTEL_RATE                        STRING 

, PREFERRED_ROOMMATE                          STRING 

, HOME_STORE_CITY_TO                          STRING 

, TRAINING_STORE_CITY_TO                      STRING 

, SYS_MODIFIED_TSTMP                          TIMESTAMP 

, SYS_MODIFIED_BY                             STRING 

, UPDATE_DT                                   TIMESTAMP 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-empl-sensitive-nzlegacy-p1-gcs-gbl/BA_Education/gs_travel';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GS_POSITION" , ***** Creating table: "GS_POSITION"


use legacy;
 CREATE TABLE  GS_POSITION 
(  GS_POSITION_ID INT not null

, GS_POSITION_NAME                            STRING 

, GS_TRAINING_TYPE_ID INT

, GS_POSITION_TYPE                            STRING 

, UPDATE_DT                                   TIMESTAMP 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/gs_position';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GS_TRAINING_TYPE" , ***** Creating table: "GS_TRAINING_TYPE"


use legacy;
 CREATE TABLE  GS_TRAINING_TYPE 
(  GS_TRAINING_TYPE_ID INT not null

, GS_TRAINING_TYPE_NAME                       STRING 

, CUT_OFF_DAYS INT

, IS_ACTIVE TINYINT

, UPDATE_DT                                   TIMESTAMP 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/gs_training_type';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "GS_TRAINING_STATUS" , ***** Creating table: "GS_TRAINING_STATUS"


use legacy;
 CREATE TABLE  GS_TRAINING_STATUS 
(  GS_TRAINING_STATUS_ID INT not null

, GS_TRAINING_STATUS_NAME                     STRING 

, UPDATE_DT                                   TIMESTAMP 

, LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/gs_training_status';

--DISTRIBUTE ON RANDOM

