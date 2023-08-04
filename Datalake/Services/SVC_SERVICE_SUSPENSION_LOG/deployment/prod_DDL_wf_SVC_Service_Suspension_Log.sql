--*****  Creating table:  "SVC_SERVICE_SUSPENSION_LOG" , ***** Creating table: "SVC_SERVICE_SUSPENSION_LOG"


use legacy;
CREATE TABLE  SVC_SERVICE_SUSPENSION_LOG
( DAY_DT       DATE                                 not null

, LOCATION_ID INT not null

, SERVICE_AREA_ID TINYINT not null

, SUSPENSION_REASON_ID TINYINT not null

, SUBMITTED_BY                                      STRING 

, SUBMITTED_BY_POSITION_ID TINYINT not null

, COMMENTS      STRING 

, REVERSAL_TSTMP                                    TIMESTAMP 

, CREATE_TSTMP                                      TIMESTAMP 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/services/svc_service_suspension_log' 
 TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (DAY_DT)


--*****  Creating table:  "SVC_SUSPENSION_SUBMITTER_TITLE" , ***** Creating table: "SVC_SUSPENSION_SUBMITTER_TITLE"


use legacy;
CREATE TABLE  SVC_SUSPENSION_SUBMITTER_TITLE
( SUBMITTED_BY_POSITION_ID TINYINT not null

, SUBMITTED_BY_POSITION_DESC                        STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/services/svc_suspension_submitter_title' 
 TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (SUBMITTED_BY_POSITION_ID)


--*****  Creating table:  "SVC_SERVICE_AREA" , ***** Creating table: "SVC_SERVICE_AREA"


use legacy;
CREATE TABLE  SVC_SERVICE_AREA
( SERVICE_AREA_ID TINYINT not null

, SERVICE_AREA_DESC                                 STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/services/svc_service_area' 
 TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (SERVICE_AREA_ID)


--*****  Creating table:  "SVC_SERVICE_SUSPENSION_REASON" , ***** Creating table: "SVC_SERVICE_SUSPENSION_REASON"


use legacy;
CREATE TABLE  SVC_SERVICE_SUSPENSION_REASON
( SUSPENSION_REASON_ID TINYINT not null

, SUSPENSION_REASON_DESC                            STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/services/svc_service_suspension_reason' 
 TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (SUSPENSION_REASON_ID)


