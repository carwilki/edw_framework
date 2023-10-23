

--*****  Creating table:  "SITE_HOURS_DAY_PRE" , ***** Creating table: "SITE_HOURS_DAY_PRE"


use raw;
CREATE TABLE  SITE_HOURS_DAY_PRE
(
 DAY_DT        TIMESTAMP 
, LOCATION_NBR                                      STRING 
, LOCATION_TYPE_ID TINYINT
, BUSINESS_AREA                                     STRING 
, OPEN_TSTMP                                        TIMESTAMP 
, CLOSE_TSTMP                                       TIMESTAMP 
, IS_CLOSED SMALLINT
, LOAD_TSTMP                                        TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/dimension/site_hours_day_pre' 
;

--DISTRIBUTE ON (LOCATION_NBR)

--*****  Creating table:  "SITE_HOURS_DAY" , ***** Creating table: "SITE_HOURS_DAY"


use legacy;
CREATE TABLE  SITE_HOURS_DAY
(
 DAY_DT       DATE                                 not null
, LOCATION_ID INT not null
, BUSINESS_AREA                                     STRING                 not null
, LOCATION_TYPE_ID TINYINT
, STORE_NBR INT
, CLOSE_FLAG                                        TINYINT 
, TIME_ZONE     STRING 
, OPEN_TSTMP                                        TIMESTAMP 
, CLOSE_TSTMP                                       TIMESTAMP 
, UPDATE_TSTMP                                      TIMESTAMP 
, LOAD_TSTMP                                        TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/dimension/site_hours_day' 
;

--DISTRIBUTE ON (LOCATION_ID)

--ORGANIZE   ON (DAY_DT)

