




--*****  Creating table:  "TS_ACTIVITY" , ***** Creating table: "TS_ACTIVITY"


use legacy;
CREATE TABLE IF NOT EXISTS TS_ACTIVITY
(
 TS_ACTIVITY_ID INT not null

, TS_ACTIVITY_NAME                                  STRING 

, TS_ACTIVITY_DESC                                  STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP                             

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/ts_activity';


--DISTRIBUTE ON (TS_ACTIVITY_ID)







--*****  Creating table:  "TS_ACTIVITY_CATEGORY" , ***** Creating table: "TS_ACTIVITY_CATEGORY"


use legacy;
CREATE TABLE IF NOT EXISTS TS_ACTIVITY_CATEGORY
(
 TS_ACTIVITY_CAT_ID INT not null

, TS_ACTIVITY_CAT_DESC                              STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP                             

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/ts_activity_category';


--DISTRIBUTE ON (TS_ACTIVITY_CAT_ID)







--*****  Creating table:  "TS_ACTIVITY_TYPE" , ***** Creating table: "TS_ACTIVITY_TYPE"


use legacy;
CREATE TABLE IF NOT EXISTS TS_ACTIVITY_TYPE
(
 TS_ACTIVITY_TYPE_ID INT not null

, TS_ACTIVITY_TYPE_DESC                             STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP                             

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/ts_activity_type';


--DISTRIBUTE ON (TS_ACTIVITY_TYPE_ID)







--*****  Creating table:  "TS_ACTIVITY_XREF" , ***** Creating table: "TS_ACTIVITY_XREF"


use legacy;
CREATE TABLE IF NOT EXISTS TS_ACTIVITY_XREF
(
 TS_ACTIVITY_XREF_ID INT not null

, TS_ACTIVITY_ID INT

, TS_ACTIVITY_TYPE_ID INT

, TS_ACTIVITY_CAT_ID INT

, TS_ACTIVITY_STATUS_ID INT

, TS_RFC_DESC                                       STRING 

, TS_UDH_WORK_ASSIGN_CD                             STRING 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/ts_activity_xref';

--DISTRIBUTE ON (TS_ACTIVITY_XREF_ID)







--*****  Creating table:  "TS_EMPLOYEE_TIME" , ***** Creating table: "TS_EMPLOYEE_TIME"


use legacy;
CREATE TABLE IF NOT EXISTS TS_EMPLOYEE_TIME
(
 TS_DAY_DT    DATE                                 not null

, EMPLOYEE_ID INT not null

, TS_ACTIVITY_XREF_ID INT not null

, TS_ACTIVITY_ID INT

, TS_ACTIVITY_NAME                                  STRING 

, TS_ACTIVITY_DESC                                  STRING 

, TS_ACTIVITY_CAT_ID INT

, TS_ACTIVITY_CAT_DESC                              STRING 

, TS_ACTIVITY_TYPE_ID INT

, TS_ACTIVITY_TYPE_DESC                             STRING 

, TS_RFC_DESC                                       STRING 

, TS_WORK_ASSIGN_CD                                 STRING 

, TS_WORK_HOURS                                     DECIMAL(4,2) 

, TS_COMMENT                                        STRING 

, TS_RECORD_CREATE_DT DATE

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP                             

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/financials/ts_employee_time';



--DISTRIBUTE ON (TS_DAY_DT, EMPLOYEE_ID, TS_ACTIVITY_XREF_ID)







--*****  Creating table:  "TS_ACTIVITY_PRE" , ***** Creating table: "TS_ACTIVITY_PRE"


use raw;
CREATE TABLE IF NOT EXISTS TS_ACTIVITY_PRE
(
 ACTIVITYID INT not null

, ACTIVITYNAME                                      STRING 

, ACTIVITYDESC                                      STRING 

, CREATORID INT

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/ts_activity_pre';

--DISTRIBUTE ON (ACTIVITYID)







--*****  Creating table:  "TS_ACTIVITY_CATEGORY_PRE" , ***** Creating table: "TS_ACTIVITY_CATEGORY_PRE"


use raw;
CREATE TABLE IF NOT EXISTS TS_ACTIVITY_CATEGORY_PRE
(
 ACTCATEGORYID INT not null

, ACTCATDESC                                        STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/ts_activity_category_pre';


--DISTRIBUTE ON (ACTCATEGORYID)







--*****  Creating table:  "TS_ACTIVITY_TYPE_PRE" , ***** Creating table: "TS_ACTIVITY_TYPE_PRE"


use raw;
CREATE TABLE IF NOT EXISTS TS_ACTIVITY_TYPE_PRE
(
 ACTTYPEID INT not null

, ACTIVITYTYPE                                      STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/ts_activity_type_pre';


--DISTRIBUTE ON (ACTTYPEID)







--*****  Creating table:  "TS_ACTIVITY_XREF_PRE" , ***** Creating table: "TS_ACTIVITY_XREF_PRE"


use raw;
CREATE TABLE IF NOT EXISTS TS_ACTIVITY_XREF_PRE
(
 ACTXREFID INT not null

, ACTIVITYID INT

, ACTTYPEID INT

, ACTCATEGORYID INT

, ACTSTATUSID INT

, RFCNBR        STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/ts_activity_xref_pre';


--DISTRIBUTE ON (ACTXREFID)







--*****  Creating table:  "TS_EMPLOYEE_TIME_PRE" , ***** Creating table: "TS_EMPLOYEE_TIME_PRE"


use raw;
CREATE TABLE IF NOT EXISTS TS_EMPLOYEE_TIME_PRE
(
 DAYDT        DATE                                 not null

, EMPID INT not null

, ACTXREFID INT not null

, HOURS         DECIMAL(4,2) 

, RFCNBR        STRING 

, CREATEDATE DATE

, COMMENT       STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/financials/ts_employee_time_pre';


--DISTRIBUTE ON (DAYDT, EMPID, ACTXREFID)


