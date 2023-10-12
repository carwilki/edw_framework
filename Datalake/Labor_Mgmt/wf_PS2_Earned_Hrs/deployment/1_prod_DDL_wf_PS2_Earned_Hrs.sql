
--*****  Creating table:  "PS2_EARNED_HRS" , ***** Creating table: "PS2_EARNED_HRS"
use legacy;
CREATE TABLE PS2_EARNED_HRS
(
 DAY_DT        TIMESTAMP                             not null
, LOCATION_ID INT not null
, WFA_BUSN_AREA_ID SMALLINT not null
, WFA_DEPT_ID SMALLINT not null
, WFA_TASK_ID SMALLINT not null
, STORE_NBR INT
, WFA_BUSN_AREA_DESC                                STRING 
, WFA_DEPT_DESC                                     STRING 
, WFA_TASK_DESC                                     STRING 
, EARNED_HRS                                        DECIMAL(16,6) 
, UPDATE_DT     TIMESTAMP 
, LOAD_DT       TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/labor_mgmt/ps2_earned_hrs';
--DISTRIBUTE ON (LOCATION_ID)
--*****  Creating table:  "PS2_EARNED_HRS_PRE" , ***** Creating table: "PS2_EARNED_HRS_PRE"

use raw;
CREATE TABLE PS2_EARNED_HRS_PRE
(
 DAY_DT        TIMESTAMP                             not null
, ORG_IDS_ID                                        BIGINT                         not null
, EARNED_HRS                                        DECIMAL(16,6) 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/labor_mgmt/ps2_earned_hrs_pre';
--DISTRIBUTE ON (ORG_IDS_ID)