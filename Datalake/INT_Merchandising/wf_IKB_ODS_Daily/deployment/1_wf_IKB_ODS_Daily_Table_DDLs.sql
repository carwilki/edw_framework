-- ZTB_POG_GRP_RLS_PRE
-- POG_FLOOR_VERSION
-- FLOOR_FIXTURE
-- POG_FLOOR_FIXTURE_SECTION
-- FLOORPLAN_VERSION
-- POG_GROUP_INCLUSION_LIST
-- POG_GROUP_EXCLUSION_LIST
-- IKB_STATUS
-- POG_GROUP_VERSION

--*****  Creating table:  "POG_FLOOR_VERSION" , ***** Creating table: "POG_FLOOR_VERSION"
use legacy;
CREATE TABLE POG_FLOOR_VERSION
(
 FLOORPLAN_DBKEY INT not null
, POG_DBKEY INT not null
, FLOORPLAN_NM  STRING 
, FLOORPLAN_LOCATION STRING 
, POG_IKB_STATUS_ID INT
, FLOORPLAN_IKB_STATUS_ID INT
, DB_TIME       TIMESTAMP 
, DB_USER       STRING 
, LOAD_TSTMP    TIMESTAMP                           
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/merchandising/pog_floor_version';
--DISTRIBUTE ON (POG_DBKEY, FLOORPLAN_DBKEY)
--ORGANIZE   ON (FLOORPLAN_DBKEY, POG_DBKEY)
--*****  Creating table:  "FLOOR_FIXTURE" , ***** Creating table: "FLOOR_FIXTURE"
use legacy;
CREATE TABLE FLOOR_FIXTURE
(
 FLOORPLAN_DBKEY INT not null
, FIXTURE_DBKEY INT not null
, LOAD_TSTMP TIMESTAMP                         
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/merchandising/floor_fixture';
--DISTRIBUTE ON (FLOORPLAN_DBKEY)
--ORGANIZE   ON (FLOORPLAN_DBKEY)
--*****  Creating table:  "POG_FLOOR_FIXTURE_SECTION" , ***** Creating table: "POG_FLOOR_FIXTURE_SECTION"
use legacy;
CREATE TABLE POG_FLOOR_FIXTURE_SECTION
(
 FLOORPLAN_DBKEY INT not null
, POG_DBKEY INT not null
, FIXTURE_DBKEY INT not null
, SECTION_DBKEY INT not null
, DB_TIME       TIMESTAMP 
, DB_USER       STRING 
, LOAD_TSTMP TIMESTAMP                            
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/merchandising/pog_floor_fixture_section';
--DISTRIBUTE ON (FLOORPLAN_DBKEY)
--ORGANIZE   ON (FLOORPLAN_DBKEY)
--*****  Creating table:  "FLOORPLAN_VERSION" , ***** Creating table: "FLOORPLAN_VERSION"
use legacy;
CREATE TABLE  FLOORPLAN_VERSION
(
 FLOORPLAN_DBKEY INT not null
, FLOORPLAN_EFF_FROM_DT                      DATE                                not null
, FLOORPLAN_EFF_TO_DT DATE
, FLOORPLAN_NM                                STRING 
, FLOORPLAN_LOCATION                          STRING 
, DB_VERSION_KEY INT
, FLOORPLAN_TYPE                              STRING 
, FLOORPLAN_IKB_STATUS_ID INT
, DB_TIME TIMESTAMP 
, DB_USER STRING 
, LOAD_TSTMP TIMESTAMP                          
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/merchandising/floorplan_version';
--DISTRIBUTE ON (FLOORPLAN_DBKEY)
--ORGANIZE   ON (FLOORPLAN_DBKEY)
--*****  Creating table:  "POG_GROUP_EXCLUSION_LIST" , ***** Creating table: "POG_GROUP_EXCLUSION_LIST"
use legacy;
CREATE TABLE POG_GROUP_EXCLUSION_LIST
(
 FLOORPLAN_NM  STRING not null
, POG_GROUP_DESC STRING  not null
, DELETE_TSTMP TIMESTAMP 
, LOAD_TSTMP TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/merchandising/pog_group_exclusion_list';
--DISTRIBUTE ON RANDOM
--*****  Creating table:  "POG_GROUP_INCLUSION_LIST" , ***** Creating table: "POG_GROUP_INCLUSION_LIST"
use legacy;
CREATE TABLE POG_GROUP_INCLUSION_LIST
(
 FLOORPLAN_LOCATION STRING not null
, POG_GROUP_DESC STRING not null
, DELETE_TSTMP TIMESTAMP 
, LOAD_TSTMP TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/merchandising/pog_group_inclusion_list';
--DISTRIBUTE ON RANDOM
--*****  Creating table:  "POG_GROUP_VERSION" , ***** Creating table: "POG_GROUP_VERSION"
use legacy;
CREATE TABLE  POG_GROUP_VERSION
(
 POG_DBKEY INT not null
, POG_GROUP_ID INT not null
, POG_GROUP_DESC STRING 
, DELETE_TSTMP TIMESTAMP 
, UPDATE_TSTMP TIMESTAMP 
, LOAD_TSTMP TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/merchandising/pog_group_version';
--DISTRIBUTE ON RANDOM
--*****  Creating table:  "IKB_STATUS" , ***** Creating table: "IKB_STATUS"

use legacy;
CREATE TABLE IKB_STATUS
(
 IKB_STATUS_ID INT not null
, IKB_STATUS_DESC STRING not null
, UPDATE_TSTMP TIMESTAMP 
, LOAD_TSTMP TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/merchandising/ikb_status';

--DISTRIBUTE ON (POG_GROUP_ID)
--ORGANIZE   ON (POG_GROUP_ID)
--*****  Creating table:  "ZTB_POG_GRP_RLS_PRE" , ***** Creating table: "ZTB_POG_GRP_RLS_PRE"
use raw;
CREATE TABLE ZTB_POG_GRP_RLS_PRE
(
 MANDT STRING not null
, POG_GROUP STRING not null
, SALES_ORG STRING  not null
, FLRPLN_EXCLUSION STRING 
, CREATED_ON TIMESTAMP 
, CREATED_TIME TIMESTAMP 
, CREATED_BY STRING 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/merchandising/ztb_pog_grp_rls_pre';
--DISTRIBUTE ON RANDOM