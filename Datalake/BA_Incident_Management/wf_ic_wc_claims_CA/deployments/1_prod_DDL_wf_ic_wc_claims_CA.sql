
--*****  Creating table:  "IC_CA_CLAIMS" , ***** Creating table: "IC_CA_CLAIMS"
use legacy;
CREATE TABLE  IC_CA_CLAIMS
(
 LOAD_DT                                     TIMESTAMP                            not null
, CLAIM_NBR                                   STRING                not null
, STORE_NBR                                   STRING 
, GEO_LOCATION_NAME                           STRING 
, JURISDICTION                                STRING 
, LOCATION_NAME                               STRING 
, CLASSIFICATION                              STRING 
, EMPLOYEE_NAME                               STRING 
, INJURED_DT                                  TIMESTAMP 
, REPORTED_DT                                 TIMESTAMP 
, OPENED_DT                                   TIMESTAMP 
, DESCRIPTION                                 STRING 
, BODY_PART                                   STRING 
, NATYRE_OF_INJURY_DESC                       STRING 
, CLAIM_STATUS                                STRING 
, CASE_MANAGER                                STRING 
, CURRENT_STATUS                              STRING 
, ACCIDENT_LOCATION                           STRING 
, EMPLOYEE_JOB_TITLE                          STRING 
, DEPARTMENT                                  STRING 
, EMPLOYEE_ID                                 STRING 
, CLAIMANT_ACTIVITY_AT_INCIDENT               STRING 
, ANIMAL_INCIDENT_TYPE                        STRING 
, PET_BREED_TYPE                              STRING 
, TIME_OF_INCIDENT                            STRING 
, ANIMAL_CARE_CUSTODY                         STRING 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/incident_management/ic_ca_claims';

--DISTRIBUTE ON (CLAIM_NBR)
--*****  Creating table:  "IC_WC_CLAIMS" , ***** Creating table: "IC_WC_CLAIMS"
use empl_protected;
CREATE TABLE  legacy_IC_WC_CLAIMS
(
CLAIM_NBR                                   STRING                not null
, DAY_DT                                      TIMESTAMP                            not null
, LOCATION_ID INT not null
, SOURCE_TYPE_ID                              TINYINT 
, LINE_TYPE_CD                                STRING 
, ANIMAL_IND                                  TINYINT 
, CLAIM_SUB_STATUS_CD                         STRING 
, INCURRED_EXP_AMT                            DECIMAL(11,2) 
, INCURRED_MEDICAL_AMT                        DECIMAL(11,2) 
, INCURRED_INDEM_LOSS_AMT                     DECIMAL(11,2) 
, INCURRED_OTHER_EXP_AMT                      DECIMAL(11,2) 
, INCURRED_TOTAL_AMT                          DECIMAL(11,2) 
, LOAD_DT                                     TIMESTAMP 
, UPDATE_DT                                   TIMESTAMP 
)
USING delta 
LOCATION 'gs://petm-bdpl-prod-empl-protected-nzlegacy-p1-gcs-gbl/incident_management/ic_wc_claims'; 
--DISTRIBUTE ON (CLAIM_NBR)