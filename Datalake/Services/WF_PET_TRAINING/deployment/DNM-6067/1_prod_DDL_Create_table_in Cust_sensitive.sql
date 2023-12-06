-- Databricks notebook source
use cust_sensitive;
CREATE TABLE  TRAINING_PET_PRE

(PET_ID INT not null

, PROVIDER_ID                STRING

, NAME                       STRING

, BREED                      STRING

, BIRTH_DATE DATE

, NOTES                      STRING

, CREATE_DATE_TIME           TIMESTAMP

, EXTERNAL_PET_ID BIGINT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta
LOCATION 'gs://petm-bdpl-prod-cust-sensitive-raw-p1-gcs-gbl/services/training_pet_pre' 

-- COMMAND ----------


--*****  Creating table:  "TRAINING_PET" , ***** Creating table: "TRAINING_PET"

use cust_sensitive;
CREATE TABLE legacy_TRAINING_PET
( TRAINING_PET_ID INT not null

, TRAINING_EXT_PET_ID BIGINT

, TRAINING_PROVIDER_ID     STRING 

, TRAINING_PET_NAME        STRING 

, PET_BREED                STRING 

, PET_BIRTH_DT DATE

, NOTES                    STRING 

, SRC_CREATE_TSTMP         TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-cust-sensitive-nzlegacy-p1-gcs-gbl/services/training_pet';



-- COMMAND ----------

-- DBTITLE 1,Copy from old table in legacy schema to the new cust_sensitive table
INSERT INTO cust_sensitive.legacy_TRAINING_PET
select * from legacy.TRAINING_PET;
