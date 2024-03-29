
use legacy;

CREATE VIEW pods_pet (
  PODS_PET_ID,
  PET_NM,
  BIRTH_DT,
  PETM_GENDER_ID,
  PETM_PET_SPECIES_ID,
  PETM_PET_BREED_ID,
  MIXED_BREED_FLG,
  PETM_PET_COLOR_ID,
  MARKING,
  WEIGHT,
  CHILD_FLG,
  SPAYED_NEUTERED_FLG,
  ADOPTION_FLG,
  DO_NOT_BOOK_FLG,
  DO_NOT_BOOK_REASON,
  SERVICE_RESTRICTIONS,
  MEDICAL_CONDITIONS,
  MIGRATED_SMS_PET_ID,
  ACTIVE_FLAG,
  PODS_CREATE_SRC_ID,
  PODS_UPDATE_SRC_ID,
  PODS_CREATE_TSTMP,
  PODS_UPDATE_TSTMP,
  UPDATE_TSTMP,
  LOAD_TSTMP)
AS SELECT 
PET_ID AS PODS_PET_ID
,PET_NAME AS PET_NM
,BIRTH_DATE AS BIRTH_DT
,GENDER_ID AS PETM_GENDER_ID
,SPECIES_ID AS PETM_PET_SPECIES_ID
,BREED_ID AS PETM_PET_BREED_ID
,MIXED_BREED_FLAG AS MIXED_BREED_FLG
,COLOR_ID AS PETM_PET_COLOR_ID
,MARKINGS AS MARKING
,WEIGHT AS WEIGHT
,CHILD_FLAG AS CHILD_FLG
,SPAYED_NEUTERED_FLAG AS SPAYED_NEUTERED_FLG
,ADOPTION_FLAG AS ADOPTION_FLG
,DO_NOT_BOOK_FLAG AS DO_NOT_BOOK_FLG
,DO_NOT_BOOK_REASON AS DO_NOT_BOOK_REASON
,SERVICE_RESTRICTIONS AS SERVICE_RESTRICTIONS
,MEDICAL_CONDITIONS AS MEDICAL_CONDITIONS
,MIGRATED_SMS_ID AS MIGRATED_SMS_PET_ID
,ACTIVE_FLAG AS ACTIVE_FLAG
,CREATE_SOURCE_ID AS PODS_CREATE_SRC_ID
,UPDATE_SOURCE_ID AS PODS_UPDATE_SRC_ID
,CREATE_DT_TM AS PODS_CREATE_TSTMP
,UPDATE_DT_TM AS PODS_UPDATE_TSTMP
,current_timestamp() AS UPDATE_TSTMP
,current_timestamp() AS LOAD_TSTMP
from refine.pods_pet;