
use empl_sensitive;

CREATE TABLE REFINE_HONORARY_DESIGNEE

(

     HD_ID INT not null,

     HD_TYPE_CD STRING not null,

     HD_TYPE_DESC STRING,

     HD_FIRST_NAME STRING,

     HD_LAST_NAME STRING,

     HD_SUFFIX STRING,

     HD_BIRTH_MON INT,

     HD_BIRTH_DAY INT,

     HD_SPOUSE_FIRST_NAME STRING,

     HD_SPOUSE_LAST_NAME STRING,

     HD_ADDRESS  STRING,

     HD_CITY STRING,

     HD_STATE    STRING,

     HD_ZIP_CD   STRING,

     HD_COUNTRY    STRING,

     HD_PHONE_NBR BIGINT,

     HD_EXP_DT DATE,

     UPDATE_TSTMP TIMESTAMP,

     LOAD_TSTMP TIMESTAMP

) USING delta

LOCATION 'gs://petm-bdpl-prod-empl-sensitive-refine-p1-gcs-gbl/services/refine_honorary_designee' ;

