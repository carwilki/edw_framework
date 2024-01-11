
--*****  Creating table:  "EDU_CERT_SITE_DAILY" , ***** Creating table: "EDU_CERT_SITE_DAILY"


use legacy;
 CREATE TABLE  EDU_CERT_SITE_DAILY 
(  DAY_DT                                      TIMESTAMP                            not null
,LOCATION_ID INT not null
,ASSESSMENT_MID BIGINT not null
,ASSESSMENT_LID BIGINT not null
,EMPL_CNT BIGINT
,COMPLIANT_EMPL_CNT BIGINT
,EMPL_ATTEMPTS_CNT BIGINT
,COMPLIANT_EMPL_ATTEMPS_CNT BIGINT
,LOAD_DT                                     TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Education/edu_cert_site_daily';

--DISTRIBUTE ON (LOCATION_ID)
