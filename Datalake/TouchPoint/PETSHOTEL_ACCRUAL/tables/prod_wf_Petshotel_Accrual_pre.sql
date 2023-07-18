




--*****  Creating table:  "PETSHOTEL_EXCH_RATE_PRE" , ***** Creating table: "PETSHOTEL_EXCH_RATE_PRE"

use raw;
CREATE TABLE  PETSHOTEL_EXCH_RATE_PRE
(
  DAY_DT        TIMESTAMP                             not null

, COUNTRY_CD                                        STRING                          not null

, EXCH_RATE_PCT                                     DECIMAL(9,6) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/touchpoint/petshotel_exch_rate_pre'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

--DISTRIBUTE ON (DAY_DT)


