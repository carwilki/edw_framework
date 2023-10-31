




--*****  Creating table:  "STORE_LAT_LONG_PRE" , ***** Creating table: "STORE_LAT_LONG_PRE"


use raw;
CREATE TABLE  STORE_LAT_LONG_PRE
(
 STORE_NBR     DECIMAL(18,13)                        not null

, LAT           DECIMAL(12,6) 

, LON           DECIMAL(12,6) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Dimension/store_lat_long_pre' ;

--DISTRIBUTE ON RANDOM


