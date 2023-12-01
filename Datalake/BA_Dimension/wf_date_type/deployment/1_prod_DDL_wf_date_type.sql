




--*****  Creating table:  "DATE_TYPE_LKUP" , ***** Creating table: "DATE_TYPE_LKUP"


use legacy;
 CREATE TABLE IF EXISTS  DATE_TYPE_LKUP 
(
 DATE_TYPE_ID BIGINT not null

, DATE_TYPE_DESC                            STRING 

, DATE_TYPE_SORT_ID INT not null

, DATE_TYPE_DESC2                           STRING 

, DATE_TYPE_DESC3                           STRING 

, DATE_TYPE_5WK_STATUS                      STRING 

, TW_LW_FLAG SMALLINT

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/date_type_lkup';

--DISTRIBUTE ON (DATE_TYPE_ID)







--*****  Creating table:  "DATE_TYPE_DAY" , ***** Creating table: "DATE_TYPE_DAY"


use legacy;
 CREATE TABLE IF EXISTS  DATE_TYPE_DAY 
(
 DATE_TYPE_ID BIGINT not null

, DAY_DT                                    TIMESTAMP                            not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/date_type_day';

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "DATE_TYPE_WEEK" , ***** Creating table: "DATE_TYPE_WEEK"


use legacy;
 CREATE TABLE IF EXISTS  DATE_TYPE_WEEK 
(
 DATE_TYPE_ID BIGINT not null

, WEEK_DT                                   TIMESTAMP                            not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/date_type_week';

--DISTRIBUTE ON (WEEK_DT)





