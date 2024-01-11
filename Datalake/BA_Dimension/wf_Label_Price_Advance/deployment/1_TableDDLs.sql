--*****  Creating table:  "ZTB_ADV_LBL_CHGS_PRE" , ***** Creating table: "ZTB_ADV_LBL_CHGS_PRE"


use raw;
 CREATE TABLE  ZTB_ADV_LBL_CHGS_PRE 
(
 MANDT         STRING                  not null

, EFFECTIVE_DATE                                    STRING                  not null

, ARTICLE       STRING                 not null

, SITE          STRING                  not null

, POG_TYPE      STRING                  not null

, LABEL_SIZE                                        STRING                  not null

, LABEL_TYPE                                        STRING                  not null

, EXP_LABEL_TYPE                                    STRING                  not null

, SUPPRESS_IND                                      STRING 

, NUM_LABELS                                        STRING 

, CREATE_DATE                                       STRING 

, ENH_LBL_ID                                        STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Dimension/ztb_adv_lbl_chgs_pre';

--DISTRIBUTE ON (MANDT, EFFECTIVE_DATE, ARTICLE, SITE)

--*****  Creating table:  "LABEL_DAY_STORE_SKU" , ***** Creating table: "LABEL_DAY_STORE_SKU"


use legacy;
 CREATE TABLE  LABEL_DAY_STORE_SKU 
(
 LABEL_CHANGE_DT                                  DATE                                not null

, LOCATION_ID INT not null

, PRODUCT_ID INT not null

, ACTUAL_FLAG INT not null

, LABEL_POG_TYPE_CD                                 STRING                 not null

, LABEL_SIZE_ID                                     STRING                 not null

, LABEL_TYPE_ID INT not null

, EXPIRATION_FLAG INT not null

, SKU_NBR INT

, STORE_NBR INT

, WEEK_DT DATE

, FISCAL_WK INT

, FISCAL_MO INT

, FISCAL_YR INT

, SUPPRESSED_FLAG INT

, LABEL_CNT INT

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/label_day_store_sku';

--DISTRIBUTE ON (LABEL_CHANGE_DT, LOCATION_ID, PRODUCT_ID, ACTUAL_FLAG)

--*****  Creating table:  "LABEL_SIZE" , ***** Creating table: "LABEL_SIZE"


use legacy;
 CREATE TABLE  LABEL_SIZE 
(
 LABEL_SIZE_ID                                     STRING                 not null

, LABEL_SIZE_DESC                                   STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/label_size';

--DISTRIBUTE ON (LABEL_SIZE_ID)

--*****  Creating table:  "LABEL_TYPE" , ***** Creating table: "LABEL_TYPE"


use legacy;
 CREATE TABLE  LABEL_TYPE 
(
 LABEL_TYPE_ID TINYINT not null

, LABEL_TYPE_DESC                                   STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/label_type';

--DISTRIBUTE ON (LABEL_TYPE_ID)

--*****  Creating table:  "LABEL_POG_TYPE" , ***** Creating table: "LABEL_POG_TYPE"


use legacy;
 CREATE TABLE  LABEL_POG_TYPE 
(
 LABEL_POG_TYPE_CD                                 STRING                 not null

, LABEL_POG_TYPE_DESC                               STRING 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/label_pog_type';

--DISTRIBUTE ON (LABEL_POG_TYPE_CD)