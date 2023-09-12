--*****  Creating table:  "TRAINING_CATEGORY_TYPE_PRE" , ***** Creating table: "TRAINING_CATEGORY_TYPE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_CATEGORY_TYPE_PRE

(CATEGORY_ID INT not null

, CATEGORY_NAME              STRING

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_category_type_pre';

 

 

--DISTRIBUTE ON (CATEGORY_ID)

 

--*****  Creating table:  "TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE" , ***** Creating table: "TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE

(CATEGORY_TYPE_FOCUS_AREA_ID INT not null

, CATEGORY_ID INT

, FOCUS_AREA_ID INT

, LAST_MODIFIED              TIMESTAMP

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_category_type_focus_area_pre';

 

 

--DISTRIBUTE ON (CATEGORY_TYPE_FOCUS_AREA_ID)

 

--*****  Creating table:  "TRAINING_CLASS_PROMOTION_PRE" , ***** Creating table: "TRAINING_CLASS_PROMOTION_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_CLASS_PROMOTION_PRE

(CLASS_PROMOTION_ID INT not null

, NAME                       STRING

, DESCRIPTION                STRING

, DISCOUNT_AMOUNT            DECIMAL(19,4)

, START_DATE DATE

, END_DATE DATE

, CLASS_TYPE_ID INT

, COUNTRY_ABBREVIATION       STRING

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_class_promotion_pre';

 

 

--DISTRIBUTE ON (CLASS_PROMOTION_ID)

 

--*****  Creating table:  "TRAINING_CLASS_TYPE_PRE" , ***** Creating table: "TRAINING_CLASS_TYPE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_CLASS_TYPE_PRE

(CLASS_TYPE_ID INT not null

, NAME                       STRING

, SHORT_DESCRIPTION          STRING

, DURATION INT

, PRICE                      DECIMAL(19,4)

, UPC                        STRING

, INFO_URL                   STRING

, LAST_MODIFIED              TIMESTAMP

, SORT_ORDER_ID INT

, IS_ACTIVE INT

, CATEGORY_ID INT

, DURATION_UNIT_OF_MEASURE_ID INT

, SESSION_LENGTH INT

, SESSION_UNIT_OF_MEASURE_ID INT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_class_type_pre';

 

 

--DISTRIBUTE ON (CLASS_TYPE_ID)

 

--*****  Creating table:  "TRAINING_CLASS_TYPE_DETAIL_PRE" , ***** Creating table: "TRAINING_CLASS_TYPE_DETAIL_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_CLASS_TYPE_DETAIL_PRE

(CLASS_TYPE_DETAIL_ID INT not null

, CLASS_TYPE_ID INT

, NAME                       STRING

, SHORT_DESCRIPTION          STRING

, DURATION INT

, PRICE                      DECIMAL(19,4)

, UPC                        STRING

, INFO_URL                   STRING

, LAST_MODIFIED              TIMESTAMP

, SKU                        STRING

, COUNTRY_ABBREVIATION       STRING

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_class_type_detail_pre';

 

 

--DISTRIBUTE ON (CLASS_TYPE_DETAIL_ID)

 

--*****  Creating table:  "TRAINING_CUSTOMER_PRE" , ***** Creating table: "TRAINING_CUSTOMER_PRE"

 

use cust_sensitive;

 

CREATE TABLE IF NOT EXISTS raw_TRAINING_CUSTOMER_PRE

 

( CUSTOMER_ID INT not null

, PROVIDER_ID                STRING

, FIRST_NAME                 STRING

, LAST_NAME                  STRING

, EMAIL_ADDRESS              STRING

, PRIMARY_PHONE              STRING

, PRIMARY_PHONE_TYPE         STRING

, ALTERNATE_PHONE            STRING

, ALTERNATE_PHONE_TYPE       STRING

, CREATE_DATE_TIME           TIMESTAMP

, EXTERNAL_CUSTOMER_ID BIGINT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-cust-sensitive-raw-p1-gcs-gbl/services/training_customer_pre';

 

 

 

--DISTRIBUTE ON (CUSTOMER_ID)

 

 

--*****  Creating table:  "TRAINING_FOCUS_AREA_PRE" , ***** Creating table: "TRAINING_FOCUS_AREA_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_FOCUS_AREA_PRE

(FOCUS_AREA_ID INT not null

, FOCUS_AREA_NAME            STRING       not null

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_focus_area_pre';

 

 

--DISTRIBUTE ON (FOCUS_AREA_ID)

 

--*****  Creating table:  "TRAINING_INVOICE_PRE" , ***** Creating table: "TRAINING_INVOICE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_INVOICE_PRE

(INVOICE_ID INT not null

, STATUS INT

, COUNTRY_ABBREVIATION       STRING

, SKU                        STRING

, UPC                        STRING

, BASE_PRICE                 DECIMAL(19,4)

, DISCOUNT_AMOUNT            DECIMAL(19,4)

, SUB_TOTAL                  DECIMAL(19,4)

, CREATE_DATE_TIME           TIMESTAMP

, LAST_MODIFIED_DATE_TIME                                        TIMESTAMP

, CLASS_PROMOTION_ID INT

, PACKAGE_PROMOTION_ID INT

, RESERVATION_ID INT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_invoice_pre';

 

 

--DISTRIBUTE ON (INVOICE_ID)

 

--*****  Creating table:  "TRAINING_PACKAGE_PRE" , ***** Creating table: "TRAINING_PACKAGE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_PACKAGE_PRE

(PACKAGE_ID INT not null

, NAME                       STRING

, LAST_MODIFIED              TIMESTAMP

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_package_pre';

 

 

--DISTRIBUTE ON (PACKAGE_ID)

 

--*****  Creating table:  "TRAINING_PACKAGE_OPTION_PRE" , ***** Creating table: "TRAINING_PACKAGE_OPTION_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_PACKAGE_OPTION_PRE

(PACKAGE_OPTION_ID INT not null

, PACKAGE_ID INT

, NAME                       STRING

, DISPLAY_NAME               STRING

, DESCRIPTION                STRING

, UPC                        STRING

, PRICE                      DECIMAL(19,4)

, LAST_MODIFIED              TIMESTAMP

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_package_option_pre';

 

 

--DISTRIBUTE ON (PACKAGE_OPTION_ID)

 

--*****  Creating table:  "TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE" , ***** Creating table: "TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE

(PACKAGE_OPTION_CLASS_TYPE_ID INT not null

, PACKAGE_OPTION_ID INT

, CLASS_TYPE_ID INT

, LAST_MODIFIED              TIMESTAMP

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_package_option_class_type_pre';

 

 

--DISTRIBUTE ON (PACKAGE_OPTION_CLASS_TYPE_ID)

 

--*****  Creating table:  "TRAINING_PACKAGE_OPTION_DETAIL_PRE" , ***** Creating table: "TRAINING_PACKAGE_OPTION_DETAIL_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_PACKAGE_OPTION_DETAIL_PRE

(PACKAGE_OPTION_DETAIL_ID INT not null

, PACKAGE_OPTION_ID INT

, PACKAGE_ID INT

, NAME                       STRING

, DISPLAY_NAME               STRING

, DESCRIPTION                STRING

, UPC                        STRING

, PRICE                      DECIMAL(19,4)

, LAST_MODIFIED              TIMESTAMP

, SKU                        STRING

, COUNTRY_ABBREVIATION       STRING

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_package_option_detail_pre';

 

 

--DISTRIBUTE ON (PACKAGE_OPTION_DETAIL_ID)

 

--*****  Creating table:  "TRAINING_PACKAGE_PROMOTION_PRE" , ***** Creating table: "TRAINING_PACKAGE_PROMOTION_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_PACKAGE_PROMOTION_PRE

(PACKAGE_PROMOTION_ID INT not null

, NAME                       STRING

, DESCRIPTION                STRING

, DISCOUNT_AMOUNT            DECIMAL(19,4)

, START_DATE DATE

, END_DATE DATE

, PACKAGE_ID INT

, COUNTRY_ABBREVIATION       STRING

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_package_promotion_pre';

 

 

--DISTRIBUTE ON (PACKAGE_PROMOTION_ID)

 

--*****  Creating table:  "TRAINING_PET_PRE" , ***** Creating table: "TRAINING_PET_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_PET_PRE

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

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_pet_pre';

 

 

--DISTRIBUTE ON (PET_ID)

 

--*****  Creating table:  "TRAINING_PET_FOCUS_AREA_PRE" , ***** Creating table: "TRAINING_PET_FOCUS_AREA_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_PET_FOCUS_AREA_PRE

(PET_FOCUS_AREA_ID INT not null

, PET_ID INT not null

, FOCUS_AREA_ID INT not null

, LAST_MODIFIED              TIMESTAMP                    not null

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_pet_focus_area_pre';

 

 

--DISTRIBUTE ON (PET_FOCUS_AREA_ID)

 

--*****  Creating table:  "TRAINING_RESERVATION_PRE" , ***** Creating table: "TRAINING_RESERVATION_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_RESERVATION_PRE

(RESERVATION_ID INT not null

, CUSTOMER_ID INT

, PET_ID INT

, STORE_CLASS_ID INT

, STORE_NUMBER INT

, ENROLLMENT_DATE_TIME       TIMESTAMP

, CREATE_DATE_TIME           TIMESTAMP

, PACKAGE_OPTION_ID INT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_reservation_pre';

 

 

--DISTRIBUTE ON (RESERVATION_ID)

 

--*****  Creating table:  "TRAINING_RESERVATION_HISTORY_PRE" , ***** Creating table: "TRAINING_RESERVATION_HISTORY_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_RESERVATION_HISTORY_PRE

(RESERVATION_HISTORY_ID INT not null

, RESERVATION_ID INT

, STORE_CLASS_ID INT

, CLASS_START_DATE_TIME      TIMESTAMP

, CLASS_TYPE_NAME            STRING

, CLASS_DURATION_WEEKS INT

, CLASS_PRICE                DECIMAL(19,4)

, CLASS_UPC                  STRING

, TRAINER_NAME               STRING

, CREATE_DATE_TIME           TIMESTAMP

, TRAINING_PACKAGE_NAME      STRING

, TRAINING_PACKAGE_OPTION_NAME                                   STRING

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_reservation_history_pre';

 

 

--DISTRIBUTE ON (RESERVATION_HISTORY_ID)

 

--*****  Creating table:  "TRAINING_SCHED_CHANGE_CAPTURE_PRE" , ***** Creating table: "TRAINING_SCHED_CHANGE_CAPTURE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_CHANGE_CAPTURE_PRE

(CHANGE_ID INT not null

, CHANGE_TYPE_ID INT

, ENTITY_ID INT

, ENTITY_TYPE_ID INT

, STORE_NUMBER INT

, CHANGE_USER                STRING

, CHANGE_DATE_TIME           TIMESTAMP

, CHANGE_STATE_ID INT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_sched_change_capture_pre';

 

 

--DISTRIBUTE ON (CHANGE_ID)

 

--*****  Creating table:  "TRAINING_SCHED_CHANGE_STATE_PRE" , ***** Creating table: "TRAINING_SCHED_CHANGE_STATE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_CHANGE_STATE_PRE

 

( CHANGE_STATE_ID INT not null

, CHANGE_STATE_NAME          STRING

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_sched_change_state_pre';

 

 

--DISTRIBUTE ON (CHANGE_STATE_ID)

 

--*****  Creating table:  "TRAINING_SCHED_CHANGE_TYPE_PRE" , ***** Creating table: "TRAINING_SCHED_CHANGE_TYPE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_CHANGE_TYPE_PRE

 

( CHANGE_TYPE_ID INT not null

, CHANGE_TYPE_NAME           STRING

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_sched_change_type_pre';

 

 

--DISTRIBUTE ON (CHANGE_TYPE_ID)

 

--*****  Creating table:  "TRAINING_SCHED_CLASS_TYPE_PRE" , ***** Creating table: "TRAINING_SCHED_CLASS_TYPE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_CLASS_TYPE_PRE

 

( CLASS_TYPE_ID INT not null

, NAME                       STRING

, SHORT_DESCRIPTION          STRING

, DURATION INT

, PRICE                      DECIMAL(8,2)

, UPC                        STRING

, INFO_PAGE                  STRING

, IS_ACTIVE                  TINYINT

, VISIBILITY_LEVEL INT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_sched_class_type_pre';

 

 

--DISTRIBUTE ON (CLASS_TYPE_ID)

 

--*****  Creating table:  "TRAINING_SCHED_ENTITY_TYPE_PRE" , ***** Creating table: "TRAINING_SCHED_ENTITY_TYPE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_ENTITY_TYPE_PRE

 

( ENTITY_TYPE_ID INT not null

, ENTITY_TYPE_NAME           STRING

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_sched_entity_type_pre';

 

 

--DISTRIBUTE ON (ENTITY_TYPE_ID)

 

--*****  Creating table:  "TRAINING_SCHED_STORE_CLASS_PRE" , ***** Creating table: "TRAINING_SCHED_STORE_CLASS_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_STORE_CLASS_PRE

 

( STORE_CLASS_ID INT not null

, CLASS_TYPE_ID INT

, STORE_NUMBER INT

, START_DATE_TIME            TIMESTAMP

, LAST_MODIFIED              TIMESTAMP

, IS_ACTIVE                  TINYINT

, IS_FULL                    TINYINT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_sched_store_class_pre';

 

 

--DISTRIBUTE ON (STORE_CLASS_ID)

 

--*****  Creating table:  "TRAINING_SCHED_STORE_CLASS_DETAIL_PRE" , ***** Creating table: "TRAINING_SCHED_STORE_CLASS_DETAIL_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_STORE_CLASS_DETAIL_PRE

 

( STORE_CLASS_DETAIL_ID INT not null

, STORE_CLASS_ID INT

, CLASS_DATE_TIME            TIMESTAMP

, CLASS_TYPE_ID INT

, IS_ACTIVE                  TINYINT

, LAST_MODIFIED              TIMESTAMP

, TRAINER_ID INT

, STORE_NUMBER INT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_sched_store_class_detail_pre';

 

 

--DISTRIBUTE ON (STORE_CLASS_DETAIL_ID)

 

--*****  Creating table:  "TRAINING_SCHED_TRAINER_PRE" , ***** Creating table: "TRAINING_SCHED_TRAINER_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_TRAINER_PRE

 

( TRAINER_ID INT not null

, STORE_NUMBER INT

, FIRST_NAME                 STRING

, LAST_NAME                  STRING

, ASSOCIATE_ID INT

, IS_ACTIVE                  TINYINT

, LAST_MODIFIED              TIMESTAMP

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_sched_trainer_pre';

 

 

--DISTRIBUTE ON (TRAINER_ID)

 

--*****  Creating table:  "TRAINING_STORE_BLACKOUTS_PRE" , ***** Creating table: "TRAINING_STORE_BLACKOUTS_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_STORE_BLACKOUTS_PRE

 

( STORE_NUMBER INT not null

, BLACK_OUT_START_DATE DATE

, BLACK_OUT_END_DATE DATE

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_store_blackouts_pre';

 

 

--DISTRIBUTE ON (STORE_NUMBER)

 

--*****  Creating table:  "TRAINING_STORE_CLASS_PRE" , ***** Creating table: "TRAINING_STORE_CLASS_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_STORE_CLASS_PRE

 

( STORE_CLASS_ID INT not null

, CLASS_TYPE_ID INT

, STORE_NUMBER INT

, START_DATE_TIME            TIMESTAMP

, LAST_MODIFIED              TIMESTAMP

, TRAINER_ID INT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_store_class_pre';

 

 

--DISTRIBUTE ON (STORE_CLASS_ID)

 

--*****  Creating table:  "TRAINING_STORE_CLASS_DETAIL_PRE" , ***** Creating table: "TRAINING_STORE_CLASS_DETAIL_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_STORE_CLASS_DETAIL_PRE

 

( STORE_CLASS_DETAIL_ID INT not null

, STORE_CLASS_ID INT

, CLASS_DATE_TIME            TIMESTAMP

, CLASS_TYPE_ID INT

, LAST_MODIFIED              TIMESTAMP

, STORE_NUMBER INT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_store_class_detail_pre';

 

 

--DISTRIBUTE ON (STORE_CLASS_DETAIL_ID)

 

--*****  Creating table:  "TRAINING_TRAINER_PRE" , ***** Creating table: "TRAINING_TRAINER_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_TRAINER_PRE

 

( TRAINER_ID INT not null

, FORMATTED_NAME             STRING

, IS_DELETED                 TINYINT

, LOAD_TSTMP                 TIMESTAMP

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_trainer_pre';

 

 

--DISTRIBUTE ON (TRAINER_ID)

 

--*****  Creating table:  "TRAINING_UNIT_OF_MEASURE_PRE" , ***** Creating table: "TRAINING_UNIT_OF_MEASURE_PRE"

 

use raw;

 

CREATE TABLE IF NOT EXISTS  TRAINING_UNIT_OF_MEASURE_PRE

 

( UNIT_OF_MEASURE_ID INT not null

, UNIT_OF_MEASURE            STRING

, UNIT_OF_MEASURE_PLURAL     STRING

)

USING delta

 

LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/services/training_unit_of_measure_pre';

 

 

--DISTRIBUTE ON (UNIT_OF_MEASURE_ID)

 

----**********






--*****  Creating table:  "PET_TRAINING_RESERVATION" , ***** Creating table: "PET_TRAINING_RESERVATION"


use cust_sensitive;
 CREATE TABLE IF NOT EXISTS legacy_PET_TRAINING_RESERVATION 
( PET_TRAINING_RESERVATION_ID INT not null

, LOCATION_ID INT

, ENROLLMENT_TSTMP                                  TIMESTAMP 

, UPC_ID BIGINT

, STORE_CLASS_TYPE_ID INT

, STORE_CLASS_TYPE_DESC                             STRING 

, CLASS_START_TSTMP                                 TIMESTAMP 

, CLASS_DURATION_WEEKS SMALLINT

, PET_TRAINING_CUSTOMER_ID INT

, CUSTOMER_FIRST_NAME                               STRING 

, CUSTOMER_LAST_NAME                                STRING 

, PET_TRAINING_PET_ID INT

, PET_NAME      STRING 

, PET_BREED_DESC                                    STRING 

, PET_TRAINING_TRAINER_ID INT

, TRAINER_NAME                                      STRING 

, CREATED_TSTMP                                     TIMESTAMP 

, UPDATE_TSTMP                                      TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-cust-sensitive-nzlegacy-p1-gcs-gbl/services/pet_training_reservation';

--DISTRIBUTE ON (PET_TRAINING_RESERVATION_ID)







--*****  Creating table:  "TRAINING_CATEGORY_TYPE" , ***** Creating table: "TRAINING_CATEGORY_TYPE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_CATEGORY_TYPE
( TRAINING_CATEGORY_ID INT not null

, TRAINING_CATEGORY_NAME                                       STRING 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_category_type';

--DISTRIBUTE ON (TRAINING_CATEGORY_ID)







--*****  Creating table:  "TRAINING_CATEGORY_TYPE_FOCUS_AREA" , ***** Creating table: "TRAINING_CATEGORY_TYPE_FOCUS_AREA"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_CATEGORY_TYPE_FOCUS_AREA
( TRAINING_CATEGORY_TYPE_FOCUS_AREA_ID INT not null

, TRAINING_CATEGORY_ID INT

, TRAINING_FOCUS_AREA_ID INT

, LAST_MODIFIED            TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_category_type_focus_area';

--DISTRIBUTE ON (TRAINING_CATEGORY_TYPE_FOCUS_AREA_ID)







--*****  Creating table:  "TRAINING_CLASS_PROMO" , ***** Creating table: "TRAINING_CLASS_PROMO"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_CLASS_PROMO
( TRAINING_CLASS_PROMO_ID INT not null

, TRAINING_CLASS_PROMO_NAME                                    STRING 

, TRAINING_CLASS_PROMO_DESC                                    STRING 

, TRAINING_CLASS_TYPE_ID INT

, COUNTRY_CD               STRING 

, START_DT DATE

, END_DT DATE

, DISC_AMT                 DECIMAL(19,4) 

, DELETED_FLAG             TINYINT 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_class_promo';

--DISTRIBUTE ON (TRAINING_CLASS_PROMO_ID)







--*****  Creating table:  "TRAINING_CLASS_TYPE" , ***** Creating table: "TRAINING_CLASS_TYPE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_CLASS_TYPE
( TRAINING_CLASS_TYPE_ID INT not null

, TRAINING_CLASS_TYPE_NAME                                     STRING 

, TRAINING_CLASS_TYPE_SHORT_DESC                               STRING 

, UPC_ID BIGINT

, TRAINING_CLASS_DURATION INT

, INFO_URL                 STRING 

, PRICE_AMT                DECIMAL(19,4) 

, SRC_LAST_MODIFIED_TSTMP                                      TIMESTAMP 

, SORT_ORDER_ID INT

, IS_ACTIVE INT

, TRAINING_CATEGORY_ID INT

, DURATION_UNIT_OF_MEASURE_ID INT

, SESSION_LENGTH INT

, SESSION_UNIT_OF_MEASURE_ID INT

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_class_type';

--DISTRIBUTE ON (TRAINING_CLASS_TYPE_ID)







--*****  Creating table:  "TRAINING_CLASS_TYPE_DETAIL" , ***** Creating table: "TRAINING_CLASS_TYPE_DETAIL"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_CLASS_TYPE_DETAIL
( TRAINING_CLASS_TYPE_DETAIL_ID INT not null

, TRAINING_CLASS_TYPE_DETAIL_NAME                              STRING 

, TRAINING_CLASS_TYPE_DETAIL_SHORT_DESC                        STRING 

, TRAINING_CLASS_TYPE_ID INT

, COUNTRY_CD               STRING 

, SKU_NBR INT

, UPC_ID BIGINT

, TRAINING_CLASS_DURATION INT

, INFO_URL                 STRING 

, PRICE_AMT                DECIMAL(19,4) 

, SRC_LAST_MODIFIED_TSTMP                                      TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_class_type_detail';

--DISTRIBUTE ON (TRAINING_CLASS_TYPE_DETAIL_ID)







--*****  Creating table:  "TRAINING_CUSTOMER" , ***** Creating table: "TRAINING_CUSTOMER"


use cust_sensitive;
 CREATE TABLE IF NOT EXISTS legacy_TRAINING_CUSTOMER 
( TRAINING_CUSTOMER_ID INT not null

, TRAINING_EXT_CUSTOMER_ID BIGINT

, TRAINING_PROVIDER_ID     STRING 

, FIRST_NAME               STRING 

, LAST_NAME                STRING 

, EMAIL_ADDR               STRING 

, PRIMARY_PHONE_NBR        STRING 

, PRIMARY_PHONE_TYPE       STRING 

, ALTERNATE_PHONE_NBR      STRING 

, ALTERNATE_PHONE_TYPE     STRING 

, SRC_CREATE_TSTMP         TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-cust-sensitive-nzlegacy-p1-gcs-gbl/services/training_customer';

--DISTRIBUTE ON (TRAINING_CUSTOMER_ID)







--*****  Creating table:  "TRAINING_FOCUS_AREA" , ***** Creating table: "TRAINING_FOCUS_AREA"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_FOCUS_AREA
( TRAINING_FOCUS_AREA_ID INT not null

, TRAINING_FOCUS_AREA_NAME                                     STRING       not null

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_focus_area';

--DISTRIBUTE ON (TRAINING_FOCUS_AREA_ID)







--*****  Creating table:  "TRAINING_INVOICE" , ***** Creating table: "TRAINING_INVOICE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_INVOICE
( TRAINING_INVOICE_ID INT not null

, TRAINING_INVOICE_STATUS INT

, TRAINING_CLASS_PROMO_ID INT

, TRAINING_PACKAGE_PROMO_ID INT

, TRAINING_RESERVATION_ID INT

, COUNTRY_CD               STRING 

, SKU_NBR INT

, UPC_ID BIGINT

, BASE_PRICE_AMT           DECIMAL(19,4) 

, DISC_AMT                 DECIMAL(19,4) 

, SUB_TOTAL_AMT            DECIMAL(19,4) 

, SRC_CREATE_TSTMP         TIMESTAMP 

, SRC_LAST_MODIFIED_TSTMP                                      TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_invoice';

--DISTRIBUTE ON (TRAINING_INVOICE_ID)







--*****  Creating table:  "TRAINING_PACKAGE" , ***** Creating table: "TRAINING_PACKAGE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_PACKAGE
( TRAINING_PACKAGE_ID INT not null

, TRAINING_PACKAGE_NAME                                        STRING 

, SRC_LAST_MODIFIED_TSTMP                                      TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_package';

--DISTRIBUTE ON (TRAINING_PACKAGE_ID)







--*****  Creating table:  "TRAINING_PACKAGE_OPTION" , ***** Creating table: "TRAINING_PACKAGE_OPTION"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_PACKAGE_OPTION
( TRAINING_PACKAGE_OPTION_ID INT not null

, TRAINING_PACKAGE_OPTION_NAME                                 STRING 

, TRAINING_PACKAGE_OPTION_DISPLAY_NAME                         STRING 

, TRAINING_PACKAGE_OPTION_DESC                                 STRING 

, TRAINING_PACKAGE_ID INT

, UPC_ID BIGINT

, PRICE_AMT                DECIMAL(19,4) 

, SRC_LAST_MODIFIED_TSTMP                                      TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_package_option';

--DISTRIBUTE ON (TRAINING_PACKAGE_OPTION_ID)







--*****  Creating table:  "TRAINING_PACKAGE_OPTION_CLASS_TYPE" , ***** Creating table: "TRAINING_PACKAGE_OPTION_CLASS_TYPE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_PACKAGE_OPTION_CLASS_TYPE
( TRAINING_PACKAGE_OPTION_CLASS_TYPE_ID INT not null

, TRAINING_PACKAGE_OPTION_ID INT

, TRAINING_CLASS_TYPE_ID INT

, SRC_LAST_MODIFIED_TSTMP                                      TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_package_option_class_type';

--DISTRIBUTE ON (TRAINING_PACKAGE_OPTION_CLASS_TYPE_ID)







--*****  Creating table:  "TRAINING_PACKAGE_OPTION_DETAIL" , ***** Creating table: "TRAINING_PACKAGE_OPTION_DETAIL"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_PACKAGE_OPTION_DETAIL
( TRAINING_PACKAGE_OPTION_DETAIL_ID INT not null

, TRAINING_PACKAGE_OPTION_DETAIL_NAME                          STRING 

, TRAINING_PACKAGE_OPTION_DETAIL_DISPLAY_NAME                  STRING 

, TRAINING_PACKAGE_OPTION_DETAIL_DESC                          STRING 

, TRAINING_PACKAGE_OPTION_ID INT

, TRAINING_PACKAGE_ID INT

, COUNTRY_CD               STRING 

, SKU_NBR INT

, UPC_ID BIGINT

, PRICE_AMT                DECIMAL(19,4) 

, SRC_LAST_MODIFIED_TSTMP                                      TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_package_option_detail';

--DISTRIBUTE ON (TRAINING_PACKAGE_OPTION_DETAIL_ID)







--*****  Creating table:  "TRAINING_PACKAGE_PROMO" , ***** Creating table: "TRAINING_PACKAGE_PROMO"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_PACKAGE_PROMO
( TRAINING_PACKAGE_PROMO_ID INT not null

, TRAINING_PACKAGE_PROMO_NAME                                  STRING 

, TRAINING_PACKAGE_PROMO_DESC                                  STRING 

, TRAINING_PACKAGE_ID INT

, COUNTRY_CD               STRING 

, START_DT DATE

, END_DT DATE

, DISC_AMT                 DECIMAL(19,4) 

, DELETED_FLAG             TINYINT 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_package_promo';

--DISTRIBUTE ON (TRAINING_PACKAGE_PROMO_ID)







--*****  Creating table:  "TRAINING_PET" , ***** Creating table: "TRAINING_PET"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_PET
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
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_pet';

--DISTRIBUTE ON (TRAINING_PET_ID)







--*****  Creating table:  "TRAINING_PET_FOCUS_AREA" , ***** Creating table: "TRAINING_PET_FOCUS_AREA"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_PET_FOCUS_AREA
( TRAINING_PET_FOCUS_AREA_ID INT not null

, TRAINING_PET_ID INT not null

, TRAINING_FOCUS_AREA_ID INT not null

, TRAINING_LAST_MODIFIED                                       TIMESTAMP                    not null

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_pet_focus_area' ;

--DISTRIBUTE ON (TRAINING_PET_FOCUS_AREA_ID)







--*****  Creating table:  "TRAINING_RESERVATION" , ***** Creating table: "TRAINING_RESERVATION"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_RESERVATION
( TRAINING_RESERVATION_ID INT not null

, TRAINING_CUSTOMER_ID INT

, TRAINING_PET_ID INT

, TRAINING_STORE_CLASS_ID INT

, TRAINING_PACKAGE_OPTION_ID INT

, TRAINING_ENROLLMENT_TSTMP                                    TIMESTAMP 

, STORE_NBR INT

, SRC_CREATE_TSTMP         TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_reservation';

--DISTRIBUTE ON (TRAINING_RESERVATION_ID)







--*****  Creating table:  "TRAINING_RESERVATION_HISTORY" , ***** Creating table: "TRAINING_RESERVATION_HISTORY"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_RESERVATION_HISTORY
( TRAINING_RESERVATION_HISTORY_ID INT not null

, TRAINING_RESERVATION_ID INT

, TRAINING_STORE_CLASS_ID INT

, TRAINING_PACKAGE_NAME                                        STRING 

, TRAINING_PACKAGE_OPTION_NAME                                 STRING 

, CLASS_TYPE_NAME          STRING 

, TRAINER_NAME             STRING 

, CLASS_DURATION_WEEKS INT

, CLASS_START_TSTMP        TIMESTAMP 

, UPC_ID BIGINT

, CLASS_PRICE_AMT          DECIMAL(19,4) 

, SRC_CREATE_TSTMP         TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_reservation_history';

--DISTRIBUTE ON (TRAINING_RESERVATION_HISTORY_ID)







--*****  Creating table:  "TRAINING_SCHED_CHANGE_CAPTURE" , ***** Creating table: "TRAINING_SCHED_CHANGE_CAPTURE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_CHANGE_CAPTURE
( TRAINING_SCHED_CHANGE_ID INT not null

, TRAINING_SCHED_CHANGE_TYPE_ID INT

, TRAINING_SCHED_ENTITY_ID INT

, TRAINING_SCHED_ENTITY_TYPE_ID INT

, STORE_NBR INT

, TRAINING_SCHED_CHANGE_STATE_ID INT

, CHANGE_USER              STRING 

, CHANGE_TSTMP             TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_sched_change_capture' ;

--DISTRIBUTE ON (TRAINING_SCHED_CHANGE_ID)







--*****  Creating table:  "TRAINING_SCHED_CHANGE_STATE" , ***** Creating table: "TRAINING_SCHED_CHANGE_STATE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_CHANGE_STATE
( TRAINING_SCHED_CHANGE_STATE_ID INT not null

, TRAINING_SCHED_CHANGE_STATE_NAME                             STRING 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_sched_change_state';

--DISTRIBUTE ON (TRAINING_SCHED_CHANGE_STATE_ID)







--*****  Creating table:  "TRAINING_SCHED_CHANGE_TYPE" , ***** Creating table: "TRAINING_SCHED_CHANGE_TYPE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_CHANGE_TYPE
( TRAINING_SCHED_CHANGE_TYPE_ID INT not null

, TRAINING_SCHED_CHANGE_TYPE_NAME                              STRING 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_sched_change_type';

--DISTRIBUTE ON (TRAINING_SCHED_CHANGE_TYPE_ID)







--*****  Creating table:  "TRAINING_SCHED_CLASS_TYPE" , ***** Creating table: "TRAINING_SCHED_CLASS_TYPE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_CLASS_TYPE
( TRAINING_SCHED_CLASS_TYPE_ID INT not null

, TRAINING_SCHED_CLASS_TYPE_NAME                               STRING 

, TRAINING_SCHED_CLASS_TYPE_SHORT_DESC                         STRING 

, DURATION INT

, PRICE_AMT                DECIMAL(8,2) 

, UPC_CD BIGINT

, INFO_PAGE                STRING 

, VISIBILITY_LEVEL INT

, ACTIVE_FLAG              TINYINT 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_sched_class_type' ;

--DISTRIBUTE ON (TRAINING_SCHED_CLASS_TYPE_ID)







--*****  Creating table:  "TRAINING_SCHED_ENTITY_TYPE" , ***** Creating table: "TRAINING_SCHED_ENTITY_TYPE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_ENTITY_TYPE
( TRAINING_SCHED_ENTITY_TYPE_ID INT not null

, TRAINING_SCHED_ENTITY_TYPE_NAME                              STRING 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_sched_entity_type' ;

--DISTRIBUTE ON (TRAINING_SCHED_ENTITY_TYPE_ID)







--*****  Creating table:  "TRAINING_SCHED_STORE_CLASS" , ***** Creating table: "TRAINING_SCHED_STORE_CLASS"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_STORE_CLASS
( TRAINING_SCHED_STORE_CLASS_ID INT not null

, TRAINING_SCHED_CLASS_TYPE_ID INT

, STORE_NBR INT

, START_TSTMP              TIMESTAMP 

, FULL_FLAG                TINYINT 

, ACTIVE_FLAG              TINYINT 

, LAST_MODIFIED_TSTMP      TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_sched_store_class';

--DISTRIBUTE ON (TRAINING_SCHED_STORE_CLASS_ID)







--*****  Creating table:  "TRAINING_SCHED_STORE_CLASS_DETAIL" , ***** Creating table: "TRAINING_SCHED_STORE_CLASS_DETAIL"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_STORE_CLASS_DETAIL
( TRAINING_SCHED_STORE_CLASS_DETAIL_ID INT not null

, TRAINING_SCHED_STORE_CLASS_ID INT

, CLASS_TSTMP              TIMESTAMP 

, TRAINING_SCHED_CLASS_TYPE_ID INT

, TRAINING_SCHED_TRAINER_ID INT

, STORE_NBR INT

, ACTIVE_FLAG              TINYINT 

, LAST_MODIFIED_TSTMP      TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_sched_store_class_detail';

--DISTRIBUTE ON (TRAINING_SCHED_STORE_CLASS_DETAIL_ID)







--*****  Creating table:  "TRAINING_SCHED_TRAINER" , ***** Creating table: "TRAINING_SCHED_TRAINER"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_SCHED_TRAINER
( TRAINING_SCHED_TRAINER_ID INT not null

, STORE_NBR INT

, EMPLOYEE_ID INT

, FIRST_NAME               STRING 

, LAST_NAME                STRING 

, ACTIVE_FLAG              TINYINT 

, LAST_MODIFIED_TSTMP      TIMESTAMP 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_sched_trainer';

--DISTRIBUTE ON (TRAINING_SCHED_TRAINER_ID)







--*****  Creating table:  "TRAINING_STORE_BLACKOUTS" , ***** Creating table: "TRAINING_STORE_BLACKOUTS"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_STORE_BLACKOUTS
( STORE_NBR INT not null

, BLACK_OUT_START_DT DATE

, BLACK_OUT_END_DT DATE

, DELETED_FLAG             TINYINT 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_store_blackouts';

--DISTRIBUTE ON (STORE_NBR)







--*****  Creating table:  "TRAINING_STORE_CLASS" , ***** Creating table: "TRAINING_STORE_CLASS"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_STORE_CLASS
( TRAINING_STORE_CLASS_ID INT not null

, TRAINING_CLASS_TYPE_ID INT

, STORE_NBR INT

, TRAINING_TRAINER_ID INT

, START_TSTMP              TIMESTAMP 

, SRC_LAST_MODIFIED_TSTMP                                      TIMESTAMP 

, DELETED_FLAG             TINYINT 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_store_class';

--DISTRIBUTE ON (TRAINING_STORE_CLASS_ID)







--*****  Creating table:  "TRAINING_STORE_CLASS_DETAIL" , ***** Creating table: "TRAINING_STORE_CLASS_DETAIL"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_STORE_CLASS_DETAIL
( TRAINING_STORE_CLASS_DETAIL_ID INT not null

, TRAINING_STORE_CLASS_ID INT

, TRAINING_CLASS_TYPE_ID INT

, STORE_NBR INT

, CLASS_TSTMP              TIMESTAMP 

, SRC_LAST_MODIFIED_TSTMP                                      TIMESTAMP 

, DELETED_FLAG             TINYINT 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_store_class_detail' ;

--DISTRIBUTE ON (TRAINING_STORE_CLASS_DETAIL_ID)







--*****  Creating table:  "TRAINING_TRAINER" , ***** Creating table: "TRAINING_TRAINER"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_TRAINER
( TRAINING_TRAINER_ID INT not null

, TRAINING_TRAINER_NAME                                        STRING 

, DELETED_FLAG             TINYINT 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_trainer' ;

--DISTRIBUTE ON (TRAINING_TRAINER_ID)







--*****  Creating table:  "TRAINING_UNIT_OF_MEASURE" , ***** Creating table: "TRAINING_UNIT_OF_MEASURE"


use legacy;
CREATE TABLE IF NOT EXISTS  TRAINING_UNIT_OF_MEASURE
( TRAINING_UNIT_OF_MEASURE_ID INT not null

, TRAINING_UNIT_OF_MEASURE                                     STRING 

, TRAINING_UNIT_OF_MEASURE_PLURAL                              STRING 

, UPDATE_TSTMP             TIMESTAMP 

, LOAD_TSTMP               TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/services/training_unit_of_measure' ;

--DISTRIBUTE ON (TRAINING_UNIT_OF_MEASURE_ID)


--*****  Creating table:  "PET_TRAINING_RESERVATION_PRE" , ***** Creating table: "PET_TRAINING_RESERVATION_PRE"


use cust_sensitive;
CREATE TABLE  PET_TRAINING_RESERVATION_PRE
(
 PET_TRAINING_RESERVATION_ID INT not null

, LOCATION_ID INT

, ENROLLMENT_TSTMP                                  TIMESTAMP 

, UPC_ID BIGINT

, STORE_CLASS_TYPE_ID INT

, STORE_CLASS_TYPE_DESC                             STRING 

, CLASS_START_TSTMP                                 TIMESTAMP 

, CLASS_DURATION_WEEKS SMALLINT

, PET_TRAINING_CUSTOMER_ID INT

, CUSTOMER_FIRST_NAME                               STRING 

, CUSTOMER_LAST_NAME                                STRING 

, PET_TRAINING_PET_ID INT

, PET_NAME      STRING 

, PET_BREED_DESC                                    STRING 

, PET_TRAINING_TRAINER_ID INT

, TRAINER_NAME                                      STRING 

, CREATED_TSTMP                                     TIMESTAMP 

, LOAD_TSTMP                                        TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-cust-sensitive-raw-p1-gcs-gbl/services/pet_training_reservation_pre' ;

--DISTRIBUTE ON (PET_TRAINING_RESERVATION_ID)





