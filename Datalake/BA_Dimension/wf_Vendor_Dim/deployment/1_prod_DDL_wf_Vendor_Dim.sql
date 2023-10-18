




--*****  Creating table:  "VENDOR_TYPE" , ***** Creating table: "VENDOR_TYPE"


use legacy;
CREATE TABLE   VENDOR_TYPE
(
 VENDOR_TYPE_ID TINYINT not null COMMENT "PRIMARY KEY"

, VENDOR_TYPE_DESC                                  STRING                 not null

, VENDOR_ACCOUNT_GROUP_CD                           STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/vendor_type' ; 

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "VENDOR_PROFILE" , ***** Creating table: "VENDOR_PROFILE"


use legacy;
CREATE TABLE   VENDOR_PROFILE
(
 VENDOR_ID BIGINT not null COMMENT "PRIMARY KEY"

, VENDOR_NAME                                       STRING 

, VENDOR_TYPE_ID TINYINT not null

, VENDOR_NBR                                        STRING                 not null

, LOCATION_ID INT

, SUPERIOR_VENDOR_ID BIGINT

, PARENT_VENDOR_ID BIGINT

, PARENT_VENDOR_NAME                                STRING 

, PURCH_GROUP_ID BIGINT

, EDI_ELIG_FLAG                                     STRING 

, PURCHASE_BLOCK                                    STRING 

, POSTING_BLOCK                                     STRING 

, DELETION_FLAG                                     STRING 

, VIP_CD        STRING 

, INACTIVE_FLAG                                     STRING 

, PAYMENT_TERM_CD                                   STRING 

, INCO_TERM_CD                                      STRING 

, ADDRESS       STRING 

, CITY          STRING 

, STATE         STRING 

, COUNTRY_CD                                        STRING 

, ZIP           STRING 

, CONTACT       STRING 

, CONTACT_PHONE                                     STRING 

, PHONE         STRING 

, PHONE_EXT     STRING 

, FAX           STRING 

, RTV_ELIG_FLAG                                     STRING 

, RTV_TYPE_CD                                       STRING 

, RTV_FREIGHT_TYPE_CD                               STRING 

, INDUSTRY_CD                                       STRING 

, LATITUDE      DECIMAL(9,5) 

, LONGITUDE     DECIMAL(9,5) 

, TIME_ZONE_ID                                      STRING 

, ADD_DT        TIMESTAMP 

, UPDATE_DT     TIMESTAMP 

, LOAD_DT       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/vendor_profile' ; 

--DISTRIBUTE ON (VENDOR_ID)







--*****  Creating table:  "VENDOR_SUBRANGE" , ***** Creating table: "VENDOR_SUBRANGE"


use legacy;
CREATE TABLE   VENDOR_SUBRANGE
(
 VENDOR_ID BIGINT not null COMMENT "PRIMARY KEY"

, VENDOR_SUBRANGE_CD                                STRING                         not null COMMENT "PRIMARY KEY"

, VENDOR_SUBRANGE_DESC                              STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Dimension/vendor_subrange' ; 

--DISTRIBUTE ON (VENDOR_ID, VENDOR_SUBRANGE_CD)







--*****  Creating table:  "VENDOR_SUBRANGE_PRE" , ***** Creating table: "VENDOR_SUBRANGE_PRE"


use raw;
CREATE TABLE   VENDOR_SUBRANGE_PRE
(
 VENDOR_NBR                                        STRING                 not null COMMENT "PRIMARY KEY"

, VENDOR_TYPE_ID TINYINT not null COMMENT "PRIMARY KEY"

, VENDOR_SUBRANGE_CD                                STRING                 not null COMMENT "PRIMARY KEY"

, VENDOR_SUBRANGE_DESC                              STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Dimension/vendor_subrange_pre' ; 

--DISTRIBUTE ON RANDOM

