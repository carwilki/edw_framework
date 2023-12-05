
--*****  Creating table:  "DP_SITE_VEND_PROFILE" , ***** Creating table: "DP_SITE_VEND_PROFILE"


use legacy;
 CREATE TABLE  DP_SITE_VEND_PROFILE 
(  LOCATION_ID INT not null

, VENDOR_ID BIGINT not null

, VENDOR_SUBGROUP_ID                          STRING                 not null

, DPR_ORDER_ARRAY                             STRING 

, DP_PURCH_GROUP_ID                           STRING 

, PROD_LEADTIME                               SMALLINT 

, ADJUST_LEADTIME                             DECIMAL(5,2) 

, TRANSIT_LEADTIME                            SMALLINT 

, TOTAL_LEADTIME                              SMALLINT 

, ADJUST_TIME_SUN                             DECIMAL(5,2) 

, TRANSIT_TIME_SUN                            SMALLINT 

, ADJUST_TIME_MON                             DECIMAL(5,2) 

, TRANSIT_TIME_MON                            SMALLINT 

, ADJUST_TIME_TUE                             DECIMAL(5,2) 

, TRANSIT_TIME_TUE                            SMALLINT 

, ADJUST_TIME_WED                             DECIMAL(5,2) 

, TRANSIT_TIME_WED                            SMALLINT 

, ADJUST_TIME_THU                             DECIMAL(5,2) 

, TRANSIT_TIME_THU                            SMALLINT 

, ADJUST_TIME_FRI                             DECIMAL(5,2) 

, TRANSIT_TIME_FRI                            SMALLINT 

, ADJUST_TIME_SAT                             DECIMAL(5,2) 

, TRANSIT_TIME_SAT                            SMALLINT 

, ALT_ORIGIN                                  STRING 

, DEL_START_TIME_SUN STRING

, DEL_END_TIME_SUN STRING

, DEL_START_TIME_MON STRING

, DEL_END_TIME_MON STRING

, DEL_START_TIME_TUE STRING

, DEL_END_TIME_TUE STRING

, DEL_START_TIME_WED STRING

, DEL_END_TIME_WED STRING

, DEL_START_TIME_THU STRING

, DEL_END_TIME_THU STRING

, DEL_START_TIME_FRI STRING

, DEL_END_TIME_FRI STRING

, DEL_START_TIME_SAT STRING

, DEL_END_TIME_SAT STRING

, PICK_DAY1                                   STRING 

, PICK_DAY2                                   STRING 

, PICK_DAY3                                   STRING 

, PICK_DAY4                                   STRING 

, PICK_DAY5                                   STRING 

, PICK_DAY6                                   STRING 

, PICK_DAY7                                   STRING 

, DEL_DAY1                                    STRING 

, DEL_DAY2                                    STRING 

, DEL_DAY3                                    STRING 

, DEL_DAY4                                    STRING 

, DEL_DAY5                                    STRING 

, DEL_DAY6                                    STRING 

, DEL_DAY7                                    STRING 

, BLUE_GREEN_FLAG                             STRING 

, PROTECTION_LEVEL                            STRING 

, OVERRIDE_ORDER_WEIGHT                       DECIMAL(13,3) 

, OVERRIDE_ORDER_VOLUME                       DECIMAL(13,3) 

, CHANGE_DATE                                 TIMESTAMP 

, CHANGE_TIME STRING

, NOTES                                       STRING 

, OVERRIDE_FLAG                               STRING 

, CROSSDOCK_ID                                STRING 

, ALT_ORIGIN1                                 STRING 

, ALT_ORIGIN2                                 STRING 

, ALT_ORIGIN3                                 STRING 

, ALT_ORIGIN4                                 STRING 

, ALT_ORIGIN5                                 STRING 

, ALT_ORIGIN6                                 STRING 

, ALT_ORIGIN7                                 STRING 

, CROSSDOCK_ID2                               STRING 

, CROSSDOCK_ID3                               STRING 

, CROSSDOCK_ID4                               STRING 

, CROSSDOCK_ID5                               STRING 

, CROSSDOCK_ID6                               STRING 

, CROSSDOCK_ID7                               STRING 

, DUE_BASED_ON_CD                             STRING 

, ORDER_DAY_OF_WK_ARRAY                       STRING 

, WEEK_FREQ_CD                                STRING 

, QUOTED_LT_DAY_CNT SMALLINT

, LT_DAY_CNT                                  DECIMAL(5,2) 

, LOAD_DT                                     TIMESTAMP                            not null

, MANDT                                       STRING 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Demand_Planning/dp_site_vend_profile';

--DISTRIBUTE ON (VENDOR_ID, LOCATION_ID)


--*****  Creating table:  "SAP_SKU_LINK_TYPE" , ***** Creating table: "SAP_SKU_LINK_TYPE"


use legacy;
 CREATE TABLE  SAP_SKU_LINK_TYPE 
(  SAP_SKU_LINK_TYPE_CD                              STRING                  not null

, SAP_SKU_LINK_TYPE_DESC                            STRING 

, EDW_SKU_LINK_TYPE_CD                              STRING 

, UPDATE_TSTMP                                      TIMESTAMP                             not null

, LOAD_TSTMP                                        TIMESTAMP                             not null 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Demand_Planning/sap_sku_link_type';

--DISTRIBUTE ON RANDOM


--*****  Creating table:  "DP_SKU_LINK" , ***** Creating table: "DP_SKU_LINK"


use legacy;
 CREATE TABLE  DP_SKU_LINK 
(  PRODUCT_ID INT not null

, LOCATION_ID INT not null

, SKU_LINK_TYPE_CD                            STRING                 not null

, SKU_LINK_EFF_DT                             TIMESTAMP                            not null

, LINK_PRODUCT_ID INT not null

, LINK_LOCATION_ID INT not null

, SKU_LINK_END_DT                             TIMESTAMP 

, LOAD_DT                                     TIMESTAMP                            not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Demand_Planning/dp_sku_link';

--DISTRIBUTE ON (PRODUCT_ID, LOCATION_ID)



--*****  Creating table:  "DP_SITE_VEND_PRE" , ***** Creating table: "DP_SITE_VEND_PRE"


use raw;
 CREATE TABLE  DP_SITE_VEND_PRE 
(  STORE_NBR INT not null

, VENDOR_ID BIGINT not null

, VENDOR_SUBGROUP_ID                          STRING                 not null

, DP_PURCH_GROUP_ID                           STRING 

, DUE_BASED_ON_CD                             STRING 

, ORDER_DAY_OF_WK_ARRAY                       STRING 

, WEEK_FREQ_CD                                STRING 

, QUOTED_LT_DAY_CNT SMALLINT

, LT_DAY_CNT                                  DECIMAL(5,2) 

, LOAD_DT                                     TIMESTAMP                            not null

, MANDT                                       STRING 

, DPR_ORDER_ARRAY                             STRING 

, PROD_LEADTIME INT

, TRANSIT_LEADTIME INT

, ADJUST_TIME_SUN                             DECIMAL(5,2) 

, TRANSIT_TIME_SUN INT

, ADJUST_TIME_MON                             DECIMAL(5,2) 

, TRANSIT_TIME_MON INT

, ADJUST_TIME_TUE                             DECIMAL(5,2) 

, TRANSIT_TIME_TUE INT

, ADJUST_TIME_WED                             DECIMAL(5,2) 

, TRANSIT_TIME_WED INT

, ADJUST_TIME_THU                             DECIMAL(5,2) 

, TRANSIT_TIME_THU INT

, ADJUST_TIME_FRI                             DECIMAL(5,2) 

, TRANSIT_TIME_FRI INT

, ADJUST_TIME_SAT                             DECIMAL(5,2) 

, TRANSIT_TIME_SAT INT

, ALT_ORIGIN                                  STRING 

, DEL_START_TIME_SUN TIMESTAMP

, DEL_END_TIME_SUN TIMESTAMP

, DEL_START_TIME_MON TIMESTAMP

, DEL_END_TIME_MON TIMESTAMP

, DEL_START_TIME_TUE TIMESTAMP

, DEL_END_TIME_TUE TIMESTAMP

, DEL_START_TIME_WED TIMESTAMP

, DEL_END_TIME_WED TIMESTAMP

, DEL_START_TIME_THU TIMESTAMP

, DEL_END_TIME_THU TIMESTAMP

, DEL_START_TIME_FRI TIMESTAMP

, DEL_END_TIME_FRI TIMESTAMP

, DEL_START_TIME_SAT TIMESTAMP

, DEL_END_TIME_SAT TIMESTAMP

, PICK_DAY1                                   STRING 

, PICK_DAY2                                   STRING 

, PICK_DAY3                                   STRING 

, PICK_DAY4                                   STRING 

, PICK_DAY5                                   STRING 

, PICK_DAY6                                   STRING 

, PICK_DAY7                                   STRING 

, DEL_DAY1                                    STRING 

, DEL_DAY2                                    STRING 

, DEL_DAY3                                    STRING 

, DEL_DAY4                                    STRING 

, DEL_DAY5                                    STRING 

, DEL_DAY6                                    STRING 

, DEL_DAY7                                    STRING 

, BLUE_GREEN_FLAG                             STRING 

, PROTECTION_LEVEL                            STRING 

, OVERRIDE_ORDER_WEIGHT INT

, OVERRIDE_ORDER_VOLUME INT

, CHANGE_DATE                                 TIMESTAMP 

, CHANGE_TIME TIMESTAMP

, NOTES                                       STRING 

, OVERRIDE_FLAG                               STRING 

, CROSSDOCK_ID                                STRING 

, ALT_ORIGIN1                                 STRING 

, ALT_ORIGIN2                                 STRING 

, ALT_ORIGIN3                                 STRING 

, ALT_ORIGIN4                                 STRING 

, ALT_ORIGIN5                                 STRING 

, ALT_ORIGIN6                                 STRING 

, ALT_ORIGIN7                                 STRING 

, CROSSDOCK_ID2                               STRING 

, CROSSDOCK_ID3                               STRING 

, CROSSDOCK_ID4                               STRING 

, CROSSDOCK_ID5                               STRING 

, CROSSDOCK_ID6                               STRING 

, CROSSDOCK_ID7                               STRING 

, ADJUST_LEADTIME                             DECIMAL(5,2) 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Demand_Planning/dp_site_vend_pre';

--DISTRIBUTE ON (VENDOR_ID, STORE_NBR)



--*****  Creating table:  "DP_SKU_LINK_PRE" , ***** Creating table: "DP_SKU_LINK_PRE"


use raw;
 CREATE TABLE  DP_SKU_LINK_PRE 
(  SKU_LINK_TYPE_CD                            STRING                 not null

, FROM_STORE_NBR INT not null

, FROM_SKU_NBR INT not null

, TO_STORE_NBR INT not null

, TO_SKU_NBR INT not null

, SKU_LINK_EFF_DT                             TIMESTAMP                            not null

, SKU_LINK_END_DT                             TIMESTAMP 

, LOAD_DT                                     TIMESTAMP                            not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_Demand_Planning/dp_sku_link_pre';

--DISTRIBUTE ON (FROM_SKU_NBR, FROM_STORE_NBR)

