
--*****  Creating table:  "SAP_ZTB_RF_AUDIT_PRE" , ***** Creating table: "SAP_ZTB_RF_AUDIT_PRE"


use raw;
 CREATE TABLE   SAP_ZTB_RF_AUDIT_PRE 
(
 CLIENT        STRING                  not null

, SITE          STRING                  not null

, ARTICLE       STRING                 not null

, CREATE_DATE                                       TIMESTAMP                             not null

, CREATE_TIME                                       TIMESTAMP                             not null

, ARTICLE_SLIP                                      STRING 

, UPC_CODE      STRING 

, DESCRIPTION                                       STRING 

, ADJUST_QTY                                        INT 

, REASON_CODE                                       SMALLINT 

, MOVE_TYPE     STRING 

, STATUS        STRING 

, RETAIL_PRICE                                      DECIMAL(11,2) 

, UNIT_COST     DECIMAL(11,2) 

, POST_DATE     TIMESTAMP 

, USER_NAME     STRING 

, CHANGE_IND                                        STRING 

, ERROR_MSG     STRING 

, DOC_NUMBER                                        STRING 

, VENDOR        STRING 

, RTV_IND       STRING 

, POST_NAME     STRING 

, CHANGE_TIME                                       TIMESTAMP 

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/BA_StoreOps/sap_ztb_rf_audit_pre';

--DISTRIBUTE ON (ARTICLE)







--*****  Creating table:  "MOVEMENT_LIVE_PET" , ***** Creating table: "MOVEMENT_LIVE_PET"


use legacy;
 CREATE TABLE   MOVEMENT_LIVE_PET 
(
 LOCATION_ID INT not null

, PRODUCT_ID INT not null

, CREATE_TSTMP                                      TIMESTAMP                             not null

, STORE_NBR INT

, SKU_NBR INT

, SKU_DESC      STRING 

, ADJUST_QTY                                        INT 

, VENDOR_ID BIGINT

, VENDOR_NBR                                        STRING 

, MOVE_REASON_ID SMALLINT

, MOVE_REASON_DESC                                  STRING 

, PET_MOVE_TYPE_CD                                  STRING 

, PET_MOVE_STATUS_CD                                STRING 

, RETAIL_PRICE_AMT                                  DECIMAL(11,2) 

, UNIT_COST     DECIMAL(11,2) 

, USER_NAME     STRING 

, LOAD_TSTMP                                        TIMESTAMP                             not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_StoreOps/movement_live_pet';

--DISTRIBUTE ON (PRODUCT_ID)

