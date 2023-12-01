use legacy;

CREATE TABLE ASN_REJECT_RSN 

(

MESSAGE_ID                             STRING
 
, MESSAGE_NO                             STRING
 
, MESSAGE_TEXT                           STRING
 
, ASN_CREATE_DATE DATE
 
, ASN_CREATE_TIME TIMESTAMP
 
, IDOC_NUMBER                            BIGINT
 
, PO_NBR                                 STRING
 
, VENDOR_ID                              STRING
 
, VENDOR_NAME                            STRING
 
, UPC_ID                                 STRING
 
, MATERIAL_DESC                          STRING
 
, SKU_NBR                                STRING
 
, BOLNR                                  STRING
 
, PO_DELIVERY_DATE DATE
 
, STORE_NBR INT
 
, GR_CREATE_DATE DATE
 
, GR_CREATE_TIME TIMESTAMP
 
, PRODUCT_ID INT
 
, LOCATION_ID INT
 
, LOAD_TS                                TIMESTAMP
 
)

USING delta 

LOCATION 'gs://petm-bdpl-prod-nzlegacy-p1-gcs-gbl/BA_Mvmt_Purch/asn_reject_rsn';
 
--DISTRIBUTE ON RANDOM
