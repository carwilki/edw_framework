

--*****  Creating table:  "WM_ASN_DETAIL_PRE" , ***** Creating table: "WM_ASN_DETAIL_PRE"
use raw;

CREATE TABLE  WM_ASN_DETAIL_PRE
( DC_NBR                              SMALLINT               not null

, ASN_DETAIL_ID                       BIGINT              not null

, ASN_ID                              BIGINT 

, TC_PURCHASE_ORDERS_ID               STRING 

, PURCHASE_ORDERS_ID                  BIGINT 

, SKU_ID                              INT 

, SKU_NAME                            STRING 

, SKU_ATTR_1                          STRING 

, SKU_ATTR_2                          STRING 

, SKU_ATTR_3                          STRING 

, SKU_ATTR_4                          STRING 

, SKU_ATTR_5                          STRING 

, BUSINESS_PARTNER_ID                 STRING 

, PACKAGE_TYPE_ID                     INT 

, PACKAGE_TYPE_DESC                   STRING 

, PACKAGE_TYPE_INSTANCE               STRING 

, EPC_TRACKING_RFID_VALUE             STRING 

, ORDER_TYPE_DESC                     STRING 

, GTIN                                STRING 

, SHIPPED_QTY                         DECIMAL(16,4) 

, STD_PACK_QTY                        DECIMAL(13,4) 

, STD_CASE_QTY                        DECIMAL(16,4) 

, ASN_DETAIL_STATUS                   SMALLINT 

, RECEIVED_QTY                        DECIMAL(16,4) 

, STD_SUB_PACK_QTY                    DECIMAL(13,4) 

, LPN_PER_TIER                        INT 

, TIER_PER_PALLET                     INT 

, MFG_DATE                            TIMESTAMP 

, SHIP_BY_DATE                        TIMESTAMP 

, MFG_PLNT                            STRING 

, EXPIRE_DATE                         TIMESTAMP 

, INCUBATION_DATE                     TIMESTAMP 

, EPC_REQ_ON_ALL_CASES                STRING 

, WEIGHT_UOM_ID_BASE                  INT 

, REGION_ID                           INT 

, IS_ASSOCIATED_TO_OUTBOUND           TINYINT 

, IS_CANCELLED                        TINYINT 

, IS_CLOSED                           TINYINT 

, INVN_TYPE                           STRING 

, PROD_STAT                           STRING 

, BATCH_NBR                           STRING 

, CNTRY_OF_ORGN                       STRING 

, SHIPPED_LPN_COUNT                   INT 

, RECEIVED_LPN_COUNT                  INT 

, UNITS_ASSIGNED_TO_LPN               DECIMAL(16,4) 

, PROC_IMMD_NEEDS                     STRING 

, QUALITY_CHECK_HOLD_UPON_RCPT        STRING 

, REFERENCE_ORDER_NBR                 STRING 

, ACTUAL_WEIGHT                       DECIMAL(13,4) 

, ACTUAL_WEIGHT_PACK_COUNT            DECIMAL(13,4) 

, NBR_OF_PACK_FOR_CATCH_WT            DECIMAL(13,4) 

, PUTWY_TYPE                          STRING 

, RETAIL_PRICE                        DECIMAL(16,4) 

, PRICE_TIX_AVAIL                     STRING 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, HIBERNATE_VERSION                   BIGINT 

, TC_COMPANY_ID                       INT 

, TC_PO_LINE_ID                       STRING 

, INVENTORY_SEGMENT_ID                BIGINT 

, PPACK_GRP_CODE                      STRING 

, CUT_NBR                             STRING 

, QTY_CONV_FACTOR                     DECIMAL(17,8) 

, QTY_UOM_ID                          BIGINT 

, WEIGHT_UOM_ID                       INT 

, QTY_UOM_ID_BASE                     INT 

, TC_ORDER_ID                         STRING 

, ORDER_ID                            BIGINT 

, TC_ORDER_LINE_ID                    STRING 

, ORDER_LINE_ITEM_ID                  BIGINT 

, SEQ_NBR                             STRING 

, EXP_RECEIVE_CONDITION_CODE          STRING 

, ASN_RECV_RULES                      STRING 

, CHECKSUM                            STRING 

, ACTUAL_WEIGHT_RECEIVED              DECIMAL(16,4) 

, REF_FIELD_1                         STRING 

, REF_FIELD_3                         STRING 

, REF_FIELD_2                         STRING 

, REF_FIELD_4                         STRING 

, REF_FIELD_5                         STRING 

, REF_FIELD_6                         STRING 

, REF_FIELD_7                         STRING 

, REF_FIELD_8                         STRING 

, REF_FIELD_9                         STRING 

, REF_FIELD_10                        STRING 

, REF_NUM1                            DECIMAL(13,5) 

, REF_NUM2                            DECIMAL(13,5) 

, REF_NUM3                            DECIMAL(13,5) 

, REF_NUM4                            DECIMAL(13,5) 

, REF_NUM5                            DECIMAL(13,5) 

, DISPOSITION_TYPE                    STRING 

, PRE_RECEIPT_STATUS                  STRING 

, INV_DISPOSITION                     STRING 

, EXT_PLAN_ID                         STRING 

, PURCHASE_ORDERS_LINE_ITEM_ID        BIGINT 

, PROCESSED_FOR_TRLR_MOVES            TINYINT 

, LOAD_TSTMP                          TIMESTAMP                  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_asn_detail_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (ASN_DETAIL_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ASN_DETAIL_STATUS_PRE" , ***** Creating table: "WM_ASN_DETAIL_STATUS_PRE"


CREATE TABLE  WM_ASN_DETAIL_STATUS_PRE
( DC_NBR                              SMALLINT               not null

, ASN_DETAIL_STATUS                   SMALLINT               not null

, DESCRIPTION                         STRING 

, LOAD_TSTMP                          TIMESTAMP                  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_asn_detail_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ASN_PRE" , ***** Creating table: "WM_ASN_PRE"


CREATE TABLE  WM_ASN_PRE
( DC_NBR                              SMALLINT               not null

, ASN_ID                              BIGINT              not null

, TC_COMPANY_ID                       INT 

, TC_ASN_ID                           STRING 

, TC_ASN_ID_U                         STRING 

, ASN_TYPE                            TINYINT 

, ASN_STATUS                          SMALLINT 

, TRACTOR_NUMBER                      STRING 

, DELIVERY_STOP_SEQ                   SMALLINT 

, ORIGIN_FACILITY_ALIAS_ID            STRING 

, ORIGIN_FACILITY_ID                  INT 

, PICKUP_END_DTTM                     TIMESTAMP 

, ACTUAL_DEPARTURE_DTTM               TIMESTAMP 

, DESTINATION_FACILITY_ALIAS_ID       STRING 

, DESTINATION_FACILITY_ID             INT 

, DELIVERY_START_DTTM                 TIMESTAMP 

, DELIVERY_END_DTTM                   TIMESTAMP 

, ACTUAL_ARRIVAL_DTTM                 TIMESTAMP 

, RECEIPT_DTTM                        TIMESTAMP 

, INBOUND_REGION_NAME                 STRING 

, OUTBOUND_REGION_NAME                STRING 

, REF_FIELD_1                         STRING 

, REF_FIELD_2                         STRING 

, REF_FIELD_3                         STRING 

, TOTAL_WEIGHT                        DECIMAL(13,4) 

, APPOINTMENT_ID                      STRING 

, APPOINTMENT_DTTM                    TIMESTAMP 

, APPOINTMENT_DURATION                INT 

, PALLET_FOOTPRINT                    STRING 

, LAST_TRANSMITTED_DTTM               TIMESTAMP 

, TOTAL_SHIPPED_QTY                   DECIMAL(16,4) 

, TOTAL_RECEIVED_QTY                  DECIMAL(16,4) 

, ASSIGNED_CARRIER_CODE               STRING 

, BILL_OF_LADING_NUMBER               STRING 

, PRO_NUMBER                          STRING 

, EQUIPMENT_CODE                      STRING 

, EQUIPMENT_CODE_ID                   INT 

, ASSIGNED_CARRIER_CODE_ID            INT 

, TOTAL_VOLUME                        DECIMAL(13,4) 

, VOLUME_UOM_ID_BASE                  INT 

, FIRM_APPT_IND                       TINYINT 

, TRACTOR_CARRIER_CODE_ID             INT 

, TRACTOR_CARRIER_CODE                STRING 

, BUYER_CODE                          STRING 

, REP_NAME                            STRING 

, CONTACT_ADDRESS_1                   STRING 

, CONTACT_ADDRESS_2                   STRING 

, CONTACT_ADDRESS_3                   STRING 

, CONTACT_CITY                        STRING 

, CONTACT_STATE_PROV                  STRING 

, CONTACT_ZIP                         STRING 

, CONTACT_NUMBER                      STRING 

, EXT_CREATED_DTTM                    TIMESTAMP 

, INBOUND_REGION_ID                   INT 

, OUTBOUND_REGION_ID                  INT 

, REGION_ID                           INT 

, ASN_LEVEL                           SMALLINT 

, RECEIPT_VARIANCE                    TINYINT 

, HAS_IMPORT_ERROR                    TINYINT 

, HAS_SOFT_CHECK_ERROR                TINYINT 

, HAS_ALERTS                          TINYINT 

, SYSTEM_ALLOCATED                    TINYINT 

, IS_COGI_GENERATED                   TINYINT 

, ASN_PRIORITY                        TINYINT 

, SCHEDULE_APPT                       TINYINT 

, IS_ASSOCIATED_TO_OUTBOUND           TINYINT 

, IS_CANCELLED                        TINYINT 

, IS_CLOSED                           TINYINT 

, BUSINESS_PARTNER_ID                 STRING 

, MANIF_NBR                           STRING 

, MANIF_TYPE                          STRING 

, WORK_ORD_NBR                        STRING 

, CUT_NBR                             STRING 

, MFG_PLNT                            STRING 

, IS_WHSE_TRANSFER                    STRING 

, QUALITY_CHECK_HOLD_UPON_RCPT        STRING 

, QUALITY_AUDIT_PERCENT               DECIMAL(5,2) 

, SHIPPED_LPN_COUNT                   INT 

, RECEIVED_LPN_COUNT                  INT 

, ACTUAL_SHIPPED_DTTM                 TIMESTAMP 

, LAST_RECEIPT_DTTM                   TIMESTAMP 

, VERIFIED_DTTM                       TIMESTAMP 

, LABEL_PRINT_REQD                    STRING 

, INITIATE_FLAG                       STRING 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, SHIPMENT_ID                         BIGINT 

, TC_SHIPMENT_ID                      STRING 

, VERIFICATION_ATTEMPTED              STRING 

, FLOW_THRU_ALLOC_IN_PROG             STRING 

, ALLOCATION_COMPLETED                STRING 

, EQUIPMENT_TYPE                      STRING 

, ASN_ORGN_TYPE                       STRING 

, DC_ORD_NBR                          STRING 

, CONTRAC_LOCN                        STRING 

, MHE_SENT                            STRING 

, FLOW_THROUGH_ALLOCATION_METHOD      STRING 

, FIRST_RECEIPT_DTTM                  TIMESTAMP 

, MISC_INSTR_CODE_1                   STRING 

, MISC_INSTR_CODE_2                   STRING 

, DOCK_DOOR_ID                        BIGINT 

, MODE_ID                             INT 

, SHIPPING_COST                       DECIMAL(13,4) 

, SHIPPING_COST_CURRENCY_CODE         STRING 

, FLOWTHROUGH_ALLOCATION_STATUS       TINYINT 

, HIBERNATE_VERSION                   BIGINT 

, QTY_UOM_ID_BASE                     INT 

, WEIGHT_UOM_ID_BASE                  INT 

, DRIVER_NAME                         STRING 

, TRAILER_NUMBER                      STRING 

, DESTINATION_TYPE                    STRING 

, CONTACT_COUNTY                      STRING 

, CONTACT_COUNTRY_CODE                STRING 

, RECEIPT_TYPE                        TINYINT 

, VARIANCE_TYPE                       SMALLINT 

, HAS_NOTES                           TINYINT 

, ORIGINAL_ASN_ID                     BIGINT 

, RETURN_REFERENCE_NUMBER             STRING 

, REF_FIELD_4                         STRING 

, REF_FIELD_5                         STRING 

, REF_FIELD_6                         STRING 

, REF_FIELD_7                         STRING 

, REF_FIELD_8                         STRING 

, REF_FIELD_9                         STRING 

, REF_FIELD_10                        STRING 

, REF_NUM1                            DECIMAL(13,5) 

, REF_NUM2                            DECIMAL(13,5) 

, REF_NUM3                            DECIMAL(13,5) 

, REF_NUM4                            DECIMAL(13,5) 

, REF_NUM5                            DECIMAL(13,5) 

, INVOICE_DATE                        TIMESTAMP 

, INVOICE_NUMBER                      STRING 

, PRE_RECEIPT_STATUS                  STRING 

, PRE_ALLOCATION_FIT_PERCENTAGE       SMALLINT 

, TRAILER_CLOSED                      TINYINT 

, IS_GIFT                             TINYINT 

, ORIGINAL_ORDER_NUMBER               STRING 

, ASN_SHPMT_TYPE                      STRING 

, LOAD_TSTMP                          TIMESTAMP                  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_asn_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (ASN_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ASN_STATUS_PRE" , ***** Creating table: "WM_ASN_STATUS_PRE"


CREATE TABLE  WM_ASN_STATUS_PRE
( DC_NBR                              SMALLINT               not null

, ASN_STATUS                          SMALLINT               not null

, DESCRIPTION                         STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_asn_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_BUSINESS_PARTNER_PRE" , ***** Creating table: "WM_BUSINESS_PARTNER_PRE"


CREATE TABLE  WM_BUSINESS_PARTNER_PRE
( DC_NBR                              SMALLINT               not null

, TC_COMPANY_ID                       INT               not null

, BUSINESS_PARTNER_ID                 STRING       not null

, DESCRIPTION                         STRING 

, MARK_FOR_DELETION                   TINYINT 

, BP_ID                               BIGINT 

, BP_COMPANY_ID                       INT 

, ACCREDITED_BP                       TINYINT 

, BUSINESS_NUMBER                     STRING 

, ADDRESS_1                           STRING 

, CITY                                STRING 

, STATE_PROV                          STRING 

, POSTAL_CODE                         STRING 

, COUNTY                              STRING 

, COUNTRY_CODE                        STRING 

, COMMENTS                            STRING 

, TEL_NBR                             STRING 

, LAST_UPDATED_SOURCE                 STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, CREATED_SOURCE                      STRING 

, ADDRESS_2                           STRING 

, ADDRESS_3                           STRING 

, HIBERNATE_VERSION                   BIGINT 

, PREFIX                              STRING 

, ATTRIBUTE_1                         TINYINT 

, ATTRIBUTE_2                         STRING 

, ATTRIBUTE_3                         STRING 

, ATTRIBUTE_4                         STRING 

, ATTRIBUTE_5                         STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_business_partner_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (TC_COMPANY_ID, BUSINESS_PARTNER_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_CARRIER_CODE_PRE" , ***** Creating table: "WM_CARRIER_CODE_PRE"


CREATE TABLE  WM_CARRIER_CODE_PRE
( DC_NBR                              SMALLINT                not null

, CARRIER_ID                          BIGINT               not null

, TC_COMPANY_ID                       INT 

, CARRIER_CODE                        STRING 

, TP_COMPANY_ID                       INT 

, TP_COMPANY_NAME                     STRING 

, CARRIER_CODE_NAME                   STRING 

, ADDRESS                             STRING 

, CITY                                STRING 

, STATE_PROV                          STRING 

, POSTAL_CODE                         STRING 

, COUNTRY_CODE                        STRING 

, CARRIER_CODE_STATUS                 TINYINT 

, MARK_FOR_DELETION                   TINYINT 

, COMM_METHOD                         STRING 

, IS_UNION                            TINYINT 

, IS_AUTO_DELIVER                     TINYINT 

, AUTO_ACCEPT_TYPE                    TINYINT 

, ACCEPT_MSG_REQD                     TINYINT 

, APPT_MSG_REQD                       TINYINT 

, DEPART_MSG_REQD                     TINYINT 

, ARRIVE_MSG_REQD                     TINYINT 

, CONFIRM_RECALL_REQD                 TINYINT 

, CONFIRM_UPDATE_REQD                 TINYINT 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, IS_BROKER                           TINYINT 

, BROKER_RECEIVE_UPDATES              TINYINT 

, IS_PREPAY                           TINYINT 

, ALLOW_BOL_ENTRY                     TINYINT 

, AUTO_DELIVER_DELAY_HOURS            INT 

, DOCK_SCHEDULE_PERMISSION            TINYINT 

, ALLOW_LOAD_COMPLETION               TINYINT 

, RECEIVES_DOCK_INFO                  TINYINT 

, ALLOW_TRAILER_ENTRY                 TINYINT 

, ALLOW_TRACTOR_ENTRY                 TINYINT 

, PAYMENT_REFERENCE_NUMBER            STRING 

, IS_PAYEE                            TINYINT 

, ALLOW_CARRIER_CHARGES_VIEW          TINYINT 

, SUPPORTS_PARCEL                     TINYINT 

, TARIFF_ID                           STRING 

, IS_AUTO_CREATE_INVOICE              TINYINT 

, DOES_RECEIVE_FACILITY_INFO          TINYINT 

, IS_PRIVATE_FLEET                    TINYINT 

, INVOICE_PAYMENT_TERMS               STRING 

, MATCH_PAY_EMAIL                     STRING 

, DAYS_TO_DISPUTE_CLAIM               INT 

, PAYMENT_METHOD                      STRING 

, IS_PREFERRED                        TINYINT 

, CUT_PARCEL_INVOICE                  TINYINT 

, USE_CARRIER_ADDR                    TINYINT 

, CARRIER_ADDR_FOR_CL                 STRING 

, CLAIM_ON_DETENTION_APPROVAL         TINYINT 

, IS_RES_BASED_COSTING_ALLOWED        TINYINT 

, IS_ACTIVITY_BASED_COSTING           TINYINT 

, PCT_OVER_CAPACITY                   DECIMAL(13,4) 

, ALLOW_CARRIER_BOOKING_REF           TINYINT 

, ALLOW_FORWARDER_AWB                 TINYINT 

, ALLOW_MASTER_AWB                    TINYINT 

, VALIDATE_INVOICE                    INT 

, MAX_FLEET_APPROVED_AMOUNT           DECIMAL(13,2) 

, HAS_HAZMAT                          TINYINT 

, CARRIER_TYPE_ID                     SMALLINT 

, CHECK_NON_MACHINABLE                TINYINT 

, PRO_NUMBER_LEVEL                    STRING 

, DEFAULT_PALLET_TYPE                 STRING 

, LANG_ID                             STRING 

, AWB_UOM                             STRING 

, TRANS_EXCEL                         SMALLINT 

, INV_ON_CARR_SIZES                   TINYINT 

, PRO_NUMBER_REQD                     TINYINT 

, ALLOW_COUNTER_OFFER                 TINYINT 

, DEPART_MSG_REQD_AT_FIRST_STOP       TINYINT 

, DOT_NUMBER                          STRING 

, IS_BENCHMARK                        TINYINT 

, EPI_CARRIER_CODE                    STRING 

, SCAC_CODE                           STRING 

, EPI_SUPPORTS_HOLD_PROCESS           TINYINT 

, MOTOR_CARRIER_NBR                   INT 

, FREIGHT_FRWRDR_NBR                  INT 

, TAX_ID                              STRING 

, HAZMAT_REG_NBR                      STRING 

, EXPIRY_DATE                         TIMESTAMP 

, EPI_SUPPORTS_EOD                    TINYINT 

, LOAD_PRO_NEXT_UP_COUNTER            STRING 

, STOP_PRO_NEXT_UP_COUNTER            STRING 

, ALLOW_TENDER_REJECT                 TINYINT 

, REF_FIELD1                          STRING 

, REF_FIELD2                          STRING 

, REF_FIELD3                          STRING 

, REF_FIELD4                          STRING 

, REF_FIELD5                          STRING 

, REF_NUM1                            DECIMAL(13,5) 

, REF_NUM2                            DECIMAL(13,5) 

, REF_NUM3                            DECIMAL(13,5) 

, REF_NUM4                            DECIMAL(13,5) 

, REF_NUM5                            DECIMAL(13,5) 

, MOBILE_SHIP_TRACK                   TINYINT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_carrier_code_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_COMMODITY_CODE_PRE" , ***** Creating table: "WM_COMMODITY_CODE_PRE"


CREATE TABLE  WM_COMMODITY_CODE_PRE
( DC_NBR                              SMALLINT                not null

, COMMODITY_CODE_ID                   BIGINT               not null

, COMM_CODE_SECTION                   SMALLINT 

, COMM_CODE_CHAPTER                   SMALLINT 

, TC_COMPANY_ID                       INT 

, DESCRIPTION_SHORT                   STRING 

, DESCRIPTION_LONG                    STRING 

, MARK_FOR_DELETION                   TINYINT 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_commodity_code_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_C_LEADER_AUDIT_PRE" , ***** Creating table: "WM_C_LEADER_AUDIT_PRE"


CREATE TABLE  WM_C_LEADER_AUDIT_PRE
( DC_NBR                              SMALLINT                not null

, C_LEADER_AUDIT_ID                   INT                not null

, LEADER_USER_ID                      STRING 

, PICKER_USER_ID                      STRING 

, STATUS                              TINYINT 

, ITEM_NAME                           STRING 

, LPN                                 STRING 

, EXPECTED_QTY                        INT 

, ACTUAL_QTY                          INT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_c_leader_audit_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (C_LEADER_AUDIT_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_C_TMS_PLAN_PRE" , ***** Creating table: "WM_C_TMS_PLAN_PRE"


CREATE TABLE  WM_C_TMS_PLAN_PRE
( DC_NBR                              SMALLINT                not null

, C_TMS_PLAN_ID                       INT                not null

, PLAN_ID                             STRING 

, STO_NBR                             STRING 

, ROUTE_ID                            STRING 

, STOP_ID                             INT 

, STOP_LOC                            STRING 

, ORIGIN                              STRING 

, DESTINATION                         STRING 

, CONSOLIDATOR                        STRING 

, CONS_ADDR_1                         STRING 

, CONS_ADDR_2                         STRING 

, CONS_ADDR_3                         STRING 

, CITY                                STRING 

, STATE                               STRING 

, ZIP                                 STRING 

, CNTRY                               STRING 

, CONTACT_PERSON                      STRING 

, PHONE                               STRING 

, COMMODITY                           STRING 

, NUM_PALLETS                         INT 

, WEIGHT                              DECIMAL(13,4) 

, VOLUME                              DECIMAL(13,4) 

, DROP_DEAD_DT                        TIMESTAMP 

, ETA                                 TIMESTAMP 

, SPLIT_ROUTE                         STRING 

, TMS_CARRIER_ID                      STRING 

, TMS_CARRIER_NAME                    STRING 

, LOAD_STAT                           STRING 

, SHIP_VIA                            STRING 

, MILES                               INT 

, DRIVE_TIME                          STRING 

, EQUIP_TYPE                          STRING 

, DEL_TYPE                            STRING 

, TMS_TRAILER_ID                      STRING 

, CARRIER_REF_NUM                     STRING 

, TMS_PLAN_TIME                       TIMESTAMP 

, PLT_WEIGHT                          DECIMAL(13,4) 

, ERROR_SEQ_NBR                       INT 

, STAT_CODE                           TINYINT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_c_tms_plan_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_DOCK_DOOR_PRE" , ***** Creating table: "WM_DOCK_DOOR_PRE"


CREATE TABLE  WM_DOCK_DOOR_PRE
( DC_NBR                              SMALLINT                not null

, DOCK_DOOR_ID                        BIGINT               not null

, FACILITY_ID                         INT 

, DOCK_ID                             STRING 

, TC_COMPANY_ID                       INT 

, DOCK_DOOR_NAME                      STRING 

, DOCK_DOOR_STATUS                    SMALLINT 

, DESCRIPTION                         STRING 

, MARK_FOR_DELETION                   SMALLINT 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, OLD_DOCK_DOOR_STATUS                SMALLINT 

, ACTIVITY_TYPE                       STRING 

, APPOINTMENT_TYPE                    STRING 

, BARCODE                             STRING 

, TIME_FROM_INDUCTION                 DECIMAL(5,2) 

, PALLETIZATION_SPUR                  STRING 

, SORT_ZONE                           STRING 

, ILM_APPOINTMENT_NUMBER              STRING 

, FLOWTHRU_ALLOC_SORT_PRTY            STRING 

, LOCN_HDR_ID                         INT 

, DOCK_DOOR_LOCN_ID                   STRING 

, OUTBD_STAGING_LOCN_ID               STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_dock_door_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (DOCK_DOOR_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_DO_STATUS_PRE" , ***** Creating table: "WM_DO_STATUS_PRE"


CREATE TABLE  WM_DO_STATUS_PRE
( DC_NBR                              SMALLINT                not null

, ORDER_STATUS                        SMALLINT                not null

, DESCRIPTION                         STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_do_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_EQUIPMENT_INSTANCE_PRE" , ***** Creating table: "WM_EQUIPMENT_INSTANCE_PRE"


CREATE TABLE  WM_EQUIPMENT_INSTANCE_PRE
( DC_NBR                              SMALLINT                not null

, EQUIPMENT_INSTANCE_ID               BIGINT               not null

, TC_COMPANY_ID                       INT 

, EQUIP_INST_STATUS                   SMALLINT 

, CONDITION_TYPE                      SMALLINT 

, DESCRIPTION                         STRING 

, IS_LOCKED                           SMALLINT 

, MARK_FOR_DELETION                   SMALLINT 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LICENSE_TAG_NUMBER                  STRING 

, LICENSE_STATE_PROV                  STRING 

, LICENSE_COUNTRY_CODE                STRING 

, EQUIP_DOOR_TYPE                     SMALLINT 

, COMMENTS                            STRING 

, CURRENT_LOCATION_ID                 BIGINT 

, ASSIGNED_LOCATION_ID                BIGINT 

, CURRENT_APPOINTMENT_ID              INT 

, EQUIP_INST_REF_CARRIER              STRING 

, TP_COMPANY_ID                       INT 

, FOB_INDICATOR                       SMALLINT 

, DISPATCH_STATUS                     SMALLINT 

, HOME_DOMICILE_FACILITY_ID           INT 

, HOME_DOMICILE_FAC_ALIAS_ID          STRING 

, CURRENT_FACILITY_ID                 INT 

, CURRENT_FACILITY_ALIAS_ID           STRING 

, LAST_MAINTENANCE_DTTM               TIMESTAMP 

, NEXT_AVAILABLE_DTTM                 TIMESTAMP 

, SEAL_NUMBER                         STRING 

, NEXT_AVAIL_ZIP                      STRING 

, NEXT_AVAILABLE_TERM                 STRING 

, NEXT_AVAIL_FACILITY_ID              INT 

, IS_OBC_ONBOARD                      SMALLINT 

, IS_NON_SMOKING                      SMALLINT 

, HAS_NIGHT_HEATER                    SMALLINT 

, IS_RIGID                            SMALLINT 

, RIGID_EQUIPMENT_ID                  BIGINT 

, SLEEPER_TYPE                        SMALLINT 

, ZONE_FACILITY_ID                    BIGINT 

, ZONE_FACILITY_ALIAS_ID              STRING 

, ZONE_TYPE                           SMALLINT 

, MAINTENANCE_FACILITY_ID             BIGINT 

, MAINTENANCE_FACILITY_ALIAS_ID       STRING 

, MAINTENANCE_START_DTTM              TIMESTAMP 

, MAINTENANCE_FREQUENCY_WEEKS         INT 

, MAINTENANCE_DURATION                DECIMAL(12,4) 

, NEXT_MAINTENANCE_DTTM               TIMESTAMP 

, HAS_ALERTS                          SMALLINT 

, LAST_KNOWN_DTTM                     TIMESTAMP 

, HAS_IMPORT_ERROR                    SMALLINT 

, HAS_SOFT_CHECK_ERROR                SMALLINT 

, LAST_KNOWN_FACILITY_ID              INT 

, LAST_KNOWN_FACILITY_ALIAS_ID        STRING 

, CONTRACT_START_DATE                 TIMESTAMP 

, CONTRACT_END_DATE                   TIMESTAMP 

, MAKE                                STRING 

, MODEL                               STRING 

, OWNERSHIP_TYPE                      SMALLINT 

, PLATED_WEIGHT                       DECIMAL(13,2) 

, TAX_BAND_NAME                       STRING 

, HOME_DSP_REGION_ID                  BIGINT 

, LAST_KNOWN_DSP_REGION_ID            BIGINT 

, PER_USAGE_COST                      DECIMAL(13,2) 

, PER_USAGE_COST_CURRENCY_CODE        STRING 

, CUST_LK_LOCATION_NAME               STRING 

, CUST_LK_ADDRESS                     STRING 

, CUST_LK_CITY                        STRING 

, CUST_LK_STATE                       STRING 

, CUST_LK_POSTAL_CODE                 STRING 

, CUST_LK_COUNTRY                     STRING 

, LAST_KNOWN_TERM                     STRING 

, IS_TANDEM_LEAD                      SMALLINT 

, CARRIER_ID                          BIGINT 

, EQUIPMENT_ID                        BIGINT 

, REASON_ID                           INT 

, PLATED_WEIGHT_SIZE_UOM_ID           INT 

, IS_TANDEM_CAPABLE                   TINYINT 

, USR_ENT_NEXT_AVAILABLE_DTTM         TIMESTAMP 

, USR_ENT_NEXT_AVAILABLE_TERM         STRING 

, USR_ENT_NEXT_AVAIL_FACILITY_ID      INT 

, CUST_NA_LOCATION_NAME               STRING 

, CUST_NA_ADDRESS                     STRING 

, CUST_NA_CITY                        STRING 

, CUST_NA_STATE                       STRING 

, CUST_NA_POSTAL_CODE                 STRING 

, CUST_NA_COUNTRY                     STRING 

, HIBERNATE_VERSION                   BIGINT 

, MILEAGE_SCHEDULE                    BIGINT 

, MILEAGE_UOM                         STRING 

, CURRENT_MILEAGE                     DECIMAL(13,2) 

, LAST_MNT_MILEAGE                    DECIMAL(13,2) 

, NEXT_MNT_MILEAGE                    DECIMAL(13,2) 

, IS_LABOR                            TINYINT 

, EQUIPMENT_BRCD                      STRING 

, LOCK_REASON_CODE                    STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_equipment_instance_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_EQUIPMENT_PRE" , ***** Creating table: "WM_EQUIPMENT_PRE"


CREATE TABLE  WM_EQUIPMENT_PRE
( DC_NBR                              SMALLINT                not null

, EQUIPMENT_ID                        BIGINT               not null

, TC_COMPANY_ID                       INT 

, EQUIPMENT_CODE                      STRING 

, DESCRIPTION                         STRING 

, MARK_FOR_DELETION                   SMALLINT 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, SHAPE_TYPE                          SMALLINT 

, DIM01_VALUE                         DECIMAL(16,4) 

, DIM01_STANDARD_UOM                  INT 

, DIM02_VALUE                         DECIMAL(16,4) 

, DIM02_STANDARD_UOM                  INT 

, DIM03_VALUE                         DECIMAL(16,4) 

, DIM03_STANDARD_UOM                  INT 

, VOLUME_CALC_VALUE                   DECIMAL(16,4) 

, VOLUME_CALC_STANDARD_UOM            INT 

, WEIGHT_EMPTY_VALUE                  DECIMAL(16,4) 

, WEIGHT_EMPTY_STANDARD_UOM           INT 

, EQUIPMENT_TYPE                      SMALLINT 

, EQUIP_DW_HEIGHT_VALUE               DECIMAL(16,4) 

, EQUIP_DW_HEIGHT_STANDARD_UOM        INT 

, TRAILER_TYPE                        TINYINT 

, OWNERSHIP_TYPE                      SMALLINT 

, NUMBER_OF_AXLES                     SMALLINT 

, PER_USAGE_COST                      DECIMAL(13,2) 

, PER_USAGE_COST_CURRENCY_CODE        STRING 

, PLATED_WEIGHT                       DECIMAL(13,2) 

, TAX_BAND_NAME                       STRING 

, WEIGHT_VALUE                        DECIMAL(13,4) 

, WEIGHT_STANDARD_UOM                 INT 

, EQUIP_HEIGHT_VALUE                  INT 

, EQUIP_HEIGHT_STANDARD_UOM           INT 

, IS_ALLOW_TRAILER_SWAPPING           SMALLINT 

, IS_TANDEM_CAPABLE                   TINYINT 

, MASTER_EQUIPMENT_ID                 BIGINT 

, WEIGHT_EMPTY_SIZE_UOM_ID            INT 

, PLATED_WEIGHT_SIZE_UOM_ID           INT 

, BACKIN_TIME                         SMALLINT 

, BACKOUT_TIME                        SMALLINT 

, FLOOR_SPACE_VALUE                   BIGINT 

, FLOOR_SPACE_SIZE_UOM_ID             INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_equipment_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_ACT_ELM_CRIT_PRE" , ***** Creating table: "WM_E_ACT_ELM_CRIT_PRE"


CREATE TABLE  WM_E_ACT_ELM_CRIT_PRE
( DC_NBR                              SMALLINT                not null

, ACT_ID                              INT                not null

, ELM_ID                              INT                not null

, CRIT_ID                             INT                not null

, CRIT_VAL_ID                         INT                not null

, TIME_ALLOW                          DECIMAL(9,4) 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_act_elm_crit_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (ACT_ID, ELM_ID, CRIT_ID, CRIT_VAL_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_ACT_ELM_PRE" , ***** Creating table: "WM_E_ACT_ELM_PRE"


CREATE TABLE  WM_E_ACT_ELM_PRE
( DC_NBR                              SMALLINT                not null

, ACT_ID                              INT                not null

, ELM_ID                              INT                not null

, TIME_ALLOW                          DECIMAL(9,4) 

, THRUPUT_MSRMNT                      STRING 

, SEQ_NBR                             INT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, AVG_ACT_ID                          INT 

, AVG_BY                              STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_act_elm_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (ACT_ID, ELM_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_ACT_PRE" , ***** Creating table: "WM_E_ACT_PRE"


CREATE TABLE  WM_E_ACT_PRE
( DC_NBR                              SMALLINT                not null

, ACT_ID                              INT                not null

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, JOB_FUNC_ID                         INT 

, WHSE                                STRING 

, LABOR_TYPE_ID                       INT 

, MONITOR_UOM                         STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, OVERRIDE_PROC_ZONE_ID               INT 

, PROC_ZONE_LOCN_IND                  INT 

, LM_MAN_EVNT_REQ_APRV                STRING 

, LM_KIOSK_REQ_APRV                   STRING 

, WM_REQ_APRV                         STRING 

, VERSION_ID                          INT 

, LABOR_ACTIVITY_ID                   INT 

, VHCL_TYPE_ID                        INT 

, UNQ_SEED_ID                         INT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, DISPLAY_UOM                         STRING 

, THRUPUT_GOAL                        DECIMAL(20,7) 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_act_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (ACT_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_AUD_LOG_PRE" , ***** Creating table: "WM_E_AUD_LOG_PRE"


CREATE TABLE  WM_E_AUD_LOG_PRE
( DC_NBR                              SMALLINT                not null

, AUD_ID                              DECIMAL(20,0)               not null

, WHSE                                STRING 

, TRAN_NBR                            INT 

, SKU_ID                              STRING 

, SKU_HNDL_ATTR                       STRING 

, CRITERIA                            STRING 

, SLOT_ATTR                           STRING 

, FACTOR_TIME                         DECIMAL(20,7) 

, CURR_SLOT                           STRING 

, NEXT_SLOT                           STRING 

, DISTANCE                            DECIMAL(20,7) 

, HEIGHT                              DECIMAL(20,7) 

, TRVL_DIR                            STRING 

, ELEM_TIME                           DECIMAL(13,5) 

, UOM                                 STRING 

, CURR_LOCN_CLASS                     STRING 

, NEXT_LOCN_CLASS                     STRING 

, TOT_TIME                            DECIMAL(15,7) 

, TOT_UNITS                           DECIMAL(13,5) 

, UNITS_PER_GRAB                      DECIMAL(13,5) 

, LPN                                 STRING 

, ELEM_DESC                           STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, TA_MULTIPLIER                       DECIMAL(13,7) 

, ELS_TRAN_ID                         DECIMAL(20,0) 

, CRIT_VAL                            STRING 

, CRIT_TIME                           DECIMAL(13,5) 

, ELM_ID                              INT 

, FACTOR                              STRING 

, THRUPUT_MSRMNT                      STRING 

, MODULE_TYPE                         STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, ADDTL_TIME_ALLOW                    DECIMAL(9,4) 

, SLOT_TYPE_TIME_ALLOW                DECIMAL(9,4) 

, UNIT_PICK_TIME_ALLOW                DECIMAL(9,4) 

, PFD_TIME                            DECIMAL(20,7) 

, VERSION_ID                          INT 

, ACT_ID                              INT 

, COMPONENT_BK                        STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_aud_log_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (AUD_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_CRIT_VAL_PRE" , ***** Creating table: "WM_E_CRIT_VAL_PRE"


CREATE TABLE  WM_E_CRIT_VAL_PRE
( DC_NBR                              SMALLINT                not null

, CRIT_VAL_ID                         INT                not null

, CRIT_ID                             INT                not null

, CRIT_VAL                            STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_crit_val_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (CRIT_VAL_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_ELM_CRIT_PRE" , ***** Creating table: "WM_E_ELM_CRIT_PRE"


CREATE TABLE  WM_E_ELM_CRIT_PRE
( DC_NBR                              SMALLINT                not null

, ELM_ID                              INT                not null

, CRIT_ID                             INT                not null

, CRIT_VAL_ID                         INT                not null

, TIME_ALLOW                          DECIMAL(9,4) 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_elm_crit_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_ELM_PRE" , ***** Creating table: "WM_E_ELM_PRE"


CREATE TABLE  WM_E_ELM_PRE
( DC_NBR                              SMALLINT                not null

, ELM_ID                              INT                not null

, NAME                                STRING 

, DESCRIPTION                         STRING 

, CORE_FLAG                           STRING 

, MSRMNT_ID                           INT 

, TIME_ALLOW                          DECIMAL(9,4) 

, ELM_GRP_ID                          INT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, UNQ_SEED_ID                         INT 

, SIM_WHSE                            STRING 

, ORIG_NAME                           STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_elm_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_EMP_DTL_PRE" , ***** Creating table: "WM_E_EMP_DTL_PRE"


CREATE TABLE  WM_E_EMP_DTL_PRE
( DC_NBR                              SMALLINT                not null

, EMP_DTL_ID                          INT                not null

, EMP_ID                              BIGINT 

, EFF_DATE_TIME                       TIMESTAMP 

, EMP_STAT_ID                         INT 

, PAY_RATE                            DECIMAL(9,2) 

, PAY_SCALE_ID                        INT 

, SPVSR_EMP_ID                        BIGINT 

, DEPT_ID                             INT 

, SHIFT_ID                            INT 

, ROLE_ID                             INT 

, USER_DEF_FIELD_1                    STRING 

, USER_DEF_FIELD_2                    STRING 

, CMNT                                STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, WHSE                                STRING 

, JOB_FUNC_ID                         INT 

, STARTUP_TIME                        DECIMAL(20,7) 

, CLEANUP_TIME                        DECIMAL(20,7) 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, DFLT_PERF_GOAL                      DECIMAL(5,2) 

, VERSION_ID                          INT 

, IS_SUPER                            STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, EXCLUDE_AUTO_CICO                   STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_emp_dtl_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_EMP_STAT_CODE_PRE" , ***** Creating table: "WM_E_EMP_STAT_CODE_PRE"


CREATE TABLE  WM_E_EMP_STAT_CODE_PRE
( DC_NBR                              SMALLINT                not null

, EMP_STAT_ID                         INT                not null

, EMP_STAT_CODE                       STRING 

, DESCRIPTION                         STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, UNQ_SEED_ID                         INT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_emp_stat_code_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_EVNT_SMRY_HDR_PRE" , ***** Creating table: "WM_E_EVNT_SMRY_HDR_PRE"


CREATE TABLE  WM_E_EVNT_SMRY_HDR_PRE
( DC_NBR                              SMALLINT                not null

, ELS_TRAN_ID                         DECIMAL(20,0)               not null

, WHSE                                STRING 

, TRAN_NBR                            INT 

, CO                                  STRING 

, DIV                                 STRING 

, SCHED_START_DATE                    TIMESTAMP 

, REF_CODE                            STRING 

, REF_NBR                             STRING 

, LOGIN_USER_ID                       STRING 

, DEPT_CODE                           STRING 

, VHCL_TYPE                           STRING 

, SHIFT_CODE                          STRING 

, USER_DEF_FIELD_1                    STRING 

, USER_DEF_FIELD_2                    STRING 

, STD_TIME_ALLOW                      DECIMAL(13,5) 

, EMP_PERF_ALLOW                      DECIMAL(13,5) 

, PERF_ADJ_AMT                        DECIMAL(13,5) 

, SCHED_ADJ_AMT                       DECIMAL(13,5) 

, SCHED_UNITS_QTY                     DECIMAL(20,7) 

, ACTL_TIME                           DECIMAL(20,7) 

, ERROR_CNT                           INT 

, EVNT_STAT_CODE                      STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, TMU_RECALC_COUNT                    INT 

, MISC                                STRING 

, LABOR_TYPE_ID                       INT 

, SOURCE                              STRING 

, EVENT_TYPE                          STRING 

, TOTAL_WEIGHT                        DECIMAL(20,7) 

, TEAM_STD_SMRY_ID                    INT 

, TEAM_CODE                           STRING 

, ORIG_LOGIN_USER_ID                  STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, PROC_ZONE_ID                        DECIMAL(20,7) 

, LAST_MOD_BY                         STRING 

, EVNT_CTGRY_1                        STRING 

, EVNT_CTGRY_2                        STRING 

, EVNT_CTGRY_3                        STRING 

, EVNT_CTGRY_4                        STRING 

, EVNT_CTGRY_5                        STRING 

, SYS_UPDATED_FLAG                    STRING 

, APRV_SPVSR                          STRING 

, APRV_SPVSR_DATE_TIME                TIMESTAMP 

, VERSION_ID                          INT 

, TEAM_CHG_ID                         DECIMAL(20,7) 

, ACTUAL_END_DATE                     TIMESTAMP 

, SPVSR_LOGIN_USER_ID                 STRING 

, PAID_BRK_OVERLAP                    DECIMAL(20,7) 

, UNPAID_BRK_OVERLAP                  DECIMAL(20,7) 

, EMP_PERF_SMRY_ID                    DECIMAL(20,0) 

, HDR_MSG_ID                          DECIMAL(20,0) 

, ACT_ID                              INT 

, PERF_SMRY_TRAN_ID                   DECIMAL(20,0) 

, ORIG_EVNT_START_TIME                TIMESTAMP 

, ORIG_EVNT_END_TIME                  TIMESTAMP 

, ADJ_REASON_CODE                     STRING 

, ADJ_REF_TRAN_NBR                    DECIMAL(20,0) 

, CONFLICT                            STRING 

, TEAM_BEGIN_TIME                     TIMESTAMP 

, THRUPUT_GOAL                        DECIMAL(20,7) 

, THRUPUT_MIN                         DECIMAL(20,7) 

, DISPLAY_UOM                         STRING 

, DISPLAY_UOM_QTY                     DECIMAL(20,7) 

, UNIT_MIN                            DECIMAL(20,7) 

, CONSOL_EVNT_TIME                    DECIMAL(20,7) 

, LOCN_GRP_ATTR                       STRING 

, RESOURCE_GROUP_ID                   STRING 

, JOB_FUNC_ID                         INT 

, COMP_EMPLOYEE_DAY_ID                STRING 

, COMP_EVENT_SUMMARY_HEADER_ID        STRING 

, ASSIGNMENT_START_TIME               TIMESTAMP 

, ASSIGNMENT_END_TIME                 TIMESTAMP 

, REFLECTIVE_CODE                     STRING 

, COMP_ASSIGNMENT_ID                  STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_evnt_smry_hdr_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (ELS_TRAN_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_JOB_FUNCTION_PRE" , ***** Creating table: "WM_E_JOB_FUNCTION_PRE"


CREATE TABLE  WM_E_JOB_FUNCTION_PRE
( DC_NBR                              SMALLINT                not null

, JOB_FUNC_ID                         INT                not null

, NAME                                STRING 

, DESCRIPTION                         STRING 

, STARTUP_TIME                        DECIMAL(20,7) 

, CLEANUP_TIME                        DECIMAL(20,7) 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, WHSE                                STRING 

, TRANSITION_START_TIME               DECIMAL(20,7) 

, TRANSITION_END_TIME                 DECIMAL(20,7) 

, JF_TYPE                             STRING 

, LEVEL_1                             STRING 

, LEVEL_2                             STRING 

, LEVEL_3                             STRING 

, LEVEL_4                             STRING 

, LEVEL_5                             STRING 

, OPS_CODE_ID                         INT 

, APPLY_TEAM_SETUP_TIME               STRING 

, TEAM_STARTUP_TIME                   DECIMAL(20,7) 

, TEAM_CLEANUP_TIME                   DECIMAL(20,7) 

, TEAM_TRANSITION_START_TIME          DECIMAL(20,7) 

, TEAM_TRANSITION_END_TIME            DECIMAL(20,7) 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, DFLT_PROC_ZONE_ID                   INT 

, PROC_ZONE_TEMPL_ID                  INT 

, MSRMNT_ID                           INT 

, VERSION_ID                          INT 

, OBS_THRESHOLD_EP                    DECIMAL(20,7) 

, TRACK_HIST_TIME                     STRING 

, TRAIN_PERIOD                        INT 

, RETRAIN_PERIOD                      DECIMAL(7,2) 

, OS_ONLY                             STRING 

, RETRAIN_REQ_DURATION                DECIMAL(7,2) 

, PERF_GOAL_IND                       STRING 

, TRAINING_REQD                       STRING 

, USE_JF_TRAIN                        STRING 

, PERF_EVAL_PERIOD_ID                 STRING 

, DFLT_MAX_OCCUPANCY                  DECIMAL(20,7) 

, UNQ_SEED_ID                         INT 

, DEFAULT_ACT_ID                      INT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, PLAN_EP                             DECIMAL(13,7) 

, WHSE_VISIBILITY_GROUP               STRING 

, DEPT_CODE                           STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_job_function_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_E_LABOR_TYPE_CODE_PRE" , ***** Creating table: "WM_E_LABOR_TYPE_CODE_PRE"


CREATE TABLE  WM_E_LABOR_TYPE_CODE_PRE
( DC_NBR                              SMALLINT                not null

, LABOR_TYPE_ID                       INT                not null

, LABOR_TYPE_CODE                     STRING 

, DESCRIPTION                         STRING 

, USER_ID                             STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, SPVSR_AUTH_REQUIRED                 STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_labor_type_code_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_MSRMNT_PRE" , ***** Creating table: "WM_E_MSRMNT_PRE"


CREATE TABLE  WM_E_MSRMNT_PRE
( DC_NBR                              SMALLINT                not null

, MSRMNT_ID                           INT                not null

, MSRMNT_CODE                         STRING 

, NAME                                STRING 

, STATUS_FLAG                         STRING 

, SYS_CREATED                         STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, UNQ_SEED_ID                         INT 

, SIM_WHSE                            STRING 

, ORIG_MSRMNT_CODE                    STRING 

, ORIG_NAME                           STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_msrmnt_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_MSRMNT_RULE_CONDITION_PRE" , ***** Creating table: "WM_E_MSRMNT_RULE_CONDITION_PRE"


CREATE TABLE  WM_E_MSRMNT_RULE_CONDITION_PRE
( DC_NBR                              SMALLINT                not null

, MSRMNT_ID                           INT                not null

, RULE_NBR                            INT                not null

, RULE_SEQ_NBR                        INT                not null

, OPEN_PARAN                          STRING 

, AND_OR                              STRING 

, FIELD_NAME                          STRING 

, OPERATOR                            STRING 

, RULE_COMPARE_VALUE                  STRING 

, CLOSE_PARAN                         STRING 

, FREE_FORM_TEXT                      STRING 

, DESCRIPTION                         STRING 

, MISC                                STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_msrmnt_rule_condition_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_MSRMNT_RULE_PRE" , ***** Creating table: "WM_E_MSRMNT_RULE_PRE"


CREATE TABLE  WM_E_MSRMNT_RULE_PRE
( DC_NBR                              SMALLINT                not null

, MSRMNT_ID                           INT                not null

, RULE_NBR                            INT                not null

, DESCRIPTION                         STRING 

, STATUS_FLAG                         STRING 

, THEN_STATEMENT                      STRING 

, ELSE_STATEMENT                      STRING 

, NOTE                                STRING 

, MISC                                STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_msrmnt_rule_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_E_SHIFT_PRE" , ***** Creating table: "WM_E_SHIFT_PRE"


CREATE TABLE  WM_E_SHIFT_PRE
( DC_NBR                              SMALLINT                not null

, SHIFT_ID                            INT                not null

, EFF_DATE                            TIMESTAMP 

, SHIFT_CODE                          STRING 

, DESCRIPTION                         STRING 

, OT_INDIC                            STRING 

, OT_PAY_FCT                          DECIMAL(5,2) 

, WEEK_MIN_OT_HRS                     DECIMAL(5,2) 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, WHSE                                STRING 

, REPORT_SHIFT                        STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, VERSION_ID                          INT 

, ROT_SHIFT_IND                       STRING 

, ROT_SHIFT_NUM_DAYS                  INT 

, ROT_SHIFT_START_DATE                TIMESTAMP 

, SCHED_SHIFT                         STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_e_shift_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_FACILITY_PRE" , ***** Creating table: "WM_FACILITY_PRE"


CREATE TABLE  WM_FACILITY_PRE
( DC_NBR                              SMALLINT                not null

, FACILITY_ID                         INT                not null

, TC_COMPANY_ID                       INT 

, BUSINESS_PARTNER_ID                 STRING 

, NAME                                STRING 

, DESCRIPTION                         STRING 

, ADDRESS_1                           STRING 

, ADDRESS_2                           STRING 

, ADDRESS_3                           STRING 

, ADDRESS_KEY_1                       STRING 

, CITY                                STRING 

, STATE_PROV                          STRING 

, POSTAL_CODE                         STRING 

, COUNTY                              STRING 

, COUNTRY_CODE                        STRING 

, LONGITUDE                           DECIMAL(12,6) 

, LATITUDE                            DECIMAL(12,6) 

, EIN_NBR                             STRING 

, IATA_CODE                           STRING 

, GLOBAL_LOCN_NBR                     STRING 

, TAX_ID                              STRING 

, DUNS_NBR                            STRING 

, INBOUNDS_RS_AREA_ID                 INT 

, OUTBOUND_RS_AREA_ID                 INT 

, INBOUND_REGION_ID                   INT 

, OUTBOUND_REGION_ID                  INT 

, FACILITY_TZ                         SMALLINT 

, DROP_INDICATOR                      TINYINT 

, HOOK_INDICATOR                      TINYINT 

, HANDLER                             TINYINT 

, IS_SHIP_APPT_REQD                   TINYINT 

, IS_RCV_APPT_REQD                    TINYINT 

, TRACK_ONTIME_INDICATOR              TINYINT 

, ONTIME_PERF_METHOD                  STRING 

, LOAD_FACTOR_SIZE_VALUE              DECIMAL(13,2) 

, MARK_FOR_DELETION                   TINYINT 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, ALLOW_OVERLAPPING_APPS              TINYINT 

, EARLY_TOLERANCE_POINT               INT 

, LATE_TOLERANCE_POINT                INT 

, EARLY_TOLERANCE_WINDOW              INT 

, LATE_TOLERANCE_WINDOW               INT 

, FACILITY_TYPE_BITS                  SMALLINT 

, PP_HANDLING_TIME                    INT 

, STOP_CONSTRAINT_BITS                SMALLINT 

, IS_DOCK_SCHED_FAC                   TINYINT 

, ASN_COMMUNICATION_METHOD            SMALLINT 

, PICK_TIME                           INT 

, DISPATCH_INITIATION_TIME            INT 

, PAPERWORK_TIME                      INT 

, VEHICLE_CHECK_TIME                  INT 

, VISIT_GAP                           SMALLINT 

, MIN_HANDLING_TIME                   DECIMAL(31,0) 

, IS_CREDIT_AVAILABLE                 TINYINT 

, BUSINESS_GROUP_ID_1                 BIGINT 

, BUSINESS_GROUP_ID_2                 BIGINT 

, IS_OPERATIONAL                      TINYINT 

, IS_MAINTENANCE_FACILITY             TINYINT 

, DWELL_TIME                          INT 

, DRIVER_CHECK_IN_TIME                INT 

, DISPATCH_DTTM                       TIMESTAMP 

, DSP_DR_CONFIGURATION_ID             BIGINT 

, DROP_HOOK_TIME                      INT 

, LOADING_END_TIME                    INT 

, DRIVER_DEBRIEF_TIME                 INT 

, ACCOUNT_CODE_NUMBER                 STRING 

, LOAD_FACTOR_MOT_ID                  BIGINT 

, LOAD_UNLOAD_MOT_ID                  BIGINT 

, MIN_TRAIL_FILL                      SMALLINT 

, TANDEM_DROP_HOOK_TIME               INT 

, STORE_TYPE                          TINYINT 

, DRIVEIN_TIME                        INT 

, CHECKIN_TIME                        INT 

, DRIVEOUT_TIME                       INT 

, CHECKOUT_TIME                       INT 

, CUTOFF_DTTM                         TIMESTAMP 

, MAX_TRAILER_YARD_TIME               INT 

, HAND_OVER_TIME                      INT 

, OVER_BOOK_PERCENTAGE                SMALLINT 

, RECOMMENDATION_CRITERIA             STRING 

, NBR_OF_SLOTS_TO_SHOW                SMALLINT 

, RESTRICT_FLEET_ASMT_TIME            SMALLINT 

, GROUP_ID                            STRING 

, IS_VTBP                             INT 

, AUTO_CREATE_SHIPMENT                INT 

, LOGO_IMAGE_PATH                     STRING 

, GEN_LAST_SHIPMENT_LEG               TINYINT 

, DEF_DRIVER_TYPE_ID                  BIGINT 

, DEF_TRACTOR_ID                      BIGINT 

, DEF_EQUIPMENT_ID                    BIGINT 

, DRIVER_AVAIL_HRS                    DECIMAL(5,2) 

, WHSE                                STRING 

, OPEN_DATE                           TIMESTAMP 

, CLOSE_DATE                          TIMESTAMP 

, HOLD_DATE                           TIMESTAMP 

, GRP                                 STRING 

, CHAIN                               STRING 

, ZONE                                STRING 

, TERRITORY                           STRING 

, REGION                              STRING 

, DISTRICT                            STRING 

, SHIP_MON                            STRING 

, SHIP_TUE                            STRING 

, SHIP_WED                            STRING 

, SHIP_THU                            STRING 

, SHIP_FRI                            STRING 

, SHIP_SAT                            STRING 

, SHIP_SU                             STRING 

, ACCEPT_IRREG                        STRING 

, WAVE_LABEL_TYPE                     STRING 

, PKG_SLIP_TYPE                       STRING 

, PRINT_CODE                          STRING 

, CARTON_CNT_TYPE                     STRING 

, SHIP_VIA                            STRING 

, RTE_NBR                             STRING 

, RTE_ATTR                            STRING 

, RTE_TO                              STRING 

, RTE_TYPE_1                          STRING 

, RTE_TYPE_2                          STRING 

, RTE_ZIP                             STRING 

, SPL_INSTR_CODE_1                    STRING 

, SPL_INSTR_CODE_2                    STRING 

, SPL_INSTR_CODE_3                    STRING 

, SPL_INSTR_CODE_4                    STRING 

, SPL_INSTR_CODE_5                    STRING 

, SPL_INSTR_CODE_6                    STRING 

, SPL_INSTR_CODE_7                    STRING 

, SPL_INSTR_CODE_8                    STRING 

, SPL_INSTR_CODE_9                    STRING 

, SPL_INSTR_CODE_10                   STRING 

, ASSIGN_MERCH_TYPE                   STRING 

, ASSIGN_MERCH_GROUP                  STRING 

, ASSIGN_STORE_DEPT                   STRING 

, CARTON_LABEL_TYPE                   STRING 

, CARTON_CUBNG_INDIC                  TINYINT 

, MAX_CTN                             INT 

, MAX_PLT                             INT 

, BUSN_UNIT_CODE                      STRING 

, USE_INBD_LPN_AS_OUTBD_LPN           STRING 

, PRINT_COO                           STRING 

, PRINT_INV                           STRING 

, PRINT_SED                           STRING 

, PRINT_CANADIAN_CUST_INVC_FLAG       STRING 

, PRINT_DOCK_RCPT_FLAG                STRING 

, PRINT_NAFTA_COO_FLAG                STRING 

, PRINT_OCEAN_BOL_FLAG                STRING 

, PRINT_PKG_LIST_FLAG                 STRING 

, PRINT_SHPR_LTR_OF_INSTR_FLAG        STRING 

, AUDIT_TRANSACTION                   STRING 

, AUDIT_PARTY_ID                      INT 

, CAPTURE_OTHER_MA                    SMALLINT 

, HIBERNATE_VERSION                   BIGINT 

, STORE_TYPE_GROUPING                 STRING 

, LOAD_RATE                           DECIMAL(16,4) 

, UNLOAD_RATE                         DECIMAL(16,4) 

, HANDLING_RATE                       DECIMAL(16,4) 

, LOAD_UNLOAD_SIZE_UOM_ID             BIGINT 

, LOAD_FACTOR_SIZE_UOM_ID             BIGINT 

, PARCEL_LENGTH_RATIO                 DECIMAL(16,4) 

, PARCEL_WIDTH_RATIO                  DECIMAL(16,4) 

, PARCEL_HEIGHT_RATIO                 DECIMAL(16,4) 

, METER_NUMBER                        BIGINT 

, DIRECT_DELIVERY_ALLOWED             SMALLINT 

, TRACK_EQUIP_ID_FLAG                 TINYINT 

, STAT_CODE                           TINYINT 

, HANDLES_NON_MACHINEABLE             TINYINT 

, MAINTAINS_PERPETUAL_INVTY           TINYINT 

, IS_CUST_OWNED_FACILITY              TINYINT 

, FLOWTHRU_ALLOC_SORT_PRTY            STRING 

, RLS_HOLD_DATE                       TIMESTAMP 

, RE_COMPUTE_FEASIBLE_EQUIPMENT       SMALLINT 

, SMARTPOST_DC_NUMBER                 STRING 

, ADDRESS_TYPE                        STRING 

, SPLC_CODE                           STRING 

, ERPC_CODE                           STRING 

, R260_CODE                           STRING 

, MAX_WAIT_TIME                       INT 

, FREE_WAIT_TIME                      INT 

, IATA_SCR_NBR                        STRING 

, PICKUP_AT_STORE                     TINYINT 

, SHIP_TO_STORE                       TINYINT 

, SHIP_FROM_FACILITY                  TINYINT 

, TRANS_PLAN_FLOW                     SMALLINT 

, AVG_HNDLG_COST_PER_LN               DECIMAL(16,4) 

, AVG_HNDLG_COST_PER_LN_CURCODE       STRING 

, PENALTY_COST                        DECIMAL(13,4) 

, PENALTY_COST_CURRENCY_CODE          STRING 

, MAINTAIN_DISPOSITION                SMALLINT 

, MAINTAIN_SUBLOCATION                SMALLINT 

, MAINTAIN_SEGMENT                    SMALLINT 

, MAINTAIN_CNTRY_OF_ORGN              SMALLINT 

, MAINTAIN_BATCH_NBR                  SMALLINT 

, MAINTAIN_PROD_STAT                  SMALLINT 

, MAINTAIN_INV_TYPE                   SMALLINT 

, MAINTAIN_ITEM_ATTR_1                SMALLINT 

, MAINTAIN_ITEM_ATTR_2                SMALLINT 

, MAINTAIN_ITEM_ATTR_3                SMALLINT 

, MAINTAIN_ITEM_ATTR_4                SMALLINT 

, MAINTAIN_ITEM_ATTR_5                SMALLINT 

, GENERATE_PERPETUAL_EVENT            SMALLINT 

, GENERATE_SEGMENT_EVENT              SMALLINT 

, MAINTAIN_ON_HAND                    SMALLINT 

, MAINTAIN_IN_TRANSIT                 SMALLINT 

, MAINTAIN_ON_ORDER                   SMALLINT 

, FULFILLMENT_GROUP                   STRING 

, REF_FIELD_6                         STRING 

, REF_FIELD_7                         STRING 

, REF_FIELD_8                         STRING 

, REF_FIELD_9                         STRING 

, REF_FIELD_10                        STRING 

, REF_NUM1                            DECIMAL(13,5) 

, REF_NUM2                            DECIMAL(13,5) 

, REF_NUM3                            DECIMAL(13,5) 

, REF_NUM4                            DECIMAL(13,5) 

, REF_NUM5                            DECIMAL(13,5) 

, REF_FIELD_1                         STRING 

, REF_FIELD_2                         STRING 

, REF_FIELD_3                         STRING 

, REF_FIELD_4                         STRING 

, REF_FIELD_5                         STRING 

, WHSE_REGION                         INT 

, GEOFENCE_RADIUS                     DECIMAL(13,5) 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_facility_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_ILM_APPOINTMENTS_PRE" , ***** Creating table: "WM_ILM_APPOINTMENTS_PRE"


CREATE TABLE  WM_ILM_APPOINTMENTS_PRE
( DC_NBR                              SMALLINT                not null

, APPOINTMENT_ID                      INT                not null

, COMPANY_ID                          INT 

, FACILITY_ID                         INT 

, CURRENT_LOCATION_ID                 INT 

, APPT_REASON_CODE                    INT 

, COMMENTS                            STRING 

, CARRIER_CODE                        STRING 

, BUSINESS_PARTNER_ID                 STRING 

, APPT_TYPE                           SMALLINT 

, APPT_STATUS                         SMALLINT 

, CHECKIN_DTTM                        TIMESTAMP 

, CHECKUOUT_DTTM                      TIMESTAMP 

, LOAD_UNLOAD_ST_DTTM                 TIMESTAMP 

, LOAD_UNLOAD_END_DTTM                TIMESTAMP 

, APPT_PRIORITY                       INT 

, REQUEST_COMM_TYPE                   STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE_DTTM                 TIMESTAMP 

, CREATED_SOURCE                      STRING 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, IS_TOP_FRIEGHT                      STRING 

, TC_APPOINTMENT_ID                   STRING 

, DRIVER_ID                           INT 

, APPOINTMENT_CREATION_TYPE           SMALLINT 

, YARD_ID                             INT 

, CONTROL_NO                          STRING 

, DOCK_DOOR_ID                        STRING 

, DOOR_TYPE_ID                        STRING 

, REQUESTOR_NAME                      STRING 

, REQUESTOR_TYPE_ID                   SMALLINT 

, TEMPLATE_ID                         INT 

, TEMPLATE_FLAG                       STRING 

, TP_COMPANY_ID                       INT 

, LOADING_TYPE                        STRING 

, ILM_LOAD_POSITION                   SMALLINT 

, SEAL_NUMBER                         STRING 

, IS_SEAL_NUMBER_VERIFIED             SMALLINT 

, BEEPER_NUMBER                       STRING 

, CARRIER_ID                          INT 

, REASON_ID                           INT 

, APPT_REASON_ID                      INT 

, ORIGIN_FACILITY                     INT 

, EQUIPMENT_ID                        INT 

, BP_COMPANY_ID                       INT 

, REQUESTED_DTTM                      TIMESTAMP 

, SCHED_DEPARTURE_DTTM                TIMESTAMP 

, EST_DEPARTURE_DTTM                  TIMESTAMP 

, DRIVER_DURATION_ON_YARD             INT 

, RESERVED_APPT_ID                    INT 

, CANCELLED_REASON_CODE               INT 

, CREATED_COMPANY_TYPE                STRING 

, TRAILER_DURATION                    INT 

, DRIVER_NAME                         STRING 

, DRIVER_LICENSE                      STRING 

, DRIVER_LICENSE_STATE                STRING 

, LATEST_DELIVERY_DTTM                TIMESTAMP 

, RESERVE_TYPE                        TINYINT 

, HAS_SOFT_CHECK_ERRORS               TINYINT 

, HAS_IMPORT_ERROR                    TINYINT 

, HAS_ALERTS                          TINYINT 

, ACTUAL_CHECKIN_DTTM                 TIMESTAMP 

, SCHEDULED_DTTM                      TIMESTAMP 

, CREATED_DTTM                        TIMESTAMP 

, BP_ID                               BIGINT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_ilm_appointments_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (APPOINTMENT_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ILM_APPOINTMENT_OBJECTS_PRE" , ***** Creating table: "WM_ILM_APPOINTMENT_OBJECTS_PRE"


CREATE TABLE  WM_ILM_APPOINTMENT_OBJECTS_PRE
( DC_NBR                              SMALLINT                not null

, ID                                  INT                not null

, APPT_OBJ_TYPE                       SMALLINT 

, APPT_OBJ_ID                         INT 

, COMPANY_ID                          INT 

, APPOINTMENT_ID                      INT 

, STOP_SEQ                            SMALLINT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_ilm_appointment_objects_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ILM_APPOINTMENT_STATUS_PRE" , ***** Creating table: "WM_ILM_APPOINTMENT_STATUS_PRE"


CREATE TABLE  WM_ILM_APPOINTMENT_STATUS_PRE
( DC_NBR                              SMALLINT                not null

, APPT_STATUS_CODE                    SMALLINT                not null

, DESCRIPTION                         STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_ilm_appointment_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ILM_APPOINTMENT_TYPE_PRE" , ***** Creating table: "WM_ILM_APPOINTMENT_TYPE_PRE"


CREATE TABLE  WM_ILM_APPOINTMENT_TYPE_PRE
( DC_NBR                              SMALLINT                not null

, APPT_TYPE                           SMALLINT                not null

, DESCRIPTION                         STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_ilm_appointment_type_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ILM_APPT_EQUIPMENTS_PRE" , ***** Creating table: "WM_ILM_APPT_EQUIPMENTS_PRE"


CREATE TABLE  WM_ILM_APPT_EQUIPMENTS_PRE
( DC_NBR                              SMALLINT                not null

, APPOINTMENT_ID                      INT                not null

, COMPANY_ID                          INT                not null

, EQUIPMENT_INSTANCE_ID               INT                not null

, EQUIPMENT_NUMBER                    STRING 

, EQUIPMENT_LICENSE_NUMBER            STRING 

, EQUIPMENT_LICENSE_STATE             STRING 

, EQUIPMENT_TYPE                      SMALLINT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_ilm_appt_equipments_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ILM_TASK_STATUS_PRE" , ***** Creating table: "WM_ILM_TASK_STATUS_PRE"


CREATE TABLE  WM_ILM_TASK_STATUS_PRE
( DC_NBR                              SMALLINT                not null

, TASK_STATUS                         INT                not null

, DESCRIPTION                         STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_ilm_task_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ILM_YARD_ACTIVITY_PRE" , ***** Creating table: "WM_ILM_YARD_ACTIVITY_PRE"


CREATE TABLE  WM_ILM_YARD_ACTIVITY_PRE
( DC_NBR                              SMALLINT                not null

, ACTIVITY_ID                         INT                not null

, COMPANY_ID                          INT 

, APPOINTMENT_ID                      INT 

, EQUIPMENT_ID1                       INT 

, ACTIVITY_TYPE                       SMALLINT 

, ACTIVITY_SOURCE                     STRING 

, ACTIVITY_DTTM                       TIMESTAMP 

, DRIVER_ID                           INT 

, EQUIPMENT_ID2                       INT 

, TASK_ID                             INT 

, LOCATION_ID                         INT 

, NO_OF_PALLETS                       INT 

, EQUIP_INS_STATUS                    SMALLINT 

, FACILITY_ID                         INT 

, VISIT_DETAIL_ID                     INT 

, LOCN_ID                             STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_ilm_yard_activity_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ITEM_CBO_PRE" , ***** Creating table: "WM_ITEM_CBO_PRE"


CREATE TABLE  WM_ITEM_CBO_PRE
( DC_NBR                              SMALLINT                not null

, ITEM_ID                             INT                not null

, COMPANY_ID                          INT 

, ITEM_NAME                           STRING 

, DESCRIPTION                         STRING 

, PROTECTION_LEVEL_ID                 INT 

, COMMODITY_CLASS_ID                  INT 

, COMMODITY_CODE_ID                   BIGINT 

, PRODUCT_CLASS_ID                    INT 

, UN_NUMBER_ID                        BIGINT 

, BASE_STORAGE_UOM_ID                 INT 

, ITEM_SEASON                         STRING 

, ITEM_SEASON_YEAR                    STRING 

, ITEM_STYLE                          STRING 

, ITEM_STYLE_SFX                      STRING 

, ITEM_COLOR                          STRING 

, ITEM_COLOR_SFX                      STRING 

, ITEM_SECOND_DIM                     STRING 

, ITEM_QUALITY                        STRING 

, ITEM_SIZE_DESC                      STRING 

, ITEM_BAR_CODE                       STRING 

, ITEM_DESC_SHORT                     STRING 

, ITEM_UPC_GTIN                       STRING 

, UNIT_WEIGHT                         DECIMAL(16,4) 

, WEIGHT_UOM_ID                       INT 

, UNIT_VOLUME                         DECIMAL(16,4) 

, VOLUME_UOM_ID                       INT 

, UNIT_HEIGHT                         DECIMAL(16,4) 

, UNIT_WIDTH                          DECIMAL(16,4) 

, UNIT_LENGTH                         DECIMAL(16,4) 

, DIMENSION_UOM_ID                    INT 

, VARIABLE_WEIGHT                     TINYINT 

, DATABASE_QTY_UOM_ID                 INT 

, DISPLAY_QTY_UOM_ID                  INT 

, STATUS_CODE                         TINYINT 

, ITEM_IMAGE_FILENAME                 STRING 

, AUDIT_CREATED_SOURCE                STRING 

, AUDIT_CREATED_SOURCE_TYPE           TINYINT 

, AUDIT_CREATED_DTTM                  TIMESTAMP 

, AUDIT_LAST_UPDATED_SOURCE           STRING 

, AUDIT_LAST_UPDATED_SOURCE_TYPE      TINYINT 

, AUDIT_LAST_UPDATED_DTTM             TIMESTAMP 

, MARK_FOR_DELETION                   TINYINT 

, CATCH_WEIGHT_ITEM                   STRING 

, COMMODITY_LEVEL_DESC                STRING 

, CHANNEL_TYPE_ID                     INT 

, COLOR_DESC                          STRING 

, VERSION                             BIGINT 

, ITEM_QUALITY_CODE                   INT 

, PROD_TYPE                           STRING 

, STD_BUNDL_QTY                       DECIMAL(9,2) 

, STAB_CODE                           STRING 

, ITEM_ORIENTATION                    STRING 

, PROTN_FACTOR                        STRING 

, CAVITY_LEN                          DECIMAL(7,2) 

, CAVITY_WD                           DECIMAL(7,2) 

, CAVITY_HT                           DECIMAL(7,2) 

, INCREMENTAL_LEN                     DECIMAL(16,4) 

, INCREMENTAL_WD                      DECIMAL(16,4) 

, INCREMENTAL_HT                      DECIMAL(16,4) 

, STACKABLE_ITEM                      STRING 

, MAX_NEST_NUMBER                     SMALLINT 

, STCC_CODE_ID                        INT 

, SITC_CODE_ID                        INT 

, SCHEDULE_B_CODE_ID                  INT 

, SOLD_ONLINE                         TINYINT 

, SOLD_IN_STORES                      TINYINT 

, PICKUP_AT_STORE                     TINYINT 

, SHIP_TO_STORE                       TINYINT 

, URL                                 STRING 

, IS_RETURNABLE                       TINYINT 

, REF_FIELD1                          STRING 

, REF_FIELD2                          STRING 

, REF_FIELD3                          STRING 

, REF_FIELD4                          STRING 

, REF_FIELD5                          STRING 

, CUBISCAN_LAST_UPDATED_DTTM          TIMESTAMP 

, IS_EXCHANGEABLE                     TINYINT 

, LONG_DESCRIPTION                    STRING 

, MIN_SHIP_INNER_UOM_ID               STRING 

, PRICE_STATUS                        STRING 

, SELL_THROUGH                        DECIMAL(13,4) 

, COMMERCE_ATTRIBUTE1                 STRING 

, COMMERCE_ATTRIBUTE2                 STRING 

, COMMERCE_NUM_ATTRIBUTE1             DECIMAL(13,4) 

, COMMERCE_NUM_ATTRIBUTE2             DECIMAL(13,4) 

, COMMERCE_DATE_ATTRIBUTE1            TIMESTAMP 

, BRAND                               STRING 

, ITEM_DISPOSITION                    STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, REF_FIELD6                          STRING 

, REF_FIELD7                          STRING 

, REF_FIELD8                          STRING 

, REF_FIELD9                          STRING 

, REF_FIELD10                         STRING 

, REF_NUM1                            DECIMAL(13,5) 

, REF_NUM2                            DECIMAL(13,5) 

, REF_NUM3                            DECIMAL(13,5) 

, REF_NUM4                            DECIMAL(13,5) 

, REF_NUM5                            DECIMAL(13,5) 

, SIZE_SEQ                            SMALLINT 

, COLOR_SEQ                           SMALLINT 

, RETURNABLE_AT_STORE                 TINYINT 

, GIFT_CARD_TYPE                      STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_item_cbo_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_ITEM_FACILITY_MAPPING_WMS_PRE" , ***** Creating table: "WM_ITEM_FACILITY_MAPPING_WMS_PRE"


CREATE TABLE  WM_ITEM_FACILITY_MAPPING_WMS_PRE
( DC_NBR                              SMALLINT                not null

, ITEM_FACILITY_MAPPING_ID            DECIMAL(19,0)               not null

, LPN_PER_TIER                        INT 

, TIER_PER_PLT                        INT 

, CASE_SIZE_TYPE                      STRING 

, PICK_RATE                           DECIMAL(9,4) 

, WAGE_VALUE                          DECIMAL(9,4) 

, PACK_RATE                           DECIMAL(13,4) 

, SPL_PROC_RATE                       DECIMAL(9,4) 

, AUTO_SUB_CASE                       STRING 

, ASSIGN_DYNAMIC_ACTV_PICK_SITE       STRING 

, ASSIGN_DYNAMIC_CASE_PICK_SITE       STRING 

, PICK_LOCN_ASSIGN_TYPE               STRING 

, PUTWY_TYPE                          STRING 

, DFLT_WAVE_PROC_TYPE                 SMALLINT 

, XCESS_WAVE_NEED_PROC_TYPE           TINYINT 

, ALLOC_TYPE                          STRING 

, VIOLATE_FIFO_ALLOC_QTY_MATCH        STRING 

, QV_ITEM_GRP                         STRING 

, QUAL_INSPCT_ITEM_GRP                STRING 

, MAX_UNITS_IN_DYNAMIC_ACTV           DECIMAL(13,5) 

, MAX_CASES_IN_DYNAMIC_ACTV           INT 

, MAX_UNITS_IN_DYNAMIC_CASE_PICK      DECIMAL(13,5) 

, MAX_CASES_IN_DYNAMIC_CASE_PICK      INT 

, CASE_CNT_DATE_TIME                  TIMESTAMP 

, ACTV_CNT_DATE_TIME                  TIMESTAMP 

, CASE_PICK_CNT_DATE_TIME             TIMESTAMP 

, VENDOR_CARTON_PER_TIER              INT 

, VENDOR_TIER_PER_PLT                 INT 

, ORD_CARTON_PER_TIER                 INT 

, ORD_TIER_PER_PLT                    INT 

, DFLT_CATCH_WT_METHOD                STRING 

, DFLT_DATE_MASK                      STRING 

, CARTON_BREAK_ATTR                   STRING 

, MISC_SHORT_ALPHA_1                  STRING 

, MISC_SHORT_ALPHA_2                  STRING 

, MISC_ALPHA_1                        STRING 

, MISC_ALPHA_2                        STRING 

, MISC_ALPHA_3                        STRING 

, MISC_NUMERIC_1                      DECIMAL(9,3) 

, MISC_NUMERIC_2                      DECIMAL(9,3) 

, MISC_NUMERIC_3                      DECIMAL(9,3) 

, MISC_DATE_1                         TIMESTAMP 

, MISC_DATE_2                         TIMESTAMP 

, CHUTE_ASSIGN_TYPE                   STRING 

, ACTV_REPL_ORGN                      STRING 

, UIN_NBR                             STRING 

, FIFO_RANGE                          SMALLINT 

, PRTL_CASE_ALLOC_THRESH_UNITS        DECIMAL(13,4) 

, PRTL_CASE_PUTWY_THRESH_UNITS        DECIMAL(13,4) 

, VENDOR_TAGGED_EPC_FLAG              STRING 

, ITEM_AVG_WT                         DECIMAL(13,4) 

, DFLT_MIN_FROM_PREV_LOCN_FLAG        STRING 

, SLOT_MISC_1                         STRING 

, SLOT_MISC_2                         STRING 

, SLOT_MISC_3                         STRING 

, SLOT_MISC_4                         STRING 

, SLOT_MISC_5                         STRING 

, SLOT_MISC_6                         STRING 

, SLOT_ROTATE_EACHES_FLAG             STRING 

, SLOT_ROTATE_INNERS_FLAG             STRING 

, SLOT_ROTATE_BINS_FLAG               STRING 

, SLOT_ROTATE_CASES_FLAG              STRING 

, SLOT_3D_SLOTTING_FLAG               STRING 

, SLOT_NEST_EACHES_FLAG               STRING 

, SLOT_INCR_HT                        DECIMAL(7,2) 

, SLOT_INCR_LEN                       DECIMAL(7,2) 

, SLOT_INCR_WIDTH                     DECIMAL(7,2) 

, NBR_OF_DYN_ACTV_PICK_PER_ITEM       SMALLINT 

, NBR_OF_DYN_CASE_PICK_PER_ITEM       SMALLINT 

, AUDIT_CREATED_SOURCE                STRING 

, AUDIT_CREATED_SOURCE_TYPE           TINYINT 

, AUDIT_CREATED_DTTM                  TIMESTAMP 

, AUDIT_LAST_UPDATED_SOURCE           STRING 

, AUDIT_LAST_UPDATED_SOURCE_TYPE      TINYINT 

, AUDIT_LAST_UPDATED_DTTM             TIMESTAMP 

, MARK_FOR_DELETION                   TINYINT 

, ITEM_ID                             INT 

, FACILITY_ID                         INT 

, BUSINESS_PARTNER_ID                 STRING 

, AVERAGE_MOVEMENT                    DECIMAL(9,2) 

, SHELF_DAYS                          INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_item_facility_mapping_wms_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_ITEM_FACILITY_SLOTTING_PRE" , ***** Creating table: "WM_ITEM_FACILITY_SLOTTING_PRE"


CREATE TABLE  WM_ITEM_FACILITY_SLOTTING_PRE
( DC_NBR                              SMALLINT                not null

, ITEM_FACILITY_MAPPING_ID            INT                not null

, ITEM_ID                             INT 

, ITEM_NAME                           STRING 

, WHSE_CODE                           STRING 

, UNIT_MEAS                           BIGINT 

, MANUAL                              TINYINT 

, IS_NEW                              TINYINT 

, ALLOW_SLU                           BIGINT 

, ALLOW_SU                            BIGINT 

, DISC_TRANS                          TIMESTAMP 

, EACH_PER_BIN                        BIGINT 

, EX_RECPT_DATE                       TIMESTAMP 

, DISCONT                             TINYINT 

, PROCESSED                           TINYINT 

, INN_PER_CS                          BIGINT 

, MAX_STACKING                        BIGINT 

, MAX_LANES                           BIGINT 

, NUM_UNITS_PER_BIN                   BIGINT 

, RESERVED_1                          STRING 

, RESERVED_2                          STRING 

, RESERVED_3                          BIGINT 

, RESERVED_4                          BIGINT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, MOD_USER                            STRING 

, RESERVE_RACK_TYPE_ID                BIGINT 

, ALLOW_RESERVE                       BIGINT 

, PALPAT_RESERVE                      SMALLINT 

, THREE_D_CALC_DONE                   TINYINT 

, MAX_SLOTS                           BIGINT 

, MAX_PALLET_STACKING                 BIGINT 

, PROP_BORROWING_OBJECT               BIGINT 

, PROP_BORROWING_SPECIFIC             DECIMAL(19,0) 

, HEIGHT_CAN_BORROW                   TINYINT 

, LENGTH_CAN_BORROW                   TINYINT 

, WIDTH_CAN_BORROW                    TINYINT 

, WEIGHT_CAN_BORROW                   TINYINT 

, VEND_PACK_CAN_BORROW                TINYINT 

, INNER_PACK_CAN_BORROW               TINYINT 

, TI_CAN_BORROW                       TINYINT 

, HI_CAN_BORROW                       TINYINT 

, QTY_PER_GRAB_CAN_BORROW             TINYINT 

, HAND_ATTR_CAN_BORROW                TINYINT 

, FAM_GRP_CAN_BORROW                  TINYINT 

, COMMODITY_CAN_BORROW                TINYINT 

, CRUSHABILITY_CAN_BORROW             TINYINT 

, HAZARD_CAN_BORROW                   TINYINT 

, VEND_CODE_CAN_BORROW                TINYINT 

, MISC1_CAN_BORROW                    TINYINT 

, MISC2_CAN_BORROW                    TINYINT 

, MISC3_CAN_BORROW                    TINYINT 

, MISC4_CAN_BORROW                    TINYINT 

, MISC5_CAN_BORROW                    TINYINT 

, MISC6_CAN_BORROW                    TINYINT 

, HEIGHT_BORROWED                     TINYINT 

, LENGTH_BORROWED                     TINYINT 

, WIDTH_BORROWED                      TINYINT 

, WEIGHT_BORROWED                     TINYINT 

, VEND_PACK_BORROWED                  TINYINT 

, INNER_PACK_BORROWED                 TINYINT 

, TI_BORROWED                         TINYINT 

, HI_BORROWED                         TINYINT 

, QTY_PER_GRAB_BORROWED               TINYINT 

, HAND_ATTR_BORROWED                  TINYINT 

, FAM_GRP_BORROWED                    TINYINT 

, COMMODITY_BORROWED                  TINYINT 

, CRUSHABILITY_BORROWED               TINYINT 

, HAZARD_BORROWED                     TINYINT 

, VEND_CODE_BORROWED                  TINYINT 

, MISC1_BORROWED                      TINYINT 

, MISC2_BORROWED                      TINYINT 

, MISC3_BORROWED                      TINYINT 

, MISC4_BORROWED                      TINYINT 

, MISC5_BORROWED                      TINYINT 

, MISC6_BORROWED                      TINYINT 

, AFRAME_HT                           DECIMAL(9,4) 

, AFRAME_LEN                          DECIMAL(9,4) 

, AFRAME_WID                          DECIMAL(9,4) 

, AFRAME_WT                           DECIMAL(9,4) 

, AFRAME_ALLOW                        TINYINT 

, ITEM_NUM_1                          DECIMAL(13,4) 

, ITEM_NUM_2                          DECIMAL(13,4) 

, ITEM_NUM_3                          DECIMAL(13,4) 

, ITEM_NUM_4                          DECIMAL(13,4) 

, ITEM_NUM_5                          DECIMAL(13,4) 

, ITEM_CHAR_1                         STRING 

, ITEM_CHAR_2                         STRING 

, ITEM_CHAR_3                         STRING 

, ITEM_CHAR_4                         STRING 

, ITEM_CHAR_5                         STRING 

, SLOTTING_GROUP                      STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_item_facility_slotting_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (ITEM_FACILITY_MAPPING_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ITEM_GROUP_WMS_PRE" , ***** Creating table: "WM_ITEM_GROUP_WMS_PRE"


CREATE TABLE  WM_ITEM_GROUP_WMS_PRE
( DC_NBR                              SMALLINT                not null

, ITEM_GROUP_ID                       INT                not null

, ITEM_ID                             INT 

, GROUP_TYPE                          STRING 

, GROUP_CODE                          STRING 

, GROUP_ATTRIBUTE                     STRING 

, AUDIT_CREATED_SOURCE_TYPE           TINYINT 

, AUDIT_CREATED_DTTM                  TIMESTAMP 

, AUDIT_LAST_UPDATED_SOURCE_TYPE      TINYINT 

, AUDIT_LAST_UPDATED_DTTM             TIMESTAMP 

, MARK_FOR_DELETION                   TINYINT 

, AUDIT_CREATED_SOURCE                STRING 

, AUDIT_LAST_UPDATED_SOURCE           STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_item_group_wms_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_ITEM_PACKAGE_CBO_PRE" , ***** Creating table: "WM_ITEM_PACKAGE_CBO_PRE"


CREATE TABLE  WM_ITEM_PACKAGE_CBO_PRE
( DC_NBR                              SMALLINT                not null

, ITEM_PACKAGE_ID                     INT                not null

, ITEM_ID                             INT 

, PACKAGE_UOM_ID                      INT 

, QUANTITY                            DECIMAL(16,4) 

, WEIGHT                              DECIMAL(16,4) 

, WEIGHT_UOM_ID                       INT 

, GTIN                                STRING 

, AUDIT_CREATED_SOURCE                STRING 

, AUDIT_CREATED_SOURCE_TYPE           TINYINT 

, AUDIT_CREATED_DTTM                  TIMESTAMP 

, AUDIT_LAST_UPDATED_SOURCE           STRING 

, AUDIT_LAST_UPDATED_SOURCE_TYPE      TINYINT 

, AUDIT_LAST_UPDATED_DTTM             TIMESTAMP 

, MARK_FOR_DELETION                   TINYINT 

, DIMENSION_UOM_ID                    INT 

, VOLUME                              DECIMAL(16,4) 

, VOLUME_UOM_ID                       DECIMAL(16,4) 

, LENGTH                              DECIMAL(16,4) 

, HEIGHT                              DECIMAL(16,4) 

, WIDTH                               DECIMAL(16,4) 

, HIBERNATE_VERSION                   BIGINT 

, IS_STD                              STRING 

, BUSINESS_PARTNER_ID                 STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_item_package_cbo_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_ITEM_WMS_PRE" , ***** Creating table: "WM_ITEM_WMS_PRE"


CREATE TABLE  WM_ITEM_WMS_PRE
( DC_NBR                              SMALLINT                not null

, ITEM_ID                             INT                not null

, SIZE_RANGE_CODE                     STRING 

, SIZE_REL_POSN_IN_TABLE              STRING 

, VOLTY_CODE                          STRING 

, PKG_TYPE                            STRING 

, PROD_SUB_GRP                        STRING 

, PROD_TYPE                           STRING 

, PROD_LINE                           STRING 

, SALE_GRP                            STRING 

, COORD_1                             INT 

, COORD_2                             INT 

, CARTON_TYPE                         STRING 

, UNIT_PRICE                          DECIMAL(16,4) 

, RETAIL_PRICE                        DECIMAL(16,4) 

, OPER_CODE                           STRING 

, MAX_CASE_QTY                        DECIMAL(9,2) 

, CUBE_MULT_QTY                       DECIMAL(13,4) 

, NEST_VOL                            DECIMAL(13,4) 

, NEST_CNT                            INT 

, UNITS_PER_PICK_ACTIVE               DECIMAL(9,2) 

, HNDL_ATTR_ACTIVE                    STRING 

, UNITS_PER_PICK_CASE_PICK            DECIMAL(9,2) 

, HNDL_ATTR_CASE_PICK                 STRING 

, UNITS_PER_PICK_RESV                 DECIMAL(9,2) 

, HNDL_ATTR_RESV                      STRING 

, PROD_LIFE_IN_DAY                    INT 

, MAX_RECV_TO_XPIRE_DAYS              INT 

, AVG_DLY_DMND                        DECIMAL(13,5) 

, WT_TOL_PCNT                         DECIMAL(5,2) 

, CONS_PRTY_DATE_CODE                 STRING 

, CONS_PRTY_DATE_WINDOW               STRING 

, CONS_PRTY_DATE_WINDOW_INCR          SMALLINT 

, ACTVTN_DATE                         TIMESTAMP 

, ALLOW_RCPT_OLDER_ITEM               STRING 

, CRITCL_DIM_1                        DECIMAL(7,2) 

, CRITCL_DIM_2                        DECIMAL(7,2) 

, CRITCL_DIM_3                        DECIMAL(7,2) 

, MFG_DATE_REQD                       STRING 

, XPIRE_DATE_REQD                     STRING 

, SHIP_BY_DATE_REQD                   STRING 

, ITEM_ATTR_REQD                      STRING 

, BATCH_REQD                          STRING 

, PROD_STAT_REQD                      STRING 

, CNTRY_OF_ORGN_REQD                  STRING 

, VENDOR_ITEM_NBR                     STRING 

, PICK_WT_TOL_TYPE                    STRING 

, PICK_WT_TOL_AMNT                    SMALLINT 

, MHE_WT_TOL_TYPE                     STRING 

, MHE_WT_TOL_AMNT                     SMALLINT 

, LOAD_ATTR                           STRING 

, TEMP_ZONE                           STRING 

, TRLR_TEMP_ZONE                      STRING 

, PKT_CONSOL_ATTR                     STRING 

, BUYER_DISP_CODE                     STRING 

, CRUSH_CODE                          STRING 

, CONVEY_FLAG                         STRING 

, STORE_DEPT                          STRING 

, MERCH_TYPE                          STRING 

, MERCH_GROUP                         STRING 

, SPL_INSTR_CODE_1                    STRING 

, SPL_INSTR_CODE_2                    STRING 

, SPL_INSTR_CODE_3                    STRING 

, SPL_INSTR_CODE_4                    STRING 

, SPL_INSTR_CODE_5                    STRING 

, SPL_INSTR_CODE_6                    STRING 

, SPL_INSTR_CODE_7                    STRING 

, SPL_INSTR_CODE_8                    STRING 

, SPL_INSTR_CODE_9                    STRING 

, SPL_INSTR_CODE_10                   STRING 

, SPL_INSTR_1                         STRING 

, SPL_INSTR_2                         STRING 

, PROMPT_FOR_VENDOR_ITEM_NBR          STRING 

, PROMPT_PACK_QTY                     STRING 

, ECCN_NBR                            STRING 

, EXP_LICN_NBR                        STRING 

, EXP_LICN_XP_DATE                    TIMESTAMP 

, EXP_LICN_SYMBOL                     STRING 

, ORGN_CERT_CODE                      STRING 

, ITAR_EXEMPT_NBR                     STRING 

, NMFC_CODE                           STRING 

, FRT_CLASS                           STRING 

, DFLT_BATCH_STAT                     STRING 

, DFLT_INCUB_LOCK                     STRING 

, BASE_INCUB_FLAG                     STRING 

, INCUB_DAYS                          SMALLINT 

, INCUB_HOURS                         DECIMAL(4,2) 

, SRL_NBR_BRCD_TYPE                   STRING 

, MINOR_SRL_NBR_REQ                   TINYINT 

, DUP_SRL_NBR_FLAG                    TINYINT 

, MAX_RCPT_QTY                        DECIMAL(9,2) 

, VOCOLLECT_BASE_WT                   DECIMAL(13,4) 

, VOCOLLECT_BASE_QTY                  DECIMAL(9,2) 

, VOCOLLECT_BASE_ITEM                 STRING 

, PICK_WT_TOL_AMNT_ERROR              SMALLINT 

, PRICE_TKT_TYPE                      STRING 

, MONETARY_VALUE                      DECIMAL(16,4) 

, MV_CURRENCY_CODE                    STRING 

, CODE_DATE_PROMPT_METHOD_FLAG        STRING 

, MIN_RECV_TO_XPIRE_DAYS              INT 

, MIN_PCNT_FOR_LPN_SPLIT              SMALLINT 

, MIN_LPN_QTY_FOR_SPLIT               DECIMAL(13,5) 

, PROD_CATGRY                         STRING 

, AUDIT_CREATED_SOURCE                STRING 

, AUDIT_CREATED_SOURCE_TYPE           TINYINT 

, AUDIT_CREATED_DTTM                  TIMESTAMP 

, AUDIT_LAST_UPDATED_SOURCE           STRING 

, AUDIT_LAST_UPDATED_SOURCE_TYPE      TINYINT 

, AUDIT_LAST_UPDATED_DTTM             TIMESTAMP 

, MARK_FOR_DELETION                   TINYINT 

, TOP_SHELF_ELIGIBLE                  TINYINT 

, VOCO_ABS_PICK_TOL_AMT               INT 

, CARTON_CNT_DATE_TIME                TIMESTAMP 

, TRANS_INVN_CNT_DATE_TIME            TIMESTAMP 

, WORK_ORD_CNT_DATE_TIME              TIMESTAMP 

, SHELF_DAYS                          INT 

, VENDOR_MASTER_ID                    INT 

, NBR_OF_DYN_ACTV_PICK_PER_SKU        SMALLINT 

, NBR_OF_DYN_CASE_PICK_PER_SKU        SMALLINT 

, LET_UP_PRTY                         INT 

, PREF_CRITERIA_FLAG                  STRING 

, PRODUCER_FLAG                       STRING 

, NET_COST_FLAG                       STRING 

, MARKS_NBRS                          STRING 

, SLOTTING_OPT_STAT_CODE              SMALLINT 

, PRICE_TIX_AVAIL                     STRING 

, MV_SIZE_UOM                         INT 

, UNITS_PER_GRAB_PLT                  DECIMAL(13,4) 

, HNDL_ATTR_PLT                       STRING 

, HNDL_ATTR_ACT_UOM_ID                INT 

, HNDL_ATTR_CASE_PICK_UOM_ID          INT 

, SRL_NBR_REQD                        TINYINT 

, CC_UNIT_TOLER_VALUE                 BIGINT 

, CC_WGT_TOLER_VALUE                  DECIMAL(13,4) 

, CC_DLR_TOLER_VALUE                  DECIMAL(13,4) 

, CC_PCNT_TOLER_VALUE                 DECIMAL(13,4) 

, DISPOSITION_TYPE                    STRING 

, EXP_LICN_DESCRIPTION                STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_item_wms_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_LABOR_ACTIVITY_PRE" , ***** Creating table: "WM_LABOR_ACTIVITY_PRE"


CREATE TABLE  WM_LABOR_ACTIVITY_PRE
( DC_NBR                              SMALLINT                not null

, LABOR_ACTIVITY_ID                   INT                not null

, NAME                                STRING 

, DESCRIPTION                         STRING 

, AIL_ACT                             STRING 

, ACT_TYPE                            STRING 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, HIBERNATE_VERSION                   BIGINT 

, PROMPT_LOCN                         STRING 

, DISPL_EPP                           STRING 

, INCLD_TRVL                          STRING 

, CRIT_RULE_TYPE                      STRING 

, PERMISSION_ID                       INT 

, COMPANY_ID                          INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_labor_activity_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LABOR_CRITERIA_PRE" , ***** Creating table: "WM_LABOR_CRITERIA_PRE"


CREATE TABLE  WM_LABOR_CRITERIA_PRE
( DC_NBR                              SMALLINT                not null

, CRIT_ID                             INT                not null

, CRIT_CODE                           STRING 

, DESCRIPTION                         STRING 

, RULE_FILTER                         STRING 

, DATA_TYPE                           STRING 

, DATA_SIZE                           INT 

, HIBERNATE_VERSION                   BIGINT 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, COMPANY_ID                          INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_labor_criteria_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LABOR_MSG_CRIT_PRE" , ***** Creating table: "WM_LABOR_MSG_CRIT_PRE"


CREATE TABLE  WM_LABOR_MSG_CRIT_PRE
( DC_NBR                              SMALLINT                not null

, LABOR_MSG_CRIT_ID                   DECIMAL(20,0)               not null

, LABOR_MSG_ID                        DECIMAL(20,0) 

, TRAN_NBR                            INT 

, CRIT_SEQ_NBR                        INT 

, MSG_STAT_CODE                       STRING 

, CRIT_TYPE                           STRING 

, CRIT_VAL                            STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, WHSE                                STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, HIBERNATE_VERSION                   BIGINT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_labor_msg_crit_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (LABOR_MSG_CRIT_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LABOR_MSG_DTL_CRIT_PRE" , ***** Creating table: "WM_LABOR_MSG_DTL_CRIT_PRE"


CREATE TABLE  WM_LABOR_MSG_DTL_CRIT_PRE
( DC_NBR                              SMALLINT                not null

, LABOR_MSG_DTL_CRIT_ID               DECIMAL(20,0)               not null

, LABOR_MSG_DTL_ID                    DECIMAL(20,0) 

, TRAN_NBR                            INT 

, MSG_STAT_CODE                       STRING 

, CRIT_TYPE                           STRING 

, CRIT_VAL                            STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, WHSE                                STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, HIBERNATE_VERSION                   BIGINT 

, CRIT_SEQ_NBR                        INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_labor_msg_dtl_crit_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (LABOR_MSG_DTL_CRIT_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LABOR_MSG_DTL_PRE" , ***** Creating table: "WM_LABOR_MSG_DTL_PRE"


CREATE TABLE  WM_LABOR_MSG_DTL_PRE
( DC_NBR                              SMALLINT                not null

, LABOR_MSG_DTL_ID                    DECIMAL(20,0)               not null

, LABOR_MSG_ID                        DECIMAL(20,0) 

, TRAN_NBR                            INT 

, TRAN_SEQ_NBR                        INT 

, MSG_STAT_CODE                       STRING 

, SEQ_NBR                             STRING 

, HNDL_ATTR                           STRING 

, QTY_PER_GRAB                        DECIMAL(13,5) 

, LOCN_CLASS                          STRING 

, QTY                                 DECIMAL(13,5) 

, TC_ILPN_ID                          STRING 

, TC_OLPN_ID                          STRING 

, PALLET_ID                           STRING 

, LOCN_SLOT_TYPE                      STRING 

, LOCN_X_COORD                        DECIMAL(13,5) 

, LOCN_Y_COORD                        DECIMAL(13,5) 

, LOCN_Z_COORD                        DECIMAL(13,5) 

, LOCN_TRAV_AISLE                     STRING 

, LOCN_TRAV_ZONE                      STRING 

, BOX_QTY                             DECIMAL(13,5) 

, INNERPACK_QTY                       DECIMAL(13,5) 

, PACK_QTY                            DECIMAL(13,5) 

, CASE_QTY                            DECIMAL(13,5) 

, TIER_QTY                            DECIMAL(13,5) 

, PALLET_QTY                          DECIMAL(13,5) 

, MISC                                STRING 

, CO                                  STRING 

, DIV                                 STRING 

, DSP_LOCN                            STRING 

, ITEM_BAR_CODE                       STRING 

, WEIGHT                              DECIMAL(20,7) 

, VOLUME                              DECIMAL(20,7) 

, CRIT_DIM1                           DECIMAL(7,2) 

, CRIT_DIM2                           DECIMAL(7,2) 

, CRIT_DIM3                           DECIMAL(7,2) 

, START_DATE_TIME                     TIMESTAMP 

, HANDLING_UOM                        STRING 

, SEASON                              STRING 

, SEASON_YR                           STRING 

, STYLE                               STRING 

, STYLE_SFX                           STRING 

, COLOR                               STRING 

, COLOR_SFX                           STRING 

, SEC_DIM                             STRING 

, QUAL                                STRING 

, SIZE_RNGE_CODE                      STRING 

, SIZE_REL_POSN_IN_TABLE              STRING 

, MISC_NUM_1                          DECIMAL(13,5) 

, MISC_NUM_2                          DECIMAL(13,5) 

, MISC_2                              STRING 

, LOADED                              STRING 

, PUTAWAY_ZONE                        STRING 

, PICK_DETERMINATION_ZONE             STRING 

, WORK_GROUP                          STRING 

, WORK_AREA                           STRING 

, PULL_ZONE                           STRING 

, ASSIGNMENT_ZONE                     STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, HIBERNATE_VERSION                   BIGINT 

, ITEM_NAME                           STRING 

, LOCN_GRP_ATTR                       STRING 

, RESOURCE_GROUP_ID                   STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_labor_msg_dtl_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (LABOR_MSG_DTL_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LABOR_MSG_PRE" , ***** Creating table: "WM_LABOR_MSG_PRE"


CREATE TABLE  WM_LABOR_MSG_PRE
( DC_NBR                              SMALLINT                not null

, LABOR_MSG_ID                        DECIMAL(20,0)               not null

, STATUS                              INT 

, WHSE                                STRING 

, TRAN_NBR                            INT 

, ACT_NAME                            STRING 

, LOGIN_USER_ID                       STRING 

, SCHED_START_TIME                    TIMESTAMP 

, SCHED_START_DATE                    TIMESTAMP 

, VHCL_TYPE                           STRING 

, REF_CODE                            STRING 

, REF_NBR                             STRING 

, TC_COMPANY_ID                       INT 

, DIV                                 STRING 

, SHIFT                               STRING 

, USER_DEF_VAL_1                      STRING 

, USER_DEF_VAL_2                      STRING 

, MOD_USER_ID                         STRING 

, RESEND_TRAN                         STRING 

, REQ_SAM_REPLY                       STRING 

, PRIORITY                            INT 

, ENGINE_CODE                         STRING 

, TEAM_STD_GRP                        STRING 

, MISC                                STRING 

, MISC_2                              STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, ACTUAL_END_TIME                     TIMESTAMP 

, ACTUAL_END_DATE                     TIMESTAMP 

, PLAN_ID                             STRING 

, WAVE_NBR                            STRING 

, TRANS_TYPE                          STRING 

, TRANS_VALUE                         STRING 

, ENGINE_GROUP                        STRING 

, COMPLETED_WORK                      DECIMAL(20,7) 

, WHSE_DATE                           TIMESTAMP 

, JOB_FUNCTION                        STRING 

, LEVEL_1                             STRING 

, LEVEL_2                             STRING 

, LEVEL_3                             STRING 

, LEVEL_4                             STRING 

, LEVEL_5                             STRING 

, CARTON_NBR                          STRING 

, TASK_NBR                            STRING 

, CASE_NBR                            STRING 

, ORIG_COMPLETED_WORK                 DECIMAL(20,7) 

, TRAN_DATA_ELS_TRAN_ID               DECIMAL(20,0) 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, EVNT_CTGRY_1                        STRING 

, EVNT_CTGRY_2                        STRING 

, EVNT_CTGRY_3                        STRING 

, EVNT_CTGRY_4                        STRING 

, EVNT_CTGRY_5                        STRING 

, HIBERNATE_VERSION                   BIGINT 

, DEPT                                STRING 

, MSG_STAT_CODE                       SMALLINT 

, MONITOR_SMRY_ID                     INT 

, ORIG_ACT_NAME                       STRING 

, SOURCE                              STRING 

, HAS_MONITOR_MSG                     TINYINT 

, TOTAL_QTY                           DECIMAL(20,7) 

, ORIG_EVNT_START_TIME                TIMESTAMP 

, ORIG_EVNT_END_TIME                  TIMESTAMP 

, ADJ_REASON_CODE                     STRING 

, ADJ_REF_TRAN_NBR                    DECIMAL(20,0) 

, INT_TYPE                            STRING 

, CONFLICT                            STRING 

, DISPLAY_UOM                         STRING 

, UOM_QTY                             DECIMAL(20,7) 

, THRUPUT_MIN                         DECIMAL(20,7) 

, LOCN_GRP_ATTR                       STRING 

, RESOURCE_GROUP_ID                   STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_labor_msg_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (LABOR_MSG_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LABOR_TRAN_DTL_CRIT_PRE" , ***** Creating table: "WM_LABOR_TRAN_DTL_CRIT_PRE"


CREATE TABLE  WM_LABOR_TRAN_DTL_CRIT_PRE
( DC_NBR                              SMALLINT                not null

, LABOR_TRAN_DTL_CRIT_ID              DECIMAL(20,0)               not null

, LABOR_TRAN_DTL_ID                   DECIMAL(20,0) 

, TRAN_NBR                            INT 

, CRIT_SEQ_NBR                        INT 

, CRIT_TYPE                           STRING 

, CRIT_VAL                            STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, WHSE                                STRING 

, MISC_TXT_1                          STRING 

, MISC_TXT_2                          STRING 

, MISC_NUM_1                          DECIMAL(20,7) 

, MISC_NUM_2                          DECIMAL(20,7) 

, HIBERNATE_VERSION                   BIGINT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_labor_tran_dtl_crit_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (LABOR_TRAN_DTL_CRIT_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LOCN_GRP_PRE" , ***** Creating table: "WM_LOCN_GRP_PRE"


CREATE TABLE  WM_LOCN_GRP_PRE
( DC_NBR                              SMALLINT                not null

, LOCN_GRP_ID                         INT                not null

, GRP_TYPE                            SMALLINT 

, LOCN_ID                             STRING 

, GRP_ATTR                            STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, LOCN_HDR_ID                         INT 

, WM_VERSION_ID                       INT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_locn_grp_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_LOCN_HDR_PRE" , ***** Creating table: "WM_LOCN_HDR_PRE"


CREATE TABLE  WM_LOCN_HDR_PRE
( DC_NBR                              SMALLINT                not null

, LOCN_HDR_ID                         INT                not null

, LOCN_ID                             STRING 

, WHSE                                STRING 

, LOCN_CLASS                          STRING 

, LOCN_BRCD                           STRING 

, AREA                                STRING 

, ZONE                                STRING 

, AISLE                               STRING 

, BAY                                 STRING 

, LVL                                 STRING 

, POSN                                STRING 

, DSP_LOCN                            STRING 

, LOCN_PICK_SEQ                       STRING 

, SKU_DEDCTN_TYPE                     STRING 

, SLOT_TYPE                           STRING 

, PUTWY_ZONE                          STRING 

, PULL_ZONE                           STRING 

, PICK_DETRM_ZONE                     STRING 

, LEN                                 DECIMAL(16,4) 

, WIDTH                               DECIMAL(16,4) 

, HT                                  DECIMAL(16,4) 

, X_COORD                             DECIMAL(13,5) 

, Y_COORD                             DECIMAL(13,5) 

, Z_COORD                             DECIMAL(13,5) 

, WORK_GRP                            STRING 

, WORK_AREA                           STRING 

, LAST_FROZN_DATE_TIME                TIMESTAMP 

, LAST_CNT_DATE_TIME                  TIMESTAMP 

, CYCLE_CNT_PENDIN                    STRING 

, PRT_LABEL_FLAG                      STRING 

, TRAVEL_AISLE                        STRING 

, TRAVEL_ZONE                         STRING 

, STORAGE_UOM                         STRING 

, PICK_UOM                            STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, SLOT_UNUSABLE                       STRING 

, CHECK_DIGIT                         STRING 

, VOCO_INTRNL_REVERSE_BRCD            STRING 

, WM_VERSION_ID                       INT 

, LOCN_PUTWY_SEQ                      STRING 

, LOCN_DYN_ASSGN_SEQ                  STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, FACILITY_ID                         INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_locn_hdr_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_LPN_AUDIT_RESULTS_PRE" , ***** Creating table: "WM_LPN_AUDIT_RESULTS_PRE"


CREATE TABLE  WM_LPN_AUDIT_RESULTS_PRE
( DC_NBR                              SMALLINT                not null

, LPN_AUDIT_RESULTS_ID                INT                not null

, AUDIT_TRANSACTION_ID                INT 

, AUDIT_COUNT                         INT 

, TC_COMPANY_ID                       INT 

, FACILITY_ALIAS_ID                   STRING 

, TC_SHIPMENT_ID                      STRING 

, STOP_SEQ                            INT 

, TC_ORDER_ID                         STRING 

, TC_PARENT_LPN_ID                    STRING 

, TC_LPN_ID                           STRING 

, INBOUND_OUTBOUND_INDICATOR          STRING 

, DEST_FACILITY_ALIAS_ID              STRING 

, STATIC_ROUTE_ID                     INT 

, ITEM_ID                             INT 

, GTIN                                STRING 

, CNTRY_OF_ORGN                       STRING 

, INVENTORY_TYPE                      STRING 

, PRODUCT_STATUS                      STRING 

, BATCH_NBR                           STRING 

, ITEM_ATTR_1                         STRING 

, ITEM_ATTR_2                         STRING 

, ITEM_ATTR_3                         STRING 

, ITEM_ATTR_4                         STRING 

, ITEM_ATTR_5                         STRING 

, CREATED_SOURCE_TYPE                 INT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            INT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, AUDITOR_USERID                      STRING 

, PICKER_USERID                       STRING 

, PACKER_USERID                       STRING 

, QUAL_AUD_STAT_CODE                  INT 

, QA_FLAG                             STRING 

, COUNT_QUANTITY                      DECIMAL(13,5) 

, EXPECTED_QUANTITY                   DECIMAL(13,5) 

, VALIDATION_LEVEL                    STRING 

, TRAN_NAME                           STRING 

, FACILITY_ID                         BIGINT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_lpn_audit_results_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (LPN_AUDIT_RESULTS_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LPN_DETAIL_PRE" , ***** Creating table: "WM_LPN_DETAIL_PRE"


CREATE TABLE  WM_LPN_DETAIL_PRE
( DC_NBR                              SMALLINT                not null

, LPN_ID                              BIGINT               not null

, LPN_DETAIL_ID                       BIGINT               not null

, TC_COMPANY_ID                       INT 

, LPN_DETAIL_STATUS                   SMALLINT 

, INTERNAL_ORDER_DTL_ID               BIGINT 

, DISTRIBUTION_ORDER_DTL_ID           BIGINT 

, RECEIVED_QTY                        DECIMAL(13,4) 

, BUSINESS_PARTNER_ID                 STRING 

, ITEM_ID                             INT 

, GTIN                                STRING 

, STD_PACK_QTY                        DECIMAL(13,4) 

, STD_SUB_PACK_QTY                    DECIMAL(13,4) 

, STD_BUNDLE_QTY                      DECIMAL(13,4) 

, INCUBATION_DATE                     TIMESTAMP 

, EXPIRATION_DATE                     TIMESTAMP 

, SHIP_BY_DATE                        TIMESTAMP 

, SELL_BY_DTTM                        TIMESTAMP 

, CONSUMPTION_PRIORITY_DTTM           TIMESTAMP 

, MANUFACTURED_DTTM                   TIMESTAMP 

, CNTRY_OF_ORGN                       STRING 

, INVENTORY_TYPE                      STRING 

, PRODUCT_STATUS                      STRING 

, ITEM_ATTR_1                         STRING 

, ITEM_ATTR_2                         STRING 

, ITEM_ATTR_3                         STRING 

, ITEM_ATTR_4                         STRING 

, ITEM_ATTR_5                         STRING 

, ASN_DTL_ID                          BIGINT 

, PACK_WEIGHT                         DECIMAL(13,4) 

, ESTIMATED_WEIGHT                    DECIMAL(13,4) 

, ESTIMATED_VOLUME                    DECIMAL(13,4) 

, SIZE_VALUE                          DECIMAL(16,4) 

, WEIGHT                              DECIMAL(16,4) 

, QTY_UOM_ID                          BIGINT 

, WEIGHT_UOM_ID                       INT 

, VOLUME_UOM_ID                       INT 

, ASSORT_NBR                          STRING 

, CUT_NBR                             STRING 

, PURCHASE_ORDERS_ID                  BIGINT 

, TC_PURCHASE_ORDERS_ID               STRING 

, PURCHASE_ORDERS_LINE_ID             BIGINT 

, TC_PURCHASE_ORDERS_LINE_ID          STRING 

, HIBERNATE_VERSION                   BIGINT 

, INTERNAL_ORDER_ID                   STRING 

, INSTRTN_CODE_1                      STRING 

, INSTRTN_CODE_2                      STRING 

, INSTRTN_CODE_3                      STRING 

, INSTRTN_CODE_4                      STRING 

, INSTRTN_CODE_5                      STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, VENDOR_ITEM_NBR                     STRING 

, MANUFACTURED_PLANT                  STRING 

, BATCH_NBR                           STRING 

, ASSIGNED_QTY                        DECIMAL(13,4) 

, PREPACK_GROUP_CODE                  STRING 

, PACK_CODE                           TINYINT 

, SHIPPED_QTY                         DECIMAL(13,4) 

, INITIAL_QTY                         DECIMAL(13,4) 

, QTY_CONV_FACTOR                     DECIMAL(17,8) 

, QTY_UOM_ID_BASE                     INT 

, WEIGHT_UOM_ID_BASE                  INT 

, VOLUME_UOM_ID_BASE                  INT 

, ITEM_NAME                           STRING 

, TC_ORDER_LINE_ID                    STRING 

, HAZMAT_UOM                          INT 

, HAZMAT_QTY                          DECIMAL(13,4) 

, REF_FIELD_1                         STRING 

, REF_FIELD_2                         STRING 

, REF_FIELD_3                         STRING 

, REF_FIELD_4                         STRING 

, REF_FIELD_5                         STRING 

, REF_FIELD_6                         STRING 

, REF_FIELD_7                         STRING 

, REF_FIELD_8                         STRING 

, REF_FIELD_9                         STRING 

, REF_FIELD_10                        STRING 

, REF_NUM1                            DECIMAL(13,5) 

, REF_NUM2                            DECIMAL(13,5) 

, REF_NUM3                            DECIMAL(13,5) 

, REF_NUM4                            DECIMAL(13,5) 

, REF_NUM5                            DECIMAL(13,5) 

, CHASE_WAVE_NBR                      STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_lpn_detail_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (LPN_ID, LPN_DETAIL_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LPN_FACILITY_STATUS_PRE" , ***** Creating table: "WM_LPN_FACILITY_STATUS_PRE"


CREATE TABLE  WM_LPN_FACILITY_STATUS_PRE
( DC_NBR                              SMALLINT                not null

, LPN_FACILITY_STATUS                 SMALLINT                not null

, INBOUND_OUTBOUND_INDICATOR          STRING        not null

, DESCRIPTION                         STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_lpn_facility_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LPN_LOCK_PRE" , ***** Creating table: "WM_LPN_LOCK_PRE"


CREATE TABLE  WM_LPN_LOCK_PRE
( DC_NBR                              SMALLINT                not null

, LPN_LOCK_ID                         BIGINT               not null

, LPN_ID                              BIGINT 

, INVENTORY_LOCK_CODE                 STRING 

, REASON_CODE                         STRING 

, LOCK_COUNT                          TINYINT 

, TC_LPN_ID                           STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_lpn_lock_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (LPN_LOCK_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LPN_PRE" , ***** Creating table: "WM_LPN_PRE"


CREATE TABLE  WM_LPN_PRE
( DC_NBR                              SMALLINT                not null

, LPN_ID                              BIGINT               not null

, TC_LPN_ID                           STRING 

, BUSINESS_PARTNER_ID                 STRING 

, TC_COMPANY_ID                       INT 

, PARENT_LPN_ID                       BIGINT 

, TC_PARENT_LPN_ID                    STRING 

, LPN_TYPE                            SMALLINT 

, VERSION_NBR                         BIGINT 

, LPN_MONETARY_VALUE                  DECIMAL(11,2) 

, LPN_MONETARY_VALUE_CURR_CODE        STRING 

, TIER_QTY                            DECIMAL(9,2) 

, STD_LPN_QTY                         DECIMAL(13,4) 

, C_FACILITY_ID                       BIGINT 

, O_FACILITY_ID                       BIGINT 

, O_FACILITY_ALIAS_ID                 STRING 

, FINAL_DEST_FACILITY_ALIAS_ID        STRING 

, FINAL_DEST_FACILITY_ID              BIGINT 

, LPN_STATUS                          SMALLINT 

, LPN_STATUS_UPDATED_DTTM             TIMESTAMP 

, LPN_FACILITY_STATUS                 SMALLINT 

, TC_REFERENCE_LPN_ID                 STRING 

, TC_ORDER_ID                         STRING 

, ORIGINAL_TC_SHIPMENT_ID             STRING 

, PARCEL_SHIPMENT_NBR                 STRING 

, TRACKING_NBR                        STRING 

, MANIFEST_NBR                        STRING 

, SHIP_VIA                            STRING 

, MASTER_BOL_NBR                      STRING 

, BOL_NBR                             STRING 

, INIT_SHIP_VIA                       STRING 

, PATH_ID                             BIGINT 

, ROUTE_RULE_ID                       INT 

, IDEAL_PICK_DTTM                     TIMESTAMP 

, REPRINT_COUNT                       TINYINT 

, LAST_FROZEN_DTTM                    TIMESTAMP 

, LAST_COUNTED_DTTM                   TIMESTAMP 

, BASE_CHARGE                         DECIMAL(9,2) 

, BASE_CHARGE_CURR_CODE               STRING 

, ADDNL_OPTION_CHARGE                 DECIMAL(9,2) 

, ADDNL_OPTION_CHARGE_CURR_CODE       STRING 

, INSUR_CHARGE                        DECIMAL(9,2) 

, INSUR_CHARGE_CURR_CODE              STRING 

, ACTUAL_CHARGE                       DECIMAL(9,2) 

, ACTUAL_CHARGE_CURR_CODE             STRING 

, PRE_BULK_BASE_CHARGE                DECIMAL(9,2) 

, PRE_BULK_BASE_CHARGE_CURR_CODE      STRING 

, PRE_BULK_ADD_OPT_CHR                DECIMAL(9,2) 

, PRE_BULK_ADD_OPT_CHR_CURR_CODE      STRING 

, DIST_CHARGE                         DECIMAL(11,2) 

, DIST_CHARGE_CURR_CODE               STRING 

, FREIGHT_CHARGE                      DECIMAL(11,2) 

, FREIGHT_CHARGE_CURR_CODE            STRING 

, RECEIVED_TC_SHIPMENT_ID             STRING 

, MANUFACTURED_DTTM                   TIMESTAMP 

, MANUFACT_PLANT_FACILITY_ALIAS       STRING 

, RCVD_DTTM                           TIMESTAMP 

, TC_PURCHASE_ORDERS_ID               STRING 

, PURCHASE_ORDERS_ID                  BIGINT 

, TC_SHIPMENT_ID                      STRING 

, SHIPMENT_ID                         BIGINT 

, TC_ASN_ID                           STRING 

, ASN_ID                              BIGINT 

, RECEIPT_VARIANCE_INDICATOR          SMALLINT 

, ESTIMATED_VOLUME                    DECIMAL(13,4) 

, WEIGHT                              DECIMAL(16,4) 

, ACTUAL_VOLUME                       DECIMAL(13,4) 

, LPN_LABEL_TYPE                      STRING 

, PACKAGE_TYPE_ID                     INT 

, MISC_INSTR_CODE_1                   STRING 

, MISC_INSTR_CODE_2                   STRING 

, MISC_INSTR_CODE_3                   STRING 

, MISC_INSTR_CODE_4                   STRING 

, MISC_INSTR_CODE_5                   STRING 

, MISC_NUM_1                          DECIMAL(9,2) 

, MISC_NUM_2                          DECIMAL(9,2) 

, HIBERNATE_VERSION                   INT 

, CUBE_UOM                            STRING 

, ERROR_INDICATOR                     SMALLINT 

, WARNING_INDICATOR                   SMALLINT 

, QA_FLAG                             STRING 

, PALLET_OPEN_FLAG                    TINYINT 

, QTY_UOM_ID_BASE                     INT 

, WEIGHT_UOM_ID_BASE                  INT 

, VOLUME_UOM_ID_BASE                  INT 

, SPLIT_LPN_ID                        BIGINT 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, EXT_CREATED_DTTM                    TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, CONVEYABLE_LPN_FLAG                 SMALLINT 

, ACTIVE_LPN_FLAG                     SMALLINT 

, LPN_SHIPPED_FLAG                    SMALLINT 

, STD_QTY_FLAG                        SMALLINT 

, C_FACILITY_ALIAS_ID                 STRING 

, PALLET_MASTER_LPN_FLAG              TINYINT 

, MASTER_LPN_FLAG                     STRING 

, LPN_SIZE_TYPE_ID                    INT 

, PICK_SUB_LOCN_ID                    STRING 

, CURR_SUB_LOCN_ID                    STRING 

, PREV_SUB_LOCN_ID                    STRING 

, DEST_SUB_LOCN_ID                    STRING 

, INBOUND_OUTBOUND_INDICATOR          STRING 

, WORK_ORD_NBR                        STRING 

, INTERNAL_ORDER_ID                   STRING 

, SHIPPED_DTTM                        TIMESTAMP 

, TRAILER_STOP_SEQ_NBR                INT 

, PACK_WAVE_NBR                       STRING 

, WAVE_NBR                            STRING 

, WAVE_SEQ_NBR                        INT 

, WAVE_STAT_CODE                      TINYINT 

, DIRECTED_QTY                        DECIMAL(9,2) 

, TRANSITIONAL_INVENTORY_TYPE         SMALLINT 

, PICKER_USERID                       STRING 

, PACKER_USERID                       STRING 

, CHUTE_ID                            STRING 

, CHUTE_ASSIGN_TYPE                   STRING 

, STAGE_INDICATOR                     TINYINT 

, OUT_OF_ZONE_INDICATOR               STRING 

, LABEL_PRINT_REQD                    STRING 

, CONSUMPTION_SEQUENCE                STRING 

, CONSUMPTION_PRIORITY                STRING 

, CONSUMPTION_PRIORITY_DTTM           TIMESTAMP 

, PUTAWAY_TYPE                        STRING 

, RETURN_DISPOSITION_CODE             STRING 

, FINAL_DISPOSITION_CODE              STRING 

, LOADED_DTTM                         TIMESTAMP 

, LOADER_USERID                       STRING 

, LOADED_POSN                         STRING 

, SHIP_BY_DATE                        TIMESTAMP 

, PICK_DELIVERY_DURATION              DECIMAL(5,2) 

, LPN_BREAK_ATTR                      STRING 

, LPN_PRINT_GROUP_CODE                STRING 

, SEQ_RULE_PRIORITY                   INT 

, SPUR_LANE                           STRING 

, SPUR_POSITION                       STRING 

, FIRST_ZONE                          STRING 

, LAST_ZONE                           STRING 

, NBR_OF_ZONES                        SMALLINT 

, LPN_DIVERT_CODE                     STRING 

, INTERNAL_ORDER_CONSOL_ATTR          STRING 

, WHSE_INTERNAL_EVENT_CODE            STRING 

, SELECTION_RULE_ID                   INT 

, VOCO_INTRN_REVERSE_ID               STRING 

, VOCO_INTRN_REVERSE_PALLET_ID        STRING 

, LPN_CREATION_CODE                   TINYINT 

, PALLET_X_OF_Y                       INT 

, INCUBATION_DATE                     TIMESTAMP 

, CONTAINER_TYPE                      STRING 

, CONTAINER_SIZE                      STRING 

, LPN_NBR_X_OF_Y                      INT 

, LOAD_SEQUENCE                       INT 

, MASTER_PACK_ID                      STRING 

, SINGLE_LINE_LPN                     STRING 

, NON_INVENTORY_LPN_FLAG              TINYINT 

, EPC_MATCH_FLAG                      STRING 

, AUDITOR_USERID                      STRING 

, PRINTING_RULE_ID                    INT 

, EXPIRATION_DATE                     TIMESTAMP 

, QUAL_AUD_STAT_CODE                  TINYINT 

, DELVRECIPIENTNAME                   STRING 

, DELVRECEIPTDATETIME                 TIMESTAMP 

, DELVONTIMEFLAG                      STRING 

, DELVCOMPLFLAG                       STRING 

, ITEM_ID                             INT 

, LPN_FACILITY_STAT_UPDATED_DTTM      TIMESTAMP 

, LPN_DISP_STATUS                     STRING 

, OVERSIZE_LENGTH                     SMALLINT 

, BILLING_METHOD                      INT 

, CUSTOMER_ID                         BIGINT 

, IS_ZONE_SKIPPED                     SMALLINT 

, ZONESKIP_HUB_FACILITY_ALIAS_ID      STRING 

, ZONESKIP_HUB_FACILITY_ID            INT 

, PLANING_DEST_FACILITY_ALIAS_ID      INT 

, PLANING_DEST_FACILITY_ID            INT 

, SCHED_SHIP_DTTM                     TIMESTAMP 

, DISTRIBUTION_LEG_CARRIER_ID         BIGINT 

, DISTRIBUTION_LEG_MODE_ID            INT 

, DISTRIBUTION_LEV_SVCE_LEVEL_ID      INT 

, SERVICE_LEVEL_INDICATOR             STRING 

, COD_FLAG                            SMALLINT 

, COD_AMOUNT                          DECIMAL(9,2) 

, COD_PAYMENT_METHOD                  INT 

, RATED_WEIGHT                        DECIMAL(13,4) 

, RATE_WEIGHT_TYPE                    SMALLINT 

, RATE_ZONE                           STRING 

, FRT_FORWARDER_ACCT_NBR              STRING 

, INTL_GOODS_DESC                     STRING 

, SHIPMENT_PRINT_SED                  SMALLINT 

, EXPORT_LICENSE_NUMBER               STRING 

, EXPORT_LICENSE_SYMBOL               STRING 

, EXPORT_INFO_CODE                    STRING 

, REPRINT_SHIPPING_LABEL              SMALLINT 

, PLAN_LOAD_ID                        BIGINT 

, DOCUMENTS_ONLY                      SMALLINT 

, SCHEDULED_DELIVERY_DTTM             TIMESTAMP 

, NON_MACHINEABLE                     SMALLINT 

, D_FACILITY_ID                       BIGINT 

, D_FACILITY_ALIAS_ID                 STRING 

, VENDOR_LPN_NBR                      STRING 

, PLANNED_TC_ASN_ID                   STRING 

, ESTIMATED_WEIGHT                    DECIMAL(13,4) 

, ORDER_ID                            BIGINT 

, MARK_FOR                            STRING 

, PRE_RECEIPT_STATUS                  STRING 

, CROSSDOCK_TC_ORDER_ID               STRING 

, PHYSICAL_ENTITY_CODE                STRING 

, PROCESS_IMMEDIATE_NEEDS             STRING 

, "LENGTH"                            DECIMAL(16,4) 

, WIDTH                               DECIMAL(16,4) 

, HEIGHT                              DECIMAL(16,4) 

, TOTAL_LPN_QTY                       DECIMAL(13,4) 

, STATIC_ROUTE_ID                     INT 

, HAS_ALERTS                          TINYINT 

, ORDER_SPLIT_ID                      BIGINT 

, MHE_LOADED                          STRING 

, ITEM_NAME                           STRING 

, ALT_TRACKING_NBR                    STRING 

, RECEIPT_TYPE                        TINYINT 

, VARIANCE_TYPE                       SMALLINT 

, RETURN_TRACKING_NBR                 STRING 

, CONTAINS_DRY_ICE                    STRING 

, RETURN_REFERENCE_NUMBER             STRING 

, HAS_NOTES                           SMALLINT 

, RETURN_TRACKING_NBR_2               STRING 

, DELIVERY_TYPE                       STRING 

, REF_FIELD_1                         STRING 

, REF_FIELD_2                         STRING 

, REF_FIELD_3                         STRING 

, REF_FIELD_4                         STRING 

, REF_FIELD_5                         STRING 

, REF_FIELD_6                         STRING 

, REF_FIELD_7                         STRING 

, REF_FIELD_8                         STRING 

, REF_FIELD_9                         STRING 

, REF_FIELD_10                        STRING 

, REF_NUM1                            DECIMAL(13,5) 

, REF_NUM2                            DECIMAL(13,5) 

, REF_NUM3                            DECIMAL(13,5) 

, REF_NUM4                            DECIMAL(13,5) 

, REF_NUM5                            DECIMAL(13,5) 

, REGULATION_SET                      STRING 

, DRY_ICE_WT                          DECIMAL(13,4) 

, OVERPACK                            TINYINT 

, SALVAGE_PACK                        TINYINT 

, SALVAGE_PACK_QTY                    DECIMAL(13,4) 

, Q_VALUE                             DECIMAL(2,1) 

, ALL_PAKD_IN_ONE                     TINYINT 

, SPECIAL_PERMITS                     STRING 

, CN22_NBR                            STRING 

, DISPOSITION_TYPE                    STRING 

, DISPOSITION_SOURCE_ID               INT 

, END_OLPN_DTTM                       TIMESTAMP 

, VOCOLLECT_ASSIGN_ID                 INT 

, CALCULATED_CUT_OFF_DTTM             TIMESTAMP 

, FAILED_GUARANTEED_DELIVERY          STRING 

, EPI_PACKAGE_ID                      STRING 

, EPI_SHIPMENT_ID                     STRING 

, EPI_MANIFEST_ID                     STRING 

, EPI_PACKAGE_STATUS                  STRING 

, XREF_OLPN                           STRING 

, PALLET_CONTENT_CODE                 STRING 

, EPI_DOC_OUTPUT_TYPE                 STRING 

, ORIGINAL_ASSIGNED_SHIP_VIA          STRING 

, REF_SHIPMENT_NBR                    STRING 

, RTS_ID                              BIGINT 

, IS_CHASE_CREATED                    TINYINT 

, LPN_LEVEL_ROUTING                   TINYINT 

, PACKWAVE_RULE_ID                    INT 

, CONS_RUN_ID                         BIGINT 

, PROCESSED_FOR_TRLR_MOVES            TINYINT 

, EXCEPTION_OLPN                      TINYINT 

, BUILD_LPN_BATCH_ID                  BIGINT 

, ROUTING_LANE_ID                     INT 

, ROUTING_LANE_DETAIL_ID              BIGINT 

, RATING_LANE_ID                      INT 

, RATING_LANE_DETAIL_ID               BIGINT 

, PENDING_CANCELLATION                STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_lpn_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (LPN_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LPN_SIZE_TYPE_PRE" , ***** Creating table: "WM_LPN_SIZE_TYPE_PRE"


CREATE TABLE  WM_LPN_SIZE_TYPE_PRE
( DC_NBR                              SMALLINT                not null

, LPN_SIZE_TYPE_ID                    INT                not null

, LPN_DESC                            STRING 

, LPN_SIZE_DESC                       STRING 

, LPN_SIZE_TYPE                       STRING 

, LPN_TYPE_ID                         INT 

, PKG_DESC                            STRING 

, OVERSIZE_FLAG                       STRING 

, LENGTH                              DECIMAL(16,4) 

, WIDTH                               DECIMAL(16,4) 

, HEIGHT                              DECIMAL(16,4) 

, LPN_DIM_UOM                         STRING 

, LPN_PER_TIER                        INT 

, TIER_PER_PALLET                     INT 

, PROC_ATTR_1                         STRING 

, PROC_ATTR_2                         STRING 

, PROC_ATTR_3                         STRING 

, PROC_ATTR_4                         STRING 

, PROC_ATTR_5                         STRING 

, USER_ID                             STRING 

, WM_VERSION_ID                       INT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, HIBERNATE_VERSION                   BIGINT 

, TC_COMPANY_ID                       INT 

, MARK_FOR_DELETION                   SMALLINT 

, SHAPE_TYPE                          SMALLINT 

, IS_STACKABLE                        TINYINT 

, STACK_RANK                          SMALLINT 

, STACK_POSITION                      SMALLINT 

, LOAD_BEARING_STRENGTH               BIGINT 

, FACILITY_ID                         INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_lpn_size_type_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LPN_STATUS_PRE" , ***** Creating table: "WM_LPN_STATUS_PRE"


CREATE TABLE  WM_LPN_STATUS_PRE
( DC_NBR                              SMALLINT                not null

, LPN_STATUS                          SMALLINT                not null

, DESCRIPTION                         STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_lpn_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_LPN_TYPE_PRE" , ***** Creating table: "WM_LPN_TYPE_PRE"


CREATE TABLE  WM_LPN_TYPE_PRE
( DC_NBR                              SMALLINT                not null

, LPN_TYPE                            SMALLINT                not null

, DESCRIPTION                         STRING 

, PHYSICAL_ENTITY_CODE                STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_lpn_type_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ORDERS_PRE" , ***** Creating table: "WM_ORDERS_PRE"


CREATE TABLE  WM_ORDERS_PRE
( DC_NBR                              SMALLINT                not null

, ORDER_ID                            BIGINT               not null

, TC_ORDER_ID                         STRING 

, TC_ORDER_ID_U                       STRING 

, TC_COMPANY_ID                       INT 

, CREATION_TYPE                       TINYINT 

, BUSINESS_PARTNER_ID                 STRING 

, PARENT_ORDER_ID                     BIGINT 

, EXT_PURCHASE_ORDER                  STRING 

, CONS_RUN_ID                         BIGINT 

, O_FACILITY_ALIAS_ID                 STRING 

, O_FACILITY_ID                       INT 

, O_DOCK_ID                           STRING 

, O_ADDRESS_1                         STRING 

, O_ADDRESS_2                         STRING 

, O_ADDRESS_3                         STRING 

, O_CITY                              STRING 

, O_STATE_PROV                        STRING 

, O_POSTAL_CODE                       STRING 

, O_COUNTY                            STRING 

, O_COUNTRY_CODE                      STRING 

, D_FACILITY_ALIAS_ID                 STRING 

, D_FACILITY_ID                       INT 

, D_DOCK_ID                           STRING 

, D_ADDRESS_1                         STRING 

, D_ADDRESS_2                         STRING 

, D_ADDRESS_3                         STRING 

, D_CITY                              STRING 

, D_STATE_PROV                        STRING 

, D_POSTAL_CODE                       STRING 

, D_COUNTY                            STRING 

, D_COUNTRY_CODE                      STRING 

, BILL_TO_NAME                        STRING 

, BILL_FACILITY_ALIAS_ID              STRING 

, BILL_FACILITY_ID                    INT 

, BILL_TO_ADDRESS_1                   STRING 

, BILL_TO_ADDRESS_2                   STRING 

, BILL_TO_ADDRESS_3                   STRING 

, BILL_TO_CITY                        STRING 

, BILL_TO_STATE_PROV                  STRING 

, BILL_TO_COUNTY                      STRING 

, BILL_TO_POSTAL_CODE                 STRING 

, BILL_TO_COUNTRY_CODE                STRING 

, BILL_TO_PHONE_NUMBER                STRING 

, BILL_TO_FAX_NUMBER                  STRING 

, BILL_TO_EMAIL                       STRING 

, PLAN_O_FACILITY_ID                  BIGINT 

, PLAN_D_FACILITY_ID                  INT 

, PLAN_O_FACILITY_ALIAS_ID            STRING 

, PLAN_D_FACILITY_ALIAS_ID            STRING 

, PLAN_LEG_D_ALIAS_ID                 STRING 

, PLAN_LEG_O_ALIAS_ID                 STRING 

, INCOTERM_FACILITY_ID                INT 

, INCOTERM_FACILITY_ALIAS_ID          STRING 

, INCOTERM_LOC_AVA_DTTM               TIMESTAMP 

, INCOTERM_LOC_AVA_TIME_ZONE_ID       INT 

, PICKUP_TZ                           SMALLINT 

, DELIVERY_TZ                         SMALLINT 

, PICKUP_START_DTTM                   TIMESTAMP 

, PICKUP_END_DTTM                     TIMESTAMP 

, DELIVERY_START_DTTM                 TIMESTAMP 

, DELIVERY_END_DTTM                   TIMESTAMP 

, ORDER_DATE_DTTM                     TIMESTAMP 

, ORDER_RECON_DTTM                    TIMESTAMP 

, INBOUND_REGION_ID                   INT 

, OUTBOUND_REGION_ID                  INT 

, DSG_SERVICE_LEVEL_ID                BIGINT 

, DSG_CARRIER_ID                      BIGINT 

, DSG_EQUIPMENT_ID                    BIGINT 

, DSG_TRACTOR_EQUIPMENT_ID            BIGINT 

, DSG_MOT_ID                          BIGINT 

, BASELINE_MOT_ID                     BIGINT 

, BASELINE_SERVICE_LEVEL_ID           BIGINT 

, PRODUCT_CLASS_ID                    INT 

, PROTECTION_LEVEL_ID                 INT 

, MERCHANDIZING_DEPARTMENT_ID         BIGINT 

, WAVE_ID                             BIGINT 

, WAVE_OPTION_ID                      BIGINT 

, PATH_ID                             BIGINT 

, PATH_SET_ID                         BIGINT 

, DRIVER_TYPE_ID                      BIGINT 

, UN_NUMBER_ID                        BIGINT 

, BLOCK_AUTO_CREATE                   TINYINT 

, BLOCK_AUTO_CONSOLIDATE              TINYINT 

, HAS_ALERTS                          TINYINT 

, HAS_EM_NOTIFY_FLAG                  TINYINT 

, HAS_IMPORT_ERROR                    TINYINT 

, HAS_NOTES                           TINYINT 

, HAS_SOFT_CHECK_ERRORS               TINYINT 

, HAS_SPLIT                           TINYINT 

, IS_BOOKING_REQUIRED                 TINYINT 

, IS_CANCELLED                        TINYINT 

, IS_HAZMAT                           TINYINT 

, IS_IMPORTED                         TINYINT 

, IS_PARTIALLY_PLANNED                TINYINT 

, IS_PERISHABLE                       TINYINT 

, IS_SUSPENDED                        TINYINT 

, NORMALIZED_BASELINE_COST            DECIMAL(13,4) 

, BASELINE_COST_CURRENCY_CODE         STRING 

, ORIG_BUDG_COST                      DECIMAL(13,4) 

, BUDG_COST                           DECIMAL(13,4) 

, ACTUAL_COST                         DECIMAL(13,4) 

, BASELINE_COST                       DECIMAL(13,4) 

, TRANS_RESP_CODE                     STRING 

, BILLING_METHOD                      TINYINT 

, DROPOFF_PICKUP                      STRING 

, MOVEMENT_OPTION                     TINYINT 

, PRIORITY                            SMALLINT 

, MOVE_TYPE                           TINYINT 

, SCHED_DOW                           TINYINT 

, EQUIPMENT_TYPE                      SMALLINT 

, DELIVERY_REQ                        STRING 

, MV_CURRENCY_CODE                    STRING 

, MONETARY_VALUE                      DECIMAL(16,4) 

, COMPARTMENT_NO                      TINYINT 

, PACKAGING                           STRING 

, ORDER_LOADING_SEQ                   INT 

, REF_FIELD_1                         STRING 

, REF_FIELD_2                         STRING 

, REF_FIELD_3                         STRING 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, HIBERNATE_VERSION                   DECIMAL(20,7) 

, ACTUAL_COST_CURRENCY_CODE           STRING 

, BUDG_COST_CURRENCY_CODE             STRING 

, SHIPMENT_ID                         BIGINT 

, BILL_TO_TITLE                       STRING 

, ORDER_STATUS                        SMALLINT 

, ADDR_CODE                           STRING 

, ADDR_VALID                          STRING 

, ADVT_DATE                           TIMESTAMP 

, CHUTE_ID                            STRING 

, COD_FUNDS                           STRING 

, DC_CTR_NBR                          STRING 

, DO_STATUS                           SMALLINT 

, DO_TYPE                             TINYINT 

, DOCS_ONLY_SHPMT                     STRING 

, DUTY_AND_TAX                        DECIMAL(13,4) 

, DUTY_TAX_ACCT_NBR                   STRING 

, DUTY_TAX_PAYMENT_TYPE               TINYINT 

, EST_LPN                             BIGINT 

, EST_LPN_BRIDGED                     BIGINT 

, FTSR_NBR                            STRING 

, IS_BACK_ORDERED                     STRING 

, LPN_CUBING_INDIC                    TINYINT 

, O_PHONE_NUMBER                      STRING 

, ORDER_PRINT_DTTM                    TIMESTAMP 

, PALLET_CUBING_INDIC                 TINYINT 

, PRE_PACK_FLAG                       TINYINT 

, RTE_TYPE_1                          STRING 

, RTE_TYPE_2                          STRING 

, SHIP_GROUP_SEQUENCE                 SMALLINT 

, SHPNG_CHRG                          DECIMAL(11,2) 

, STAGE_INDIC                         TINYINT 

, STORE_NBR                           STRING 

, TOTAL_NBR_OF_LPN                    INT 

, TOTAL_NBR_OF_PLT                    INT 

, WM_ORDER_STATUS                     SMALLINT 

, D_NAME                              STRING 

, O_CONTACT                           STRING 

, SECONDARY_MAXI_ADDR_NBR             STRING 

, D_CONTACT                           STRING 

, AES_ITN                             STRING 

, BATCH_ID                            BIGINT 

, ORIGIN_SHIP_THRU_FACILITY_ID        INT 

, ORIGIN_SHIP_THRU_FAC_ALIAS_ID       STRING 

, DEST_SHIP_THRU_FACILITY_ID          INT 

, DEST_SHIP_THRU_FAC_ALIAS_ID         STRING 

, ACTUAL_SHIPPED_DTTM                 TIMESTAMP 

, BILL_OF_LADING_NUMBER               STRING 

, BOL_BREAK_ATTR                      STRING 

, D_EMAIL                             STRING 

, D_FAX_NUMBER                        STRING 

, D_PHONE_NUMBER                      STRING 

, DYNAMIC_ROUTING_REQD                STRING 

, INTL_GOODS_DESC                     STRING 

, MAJOR_ORDER_GRP_ATTR                STRING 

, ORDER_CONSOL_PROFILE                STRING 

, PACK_SLIP_PRT_CNT                   TINYINT 

, PROD_SCHED_REF_NUMBER               STRING 

, MUST_RELEASE_BY_DTTM                TIMESTAMP 

, CANCEL_DTTM                         TIMESTAMP 

, ASSIGNED_STATIC_ROUTE_ID            STRING 

, DSG_STATIC_ROUTE_ID                 STRING 

, O_FACILITY_NAME                     STRING 

, O_DOCK_DOOR_ID                      BIGINT 

, O_FAX_NUMBER                        STRING 

, O_EMAIL                             STRING 

, D_FACILITY_NAME                     STRING 

, D_DOCK_DOOR_ID                      BIGINT 

, PURCHASE_ORDER_NUMBER               STRING 

, CUSTOMER_ID                         BIGINT 

, REPL_WAVE_NBR                       STRING 

, CUBING_STATUS                       SMALLINT 

, SCHED_PICKUP_DTTM                   TIMESTAMP 

, SCHED_DELIVERY_DTTM                 TIMESTAMP 

, ZONE_SKIP_HUB_LOCATION_ID           INT 

, BILL_TO_FACILITY_NAME               STRING 

, FREIGHT_CLASS                       DECIMAL(5,1) 

, NON_MACHINEABLE                     SMALLINT 

, ASSIGNED_MOT_ID                     BIGINT 

, ASSIGNED_CARRIER_ID                 BIGINT 

, ASSIGNED_SERVICE_LEVEL_ID           BIGINT 

, ASSIGNED_EQUIPMENT_ID               BIGINT 

, DYNAMIC_REQUEST_SENT                BIGINT 

, COMMODITY_CODE_ID                   BIGINT 

, TC_SHIPMENT_ID                      STRING 

, IN_TRANSIT_ALLOCATION               TINYINT 

, RELEASE_DESTINATION                 SMALLINT 

, TEMPLATE_ID                         BIGINT 

, COD_AMOUNT                          DECIMAL(13,4) 

, BILL_TO_CONTACT_NAME                STRING 

, SHIP_GROUP_ID                       STRING 

, PURCHASE_ORDER_ID                   BIGINT 

, DSG_HUB_LOCATION_ID                 INT 

, OVERRIDE_BILLING_METHOD             TINYINT 

, PARENT_TYPE                         TINYINT 

, ACCT_RCVBL_CODE                     STRING 

, ADVT_CODE                           STRING 

, IS_CUSTOMER_PICKUP                  STRING 

, IS_DIRECT_ALLOWED                   STRING 

, RTE_ATTR                            STRING 

, LAST_RUN_ID                         BIGINT 

, PLAN_DUE_DTTM                       TIMESTAMP 

, IS_ORDER_RECONCILED                 TINYINT 

, PRTL_SHIP_CONF_FLAG                 TINYINT 

, PRTL_SHIP_CONF_STATUS               TINYINT 

, ALLOW_PRE_BILLING                   TINYINT 

, LANE_NAME                           STRING 

, DECLARED_VALUE                      DECIMAL(16,4) 

, DV_CURRENCY_CODE                    STRING 

, COD_CURRENCY_CODE                   STRING 

, WEIGHT_UOM_ID_BASE                  INT 

, LINE_HAUL_SHIP_VIA                  STRING 

, ORIGINAL_ASSIGNED_SHIP_VIA          STRING 

, DISTRIBUTION_SHIP_VIA               STRING 

, DSG_SHIP_VIA                        STRING 

, TAX_ID                              STRING 

, ORDER_RECEIVED                      TINYINT 

, IS_ORIGINAL_ORDER                   TINYINT 

, FREIGHT_FORWARDER_ACCT_NBR          STRING 

, GLOBAL_LOCN_NBR                     STRING 

, IMPORTER_DEFN                       STRING 

, LANG_ID                             STRING 

, MAJOR_MINOR_ORDER                   STRING 

, MAJOR_ORDER_CTRL_NBR                STRING 

, MHE_FLAG                            STRING 

, MHE_ORD_STATE                       STRING 

, ORDER_CONSOL_LOCN_ID                STRING 

, ORDER_TYPE                          STRING 

, PACK_WAVE_NBR                       STRING 

, PARTIAL_LPN_OPTION                  STRING 

, PARTIES_RELATED                     STRING 

, PNH_FLAG                            STRING 

, PRE_STICKER_CODE                    STRING 

, PRIMARY_MAXI_ADDR_NBR               STRING 

, RTE_SWC_NBR                         STRING 

, RTE_TO                              STRING 

, RTE_WAVE_NBR                        STRING 

, BASELINE_CARRIER_ID                 BIGINT 

, COD_RETURN_COMPANY_NAME             STRING 

, BILL_ACCT_NBR                       STRING 

, CUST_BROKER_ACCT_NBR                STRING 

, MANIFEST_NBR                        STRING 

, DESTINATION_ACTION                  STRING 

, TRANS_PLAN_OWNER                    SMALLINT 

, MARK_FOR                            STRING 

, DELIVERY_OPTIONS                    STRING 

, PRE_BILL_STATUS                     STRING 

, CONTNT_LABEL_TYPE                   STRING 

, NBR_OF_CONTNT_LABEL                 INT 

, NBR_OF_LABEL                        INT 

, NBR_OF_PAKNG_SLIPS                  INT 

, PALLET_CONTENT_LABEL_TYPE           STRING 

, LPN_LABEL_TYPE                      STRING 

, PACK_SLIP_TYPE                      STRING 

, BOL_TYPE                            STRING 

, MANIF_TYPE                          STRING 

, PRINT_CANADIAN_CUST_INVC_FLAG       STRING 

, PRINT_COO                           STRING 

, PRINT_DOCK_RCPT_FLAG                STRING 

, PRINT_INV                           STRING 

, PRINT_NAFTA_COO_FLAG                STRING 

, PRINT_OCEAN_BOL_FLAG                STRING 

, PRINT_SED                           STRING 

, PRINT_SHPR_LTR_OF_INSTR_FLAG        STRING 

, PRINT_PKG_LIST_FLAG                 STRING 

, FREIGHT_REVENUE_CURRENCY_CODE       STRING 

, FREIGHT_REVENUE                     DECIMAL(13,4) 

, TRANS_PLAN_DIRECTION                STRING 

, DSG_VOYAGE_FLIGHT                   STRING 

, IS_D_POBOX                          TINYINT 

, REF_SHIPMENT_NBR                    STRING 

, REF_STOP_SEQ                        INT 

, HAZ_OFFEROR_NAME                    STRING 

, DISTRO_NUMBER                       STRING 

, INCOTERM_ID                         INT 

, DIRECTION                           STRING 

, ORDER_SHIPMENT_SEQ                  BIGINT 

, MOVEMENT_TYPE                       STRING 

, ACCT_RCVBL_ACCT_NBR                 STRING 

, REF_FIELD_4                         STRING 

, REF_FIELD_5                         STRING 

, REF_FIELD_6                         STRING 

, REF_FIELD_7                         STRING 

, REF_FIELD_8                         STRING 

, REF_FIELD_9                         STRING 

, REF_FIELD_10                        STRING 

, FIRST_ZONE                          STRING 

, LAST_ZONE                           STRING 

, NBR_OF_ZONES                        SMALLINT 

, REF_NUM1                            DECIMAL(13,5) 

, REF_NUM2                            DECIMAL(13,5) 

, REF_NUM3                            DECIMAL(13,5) 

, REF_NUM4                            DECIMAL(13,5) 

, REF_NUM5                            DECIMAL(13,5) 

, SPL_INSTR_CODE_1                    STRING 

, SPL_INSTR_CODE_2                    STRING 

, SPL_INSTR_CODE_3                    STRING 

, SPL_INSTR_CODE_4                    STRING 

, SPL_INSTR_CODE_5                    STRING 

, SPL_INSTR_CODE_6                    STRING 

, SPL_INSTR_CODE_7                    STRING 

, SPL_INSTR_CODE_8                    STRING 

, SPL_INSTR_CODE_9                    STRING 

, SPL_INSTR_CODE_10                   STRING 

, SINGLE_UNIT_FLAG                    TINYINT 

, TOTAL_NBR_OF_UNITS                  DECIMAL(15,4) 

, IS_ROUTED                           TINYINT 

, DSG_TRAILER_NUMBER                  STRING 

, STAGING_LOCN_ID                     STRING 

, UPSMI_COST_CENTER                   STRING 

, EST_PALLET_BRIDGED                  BIGINT 

, EST_PALLET                          BIGINT 

, PICKLIST_ID                         STRING 

, EFFECTIVE_RANK                      STRING 

, CHASE_ELIGIBLE                      TINYINT 

, SHIPPING_CHANNEL                    STRING 

, IS_GUARANTEED_DELIVERY              TINYINT 

, FAILED_GUARANTEED_DELIVERY          STRING 

, APPLY_LPNTYPE_FOR_ORDER             TINYINT 

, PO_TYPE_ATTR                        STRING 

, EPI_SERVICE_GROUP                   STRING 

, IMPORTER_OF_RECORD_NBR              STRING 

, B13A_EXPORT_DECL_NBR                STRING 

, RETURN_ADDR_CODE                    STRING 

, CHANNEL_TYPE                        SMALLINT 

, STATIC_ROUTE_DELIVERY_TYPE          STRING 

, ASSIGNED_CARRIER_CODE               STRING 

, DSG_CARRIER_CODE                    STRING 

, SOURCE_ORDER                        STRING 

, AUTO_APPOINTMENT                    TINYINT 

, BASELINE_LINEHAUL_COST              DECIMAL(13,4) 

, BASELINE_ACCESSORIAL_COST           DECIMAL(13,4) 

, BASELINE_SHIP_VIA                   STRING 

, DELIVERY_CHANNEL_ID                 SMALLINT 

, ORDER_STREAM                        TINYINT 

, LINEHAUL_REVENUE                    DECIMAL(13,4) 

, ACCESSORIAL_REVENUE                 DECIMAL(13,4) 

, STOP_OFF_REVENUE                    DECIMAL(13,4) 

, REVENUE_LANE_ID                     DECIMAL(13,4) 

, REVENUE_LANE_DETAIL_ID              DECIMAL(13,4) 

, MARGIN                              DECIMAL(13,4) 

, MARGIN_CURRENCY_CODE                STRING 

, CM_DISCOUNT_REVENUE                 DECIMAL(13,3) 

, C_TMS_PLAN_ID                       INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_orders_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (ORDER_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ORDER_LINE_ITEM_PRE" , ***** Creating table: "WM_ORDER_LINE_ITEM_PRE"


CREATE TABLE  WM_ORDER_LINE_ITEM_PRE
( DC_NBR                              SMALLINT                not null

, ORDER_ID                            BIGINT               not null

, LINE_ITEM_ID                        BIGINT               not null

, PARENT_LINE_ITEM_ID                 BIGINT 

, TC_COMPANY_ID                       INT 

, COMMODITY_CLASS                     STRING 

, REF_FIELD1                          STRING 

, REF_FIELD2                          STRING 

, REF_FIELD3                          STRING 

, EXT_SYS_LINE_ITEM_ID                STRING 

, MASTER_ORDER_ID                     BIGINT 

, MO_LINE_ITEM_ID                     BIGINT 

, IS_HAZMAT                           TINYINT 

, IS_STACKABLE                        TINYINT 

, HAS_ERRORS                          TINYINT 

, ORIG_BUDG_COST                      DECIMAL(13,4) 

, BUDG_COST_CURRENCY_CODE             STRING 

, BUDG_COST                           DECIMAL(13,4) 

, ACTUAL_COST_CURRENCY_CODE           STRING 

, TOTAL_MONETARY_VALUE                DECIMAL(13,4) 

, UNIT_MONETARY_VALUE                 DECIMAL(13,4) 

, UNIT_TAX_AMOUNT                     DECIMAL(16,4) 

, RTS_ID                              BIGINT 

, RTS_LINE_ITEM_ID                    BIGINT 

, STD_PACK_QTY                        DECIMAL(13,4) 

, PRODUCT_CLASS_ID                    INT 

, PROTECTION_LEVEL_ID                 INT 

, PACKAGE_TYPE_ID                     INT 

, COMMODITY_CODE_ID                   BIGINT 

, MV_SIZE_UOM_ID                      INT 

, QTY_UOM_ID_BASE                     INT 

, MV_CURRENCY_CODE                    STRING 

, SHIPPED_QTY                         DECIMAL(13,4) 

, RECEIVED_QTY                        DECIMAL(13,4) 

, PRIORITY                            BIGINT 

, MERCHANDIZING_DEPARTMENT_ID         BIGINT 

, UN_NUMBER_ID                        BIGINT 

, PICKUP_REFERENCE_NUMBER             STRING 

, DELIVERY_REFERENCE_NUMBER           STRING 

, CNTRY_OF_ORGN                       STRING 

, PROD_STAT                           STRING 

, STACK_RANK                          SMALLINT 

, STACK_LENGTH_VALUE                  DECIMAL(16,4) 

, STACK_LENGTH_STANDARD_UOM           INT 

, STACK_WIDTH_VALUE                   DECIMAL(16,4) 

, STACK_WIDTH_STANDARD_UOM            INT 

, STACK_HEIGHT_VALUE                  DECIMAL(16,4) 

, STACK_HEIGHT_STANDARD_UOM           INT 

, STACK_DIAMETER_VALUE                DECIMAL(16,4) 

, STACK_DIAMETER_STANDARD_UOM         INT 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, HIBERNATE_VERSION                   DECIMAL(20,7) 

, CUBE_MULTIPLE_QTY                   DECIMAL(13,4) 

, DO_DTL_STATUS                       SMALLINT 

, INTERNAL_ORDER_SEQ_NBR              INT 

, IS_EMERGENCY                        STRING 

, LPN_SIZE                            DECIMAL(16,4) 

, MANUFACTURING_DTTM                  TIMESTAMP 

, ORDER_LINE_ID                       BIGINT 

, PACK_RATE                           DECIMAL(13,4) 

, ALLOCATED_QTY                       DECIMAL(13,4) 

, PLANNED_SHIP_DATE                   TIMESTAMP 

, PPACK_QTY                           DECIMAL(13,4) 

, STD_BUNDLE_QTY                      DECIMAL(13,4) 

, STD_LPN_QTY                         DECIMAL(13,4) 

, STD_LPN_VOL                         DECIMAL(13,4) 

, STD_LPN_WT                          DECIMAL(13,4) 

, UNIT_COST                           DECIMAL(16,4) 

, UNIT_PRICE_AMOUNT                   DECIMAL(13,4) 

, UNIT_VOL                            DECIMAL(13,4) 

, UNIT_WT                             DECIMAL(13,4) 

, USER_CANCELED_QTY                   DECIMAL(13,4) 

, WAVE_PROC_TYPE                      SMALLINT 

, DELIVERY_END_DTTM                   TIMESTAMP 

, DELIVERY_START_DTTM                 TIMESTAMP 

, EVENT_CODE                          STRING 

, REASON_CODE                         STRING 

, EXP_INFO_CODE                       STRING 

, PARTL_FILL                          STRING 

, LINE_TYPE                           STRING 

, REPL_PROC_TYPE                      SMALLINT 

, ORDER_QTY                           DECIMAL(13,4) 

, ORIG_ORDER_QTY                      DECIMAL(13,4) 

, RTL_TO_BE_DISTROED_QTY              DECIMAL(9,2) 

, PRICE                               DECIMAL(13,4) 

, RETAIL_PRICE                        DECIMAL(16,4) 

, UNITS_PAKD                          DECIMAL(13,4) 

, MINOR_ORDER_NBR                     STRING 

, ITEM_ID                             INT 

, TC_ORDER_LINE_ID                    STRING 

, ACTUAL_SHIPPED_DTTM                 TIMESTAMP 

, QTY_UOM_ID                          BIGINT 

, PICKUP_END_DTTM                     TIMESTAMP 

, PURCHASE_ORDER_LINE_NUMBER          STRING 

, PICKUP_START_DTTM                   TIMESTAMP 

, REFERENCE_LINE_ITEM_ID              BIGINT 

, REFERENCE_ORDER_ID                  BIGINT 

, REPL_WAVE_RUN                       STRING 

, STD_PALLET_QTY                      DECIMAL(13,4) 

, STD_SUB_PACK_QTY                    DECIMAL(13,4) 

, STORE_DEPT                          STRING 

, ALLOC_TYPE                          STRING 

, BATCH_NBR                           STRING 

, BATCH_REQUIREMENT_TYPE              STRING 

, CHUTE_ASSIGN_TYPE                   STRING 

, CUSTOM_TAG                          STRING 

, CUSTOMER_ITEM                       STRING 

, INTERNAL_ORDER_ID                   STRING 

, INVN_TYPE                           STRING 

, ITEM_ATTR_1                         STRING 

, ITEM_ATTR_2                         STRING 

, ITEM_ATTR_3                         STRING 

, ITEM_ATTR_4                         STRING 

, ITEM_ATTR_5                         STRING 

, LPN_BRK_ATTRIB                      STRING 

, MERCH_GRP                           STRING 

, MERCH_TYPE                          STRING 

, PACK_ZONE                           STRING 

, PALLET_TYPE                         STRING 

, PICK_LOCN_ASSIGN_TYPE               STRING 

, PICK_LOCN_ID                        STRING 

, PPACK_GRP_CODE                      STRING 

, PRICE_TKT_TYPE                      STRING 

, SERIAL_NUMBER_REQUIRED_FLAG         STRING 

, SKU_GTIN                            STRING 

, SKU_SUB_CODE_ID                     STRING 

, SKU_SUB_CODE_VALUE                  STRING 

, VAS_PROCESS_TYPE                    STRING 

, SUBSTITUTED_PARENT_LINE_ID          BIGINT 

, IS_CANCELLED                        STRING 

, ITEM_NAME                           STRING 

, ALLOC_LINE_ID                       INT 

, ALLOCATION_SOURCE_ID                STRING 

, ALLOCATION_SOURCE_LINE_ID           STRING 

, ALLOCATION_SOURCE                   TINYINT 

, SHELF_DAYS                          INT 

, FULFILLMENT_TYPE                    STRING 

, WAVE_NBR                            STRING 

, SHIP_WAVE_NBR                       STRING 

, REPL_WAVE_NBR                       STRING 

, SINGLE_UNIT_FLAG                    STRING 

, ACTUAL_COST                         DECIMAL(13,4) 

, QTY_CONV_FACTOR                     DECIMAL(17,8) 

, PLANNED_WEIGHT                      DECIMAL(13,4) 

, RECEIVED_WEIGHT                     DECIMAL(13,4) 

, SHIPPED_WEIGHT                      DECIMAL(13,4) 

, WEIGHT_UOM_ID_BASE                  INT 

, WEIGHT_UOM_ID                       INT 

, PLANNED_VOLUME                      DECIMAL(13,4) 

, RECEIVED_VOLUME                     DECIMAL(13,4) 

, SHIPPED_VOLUME                      DECIMAL(13,4) 

, VOLUME_UOM_ID_BASE                  INT 

, VOLUME_UOM_ID                       INT 

, SIZE1_UOM_ID                        INT 

, SIZE1_VALUE                         DECIMAL(13,4) 

, RECEIVED_SIZE1                      DECIMAL(13,4) 

, SHIPPED_SIZE1                       DECIMAL(13,4) 

, SIZE2_UOM_ID                        INT 

, SIZE2_VALUE                         DECIMAL(13,4) 

, RECEIVED_SIZE2                      DECIMAL(13,4) 

, SHIPPED_SIZE2                       DECIMAL(13,4) 

, LPN_TYPE                            STRING 

, ORDER_CONSOL_ATTR                   STRING 

, SKU_BREAK_ATTR                      STRING 

, PURCHASE_ORDER_NUMBER               STRING 

, EXT_PURCHASE_ORDER                  STRING 

, CRITCL_DIM_1                        DECIMAL(7,2) 

, CRITCL_DIM_2                        DECIMAL(7,2) 

, CRITCL_DIM_3                        DECIMAL(7,2) 

, ASSORT_NBR                          STRING 

, DESCRIPTION                         STRING 

, ORIGINAL_ORDERED_ITEM_ID            INT 

, SUBSTITUTION_TYPE                   STRING 

, BACK_ORD_REASON                     STRING 

, FREIGHT_REVENUE_CURRENCY_CODE       STRING 

, FREIGHT_REVENUE                     DECIMAL(13,4) 

, IS_CHASE_CREATED_LINE               TINYINT 

, REF_FIELD4                          STRING 

, REF_FIELD5                          STRING 

, REF_FIELD6                          STRING 

, REF_FIELD7                          STRING 

, REF_FIELD8                          STRING 

, REF_FIELD9                          STRING 

, REF_FIELD10                         STRING 

, REF_NUM1                            DECIMAL(13,5) 

, REF_NUM2                            DECIMAL(13,5) 

, REF_NUM3                            DECIMAL(13,5) 

, REF_NUM4                            DECIMAL(13,5) 

, REF_NUM5                            DECIMAL(13,5) 

, IS_GIFT                             TINYINT 

, ADJUSTED_ORDER_QTY                  DECIMAL(13,4) 

, PRE_RECEIPT_STATUS                  STRING 

, ALLOW_REVIVAL_RECEIPT_ALLOC         STRING 

, SEGMENT_NAME                        STRING 

, EFFECTIVE_RANK                      STRING 

, WM_RECEIPT_ALLOCATED                TINYINT 

, GIFT_CARD_VALUE                     DECIMAL(13,4) 

, REF_BOOLEAN1                        TINYINT 

, REF_BOOLEAN2                        TINYINT 

, REF_SYSCODE1                        STRING 

, REF_SYSCODE2                        STRING 

, REF_SYSCODE3                        STRING 

, HARD_ALLOC_REASON_CODE              STRING 

, LOCKED                              TINYINT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_order_line_item_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (ORDER_ID, LINE_ITEM_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_ORDER_STATUS_PRE" , ***** Creating table: "WM_ORDER_STATUS_PRE"


CREATE TABLE  WM_ORDER_STATUS_PRE
( DC_NBR                              SMALLINT                not null

, ORDER_STATUS                        SMALLINT                not null

, DESCRIPTION                         STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_order_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_OUTPT_LPN_DETAIL_PRE" , ***** Creating table: "WM_OUTPT_LPN_DETAIL_PRE"


CREATE TABLE  WM_OUTPT_LPN_DETAIL_PRE
( DC_NBR                              SMALLINT                not null

, OUTPT_LPN_DETAIL_ID                 BIGINT               not null

, ASSORT_NBR                          STRING 

, BUSINESS_PARTNER                    STRING 

, CONSUMPTION_PRIORITY_DTTM           TIMESTAMP 

, CREATED_DTTM                        TIMESTAMP 

, CREATED_SOURCE                      STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, GTIN                                STRING 

, INVC_BATCH_NBR                      INT 

, INVENTORY_TYPE                      STRING 

, ITEM_ATTR_1                         STRING 

, ITEM_ATTR_2                         STRING 

, ITEM_ATTR_3                         STRING 

, ITEM_ATTR_4                         STRING 

, ITEM_ATTR_5                         STRING 

, ITEM_ID                             INT 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LPN_DETAIL_ID                       BIGINT 

, MANUFACTURED_DTTM                   TIMESTAMP 

, BATCH_NBR                           STRING 

, MANUFACTURED_PLANT                  STRING 

, CNTRY_OF_ORGN                       STRING 

, PRODUCT_STATUS                      STRING 

, PROC_DTTM                           TIMESTAMP 

, PROC_STAT_CODE                      SMALLINT 

, QTY_UOM                             STRING 

, REC_PROC_INDIC                      STRING 

, SIZE_VALUE                          DECIMAL(16,4) 

, TC_COMPANY_ID                       INT 

, TC_LPN_ID                           STRING 

, VERSION_NBR                         BIGINT 

, DISTRIBUTION_ORDER_DTL_ID           BIGINT 

, ITEM_COLOR                          STRING 

, ITEM_COLOR_SFX                      STRING 

, ITEM_SEASON                         STRING 

, ITEM_SEASON_YEAR                    STRING 

, ITEM_SECOND_DIM                     STRING 

, ITEM_SIZE_DESC                      STRING 

, ITEM_QUALITY                        STRING 

, ITEM_STYLE                          STRING 

, ITEM_STYLE_SFX                      STRING 

, SIZE_RANGE_CODE                     STRING 

, SIZE_REL_POSN_IN_TABLE              STRING 

, VENDOR_ITEM_NBR                     STRING 

, MINOR_ORDER_NBR                     STRING 

, MINOR_PO_NBR                        STRING 

, ITEM_NAME                           STRING 

, TC_ORDER_LINE_ID                    STRING 

, DISTRO_NUMBER                       STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_outpt_lpn_detail_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (OUTPT_LPN_DETAIL_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_OUTPT_LPN_PRE" , ***** Creating table: "WM_OUTPT_LPN_PRE"


CREATE TABLE  WM_OUTPT_LPN_PRE
( DC_NBR                              SMALLINT                not null

, OUTPT_LPN_ID                        BIGINT               not null

, ACTUAL_CHARGE                       DECIMAL(9,2) 

, ACTUAL_CHARGE_CURRENCY              STRING 

, ACTUAL_MONETARY_VALUE               DECIMAL(13,4) 

, ACTUAL_VOLUME                       DECIMAL(13,4) 

, ADDNL_OPTION_CHARGE                 DECIMAL(9,2) 

, ADDNL_OPTION_CHARGE_CURRENCY        STRING 

, ADF_TRANSMIT_FLAG                   STRING 

, AUDITOR_USERID                      STRING 

, BASE_CHARGE                         DECIMAL(13,4) 

, BASE_CHARGE_CURRENCY                STRING 

, BILL_OF_LADING_NUMBER               STRING 

, C_FACILITY_ALIAS_ID                 STRING 

, COD_AMOUNT                          DECIMAL(13,4) 

, COD_PAYMENT_METHOD                  INT 

, CONTAINER_SIZE                      STRING 

, CONTAINER_TYPE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, CREATED_SOURCE                      STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, DECLARED_MONETARY_VALUE             DECIMAL(13,4) 

, DIST_CHARGE                         DECIMAL(11,2) 

, DIST_CHARGE_CURRENCY                STRING 

, EPC_MATCH_FLAG                      STRING 

, ESTIMATED_WEIGHT                    DECIMAL(13,4) 

, FINAL_DEST_FACILITY_ALIAS_ID        STRING 

, FREIGHT_CHARGE                      DECIMAL(11,2) 

, FREIGHT_CHARGE_CURRENCY             STRING 

, INSUR_CHARGE                        DECIMAL(9,2) 

, INSUR_CHARGE_CURRENCY               STRING 

, INVC_BATCH_NBR                      INT 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOADED_DTTM                         TIMESTAMP 

, LOADED_POSN                         STRING 

, LPN_EPC                             STRING 

, LPN_NBR_X_OF_Y                      INT 

, MANIFEST_NBR                        STRING 

, MASTER_BOL_NBR                      STRING 

, MISC_INSTR_CODE_1                   STRING 

, MISC_INSTR_CODE_2                   STRING 

, MISC_INSTR_CODE_3                   STRING 

, MISC_INSTR_CODE_4                   STRING 

, MISC_INSTR_CODE_5                   STRING 

, MISC_NUM_1                          DECIMAL(9,2) 

, MISC_NUM_2                          DECIMAL(9,2) 

, NON_INVENTORY_LPN_FLAG              SMALLINT 

, NON_MACHINEABLE                     INT 

, PACKAGE_DESCRIPTION                 STRING 

, PACKER_USERID                       STRING 

, PALLET_EPC                          STRING 

, PALLET_MASTER_LPN_FLAG              SMALLINT 

, PARCEL_SERVICE_MODE                 STRING 

, PARCEL_SHIPMENT_NBR                 STRING 

, PICKER_USERID                       STRING 

, PRE_BULK_ADD_OPT_CHR                DECIMAL(9,2) 

, PRE_BULK_ADD_OPT_CHR_CURRENCY       STRING 

, PRE_BULK_BASE_CHARGE                DECIMAL(9,2) 

, PROC_DTTM                           TIMESTAMP 

, PROC_STAT_CODE                      SMALLINT 

, QTY_UOM                             STRING 

, RATE_ZONE                           STRING 

, RATED_WEIGHT                        DECIMAL(13,4) 

, RATED_WEIGHT_UOM                    STRING 

, SERVICE_LEVEL                       STRING 

, SHIP_VIA                            STRING 

, SHIPPED_DTTM                        TIMESTAMP 

, STATIC_ROUTE_ID                     INT 

, TC_COMPANY_ID                       INT 

, TC_LPN_ID                           STRING 

, TC_ORDER_ID                         STRING 

, TC_PARENT_LPN_ID                    STRING 

, TC_SHIPMENT_ID                      STRING 

, TOTAL_LPN_QTY                       DECIMAL(13,4) 

, TRACKING_NBR                        STRING 

, ALT_TRACKING_NBR                    STRING 

, VERSION_NBR                         BIGINT 

, VOLUME_UOM                          STRING 

, WEIGHT                              DECIMAL(16,4) 

, WEIGHT_UOM                          STRING 

, LOAD_SEQUENCE                       INT 

, TC_PURCHASE_ORDER_ID                STRING 

, RETURN_TRACKING_NBR                 STRING 

, XREF_OLPN                           STRING 

, RETURN_REFERENCE_NUMBER             STRING 

, EPI_PACKAGE_ID                      STRING 

, EPI_SHIPMENT_ID                     STRING 

, REF_FIELD_2                         STRING 

, REF_FIELD_3                         STRING 

, REF_FIELD_4                         STRING 

, REF_FIELD_5                         STRING 

, REF_FIELD_6                         STRING 

, REF_FIELD_7                         STRING 

, REF_FIELD_8                         STRING 

, REF_FIELD_9                         STRING 

, REF_FIELD_10                        STRING 

, REF_FIELD_1                         STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_outpt_lpn_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (OUTPT_LPN_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_OUTPT_ORDERS_PRE" , ***** Creating table: "WM_OUTPT_ORDERS_PRE"


CREATE TABLE  WM_OUTPT_ORDERS_PRE
( DC_NBR                              SMALLINT                not null

, OUTPT_ORDERS_ID                     BIGINT               not null

, ACCT_RCVBL_ACCT_NBR                 STRING 

, ACCT_RCVBL_CODE                     STRING 

, ADDR_CODE                           STRING 

, BACKORDERED_QUANTITY                DECIMAL(13,2) 

, BATCH_CTRL_NBR                      STRING 

, BILLING_METHOD                      SMALLINT 

, BILL_ACCT_NBR                       STRING 

, BILL_FACILITY_ALIAS_ID              STRING 

, BILL_FACILITY_ID                    INT 

, BILL_OF_LADING_NUMBER               STRING 

, BILL_TO_ADDRESS_1                   STRING 

, BILL_TO_ADDRESS_2                   STRING 

, BILL_TO_ADDRESS_3                   STRING 

, BILL_TO_CITY                        STRING 

, BILL_TO_CONTACT                     STRING 

, BILL_TO_CONTACT_NAME                STRING 

, BILL_TO_COUNTRY_CODE                STRING 

, BILL_TO_COUNTY                      STRING 

, BILL_TO_EMAIL                       STRING 

, BILL_TO_FACILITY_NAME               STRING 

, BILL_TO_FAX_NUMBER                  STRING 

, BILL_TO_NAME                        STRING 

, BILL_TO_PHONE_NUMBER                STRING 

, BILL_TO_POSTAL_CODE                 STRING 

, BILL_TO_STATE_PROV                  STRING 

, BILL_TO_TITLE                       STRING 

, BOL_BREAK_ATTR                      STRING 

, BOL_TYPE                            STRING 

, BUSINESS_PARTNER                    STRING 

, CREATED_DTTM                        TIMESTAMP 

, CREATED_SOURCE                      STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CURRENCY                            STRING 

, D_ADDRESS_1                         STRING 

, D_ADDRESS_2                         STRING 

, D_ADDRESS_3                         STRING 

, D_CITY                              STRING 

, D_CONTACT                           STRING 

, D_COUNTRY_CODE                      STRING 

, D_COUNTY                            STRING 

, D_DOCK_DOOR_ID                      BIGINT 

, D_DOCK_ID                           STRING 

, D_EMAIL                             STRING 

, D_FACILITY_ALIAS_ID                 STRING 

, D_FACILITY_ID                       INT 

, D_FACILITY_NAME                     STRING 

, D_FAX_NUMBER                        STRING 

, D_NAME                              STRING 

, D_PHONE_NUMBER                      STRING 

, D_POSTAL_CODE                       STRING 

, D_STATE_PROV                        STRING 

, DC_CTR_NBR                          STRING 

, DELIVERY_START_DTTM                 TIMESTAMP 

, DISTRIBUTION_SHIP_VIA               STRING 

, DO_TYPE                             SMALLINT 

, ORIGINAL_SHIP_VIA                   STRING 

, EXT_PURCHASE_ORDER                  STRING 

, FREIGHT_CLASS                       DECIMAL(5,1) 

, FREIGHT_FORWARDER_ACCT_NBR          STRING 

, GLOBAL_LOCN_NBR                     STRING 

, HANDLING_CHARGES                    DECIMAL(13,4) 

, INCOTERM_FACILITY_ALIAS_ID          STRING 

, INCOTERM_FACILITY_ID                INT 

, INCOTERM_ID                         INT 

, INCOTERM_LOC_AVA_DTTM               TIMESTAMP 

, INCOTERM_LOC_AVA_TIME_ZONE_ID       INT 

, INCOTERM_NAME                       STRING 

, INSUR_CHRG                          DECIMAL(11,2) 

, INVC_BATCH_NBR                      INT 

, IS_BACK_ORDERED                     STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, MAJOR_MINOR_ORDER                   STRING 

, MAJOR_ORDER_CTRL_NBR                STRING 

, MAJOR_ORDER_GRP_ATTR                STRING 

, MANIFEST_ID                         STRING 

, MERCHANDIZING_DEPARTMENT            STRING 

, MISC_CHARGES                        DECIMAL(13,4) 

, MONETARY_VALUE                      DECIMAL(16,4) 

, MOVEMENT_OPTION                     SMALLINT 

, MV_CURRENCY                         STRING 

, NON_MACHINEABLE                     SMALLINT 

, O_ADDRESS_1                         STRING 

, O_ADDRESS_2                         STRING 

, O_ADDRESS_3                         STRING 

, O_CITY                              STRING 

, O_CONTACT                           STRING 

, O_COUNTRY_CODE                      STRING 

, O_COUNTY                            STRING 

, O_DOCK_DOOR_ID                      BIGINT 

, O_DOCK_ID                           STRING 

, O_EMAIL                             STRING 

, O_FACILITY_ALIAS_ID                 STRING 

, O_FACILITY_ID                       INT 

, O_FACILITY_NAME                     STRING 

, O_FAX_NUMBER                        STRING 

, O_POSTAL_CODE                       STRING 

, O_STATE_PROV                        STRING 

, ORDER_DATE_DTTM                     TIMESTAMP 

, ORDER_WEIGHT                        DECIMAL(13,4) 

, ORG_SHIP_FACILITY_ALIAS_ID          STRING 

, ORG_SHIP_FACILITY_ID                INT 

, ORIGINAL_ASSIGNED_SHIP_VIA          STRING 

, ORIGIN_SHIP_THRU_FACILITY_ID        INT 

, ORIGIN_SHIP_THRU_FAC_ALIAS_ID       STRING 

, PICKUP_END_DTTM                     TIMESTAMP 

, PICKUP_START_DTTM                   TIMESTAMP 

, PLANNED_SHIP_DATE                   TIMESTAMP 

, PRE_PACK_FLAG                       SMALLINT 

, PRIORITY                            SMALLINT 

, PRO_NUMBER                          STRING 

, PROC_DTTM                           TIMESTAMP 

, PROC_STAT_CODE                      SMALLINT 

, REC_XPANS_FIELD                     STRING 

, RTE_ID                              STRING 

, RTE_SHIPMENT_SEQ                    INT 

, SCHED_DELIVERY_DTTM                 TIMESTAMP 

, SHIPMENT_ID                         BIGINT 

, SHIP_DATE                           TIMESTAMP 

, SHIP_GROUP_ID                       STRING 

, SHIP_GROUP_SEQUENCE                 SMALLINT 

, SHPNG_CHRG                          DECIMAL(11,2) 

, ORDER_STATUS                        SMALLINT 

, DO_STATUS                           SMALLINT 

, STORE_NBR                           STRING 

, STORE_TYPE                          STRING 

, TAX_CHARGES                         DECIMAL(13,4) 

, TC_COMPANY_ID                       INT 

, TC_ORDER_ID                         STRING 

, TC_SHIPMENT_ID                      STRING 

, TOTAL_NBR_OF_LPN                    INT 

, TOTAL_NBR_OF_PLT                    INT 

, TOTAL_NBR_OF_UNITS                  DECIMAL(15,4) 

, VERSION_NBR                         BIGINT 

, LINE_HAUL_SHIP_VIA                  STRING 

, PLAN_D_FACILITY_ALIAS_ID            STRING 

, PLAN_D_FACILITY_ID                  INT 

, PLAN_LEG_D_ALIAS_ID                 STRING 

, PLAN_LEG_D_ID                       INT 

, PLAN_LEG_O_ALIAS_ID                 STRING 

, PLAN_LEG_O_ID                       INT 

, PLAN_O_FACILITY_ALIAS_ID            STRING 

, PLAN_O_FACILITY_ID                  BIGINT 

, SPL_INSTR_CODE_1                    STRING 

, SPL_INSTR_CODE_10                   STRING 

, SPL_INSTR_CODE_2                    STRING 

, SPL_INSTR_CODE_3                    STRING 

, SPL_INSTR_CODE_4                    STRING 

, SPL_INSTR_CODE_5                    STRING 

, SPL_INSTR_CODE_6                    STRING 

, SPL_INSTR_CODE_7                    STRING 

, SPL_INSTR_CODE_8                    STRING 

, SPL_INSTR_CODE_9                    STRING 

, PRE_BILL_STATUS                     SMALLINT 

, PRTL_SHIP_CONF_FLAG                 SMALLINT 

, PRTL_SHIP_CONF_STATUS               INT 

, PRE_BILLED_FLAG                     SMALLINT 

, TAX_ID                              STRING 

, ALLOW_PRE_BILLING                   TINYINT 

, O_PHONE_NUMBER                      STRING 

, PURCHASE_ORDER_NBR                  STRING 

, MARK_FOR                            STRING 

, DISTRO_NUMBER                       STRING 

, REF_SHIPMENT_NBR                    STRING 

, REF_STOP_SEQ                        INT 

, PO_TYPE_ATTR                        STRING 

, DSG_SHIP_VIA                        STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_outpt_orders_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (OUTPT_ORDERS_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_OUTPT_ORDER_LINE_ITEM_PRE" , ***** Creating table: "WM_OUTPT_ORDER_LINE_ITEM_PRE"


CREATE TABLE  WM_OUTPT_ORDER_LINE_ITEM_PRE
( DC_NBR                              SMALLINT                not null

, OUTPT_ORDER_LINE_ITEM_ID            BIGINT               not null

, ACTUAL_COST_CURRENCY_CODE           STRING 

, ACTUAL_SHIPPED_DTTM                 TIMESTAMP 

, ASSORT_NBR                          STRING 

, BACK_ORD_QTY                        DECIMAL(13,2) 

, BATCH_CTRL_NBR                      STRING 

, BATCH_NBR                           STRING 

, CNTRY_OF_ORGN                       STRING 

, COMMODITY_CLASS                     STRING 

, CREATED_DTTM                        TIMESTAMP 

, CREATED_SOURCE                      STRING 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CUSTOMER_ITEM                       STRING 

, EXP_INFO_CODE                       STRING 

, GTIN                                STRING 

, INVC_BATCH_NBR                      INT 

, INVN_TYPE                           STRING 

, ITEM_ATTR_1                         STRING 

, ITEM_ATTR_2                         STRING 

, ITEM_ATTR_3                         STRING 

, ITEM_ATTR_4                         STRING 

, ITEM_ATTR_5                         STRING 

, ITEM_ID                             INT 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LINE_ITEM_ID                        BIGINT 

, LINE_TYPE                           STRING 

, MANUFACTURING_DTTM                  TIMESTAMP 

, MISC_INSTR_10_BYTE_1                STRING 

, MISC_INSTR_10_BYTE_2                STRING 

, ORDER_QTY                           DECIMAL(13,4) 

, ORDER_QTY_UOM                       STRING 

, ORIG_ITEM_ID                        INT 

, ORIG_ORDER_LINE_ITEM_ID             BIGINT 

, ORIG_ORDER_QTY                      DECIMAL(13,4) 

, ORIG_ORDER_QTY_UOM                  STRING 

, PARENT_LINE_ITEM_ID                 BIGINT 

, PICKUP_END_DTTM                     TIMESTAMP 

, PICKUP_REFERENCE_NUMBER             STRING 

, PICKUP_START_DTTM                   TIMESTAMP 

, PLANNED_QUANTITY                    DECIMAL(13,4) 

, PLANNED_QUANTITY_UOM                STRING 

, PLANNED_SHIP_DATE                   TIMESTAMP 

, PPACK_GRP_CODE                      STRING 

, PPACK_QTY                           DECIMAL(13,4) 

, PRICE                               DECIMAL(13,4) 

, PRICE_TKT_TYPE                      STRING 

, PROC_DTTM                           TIMESTAMP 

, PROC_STAT_CODE                      SMALLINT 

, PRODUCT_STATUS                      STRING 

, PURCHASE_ORDER_LINE_NUMBER          STRING 

, REASON_CODE                         STRING 

, REC_PROC_FLAG                       STRING 

, REC_XPANS_FIELD                     STRING 

, RETAIL_PRICE                        DECIMAL(16,4) 

, SHIPPED_QTY                         DECIMAL(13,4) 

, TC_COMPANY_ID                       INT 

, UNIT_VOL                            DECIMAL(16,4) 

, UNIT_WT                             DECIMAL(16,4) 

, UOM                                 STRING 

, USER_CANCELED_QTY                   DECIMAL(13,4) 

, VERSION_NBR                         BIGINT 

, AISLE                               STRING 

, AREA                                STRING 

, BAY                                 STRING 

, LVL                                 STRING 

, POSN                                STRING 

, ZONE                                STRING 

, ITEM_COLOR                          STRING 

, ITEM_COLOR_SFX                      STRING 

, ITEM_SEASON                         STRING 

, ITEM_SEASON_YEAR                    STRING 

, ITEM_SECOND_DIM                     STRING 

, ITEM_SIZE_DESC                      STRING 

, ITEM_QUALITY                        STRING 

, ITEM_STYLE                          STRING 

, ITEM_STYLE_SFX                      STRING 

, SIZE_RANGE_CODE                     STRING 

, SIZE_REL_POSN_IN_TABLE              STRING 

, SHELF_DAYS                          INT 

, TEMP_ZONE                           STRING 

, ORIG_ITEM_COLOR                     STRING 

, ORIG_ITEM_COLOR_SFX                 STRING 

, ORIG_ITEM_SEASON                    STRING 

, ORIG_ITEM_SEASON_YEAR               STRING 

, ORIG_ITEM_SECOND_DIM                STRING 

, ORIG_ITEM_SIZE_DESC                 STRING 

, ORIG_ITEM_QUALITY                   STRING 

, ORIG_ITEM_STYLE                     STRING 

, ORIG_ITEM_STYLE_SFX                 STRING 

, TC_ORDER_ID                         STRING 

, TC_PURCHASE_ORDERS_ID               STRING 

, ITEM_NAME                           STRING 

, ACTUAL_COST                         DECIMAL(13,4) 

, TC_ORDER_LINE_ID                    STRING 

, PURCHASE_ORDER_NUMBER               STRING 

, EXT_PURCHASE_ORDER                  STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_outpt_order_line_item_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (OUTPT_ORDER_LINE_ITEM_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_PICKING_SHORT_ITEM_PRE" , ***** Creating table: "WM_PICKING_SHORT_ITEM_PRE"


CREATE TABLE  WM_PICKING_SHORT_ITEM_PRE
( DC_NBR                              SMALLINT                not null

, PICKING_SHORT_ITEM_ID               INT                not null

, ITEM_ID                             INT 

, LOCN_ID                             STRING 

, LINE_ITEM_ID                        BIGINT 

, TC_ORDER_ID                         STRING 

, WAVE_NBR                            STRING 

, SHORT_QTY                           DECIMAL(13,5) 

, STAT_CODE                           SMALLINT 

, TC_COMPANY_ID                       INT 

, CREATED_DTTM                       date
, CREATED_SOURCE                      STRING 

, LAST_UPDATED_DTTM DATE

, LAST_UPDATED_SOURCE                 STRING 

, SHORT_TYPE                          STRING 

, TC_LPN_ID                           STRING 

, LPN_DETAIL_ID                       BIGINT 

, REQD_INVN_TYPE                      STRING 

, REQD_PROD_STAT                      STRING 

, REQD_BATCH_NBR                      STRING 

, REQD_SKU_ATTR_1                     STRING 

, REQD_SKU_ATTR_2                     STRING 

, REQD_SKU_ATTR_3                     STRING 

, REQD_SKU_ATTR_4                     STRING 

, REQD_SKU_ATTR_5                     STRING 

, REQD_CNTRY_OF_ORGN                  STRING 

, SHIPMENT_ID                         BIGINT 

, TC_SHIPMENT_ID                      STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_picking_short_item_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (PICKING_SHORT_ITEM_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_PICK_LOCN_DTL_PRE" , ***** Creating table: "WM_PICK_LOCN_DTL_PRE"


CREATE TABLE  WM_PICK_LOCN_DTL_PRE
( DC_NBR                              SMALLINT                not null

, PICK_LOCN_DTL_ID                    BIGINT               not null

, LOCN_ID                             STRING 

, LOCN_SEQ_NBR                        INT 

, SKU_ATTR_1                          STRING 

, SKU_ATTR_2                          STRING 

, SKU_ATTR_3                          STRING 

, SKU_ATTR_4                          STRING 

, SKU_ATTR_5                          STRING 

, INVN_TYPE                           STRING 

, PROD_STAT                           STRING 

, BATCH_NBR                           STRING 

, CNTRY_OF_ORGN                       STRING 

, MAX_INVN_QTY                        DECIMAL(13,5) 

, MIN_INVN_QTY                        DECIMAL(13,5) 

, ACTL_INVN_CASES                     BIGINT 

, MIN_INVN_CASES                      BIGINT 

, MAX_INVN_CASES                      BIGINT 

, TRIG_REPL_FOR_SKU                   STRING 

, PRIM_LOCN_FOR_SKU                   STRING 

, FIRST_WAVE_NBR                      STRING 

, LAST_WAVE_NBR                       STRING 

, LTST_SKU_ASSIGN                     STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, LTST_PICK_ASSIGN_DATE_TIME          TIMESTAMP 

, PIKNG_LOCK_CODE                     STRING 

, TO_BE_FILLD_CASES                   INT 

, LANES                               STRING 

, STACKING                            STRING 

, MIN_QTY_TO_RLS_HELD_RPLN            DECIMAL(13,5) 

, MIN_CASES_TO_RLS_HELD_RPLN          INT 

, TASK_RELEASED                       STRING 

, PACK_QTY                            DECIMAL(9,2) 

, PICK_LOCN_HDR_ID                    INT 

, WM_VERSION_ID                       INT 

, ITEM_MASTER_ID                      INT 

, ITEM_ID                             INT 

, PRE_ALLOCATED_QTY                   DECIMAL(13,4) 

, REPLEN_CONTROL                      STRING 

, STOP_QTY                            DECIMAL(13,4) 

, STOP_DTTM                           TIMESTAMP 

, PICK_TO_ZERO_ACTION                 STRING 

, UTIL_PERCENT                        DECIMAL(3,2) 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_pick_locn_dtl_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (PICK_LOCN_DTL_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_PICK_LOCN_DTL_SLOTTING_PRE" , ***** Creating table: "WM_PICK_LOCN_DTL_SLOTTING_PRE"


CREATE TABLE  WM_PICK_LOCN_DTL_SLOTTING_PRE
( DC_NBR                              SMALLINT                not null

, PICK_LOCN_DTL_ID                    INT                not null

, HIST_MATCH                          STRING 

, CUR_ORIENTATION                     STRING 

, IGN_FOR_RESLOT                      TINYINT 

, REC_LANES                           BIGINT 

, REC_STACKING                        BIGINT 

, HI_RESIDUAL_1                       TINYINT 

, OPT_PALLET_PATTERN                  STRING 

, SI_NUM_1                            DECIMAL(13,4) 

, SI_NUM_2                            DECIMAL(13,4) 

, SI_NUM_3                            DECIMAL(13,4) 

, SI_NUM_4                            DECIMAL(13,4) 

, SI_NUM_5                            DECIMAL(13,4) 

, SI_NUM_6                            DECIMAL(13,4) 

, MULT_LOC_GRP                        STRING 

, REPLEN_GROUP                        STRING 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_pick_locn_dtl_slotting_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (PICK_LOCN_DTL_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_PICK_LOCN_HDR_PRE" , ***** Creating table: "WM_PICK_LOCN_HDR_PRE"


CREATE TABLE  WM_PICK_LOCN_HDR_PRE
( DC_NBR                              SMALLINT                not null

, PICK_LOCN_HDR_ID                    INT                not null

, LOCN_ID                             STRING 

, REPL_LOCN_BRCD                      STRING 

, PUTWY_TYPE                          STRING 

, MAX_NBR_OF_SKU                      SMALLINT 

, REPL_FLAG                           STRING 

, PICK_LOCN_ASSIGN_ZONE               STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, REPL_CHECK_DIGIT                    STRING 

, MAX_VOL                             DECIMAL(13,4) 

, MAX_WT                              DECIMAL(13,4) 

, REPL_X_COORD                        DECIMAL(13,5) 

, REPL_Y_COORD                        DECIMAL(13,5) 

, REPL_Z_COORD                        DECIMAL(13,5) 

, REPL_TRAVEL_AISLE                   STRING 

, REPL_TRAVEL_ZONE                    STRING 

, XCESS_WAVE_NEED_PROC_TYPE           TINYINT 

, PICK_LOCN_ASSIGN_TYPE               STRING 

, SUPPR_PR40_REPL                     SMALLINT 

, COMB_4050_REPL                      SMALLINT 

, PICK_TO_LIGHT_FLAG                  STRING 

, PICK_TO_LIGHT_REPL_FLAG             STRING 

, WM_VERSION_ID                       INT 

, LOCN_HDR_ID                         INT 

, LOCN_PUTAWAY_LOCK                   STRING 

, INVN_LOCK_CODE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_pick_locn_hdr_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_PICK_LOCN_HDR_SLOTTING_PRE" , ***** Creating table: "WM_PICK_LOCN_HDR_SLOTTING_PRE"


CREATE TABLE  WM_PICK_LOCN_HDR_SLOTTING_PRE
( DC_NBR                              SMALLINT                not null

, PICK_LOCN_HDR_ID                    DECIMAL(19,0)               not null

, ALLOW_EXPAND                        TINYINT 

, ALLOW_EXPAND_LFT                    TINYINT 

, ALLOW_EXPAND_LFT_OVR                TINYINT 

, ALLOW_EXPAND_OVR                    TINYINT 

, ALLOW_EXPAND_RGT                    TINYINT 

, ALLOW_EXPAND_RGT_OVR                TINYINT 

, DEPTH_OVERRIDE                      TINYINT 

, HT_OVERRIDE                         TINYINT 

, LABEL_POS                           DECIMAL(9,4) 

, LABEL_POS_OVR                       TINYINT 

, LEFT_SLOT                           DECIMAL(19,0) 

, LOCKED                              TINYINT 

, MAX_HC_OVR                          TINYINT 

, MAX_HT_CLEAR                        DECIMAL(9,4) 

, MAX_LANE_WT                         DECIMAL(13,4) 

, MAX_LANES                           BIGINT 

, MAX_LN_OVR                          TINYINT 

, MAX_LW_OVR                          DECIMAL(9,4) 

, MAX_SC_OVR                          TINYINT 

, MAX_SIDE_CLEAR                      DECIMAL(9,4) 

, MAX_ST_OVR                          TINYINT 

, MAX_STACK                           BIGINT 

, MY_RANGE                            BIGINT 

, MY_SNS                              BIGINT 

, OLD_REC_SLOT_WIDTH                  DECIMAL(13,4) 

, PROCESSED                           TINYINT 

, RACK_LEVEL_ID                       BIGINT 

, RACK_TYPE                           BIGINT 

, REACH_DIST                          DECIMAL(13,4) 

, REACH_DIST_OVERRIDE                 TINYINT 

, RESERVED_1                          STRING 

, RESERVED_2                          STRING 

, RESERVED_3                          BIGINT 

, RESERVED_4                          BIGINT 

, RIGHT_SLOT                          DECIMAL(19,0) 

, SIDE_OF_AISLE                       SMALLINT 

, SLOT_PRIORITY                       STRING 

, WIDTH_OVERRIDE                      TINYINT 

, WT_LIMIT_OVERRIDE                   TINYINT 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, SLOTTING_GROUP                      STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_pick_locn_hdr_slotting_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (PICK_LOCN_HDR_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_PIX_TRAN_PRE" , ***** Creating table: "WM_PIX_TRAN_PRE"


CREATE TABLE  WM_PIX_TRAN_PRE
( DC_NBR                              SMALLINT                not null

, PIX_TRAN_ID                         INT                not null

, TRAN_TYPE                           STRING 

, TRAN_CODE                           STRING 

, TRAN_NBR                            INT 

, PIX_SEQ_NBR                         INT 

, PROC_STAT_CODE                      TINYINT 

, WHSE                                STRING 

, CASE_NBR                            STRING 

, SEASON                              STRING 

, SEASON_YR                           STRING 

, STYLE                               STRING 

, STYLE_SFX                           STRING 

, COLOR                               STRING 

, COLOR_SFX                           STRING 

, SEC_DIM                             STRING 

, QUAL                                STRING 

, SIZE_DESC                           STRING 

, SIZE_RANGE_CODE                     STRING 

, SIZE_REL_POSN_IN_TABLE              STRING 

, INVN_TYPE                           STRING 

, PROD_STAT                           STRING 

, BATCH_NBR                           STRING 

, SKU_ATTR_1                          STRING 

, SKU_ATTR_2                          STRING 

, SKU_ATTR_3                          STRING 

, SKU_ATTR_4                          STRING 

, SKU_ATTR_5                          STRING 

, CNTRY_OF_ORGN                       STRING 

, INVN_ADJMT_QTY                      DECIMAL(13,5) 

, INVN_ADJMT_TYPE                     STRING 

, WT_ADJMT_QTY                        DECIMAL(13,5) 

, WT_ADJMT_TYPE                       STRING 

, UOM                                 STRING 

, REF_WHSE                            STRING 

, RSN_CODE                            STRING 

, RCPT_VARI                           STRING 

, RCPT_CMPL                           STRING 

, CASES_SHPD                          INT 

, UNITS_SHPD                          DECIMAL(15,5) 

, CASES_RCVD                          INT 

, UNITS_RCVD                          DECIMAL(15,5) 

, ACTN_CODE                           STRING 

, CUSTOM_REF                          STRING 

, DATE_PROC                           TIMESTAMP 

, SYS_USER_ID                         STRING 

, ERROR_CMNT                          STRING 

, REF_CODE_ID_1                       STRING 

, REF_FIELD_1                         STRING 

, REF_CODE_ID_2                       STRING 

, REF_FIELD_2                         STRING 

, REF_CODE_ID_3                       STRING 

, REF_FIELD_3                         STRING 

, REF_CODE_ID_4                       STRING 

, REF_FIELD_4                         STRING 

, REF_CODE_ID_5                       STRING 

, REF_FIELD_5                         STRING 

, REF_CODE_ID_6                       STRING 

, REF_FIELD_6                         STRING 

, REF_CODE_ID_7                       STRING 

, REF_FIELD_7                         STRING 

, REF_CODE_ID_8                       STRING 

, REF_FIELD_8                         STRING 

, REF_CODE_ID_9                       STRING 

, REF_FIELD_9                         STRING 

, REF_CODE_ID_10                      STRING 

, REF_FIELD_10                        STRING 

, XML_GROUP_ID                        STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, ITEM_ID                             BIGINT 

, TC_COMPANY_ID                       INT 

, WM_VERSION_ID                       INT 

, FACILITY_ID                         INT 

, COMPANY_CODE                        STRING 

, ITEM_NAME                           STRING 

, ESIGN_USER_NAME                     STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_pix_tran_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (PIX_TRAN_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_PRODUCT_CLASS_PRE" , ***** Creating table: "WM_PRODUCT_CLASS_PRE"


CREATE TABLE  WM_PRODUCT_CLASS_PRE
( DC_NBR                              SMALLINT                not null

, PRODUCT_CLASS_ID                    INT                not null

, TC_COMPANY_ID                       INT 

, PRODUCT_CLASS                       STRING 

, DESCRIPTION                         STRING 

, MARK_FOR_DELETION                   TINYINT 

, HAS_SPLIT                           SMALLINT 

, RANK                                SMALLINT 

, MIN_THRESHOLD                       SMALLINT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, STACKING_FACTOR                     INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_product_class_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_PURCHASE_ORDERS_LINE_ITEM_PRE" , ***** Creating table: "WM_PURCHASE_ORDERS_LINE_ITEM_PRE"


CREATE TABLE  WM_PURCHASE_ORDERS_LINE_ITEM_PRE
( DC_NBR                              SMALLINT                not null

, PURCHASE_ORDERS_ID                  BIGINT               not null

, PURCHASE_ORDERS_LINE_ITEM_ID        BIGINT               not null

, TC_COMPANY_ID                       INT 

, BUSINESS_PARTNER_ID                 STRING 

, EXT_SYS_LINE_ITEM_ID                STRING 

, DESCRIPTION                         STRING 

, O_FACILITY_ID                       INT 

, O_FACILITY_ALIAS_ID                 STRING 

, D_FACILITY_ID                       INT 

, D_FACILITY_ALIAS_ID                 STRING 

, D_NAME                              STRING 

, D_LAST_NAME                         STRING 

, D_ADDRESS_1                         STRING 

, D_ADDRESS_2                         STRING 

, D_ADDRESS_3                         STRING 

, D_CITY                              STRING 

, D_STATE_PROV                        STRING 

, D_POSTAL_CODE                       STRING 

, D_COUNTY                            STRING 

, D_COUNTRY_CODE                      STRING 

, D_PHONE_NUMBER                      STRING 

, D_FAX_NUMBER                        STRING 

, D_EMAIL                             STRING 

, BILL_FACILITY_ALIAS_ID              STRING 

, BILL_FACILITY_ID                    INT 

, BILL_TO_ADDRESS_1                   STRING 

, BILL_TO_ADDRESS_2                   STRING 

, BILL_TO_ADDRESS_3                   STRING 

, BILL_TO_CITY                        STRING 

, BILL_TO_STATE_PROV                  STRING 

, BILL_TO_COUNTY                      STRING 

, BILL_TO_POSTAL_CODE                 STRING 

, BILL_TO_COUNTRY_CODE                STRING 

, BILL_TO_PHONE_NUMBER                STRING 

, BILL_TO_FAX_NUMBER                  STRING 

, BILL_TO_EMAIL                       STRING 

, STORE_FACILITY_ALIAS_ID             STRING 

, STORE_FACILITY_ID                   INT 

, SKU_ID                              INT 

, SKU                                 STRING 

, SKU_GTIN                            STRING 

, SKU_SUB_CODE_ID                     INT 

, SKU_SUB_CODE_VALUE                  STRING 

, SKU_ATTR_1                          STRING 

, SKU_ATTR_2                          STRING 

, SKU_ATTR_3                          STRING 

, SKU_ATTR_4                          STRING 

, SKU_ATTR_5                          STRING 

, ORIG_BUDG_COST                      DECIMAL(13,4) 

, MV_CURRENCY_CODE                    STRING 

, TOTAL_MONETARY_VALUE                DECIMAL(13,4) 

, UNIT_MONETARY_VALUE                 DECIMAL(13,4) 

, UNIT_TAX_AMOUNT                     DECIMAL(16,4) 

, COMMODITY_CODE_ID                   BIGINT 

, UN_NUMBER_ID                        BIGINT 

, EPC_TRACKING_RFID_VALUE             STRING 

, PROD_SCHED_REF_NUMBER               STRING 

, PICKUP_REFERENCE_NUMBER             STRING 

, DELIVERY_REFERENCE_NUMBER           STRING 

, PROTECTION_LEVEL_ID                 INT 

, PACKAGE_TYPE_ID                     INT 

, PRODUCT_CLASS_ID                    INT 

, DSG_SERVICE_LEVEL_ID                BIGINT 

, TOTAL_SIZE_ON_ORDERS                DECIMAL(13,4) 

, STD_INBD_LPN_QTY                    DECIMAL(13,4) 

, ALLOCATED_QTY                       DECIMAL(13,4) 

, RELEASED_QTY                        DECIMAL(13,4) 

, CANCELLED_QTY                       DECIMAL(13,4) 

, PPACK_QTY                           DECIMAL(13,4) 

, STD_PACK_QTY                        DECIMAL(13,4) 

, STD_LPN_QTY                         DECIMAL(13,4) 

, STD_SUB_PACK_QTY                    DECIMAL(13,4) 

, STD_BUNDL_QTY                       DECIMAL(13,4) 

, STD_PLT_QTY                         DECIMAL(13,4) 

, DELIVERY_TZ                         INT 

, PICKUP_TZ                           INT 

, PICKUP_START_DTTM                   TIMESTAMP 

, PICKUP_END_DTTM                     TIMESTAMP 

, DELIVERY_START_DTTM                 TIMESTAMP 

, DELIVERY_END_DTTM                   TIMESTAMP 

, REQ_DLVR_DTTM                       TIMESTAMP 

, MUST_DLVR_DTTM                      TIMESTAMP 

, COMM_DLVR_DTTM                      TIMESTAMP 

, COMM_SHIP_DTTM                      TIMESTAMP 

, PROMISED_DLVR_DTTM                  TIMESTAMP 

, SHIP_BY_DATE                        TIMESTAMP 

, IS_READY_TO_SHIP                    TINYINT 

, IS_HAZMAT                           TINYINT 

, IS_CANCELLED                        TINYINT 

, IS_CLOSED                           TINYINT 

, IS_BONDED                           TINYINT 

, IS_DELETED                          TINYINT 

, HAS_ERRORS                          TINYINT 

, IS_FLOWTHROU                        TINYINT 

, IS_NEVER_OUT                        TINYINT 

, IS_QUANTITY_LOCKED                  TINYINT 

, IS_VARIABLE_WEIGHT                  TINYINT 

, HAS_ROUTING_REQUEST                 TINYINT 

, ALLOW_SUBSTITUTION                  TINYINT 

, EXPIRE_DATE_REQD                    SMALLINT 

, APPLY_PROMOTIONAL                   TINYINT 

, PRE_PACK_FLAG                       TINYINT 

, PROCESS_FLAG                        TINYINT 

, ON_HOLD                             TINYINT 

, IS_ASSOCIATED_TO_OUTBOUND           TINYINT 

, MV_SIZE_UOM_ID                      INT 

, QTY_UOM_ID_BASE                     INT 

, SHIPPED_QTY                         DECIMAL(16,4) 

, RECEIVED_QTY                        DECIMAL(16,4) 

, INVN_TYPE                           STRING 

, PROD_STAT                           STRING 

, BATCH_NBR                           STRING 

, CNTRY_OF_ORGN                       STRING 

, PPACK_GRP_CODE                      STRING 

, ASSORT_NBR                          STRING 

, MERCHANDIZING_DEPARTMENT_ID         BIGINT 

, MERCH_TYPE                          STRING 

, MERCH_GRP                           STRING 

, STORE_DEPT                          STRING 

, SHELF_DAYS                          INT 

, FRT_CLASS                           STRING 

, GROUP_ID                            INT 

, SUB_GROUP_ID                        INT 

, ALLOC_TMPL_ID                       INT 

, RELEASE_TMPL_ID                     INT 

, WORKFLOW_ID                         INT 

, VARIANT_VALUE                       STRING 

, PROCESS_TYPE                        STRING 

, PRIORITY                            BIGINT 

, STD_INBD_LPN_VOL                    DECIMAL(13,4) 

, STD_INBD_LPN_WT                     DECIMAL(13,4) 

, OUTBD_LPN_BRK_ATTRIB                STRING 

, DOM_ORDER_LINE_STATUS               SMALLINT 

, EFFECTIVE_RANK                      STRING 

, ORIG_ORD_LINE_NBR                   INT 

, SO_LN_FULFILL_OPTION                TINYINT 

, SUPPLY_ASN_DTL_ID                   BIGINT 

, SUPPLY_ASN_ID                       STRING 

, SUPPLY_PO_ID                        STRING 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, ACTUAL_COST                         DECIMAL(13,4) 

, ACTUAL_CURRENCY_CODE                STRING 

, BUDGETED_COST                       DECIMAL(13,4) 

, BUDGETED_COST_CURRENCY_CODE         STRING 

, DSG_MOT_ID                          BIGINT 

, EXPIRE_DTTM                         TIMESTAMP 

, IS_LOCKED                           TINYINT 

, INBD_LPN_ID                         BIGINT 

, INBD_LPNS_RCVD                      INT 

, NET_NEEDS                           DECIMAL(13,4) 

, OUTBD_LPN_EPC_TYPE                  TINYINT 

, OUTBD_LPN_SIZE                      STRING 

, OUTBD_LPN_TYPE                      STRING 

, DSG_CARRIER_ID                      BIGINT 

, DSG_CARRIER_CODE                    STRING 

, LPN_SIZE                            DECIMAL(16,4) 

, ORDER_CONSOL_ATTR                   STRING 

, PROC_IMMD_NEEDS                     STRING 

, MFG_PLANT                           STRING 

, MFG_DATE                            TIMESTAMP 

, PURCHASE_ORDERS_LINE_STATUS         SMALLINT 

, HIBERNATE_VERSION                   BIGINT 

, STACK_LENGTH_VALUE                  DECIMAL(16,4) 

, STACK_LENGTH_STANDARD_UOM           INT 

, STACK_WIDTH_VALUE                   DECIMAL(16,4) 

, STACK_WIDTH_STANDARD_UOM            INT 

, STACK_HEIGHT_VALUE                  DECIMAL(16,4) 

, STACK_HEIGHT_STANDARD_UOM           INT 

, STACK_DIAMETER_VALUE                DECIMAL(16,4) 

, STACK_DIAMETER_STANDARD_UOM         INT 

, COMMODITY_CLASS                     INT 

, TC_PO_LINE_ID                       STRING 

, DOM_SUB_SKU_ID                      BIGINT 

, SUPPLY_CONS_PO_ID                   STRING 

, SUPPLY_PO_LINE_ID                   STRING 

, PARENT_PO_LINE_ITEM_ID              BIGINT 

, PARENT_TC_PO_LINE_ID                STRING 

, ROOT_LINE_ITEM_ID                   BIGINT 

, ORDER_QTY                           DECIMAL(13,4) 

, WF_NODE_ID                          SMALLINT 

, WF_PROCESS_STATE                    TINYINT 

, ALLOC_FINALIZER                     STRING 

, QTY_UOM_ID                          BIGINT 

, INVENTORY_SEGMENT_ID                BIGINT 

, CUST_SKU                            STRING 

, PLT_TYPE                            STRING 

, PACKAGE_TYPE_INSTANCE               STRING 

, ACCEPTED_DTTM                       TIMESTAMP 

, IS_DO_CREATED                       TINYINT 

, RECEIVED_WEIGHT                     DECIMAL(13,4) 

, PACKED_SIZE_VALUE                   DECIMAL(13,4) 

, PLANNED_WEIGHT                      DECIMAL(13,4) 

, SHIPPED_WEIGHT                      DECIMAL(13,4) 

, WEIGHT_UOM_ID_BASE                  INT 

, WEIGHT_UOM_ID                       INT 

, PLANNED_VOLUME                      DECIMAL(13,4) 

, RECEIVED_VOLUME                     DECIMAL(13,4) 

, SHIPPED_VOLUME                      DECIMAL(13,4) 

, VOLUME_UOM_ID_BASE                  INT 

, VOLUME_UOM_ID                       INT 

, SIZE1_UOM_ID                        INT 

, SIZE1_VALUE                         DECIMAL(13,4) 

, RECEIVED_SIZE1                      DECIMAL(13,4) 

, SHIPPED_SIZE1                       DECIMAL(13,4) 

, SIZE2_UOM_ID                        INT 

, SIZE2_VALUE                         DECIMAL(13,4) 

, RECEIVED_SIZE2                      DECIMAL(13,4) 

, SHIPPED_SIZE2                       DECIMAL(13,4) 

, QTY_CONV_FACTOR                     DECIMAL(17,8) 

, ORIG_ORDER_QTY                      DECIMAL(13,4) 

, RETAIL_PRICE                        DECIMAL(16,4) 

, DSG_SHIP_VIA                        STRING 

, LINE_TOTAL                          DECIMAL(13,4) 

, IS_GIFT                             STRING 

, ADDR_VALID                          STRING 

, REASON_ID                           INT 

, HAS_CHILD                           STRING 

, NEW_LINE_TYPE                       TINYINT 

, REQ_CAPACITY_PER_UNIT               INT 

, MERGE_FACILITY                      INT 

, MERGE_FACILITY_ALIAS_ID             STRING 

, ORDER_FULFILLMENT_OPTION            STRING 

, REF_FIELD_1                         STRING 

, REF_FIELD_2                         STRING 

, REF_FIELD_3                         STRING 

, IS_RETURNABLE                       TINYINT 

, BACK_ORD_REASON                     STRING 

, FREIGHT_REVENUE_CURRENCY_CODE       STRING 

, FREIGHT_REVENUE                     DECIMAL(13,4) 

, PRICE_OVERRIDE                      TINYINT 

, ORIGINAL_PRICE                      DECIMAL(13,4) 

, EXT_CREATED_DTTM                    TIMESTAMP 

, REASON_CODES_GROUP_ID               BIGINT 

, OVER_SHIP_PCT                       INT 

, IS_EXCHANGEABLE                     TINYINT 

, ORIGINAL_PO_LINE_ITEM_ID            BIGINT 

, RMA_LINE_STATUS                     SMALLINT 

, OVER_PACK_PCT                       INT 

, ALLOW_RESIDUAL_PACK                 TINYINT 

, HAS_COMP_ITEM                       TINYINT 

, INV_DISPOSITION                     STRING 

, EXT_PLAN_ID                         STRING 

, TAX_INCLUDED                        TINYINT 

, RETURN_ACTION_TYPE                  INT 

, HUB1_FACILITY_ALIAS_ID              STRING 

, HUB1_FACILITY_ID                    INT 

, HUB2_FACILITY_ALIAS_ID              STRING 

, HUB2_FACILITY_ID                    INT 

, GIFT_CARD_VALUE                     DECIMAL(13,4) 

, PALLET_CONTENT_CODE                 STRING 

, REF_FIELD4                          STRING 

, REF_FIELD5                          STRING 

, REF_FIELD6                          STRING 

, REF_FIELD7                          STRING 

, REF_FIELD8                          STRING 

, REF_FIELD9                          STRING 

, REF_FIELD10                         STRING 

, REF_NUM1                            DECIMAL(13,5) 

, REF_NUM2                            DECIMAL(13,5) 

, REF_NUM3                            DECIMAL(13,5) 

, REF_NUM4                            DECIMAL(13,5) 

, REF_NUM5                            DECIMAL(13,5) 

, REF_BOOLEAN1                        TINYINT 

, REF_BOOLEAN2                        TINYINT 

, REF_SYSCODE1                        STRING 

, REF_SYSCODE2                        STRING 

, REF_SYSCODE3                        STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_purchase_orders_line_item_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (PURCHASE_ORDERS_ID, PURCHASE_ORDERS_LINE_ITEM_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_PURCHASE_ORDERS_LINE_STATUS_PRE" , ***** Creating table: "WM_PURCHASE_ORDERS_LINE_STATUS_PRE"


CREATE TABLE  WM_PURCHASE_ORDERS_LINE_STATUS_PRE
( DC_NBR                              SMALLINT                not null

, PURCHASE_ORDERS_LINE_STATUS         SMALLINT                not null

, DESCRIPTION                         STRING 

, NOTE                                STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_purchase_orders_line_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_PURCHASE_ORDERS_PRE" , ***** Creating table: "WM_PURCHASE_ORDERS_PRE"


CREATE TABLE  WM_PURCHASE_ORDERS_PRE
( DC_NBR                              SMALLINT                not null

, PURCHASE_ORDERS_ID                  BIGINT               not null

, TC_COMPANY_ID                       INT 

, BUSINESS_PARTNER_ID                 STRING 

, CREATION_TYPE                       TINYINT 

, PURCHASE_ORDERS_TYPE                TINYINT 

, PURCHASE_ORDERS_STATUS              SMALLINT 

, TC_PURCHASE_ORDERS_ID               STRING 

, TC_PURCHASE_ORDERS_ID_U             STRING 

, PARENT_PURCHASE_ORDERS_ID           BIGINT 

, MO_AUTO_CREATE_METHOD               TINYINT 

, O_FACILITY_ID                       INT 

, O_FACILITY_ALIAS_ID                 STRING 

, O_DOCK_ID                           STRING 

, O_NAME                              STRING 

, O_ADDRESS_1                         STRING 

, O_ADDRESS_2                         STRING 

, O_ADDRESS_3                         STRING 

, O_CITY                              STRING 

, O_STATE_PROV                        STRING 

, O_POSTAL_CODE                       STRING 

, O_COUNTY                            STRING 

, O_COUNTRY_CODE                      STRING 

, D_FACILITY_ID                       INT 

, D_FACILITY_ALIAS_ID                 STRING 

, D_DOCK_ID                           STRING 

, D_NAME                              STRING 

, D_LAST_NAME                         STRING 

, D_ADDRESS_1                         STRING 

, D_ADDRESS_2                         STRING 

, D_ADDRESS_3                         STRING 

, D_CITY                              STRING 

, D_STATE_PROV                        STRING 

, D_POSTAL_CODE                       STRING 

, D_COUNTY                            STRING 

, D_COUNTRY_CODE                      STRING 

, D_PHONE_NUMBER                      STRING 

, D_FAX_NUMBER                        STRING 

, D_EMAIL                             STRING 

, BILL_FACILITY_ID                    INT 

, BILL_FACILITY_ALIAS_ID              STRING 

, BILL_TO_WHSE                        STRING 

, BILL_TO_NAME                        STRING 

, BILL_TO_LAST_NAME                   STRING 

, BILL_TO_ADDRESS_1                   STRING 

, BILL_TO_ADDRESS_2                   STRING 

, BILL_TO_ADDRESS_3                   STRING 

, BILL_TO_CITY                        STRING 

, BILL_TO_STATE_PROV                  STRING 

, BILL_TO_COUNTY                      STRING 

, BILL_TO_POSTAL_CODE                 STRING 

, BILL_TO_COUNTRY_CODE                STRING 

, BILL_TO_PHONE_NUMBER                STRING 

, BILL_TO_FAX_NUMBER                  STRING 

, BILL_TO_EMAIL                       STRING 

, BILL_ACCOUNT_NBR                    STRING 

, ORG_SHIP_VIA_FACILITY_ALIAS_ID      STRING 

, ORG_SHIP_VIA_FACILITY_ID            INT 

, DES_SHIP_VIA_FACILITY_ALIAS_ID      STRING 

, DES_SHIP_VIA_FACILITY_ID            INT 

, PICKUP_TZ                           SMALLINT 

, DELIVERY_TZ                         SMALLINT 

, PICKUP_START_DTTM                   TIMESTAMP 

, PICKUP_END_DTTM                     TIMESTAMP 

, DELIVERY_START_DTTM                 TIMESTAMP 

, DELIVERY_END_DTTM                   TIMESTAMP 

, REQUESTED_DLVR_DTTM                 TIMESTAMP 

, MUST_DLVR_DTTM                      TIMESTAMP 

, PROMISED_DLVR_DTTM                  TIMESTAMP 

, DUE_DT                              TIMESTAMP 

, CANCEL_DT                           TIMESTAMP 

, PURCHASE_ORDERS_DATE_DTTM           TIMESTAMP 

, RELEASE_DTTM                        TIMESTAMP 

, FIRST_RCPT_DTTM                     TIMESTAMP 

, LAST_RCPT_DTTM                      TIMESTAMP 

, BILLING_METHOD                      TINYINT 

, PAYMENT_MODE                        SMALLINT 

, MV_CURRENCY_CODE                    STRING 

, MONETARY_VALUE                      DECIMAL(19,4) 

, CURRENCY_CODE                       STRING 

, ORIG_BUDG_COST                      DECIMAL(13,4) 

, BUDG_COST_CURRENCY_CODE             STRING 

, BUDG_COST                           DECIMAL(13,4) 

, TAX_CHARGES                         DECIMAL(13,4) 

, MISC_CHARGES                        DECIMAL(13,4) 

, INCOTERM_FACILITY_ID                INT 

, INCOTERM_FACILITY_ALIAS_ID          STRING 

, INCOTERM_LOC_AVA_DTTM               TIMESTAMP 

, INCOTERM_LOC_AVA_TIME_ZONE_ID       SMALLINT 

, IS_CANCELLED                        TINYINT 

, IS_IMPORTED                         TINYINT 

, UPDATE_SENT                         TINYINT 

, IS_HAZMAT                           TINYINT 

, IS_PERISHABLE                       TINYINT 

, IS_INFO_ONLY                        TINYINT 

, HAS_IMPORT_ERROR                    TINYINT 

, HAS_SOFT_CHECK_ERRORS               TINYINT 

, HAS_NOTES                           TINYINT 

, HAS_ALERTS                          TINYINT 

, IS_PURCHASE_ORDERS_RECONCILED       TINYINT 

, DELIVERY_REQ                        STRING 

, IS_ACCEPTANCE_REQUIRED              TINYINT 

, ARE_LEGAL_TERMS_REQUIRED            TINYINT 

, IS_READY_TO_SHIP                    TINYINT 

, IS_CUSTOMER_PICKUP                  TINYINT 

, IS_DIRECT_ALLOWED                   TINYINT 

, IS_PARTIALLY_PLANNED                TINYINT 

, IS_PL_MANUALLY_SET                  TINYINT 

, IS_LOCKED                           TINYINT 

, IS_PURCHASE_ORDERS_CONFIRMED        TINYINT 

, HAS_ROUTING_REQUEST                 TINYINT 

, HAS_EM_NOTIFY_FLAG                  TINYINT 

, IS_PUTAWAY                          TINYINT 

, ALLOW_PARTIAL_SHIPPING              TINYINT 

, ALLOW_ZONE_SKIPPING                 TINYINT 

, SNGL_UNIT_FLAG                      TINYINT 

, PRE_PACK_FLAG                       TINYINT 

, PNH_FLAG                            TINYINT 

, ON_HOLD                             TINYINT 

, QUAL_CHK_HOLD_UPON_RCPT             STRING 

, UN_NUMBER_ID                        BIGINT 

, BILL_OF_LADING_NUMBER               STRING 

, TRANS_RESP_CODE                     STRING 

, PRIORITY_TYPE                       TINYINT 

, PURCHASE_ORDERS_WEIGHT              DECIMAL(13,4) 

, PRO_NUMBER                          STRING 

, PRIORITY                            SMALLINT 

, EVENT_IND_TYPEID                    BIGINT 

, COMMODITY_CODE_ID                   BIGINT 

, CARRIER_ID                          BIGINT 

, PRODUCT_CLASS_ID                    INT 

, PROTECTION_LEVEL_ID                 INT 

, DSG_CARRIER_ID                      BIGINT 

, DSG_MOT_ID                          BIGINT 

, DSG_SERVICE_LEVEL_ID                BIGINT 

, DECLINE_REASON_ID                   INT 

, CHANNEL_TYPE                        SMALLINT 

, PRIMARY_CONTACT_NUMBER              STRING 

, MERCHANDIZING_DEPARTMENT_ID         BIGINT 

, BUYER_CODE                          STRING 

, REP_NAME                            STRING 

, REGION_ID                           INT 

, INBOUND_REGION_ID                   INT 

, OUTBOUND_REGION_ID                  INT 

, PLAN_SHPMT_NBR                      STRING 

, MISC_INSTR_CODE_1                   STRING 

, MISC_INSTR_CODE_2                   STRING 

, ASN_SHPMT_TYPE                      STRING 

, PROD_SCHED_REF_NUMBER               STRING 

, DELAY_TYPE                          SMALLINT 

, CUSTOMER_CODE                       STRING 

, CUSTOMER_FIRSTNAME                  STRING 

, CUSTOMER_LASTNAME                   STRING 

, CUSTOMER_ID                         BIGINT 

, EST_OUTBD_LPN                       BIGINT 

, EST_PALLET_BRIDGED                  BIGINT 

, MAJOR_PKT_GRP_ATTR                  STRING 

, MARK_FOR                            STRING 

, ONHOLD_EVENT_ID                     BIGINT 

, UNHOLD_EVENT_ID                     BIGINT 

, OUTBD_LPN_EPC_TYPE                  TINYINT 

, PACK_SLIP_PRT_CNT                   TINYINT 

, PKT_CONSOL_PROF                     STRING 

, PKT_PROFILE_ID                      STRING 

, PLT_CONTNT_LABEL_TYPE               STRING 

, PLT_CUBNG_INDIC                     TINYINT 

, PRE_STIKR_CODE                      STRING 

, STORE_NBR                           STRING 

, STORE_TYPE                          STRING 

, SWC_NBR_SEQ                         INT 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, BATCH_ID                            BIGINT 

, DROPOFF_PICKUP                      STRING 

, IS_CLOSED                           TINYINT 

, MFG_PLNT                            STRING 

, ORG_SHIP_FACILITY_ID                INT 

, OUTBD_LPN_ASN_REQD                  TINYINT 

, OUTBD_LPN_CUBNG_INDIC               TINYINT 

, OUTBD_LPN_LABEL_TYPE                STRING 

, PACKAGING                           STRING 

, RTN_MISC_NBR                        STRING 

, COMMIMTED_DLVR_DTTM                 TIMESTAMP 

, GRAND_TOTAL                         DECIMAL(13,4) 

, DSG_CARRIER_CODE                    STRING 

, TOTAL_DLRS_DISC                     DECIMAL(19,4) 

, FRT_CLASS                           STRING 

, PLAN_ID                             STRING 

, WORK_ORD_NBR                        STRING 

, MANUFACTURING_ORDER_NBR             STRING 

, DC_ORDER_NUMBER                     STRING 

, CUT_NBR                             STRING 

, EXT_PURCHASE_ORDERS_ID              STRING 

, HIBERNATE_VERSION                   BIGINT 

, ORDER_CATEGORY                      INT 

, DELIVERY_CHANNEL_ID                 SMALLINT 

, D_CONTACT_NAME                      STRING 

, BILL_TO_CONTACT_NAME                STRING 

, CHANNEL_ID                          BIGINT 

, DEPARTMENT_ID                       INT 

, DOM_STORE_TYPE                      BIGINT 

, FULFILL_MODE                        SMALLINT 

, CONS_RUN_ID                         BIGINT 

, LAST_RUN_ID                         BIGINT 

, ACCEPTED_DTTM                       TIMESTAMP 

, IS_DO_CREATED                       TINYINT 

, WEIGHT_UOM_ID_BASE                  INT 

, ENTERED_BY                          STRING 

, ENTRY_TYPE                          STRING 

, ENTRY_CODE                          STRING 

, CUSTOMER_USER_ID                    STRING 

, CUSTOMER_EMAIL                      STRING 

, CUSTOMER_PHONE                      STRING 

, CUSTOMER_TYPE                       STRING 

, PAYMENT_STATUS                      SMALLINT 

, REASON_ID                           INT 

, ADDR_VALID                          STRING 

, ORG_APPT_EXISTS                     TINYINT 

, DEST_APPT_EXISTS                    TINYINT 

, MINIMIZE_SHIPMENTS                  SMALLINT 

, ORDER_RECEIVED                      TINYINT 

, REF_FIELD_1                         STRING 

, REF_FIELD_2                         STRING 

, REF_FIELD_3                         STRING 

, CONTNT_LABEL_TYPE                   STRING 

, NBR_OF_CONTNT_LABEL                 INT 

, NBR_OF_LABEL                        INT 

, NBR_OF_PAKNG_SLIPS                  INT 

, PALLET_CONTENT_LABEL_TYPE           STRING 

, LPN_LABEL_TYPE                      STRING 

, PACK_SLIP_TYPE                      STRING 

, BOL_TYPE                            STRING 

, MANIF_TYPE                          STRING 

, PRINT_CANADIAN_CUST_INVC_FLAG       STRING 

, PRINT_COO                           STRING 

, PRINT_DOCK_RCPT_FLAG                STRING 

, PRINT_INV                           STRING 

, PRINT_NAFTA_COO_FLAG                STRING 

, PRINT_OCEAN_BOL_FLAG                STRING 

, PRINT_SED                           STRING 

, PRINT_SHPR_LTR_OF_INSTR_FLAG        STRING 

, PRINT_PKG_LIST_FLAG                 STRING 

, FREIGHT_REVENUE_CURRENCY_CODE       STRING 

, FREIGHT_REVENUE                     DECIMAL(13,4) 

, TRANS_PLAN_DIRECTION                STRING 

, DSG_VOYAGE_FLIGHT                   STRING 

, REASON_CODES_GROUP_ID               BIGINT 

, COMPLETED_EXTERNALLY                TINYINT 

, INCOTERM_ID                         INT 

, DIRECTION                           STRING 

, ENTRY_CODE_FACILITY_ID              SMALLINT 

, RETURN_REFERENCE_NUMBER             STRING 

, ORDER_CONFIRMATION_DATE             TIMESTAMP 

, RMA_STATUS                          SMALLINT 

, O_PHONE_NUMBER                      STRING 

, O_EMAIL                             STRING 

, O_FAX_NUMBER                        STRING 

, TAX_ID                              STRING 

, SHIPPING_CHANNEL                    STRING 

, EPI_SERVICE_GROUP                   STRING 

, IMPORTER_OF_RECORD_NBR              STRING 

, B13A_EXPORT_DECL_NBR                STRING 

, RETURN_ADDR_CODE                    STRING 

, STATIC_ROUTE_DELIVERY_TYPE          STRING 

, SHIP_GROUP_ID                       STRING 

, AUTO_APPOINTMENT                    TINYINT 

, SITE_ID                             BIGINT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_purchase_orders_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (PURCHASE_ORDERS_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_PUTAWAY_LOCK_PRE" , ***** Creating table: "WM_PUTAWAY_LOCK_PRE"


CREATE TABLE  WM_PUTAWAY_LOCK_PRE
( DC_NBR                              SMALLINT                not null

, LOCN_ID                             STRING       not null

, LOCK_COUNTER                        SMALLINT 

, CREATED_DTTM                        TIMESTAMP 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_putaway_lock_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_RACK_TYPE_LEVEL_PRE" , ***** Creating table: "WM_RACK_TYPE_LEVEL_PRE"


CREATE TABLE  WM_RACK_TYPE_LEVEL_PRE
( DC_NBR                              SMALLINT                not null

, RACK_LEVEL_ID                       BIGINT               not null

, RACK_TYPE                           BIGINT 

, LEVEL_NUMBER                        BIGINT 

, LEVEL_WT_LIMIT                      DECIMAL(13,4) 

, LEVEL_HT                            DECIMAL(13,4) 

, LEVEL_WIDTH                         DECIMAL(13,4) 

, LEVEL_DEPTH                         DECIMAL(13,4) 

, NBR_OF_SLOTS                        BIGINT 

, SLOT_WT_LIMIT                       DECIMAL(13,4) 

, MAX_LANE_WT                         DECIMAL(13,4) 

, HT_CLEARANCE                        DECIMAL(13,4) 

, SIDE_CLEAR                          DECIMAL(13,4) 

, MAX_STACKING                        BIGINT 

, MAX_LANES                           BIGINT 

, GUIDE_WIDTH                         DECIMAL(13,4) 

, LANE_GUIDES                         TINYINT 

, SLOT_GUIDES                         TINYINT 

, MOVE_GUIDES                         TINYINT 

, USE_FULL_WID                        TINYINT 

, SHELF_THICKNESS                     DECIMAL(9,4) 

, LABEL_POS                           DECIMAL(9,4) 

, MAX_PICK_HT                         DECIMAL(9,4) 

, SLOT_UNITS                          BIGINT 

, SHIP_UNITS                          BIGINT 

, MAX_SL_WID                          DECIMAL(13,4) 

, ONE_FACING_PER_DSW                  TINYINT 

, LEVEL_PRIORITY                      BIGINT 

, COST_TO_PICK                        DECIMAL(13,4) 

, RESERVED_1                          STRING 

, RESERVED_2                          STRING 

, RESERVED_3                          BIGINT 

, RESERVED_4                          BIGINT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, MOD_USER                            STRING 

, USE_INCREMENT                       TINYINT 

, WIDTH_INCREMENT                     DECIMAL(13,4) 

, MIN_SL_WIDTH                        DECIMAL(13,4) 

, MIN_ITEM_WIDTH                      DECIMAL(13,4) 

, MAX_OVERHANG_PCT                    BIGINT 

, LEV_MAP_STR                         STRING 

, POS_MAP_STR                         STRING 

, USE_LEV_MAP_STR                     TINYINT 

, USE_POS_MAP_STR                     TINYINT 

, USE_VERT_DIV                        TINYINT 

, VERT_DIV_HT                         DECIMAL(13,4) 

, MIN_CHANNEL_WID                     DECIMAL(13,4) 

, MAX_CHANNEL_WID                     DECIMAL(13,4) 

, MAX_CHANNEL_WT                      DECIMAL(13,4) 

, MAX_ADJ_SLOTS                       BIGINT 

, MAX_EJECTOR_WT                      DECIMAL(13,4) 

, AFRAME_ROTATE                       TINYINT 

, AFRAME_MIN_HT                       DECIMAL(13,4) 

, AFRAME_MAX_HT                       DECIMAL(13,4) 

, AFRAME_MIN_LEN                      DECIMAL(13,4) 

, AFRAME_MAX_LEN                      DECIMAL(13,4) 

, AFRAME_MIN_WID                      DECIMAL(13,4) 

, AFRAME_MAX_WID                      DECIMAL(13,4) 

, AFRAME_MIN_WT                       DECIMAL(13,4) 

, AFRAME_MAX_WT                       DECIMAL(13,4) 

, REACH_DIST                          DECIMAL(13,4) 

, MOVE_GUIDES_RGT                     TINYINT 

, MOVE_GUIDES_LFT                     TINYINT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_rack_type_level_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_RACK_TYPE_PRE" , ***** Creating table: "WM_RACK_TYPE_PRE"


CREATE TABLE  WM_RACK_TYPE_PRE
( DC_NBR                              SMALLINT                not null

, RACK_TYPE                           BIGINT               not null

, WHSE_CODE                           STRING 

, RT_NAME                             STRING 

, RACK_TYPE_DESC                      STRING 

, SEQUENCE                            BIGINT 

, WT_LIMIT                            DECIMAL(13,4) 

, HT                                  DECIMAL(13,4) 

, WIDTH                               DECIMAL(13,4) 

, DEPTH                               DECIMAL(13,4) 

, RACK_CLASS                          BIGINT 

, FIX_LABELS                          TINYINT 

, LABEL_WID                           DECIMAL(13,4) 

, OVERLAP_DIST                        DECIMAL(13,4) 

, MOVABLE_LABEL                       TINYINT 

, HN_CONSIDER                         TINYINT 

, CASE_HT_MAX                         DECIMAL(13,4) 

, CASE_LEN_MAX                        DECIMAL(13,4) 

, CASE_WID_MAX                        DECIMAL(13,4) 

, CASE_WT_MAX                         DECIMAL(13,4) 

, CASE_CUBE_MAX                       DECIMAL(13,4) 

, INNER_HT_MAX                        DECIMAL(13,4) 

, INNER_LEN_MAX                       DECIMAL(13,4) 

, INNER_WID_MAX                       DECIMAL(13,4) 

, INNER_WT_MAX                        DECIMAL(13,4) 

, INNER_CUBE_MAX                      DECIMAL(13,4) 

, EACH_HT_MAX                         DECIMAL(13,4) 

, EACH_LEN_MAX                        DECIMAL(13,4) 

, EACH_WID_MAX                        DECIMAL(13,4) 

, EACH_WT_MAX                         DECIMAL(13,4) 

, EACH_CUBE_MAX                       DECIMAL(13,4) 

, PALLET_INVEN_MAX                    DECIMAL(13,4) 

, PALLET_INVEN_MIN                    DECIMAL(13,4) 

, CASE_INVEN_MAX                      DECIMAL(13,4) 

, CASE_INVEN_MIN                      DECIMAL(13,4) 

, CASE_MOVE_MAX                       DECIMAL(13,4) 

, CASE_MOVE_MIN                       DECIMAL(13,4) 

, CUBE_MOVE_MAX                       DECIMAL(13,4) 

, CUBE_MOVE_MIN                       DECIMAL(13,4) 

, CASE_HITS_MAX                       DECIMAL(13,4) 

, CASE_HITS_MIN                       DECIMAL(13,4) 

, CUBE_HITS_MAX                       DECIMAL(13,4) 

, CUBE_HITS_MIN                       DECIMAL(13,4) 

, CASE_HT_MIN                         DECIMAL(13,4) 

, CASE_LEN_MIN                        DECIMAL(13,4) 

, CASE_WID_MIN                        DECIMAL(13,4) 

, CASE_WT_MIN                         DECIMAL(13,4) 

, CASE_CUBE_MIN                       DECIMAL(13,4) 

, INNER_HT_MIN                        DECIMAL(13,4) 

, INNER_LEN_MIN                       DECIMAL(13,4) 

, INNER_WID_MIN                       DECIMAL(13,4) 

, INNER_WT_MIN                        DECIMAL(13,4) 

, INNER_CUBE_MIN                      DECIMAL(13,4) 

, EACH_HT_MIN                         DECIMAL(13,4) 

, EACH_LEN_MIN                        DECIMAL(13,4) 

, EACH_WID_MIN                        DECIMAL(13,4) 

, EACH_WT_MIN                         DECIMAL(13,4) 

, EACH_CUBE_MIN                       DECIMAL(13,4) 

, CUBE_INVEN_MIN                      DECIMAL(13,4) 

, CUBE_INVEN_MAX                      DECIMAL(13,4) 

, WEEK_IN_SLOT                        DECIMAL(13,4) 

, REF_LEVEL_ID                        BIGINT 

, VISC_MIN                            DECIMAL(13,4) 

, VISC_MAX                            DECIMAL(13,4) 

, FILL_PERCENT_MIN                    DECIMAL(5,2) 

, FILL_PERCENT_MAX                    DECIMAL(5,2) 

, UPRIGHT_THICKNESS                   DECIMAL(9,4) 

, RESERVED_1                          STRING 

, RESERVED_2                          STRING 

, RESERVED_3                          BIGINT 

, RESERVED_4                          BIGINT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, MOD_USER                            STRING 

, FUNCTION_TYPE                       SMALLINT 

, OVERSTOCK_CONSIDER                  TINYINT 

, USE_3D_SLOT                         TINYINT 

, WEEKS_IN_SLOT_CONSTRAINT            TINYINT 

, NUM_OF_BAYS_AVAIL                   DECIMAL(13,4) 

, PARENT_BT                           BIGINT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_rack_type_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_RESV_LOCN_HDR_PRE" , ***** Creating table: "WM_RESV_LOCN_HDR_PRE"


CREATE TABLE  WM_RESV_LOCN_HDR_PRE
( DC_NBR                              SMALLINT                not null

, RESV_LOCN_HDR_ID                    INT                not null

, LOCN_ID                             STRING 

, LOCN_SIZE_TYPE                      STRING 

, LOCN_PUTAWAY_LOCK                   STRING 

, INVN_LOCK_CODE                      STRING 

, CURR_WT                             DECIMAL(13,4) 

, DIRCT_WT                            DECIMAL(13,4) 

, MAX_WT                              DECIMAL(13,4) 

, CURR_VOL                            DECIMAL(13,4) 

, DIRCT_VOL                           DECIMAL(13,4) 

, MAX_VOL                             DECIMAL(13,4) 

, CURR_UOM_QTY                        DECIMAL(9,2) 

, DIRCT_UOM_QTY                       DECIMAL(9,2) 

, MAX_UOM_QTY                         DECIMAL(15,5) 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, DEDCTN_BATCH_NBR                    STRING 

, DEDCTN_PACK_QTY                     DECIMAL(9,2) 

, PACK_ZONE                           STRING 

, SORT_LOCN_FLAG                      SMALLINT 

, INBD_STAGING_FLAG                   STRING 

, WM_VERSION_ID                       INT 

, DEDCTN_ITEM_ID                      BIGINT 

, LOCN_HDR_ID                         INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_resv_locn_hdr_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_SEC_USER_PRE" , ***** Creating table: "WM_SEC_USER_PRE"


CREATE TABLE  WM_SEC_USER_PRE
( DC_NBR                              SMALLINT                not null

, SEC_USER_ID                         INT                not null

, LOGIN_USER_ID                       STRING 

, USER_NAME                           STRING 

, USER_DESC                           STRING 

, PSWD                                STRING 

, PSWD_EXP_DATE                       TIMESTAMP 

, PSWD_CHANGE_AT_LOGIN                STRING 

, CAN_CHNG_PSWD                       STRING 

, DISABLED                            STRING 

, LOCKED_OUT                          STRING 

, LAST_LOGIN                          TIMESTAMP 

, GRACE_LOGINS                        INT 

, LOCKED_OUT_EXPIRATION               TIMESTAMP 

, FAILED_LOGIN_ATTEMPTS               INT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, SEC_POLICY_SET_ID                   INT 

, WM_VERSION_ID                       INT 

, LOAD_TSTMP                          TIMESTAMP                   

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_sec_user_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_SHIPMENT_PRE" , ***** Creating table: "WM_SHIPMENT_PRE"


CREATE TABLE  WM_SHIPMENT_PRE
( DC_NBR                              SMALLINT                not null

, SHIPMENT_ID                         BIGINT               not null

, TC_COMPANY_ID                       INT 

, RS_AREA_ID                          INT 

, TC_SHIPMENT_ID                      STRING 

, PP_SHIPMENT_ID                      BIGINT 

, SHIPMENT_STATUS                     SMALLINT 

, UPDATE_SENT                         TINYINT 

, BUSINESS_PROCESS                    STRING 

, CREATION_TYPE                       TINYINT 

, CONS_RUN_ID                         BIGINT 

, IS_CANCELLED                        TINYINT 

, RS_TYPE                             STRING 

, RS_CONFIG_ID                        INT 

, RS_CONFIG_CYCLE_ID                  INT 

, CONFIG_CYCLE_SEQ                    SMALLINT 

, CYCLE_DEADLINE_DTTM                 TIMESTAMP 

, CYCLE_EXECUTION_DTTM                TIMESTAMP 

, LAST_RS_NOTIFICATION_DTTM           TIMESTAMP 

, CFMF_STATUS                         TINYINT 

, AVAILABLE_DTTM                      TIMESTAMP 

, CMID                                BIGINT 

, CM_DISCOUNT                         DECIMAL(13,3) 

, O_FACILITY_NUMBER                   INT 

, O_FACILITY_ID                       STRING 

, O_ADDRESS                           STRING 

, O_CITY                              STRING 

, O_STATE_PROV                        STRING 

, O_POSTAL_CODE                       STRING 

, O_COUNTY                            STRING 

, O_COUNTRY_CODE                      STRING 

, D_FACILITY_NUMBER                   INT 

, D_FACILITY_ID                       STRING 

, D_ADDRESS                           STRING 

, D_CITY                              STRING 

, D_STATE_PROV                        STRING 

, D_POSTAL_CODE                       STRING 

, D_COUNTY                            STRING 

, D_COUNTRY_CODE                      STRING 

, DISTANCE                            DECIMAL(9,2) 

, DIRECT_DISTANCE                     DECIMAL(9,2) 

, OUT_OF_ROUTE_DISTANCE               DECIMAL(9,2) 

, DISTANCE_UOM                        STRING 

, BUSINESS_PARTNER_ID                 STRING 

, COMMODITY_CLASS                     INT 

, IS_HAZMAT                           TINYINT 

, IS_PERISHABLE                       TINYINT 

, BILLING_METHOD                      TINYINT 

, DSG_CARRIER_CODE                    STRING 

, REC_CMID                            BIGINT 

, REC_CM_SHIPMENT_ID                  BIGINT 

, ASSIGNED_CARRIER_CODE               STRING 

, SHIPMENT_REF_ID                     STRING 

, BILL_TO_CODE                        STRING 

, BILL_TO_TITLE                       STRING 

, BILL_TO_NAME                        STRING 

, BILL_TO_ADDRESS                     STRING 

, BILL_TO_CITY                        STRING 

, BILL_TO_STATE_PROV                  STRING 

, BILL_TO_POSTAL_CODE                 STRING 

, BILL_TO_COUNTRY_CODE                STRING 

, BILL_TO_PHONE_NUMBER                STRING 

, PURCHASE_ORDER                      STRING 

, PRO_NUMBER                          STRING 

, TRANS_RESP_CODE                     STRING 

, BASELINE_COST                       DECIMAL(13,2) 

, ESTIMATED_COST                      DECIMAL(13,2) 

, LINEHAUL_COST                       DECIMAL(13,2) 

, ACCESSORIAL_COST                    DECIMAL(13,2) 

, STOP_COST                           DECIMAL(13,2) 

, ESTIMATED_SAVINGS                   DECIMAL(13,2) 

, SPOT_CHARGE                         DECIMAL(13,2) 

, CURRENCY_CODE                       STRING 

, PICKUP_START_DTTM                   TIMESTAMP 

, PICKUP_END_DTTM                     TIMESTAMP 

, DELIVERY_START_DTTM                 TIMESTAMP 

, DELIVERY_END_DTTM                   TIMESTAMP 

, NUM_STOPS                           SMALLINT 

, NUM_DOCKS                           SMALLINT 

, ON_TIME_INDICATOR                   TINYINT 

, HAS_NOTES                           SMALLINT 

, HAS_ALERTS                          TINYINT 

, HAS_IMPORT_ERROR                    TINYINT 

, HAS_SOFT_CHECK_ERROR                TINYINT 

, HAS_TRACKING_MSG                    TINYINT 

, TRACKING_MSG_PROBLEM                TINYINT 

, REC_LINEHAUL_COST                   DECIMAL(13,2) 

, REC_ACCESSORIAL_COST                DECIMAL(13,2) 

, REC_TOTAL_COST                      DECIMAL(13,2) 

, CYCLE_RESP_DEADLINE_TZ              SMALLINT 

, TENDER_RESP_DEADLINE_TZ             SMALLINT 

, PICKUP_TZ                           SMALLINT 

, DELIVERY_TZ                         SMALLINT 

, ASSIGNED_LANE_ID                    BIGINT 

, ASSIGNED_LANE_DETAIL_ID             BIGINT 

, REC_SPOT_CHARGE                     DECIMAL(13,2) 

, REC_LANE_ID                         BIGINT 

, REC_LANE_DETAIL_ID                  BIGINT 

, TENDER_DTTM                         TIMESTAMP 

, O_STOP_LOCATION_NAME                STRING 

, D_STOP_LOCATION_NAME                STRING 

, ASSIGNED_CM_SHIPMENT_ID             BIGINT 

, REC_CM_DISCOUNT                     DECIMAL(13,2) 

, REC_STOP_COST                       DECIMAL(13,2) 

, LAST_SELECTOR_RUN_DTTM              TIMESTAMP 

, LAST_CM_OPTION_GEN_DTTM             TIMESTAMP 

, IS_CM_OPTION_GEN_ACTIVE             TINYINT 

, RS_CYCLE_REMAINING                  TINYINT 

, IS_TIME_FEAS_ENABLED                TINYINT 

, EXT_SYS_SHIPMENT_ID                 STRING 

, EXTRACTION_DTTM                     TIMESTAMP 

, ASSIGNED_BROKER_CARRIER_CODE        STRING 

, USE_BROKER_AS_CARRIER               TINYINT 

, BROKER_REF                          STRING 

, RATING_QUALIFIER                    TINYINT 

, STATUS_CHANGE_DTTM                  TIMESTAMP 

, BUDG_TOTAL_COST                     DECIMAL(13,2) 

, NORMALIZED_MARGIN                   DECIMAL(13,2) 

, NORMALIZED_TOTAL_COST               DECIMAL(13,2) 

, BUDG_NORMALIZED_TOTAL_COST          DECIMAL(13,2) 

, REC_BUDG_LINEHAUL_COST              DECIMAL(13,2) 

, REC_BUDG_STOP_COST                  DECIMAL(13,2) 

, REC_BUDG_CM_DISCOUNT                DECIMAL(13,2) 

, REC_BUDG_ACCESSORIAL_COST           DECIMAL(13,2) 

, REC_BUDG_TOTAL_COST                 DECIMAL(13,2) 

, REC_MARGIN                          DECIMAL(13,2) 

, REC_NORMALIZED_MARGIN               DECIMAL(13,2) 

, REC_NORMALIZED_TOTAL_COST           DECIMAL(13,2) 

, REC_BUDG_NORMALIZED_TOTAL_COST      DECIMAL(13,2) 

, BUDG_CURRENCY_CODE                  STRING 

, SPOT_CHARGE_CURRENCY_CODE           STRING 

, REC_CURRENCY_CODE                   STRING 

, REC_BUDG_CURRENCY_CODE              STRING 

, REC_SPOT_CHARGE_CURRENCY_CODE       STRING 

, RATING_LANE_ID                      BIGINT 

, FRT_REV_RATING_LANE_ID              BIGINT 

, REC_RATING_LANE_ID                  BIGINT 

, REC_BUDG_RATING_LANE_ID             BIGINT 

, RATING_LANE_DETAIL_ID               BIGINT 

, FRT_REV_RATING_LANE_DETAIL_ID       BIGINT 

, REC_RATING_LANE_DETAIL_ID           BIGINT 

, REC_BUDG_RATING_LANE_DETAIL_ID      BIGINT 

, BUDG_CM_DISCOUNT                    DECIMAL(13,2) 

, NORMALIZED_BASELINE_COST            DECIMAL(13,2) 

, BASELINE_COST_CURRENCY_CODE         STRING 

, REPORTED_COST                       DECIMAL(13,2) 

, TRACTOR_NUMBER                      STRING 

, IS_AUTO_DELIVERED                   TINYINT 

, SHIPMENT_TYPE                       STRING 

, ORIG_BUDG_TOTAL_COST                DECIMAL(13,2) 

, ACCESSORIAL_COST_TO_CARRIER         DECIMAL(13,2) 

, CARRIER_CHARGE                      DECIMAL(13,2) 

, ACTUAL_COST                         DECIMAL(13,2) 

, EARNED_INCOME                       DECIMAL(13,2) 

, SENT_TO_PKMS                        TINYINT 

, IS_SHIPMENT_RECONCILED              TINYINT 

, DELIVERY_REQ                        STRING 

, DROPOFF_PICKUP                      STRING 

, PACKAGING                           STRING 

, SENT_TO_PKMS_DTTM                   TIMESTAMP 

, WMS_STATUS_CODE                     SMALLINT 

, SPOT_CHARGE_AND_PAYEE_ACC           DECIMAL(13,2) 

, SPOT_CHARGE_AND_PAYEE_ACC_CC        STRING 

, NORM_SPOT_CHARGE_AND_PAYEE_ACC      DECIMAL(13,2) 

, FACILITY_SCHEDULE_ID                INT 

, EARNED_INCOME_CURRENCY_CODE         STRING 

, ACTUAL_COST_CURRENCY_CODE           STRING 

, SHIPMENT_RECON_DTTM                 TIMESTAMP 

, PLN_RATING_LANE_ID                  BIGINT 

, PLN_RATING_LANE_DETAIL_ID           BIGINT 

, PLN_TOTAL_COST                      DECIMAL(13,2) 

, PLN_LINEHAUL_COST                   DECIMAL(13,2) 

, PLN_TOTAL_ACCESSORIAL_COST          DECIMAL(13,2) 

, PLN_STOP_OFF_COST                   DECIMAL(13,2) 

, PLN_NORMALIZED_TOTAL_COST           DECIMAL(13,2) 

, PLN_CURRENCY_CODE                   STRING 

, PLN_ACCESSORL_COST_TO_CARRIER       DECIMAL(13,2) 

, PLN_CARRIER_CHARGE                  DECIMAL(13,2) 

, WAYPOINT_TOTAL_COST                 DECIMAL(10,2) 

, WAYPOINT_HANDLING_COST              DECIMAL(10,2) 

, GRS_OPERATION                       TINYINT 

, IS_GRS_OPT_CYCLE_RUNNING            TINYINT 

, IS_MANUAL_ASSIGN                    TINYINT 

, LAST_RUN_GRS_DTTM                   TIMESTAMP 

, GRS_MAX_SHIPMENT_STATUS             SMALLINT 

, PRIORITY_TYPE                       TINYINT 

, MV_CURRENCY_CODE                    STRING 

, PROD_SCHED_REF_NUMBER               STRING 

, COMMODITY_CODE_ID                   BIGINT 

, UN_NUMBER_ID                        BIGINT 

, CONTRACT_NUMBER                     STRING 

, BOOKING_REF_SHIPPER                 STRING 

, BOOKING_REF_CARRIER                 STRING 

, BK_RESOURCE_REF_EXTERNAL            STRING 

, BK_RESOURCE_NAME_EXTERNAL           STRING 

, BK_O_FACILITY_ID                    INT 

, BK_D_FACILITY_ID                    INT 

, BK_MASTER_AIRWAY_BILL               STRING 

, BK_FORWARDER_AIRWAY_BILL            STRING 

, ASSIGNED_SCNDR_CARRIER_CODE         STRING 

, CUSTOMER_CREDIT_LIMIT_ID            BIGINT 

, SHIPMENT_LEG_TYPE                   BIGINT 

, SHIPMENT_CLOSED_INDICATOR           SMALLINT 

, OCEAN_ROUTING_STAGE                 TINYINT 

, BK_DEPARTURE_DTTM                   TIMESTAMP 

, BK_DEPARTURE_TZ                     SMALLINT 

, BK_ARRIVAL_DTTM                     TIMESTAMP 

, BK_ARRIVAL_TZ                       SMALLINT 

, BK_PICKUP_DTTM                      TIMESTAMP 

, BK_PICKUP_TZ                        SMALLINT 

, BK_CUTOFF_DTTM                      TIMESTAMP 

, BK_CUTOFF_TZ                        SMALLINT 

, NUM_CHARGE_LAYOVERS                 SMALLINT 

, RATE_TYPE                           STRING 

, RATE_UOM                            STRING 

, PICK_START_DTTM                     TIMESTAMP 

, PAPERWORK_START_DTTM                TIMESTAMP 

, VEHICLE_CHECK_START_DTTM            TIMESTAMP 

, SHIPMENT_START_DTTM                 TIMESTAMP 

, SHIPMENT_END_DTTM                   TIMESTAMP 

, FEASIBLE_DRIVER_TYPE                BIGINT 

, WAVE_ID                             BIGINT 

, ESTIMATED_DISPATCH_DTTM             TIMESTAMP 

, TOTAL_TIME                          INT 

, LOC_REFERENCE                       STRING 

, BK_O_FACILITY_ALIAS_ID              STRING 

, BK_D_FACILITY_ALIAS_ID              STRING 

, EQUIP_UTIL_PER                      DECIMAL(5,2) 

, MOVE_TYPE                           TINYINT 

, DRIVER_TYPE_ID                      BIGINT 

, EQUIPMENT_TYPE                      SMALLINT 

, RADIAL_DISTANCE                     DECIMAL(9,2) 

, RADIAL_DISTANCE_UOM                 STRING 

, DESIGNATED_TRACTOR_CODE             STRING 

, DESIGNATED_DRIVER_TYPE              BIGINT 

, IS_WAVE_MAN_CHANGED                 TINYINT 

, RETAIN_CONSOLIDATOR_TIMES           TINYINT 

, TARIFF                              DECIMAL(15,4) 

, MIN_RATE                            DECIMAL(15,4) 

, FIRST_UPDATE_SENT_TO_PKMS           TINYINT 

, EVENT_IND_TYPEID                    BIGINT 

, TANDEM_PATH_ID                      INT 

, O_TANDEM_FACILITY                   INT 

, D_TANDEM_FACILITY                   INT 

, O_TANDEM_FACILITY_ALIAS             STRING 

, D_TANDEM_FACILITY_ALIAS             STRING 

, DELAY_TYPE                          INT 

, ASSIGNED_CARRIER_ID                 BIGINT 

, DSG_CARRIER_ID                      BIGINT 

, REC_CARRIER_ID                      BIGINT 

, FEASIBLE_CARRIER_ID                 BIGINT 

, PAYEE_CARRIER_ID                    BIGINT 

, SCNDR_CARRIER_ID                    BIGINT 

, BROKER_CARRIER_ID                   BIGINT 

, ASSIGNED_SCNDR_CARRIER_ID           BIGINT 

, DSG_SCNDR_CARRIER_ID                BIGINT 

, ASSIGNED_BROKER_CARRIER_ID          BIGINT 

, LH_PAYEE_CARRIER_ID                 BIGINT 

, REC_BROKER_CARRIER_ID               BIGINT 

, ASSIGNED_EQUIPMENT_ID               BIGINT 

, DSG_EQUIPMENT_ID                    BIGINT 

, FEASIBLE_EQUIPMENT_ID               BIGINT 

, FEASIBLE_EQUIPMENT2_ID              INT 

, REC_EQUIPMENT_ID                    BIGINT 

, ASSIGNED_MOT_ID                     BIGINT 

, DSG_MOT_ID                          BIGINT 

, FEASIBLE_MOT_ID                     BIGINT 

, REC_MOT_ID                          BIGINT 

, PRODUCT_CLASS_ID                    INT 

, PROTECTION_LEVEL_ID                 INT 

, ASSIGNED_SERVICE_LEVEL_ID           BIGINT 

, DSG_SERVICE_LEVEL_ID                BIGINT 

, FEASIBLE_SERVICE_LEVEL_ID           BIGINT 

, REC_SERVICE_LEVEL_ID                BIGINT 

, HAS_EM_NOTIFY_FLAG                  TINYINT 

, REGION_ID                           INT 

, INBOUND_REGION_ID                   INT 

, OUTBOUND_REGION_ID                  INT 

, FINANCIAL_WT                        DECIMAL(13,2) 

, IS_BOOKING_REQUIRED                 TINYINT 

, TOTAL_COST_EXCL_TAX                 DECIMAL(13,2) 

, TOTAL_TAX_AMOUNT                    DECIMAL(13,2) 

, IS_ASSOCIATED_TO_OUTBOUND           TINYINT 

, RECEIVED_DTTM                       TIMESTAMP 

, BOOKING_ID                          BIGINT 

, IS_MISROUTED                        TINYINT 

, FEASIBLE_CARRIER_CODE               STRING 

, SHIPMENT_WIN_ADJ_FLAG               SMALLINT 

, DT_PARAM_SET_ID                     INT 

, MERCHANDIZING_DEPARTMENT_ID         BIGINT 

, STATIC_ROUTE_ID                     INT 

, IS_FILO                             TINYINT 

, IS_COOLER_AT_NOSE                   TINYINT 

, AUTH_NBR                            STRING 

, CONS_LOCN_ID                        STRING 

, RTE_TYPE                            STRING 

, TRLR_TYPE                           STRING 

, TRLR_SIZE                           STRING 

, CONS_ADDR_CODE                      STRING 

, HUB_ID                              BIGINT 

, TRLR_GEN_CODE                       STRING 

, MAX_NBR_OF_CTNS                     INT 

, APPT_DOOR_SCHED_TYPE                STRING 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, DSG_SCNDR_CARRIER_CODE              STRING 

, LH_PAYEE_CARRIER_CODE               STRING 

, REC_BROKER_CARRIER_CODE             STRING 

, REC_CARRIER_CODE                    STRING 

, LPN_ASSIGNMENT_STOPPED              STRING 

, SEAL_NUMBER                         STRING 

, HIBERNATE_VERSION                   BIGINT 

, SHIP_GROUP_ID                       STRING 

, RATE                                DECIMAL(16,4) 

, MONETARY_VALUE                      DECIMAL(16,4) 

, DOOR                                STRING 

, RTE_TYPE_1                          STRING 

, RTE_TYPE_2                          STRING 

, SCHEDULED_PICKUP_DTTM               TIMESTAMP 

, MANIFEST_ID                         INT 

, DAYS_TO_DELIVER                     TINYINT 

, SED_GENERATED_FLAG                  STRING 

, SERV_AREA_CODE                      STRING 

, TENDER_RESP_DEADLINE_DTTM           TIMESTAMP 

, PRINT_CONS_BOL                      STRING 

, LEFT_WT                             DECIMAL(13,4) 

, RIGHT_WT                            DECIMAL(13,4) 

, LANE_NAME                           STRING 

, DECLARED_VALUE                      DECIMAL(16,4) 

, DV_CURRENCY_CODE                    STRING 

, COD_AMOUNT                          DECIMAL(13,4) 

, CUSTOMER_ID                         BIGINT 

, CUST_FRGT_CHARGE                    DECIMAL(13,2) 

, COD_CURRENCY_CODE                   STRING 

, PLANNED_WEIGHT                      DECIMAL(13,4) 

, WEIGHT_UOM_ID_BASE                  INT 

, PLANNED_VOLUME                      DECIMAL(13,4) 

, VOLUME_UOM_ID_BASE                  INT 

, SIZE1_VALUE                         DECIMAL(13,4) 

, SIZE1_UOM_ID                        INT 

, SIZE2_VALUE                         DECIMAL(13,4) 

, SIZE2_UOM_ID                        INT 

, ASSIGNED_SHIP_VIA                   STRING 

, INSURANCE_STATUS                    INT 

, RTE_SWC_NBR                         STRING 

, RTE_TO                              STRING 

, BILL_OF_LADING_NUMBER               STRING 

, TRAILER_NUMBER                      STRING 

, TRANS_PLAN_OWNER                    TINYINT 

, TOTAL_REVENUE_CURRENCY_CODE         STRING 

, TOTAL_REVENUE                       DECIMAL(13,4) 

, FRT_REV_SPOT_CHARGE_CURR_CODE       STRING 

, NORMALIZED_TOTAL_REVENUE            DECIMAL(13,4) 

, FRT_REV_SPOT_CHARGE                 DECIMAL(13,4) 

, DSG_VOYAGE_FLIGHT                   STRING 

, FEASIBLE_VOYAGE_FLIGHT              STRING 

, CURRENCY_DTTM                       TIMESTAMP 

, FRT_REV_LINEHAUL_CHARGE             DECIMAL(13,4) 

, FRT_REV_STOP_CHARGE                 DECIMAL(13,4) 

, FRT_REV_ACCESSORIAL_CHARGE          DECIMAL(13,4) 

, MARGIN                              DECIMAL(13,4) 

, TOTAL_COST                          DECIMAL(15,4) 

, INCOTERM_ID                         INT 

, ORDER_QTY                           DECIMAL(13,4) 

, QTY_UOM_ID                          INT 

, REF_SHIPMENT_NBR                    STRING 

, STAGING_LOCN_ID                     STRING 

, LOADING_SEQ_ORD                     STRING 

, HAZMAT_CERT_CONTACT                 STRING 

, HAZMAT_CERT_DECLARATION             STRING 

, ASSIGNED_CUSTOMER_ID                BIGINT 

, REVENUE_RATING_LEVEL                TINYINT 

, FRT_REV_CM_DISCOUNT                 DECIMAL(13,4) 

, PLN_MIN_TEMPERATURE                 DECIMAL(8,2) 

, PLN_MAX_TEMPERATURE                 DECIMAL(8,2) 

, TEMPERATURE_UOM                     INT 

, COST_BREAKUP                        STRING 

, HAULING_CARRIER                     STRING 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_shipment_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (SHIPMENT_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_SHIPMENT_STATUS_PRE" , ***** Creating table: "WM_SHIPMENT_STATUS_PRE"


CREATE TABLE  WM_SHIPMENT_STATUS_PRE
( DC_NBR                              SMALLINT                 not null

, SHIPMENT_STATUS                     SMALLINT                 not null

, DESCRIPTION                         STRING 

, SHORT_DESC                          STRING 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_shipment_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_SHIP_VIA_PRE" , ***** Creating table: "WM_SHIP_VIA_PRE"


CREATE TABLE  WM_SHIP_VIA_PRE
( DC_NBR                              SMALLINT                 not null

, SHIP_VIA_ID                         INT                 not null

, TC_COMPANY_ID                       INT 

, CARRIER_ID                          BIGINT 

, SERVICE_LEVEL_ID                    BIGINT 

, MOT_ID                              BIGINT 

, LABEL_TYPE                          STRING 

, SERVICE_LEVEL_ICON                  STRING 

, EXECUTION_LEVEL_ID                  INT 

, BILL_SHIP_VIA_ID                    INT 

, IS_TRACKING_NBR_REQ                 TINYINT 

, MARKED_FOR_DELETION                 SMALLINT 

, DESCRIPTION                         STRING 

, ACCESSORIAL_SEARCH_STRING           STRING 

, INS_COVER_TYPE_ID                   TINYINT 

, MIN_DECLARED_VALUE                  DECIMAL(14,3) 

, MAX_DECLARED_VALUE                  DECIMAL(14,3) 

, SERVICE_LEVEL_INDICATOR             STRING 

, DECLARED_VALUE_CURRENCY             STRING 

, SHIP_VIA                            STRING 

, CUSTOM_SHIPVIA_ATTRIB               STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_ship_via_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_SIZE_UOM_PRE" , ***** Creating table: "WM_SIZE_UOM_PRE"


CREATE TABLE  WM_SIZE_UOM_PRE
( DC_NBR                              SMALLINT                 not null

, SIZE_UOM_ID                         INT                 not null

, TC_COMPANY_ID                       INT 

, SIZE_UOM                            STRING 

, DESCRIPTION                         STRING 

, CONSOLIDATION_CODE                  STRING 

, DO_NOT_INHERIT_TO_ORDER             SMALLINT 

, SIZE_MAPPING                        STRING 

, STANDARD_UOM                        SMALLINT 

, STANDARD_UNITS                      DECIMAL(16,8) 

, SPLITTER_CONS_CODE                  STRING 

, APPLY_TO_VENDOR                     INT 

, MARK_FOR_DELETION                   SMALLINT 

, DISCRETE                            SMALLINT 

, ADJUSTMENT                          DECIMAL(8,4) 

, ADJUSTMENT_SIZE_UOM_ID              INT 

, HIBERNATE_VERSION                   BIGINT 

, AUDIT_CREATED_SOURCE                STRING 

, AUDIT_CREATED_SOURCE_TYPE           SMALLINT 

, AUDIT_CREATED_DTTM                  TIMESTAMP 

, AUDIT_LAST_UPDATED_SOURCE           STRING 

, AUDIT_LAST_UPDATED_SOURCE_TYPE      SMALLINT 

, AUDIT_LAST_UPDATED_DTTM             TIMESTAMP 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_size_uom_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_SLOT_ITEM_PRE" , ***** Creating table: "WM_SLOT_ITEM_PRE"


CREATE TABLE  WM_SLOT_ITEM_PRE
( DC_NBR                              SMALLINT                 not null

, SLOT_ITEM_ID                        DECIMAL(19,0)                not null

, SLOT_ID                             DECIMAL(19,0) 

, SKU_ID                              DECIMAL(19,0) 

, SLOT_UNIT                           BIGINT 

, SHIP_UNIT                           BIGINT 

, MAX_LANES                           BIGINT 

, DEEPS                               BIGINT 

, SLOT_LOCKED                         TINYINT 

, PRIMARY_MOVE                        TINYINT 

, SLOT_WIDTH                          DECIMAL(13,4) 

, EST_HITS                            DECIMAL(13,4) 

, EST_INVENTORY                       DECIMAL(13,4) 

, EST_MOVEMENT                        DECIMAL(13,4) 

, HIST_MATCH                          STRING 

, LAST_CHANGE                         TIMESTAMP 

, BIN_UNIT                            BIGINT 

, PALLETE_PATTERN                     BIGINT 

, SCORE                               DECIMAL(9,4) 

, CUR_ORIENTATION                     BIGINT 

, IGN_FOR_RESLOT                      TINYINT 

, REC_LANES                           BIGINT 

, REC_STACKING                        BIGINT 

, PALLET_MOV                          DECIMAL(13,4) 

, CASE_MOV                            DECIMAL(13,4) 

, INNER_MOV                           DECIMAL(13,4) 

, EACH_MOV                            DECIMAL(13,4) 

, BIN_MOV                             DECIMAL(13,4) 

, PALLET_INVEN                        DECIMAL(13,4) 

, CASE_INVEN                          DECIMAL(13,4) 

, INNER_INVEN                         DECIMAL(13,4) 

, EACH_INVEN                          DECIMAL(13,4) 

, BIN_INVEN                           DECIMAL(13,4) 

, CALC_HITS                           DECIMAL(13,4) 

, CURRENT_BIN                         BIGINT 

, CURRENT_PALLET                      BIGINT 

, OPT_PALLET_PATTERN                  BIGINT 

, ALLOW_EXPAND                        TINYINT 

, PALLET_HI                           BIGINT 

, NEEDED_RACK_TYPE                    STRING 

, LEGAL_FIT_REASON                    BIGINT 

, ITEM_COST                           DECIMAL(9,4) 

, USER_DEFINED                        DECIMAL(9,2) 

, LEGAL_FIT                           TINYINT 

, SCORE_DIRTY                         TINYINT 

, USE_ESTIMATED_HIST                  TINYINT 

, OPT_FLUID_VOL                       DECIMAL(19,7) 

, CALC_VISC                           DECIMAL(9,2) 

, EST_VISC                            DECIMAL(9,2) 

, SESSION_ID                          BIGINT 

, RANK                                BIGINT 

, INFO1                               STRING 

, INFO2                               STRING 

, INFO3                               STRING 

, INFO4                               STRING 

, INFO5                               STRING 

, INFO6                               STRING 

, RESERVED_1                          STRING 

, RESERVED_2                          STRING 

, RESERVED_3                          BIGINT 

, RESERVED_4                          BIGINT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, MOD_USER                            STRING 

, SI_NUM_1                            DECIMAL(13,4) 

, SI_NUM_2                            DECIMAL(13,4) 

, SI_NUM_3                            DECIMAL(13,4) 

, SI_NUM_4                            DECIMAL(13,4) 

, SI_NUM_5                            DECIMAL(13,4) 

, SI_NUM_6                            DECIMAL(13,4) 

, SLOT_UNIT_WEIGHT                    DECIMAL(9,4) 

, TOTAL_ITEM_WT                       DECIMAL(9,4) 

, BORROWING_OBJECT                    BIGINT 

, BORROWING_SPECIFIC                  DECIMAL(19,0) 

, EST_MVMT_CAN_BORROW                 TINYINT 

, EST_HITS_CAN_BORROW                 TINYINT 

, PLB_SCORE                           DECIMAL(9,4) 

, MULT_LOC_GRP                        STRING 

, DELETE_MULT                         TINYINT 

, GAVE_HIST_TO                        DECIMAL(19,0) 

, ABW_TEMP_TAG                        STRING 

, FORECAST_BORROWED                   TINYINT 

, NUM_VERT_DIV                        BIGINT 

, REPLEN_GROUP                        STRING 

, ADDED_FOR_MLM                       TINYINT 

, RESERVED_5                          STRING 

, ADJ_GRP_ID                          DECIMAL(19,0) 

, IGA_SCORE                           DECIMAL(9,4) 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_slot_item_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (SLOT_ITEM_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_STANDARD_UOM_PRE" , ***** Creating table: "WM_STANDARD_UOM_PRE"


CREATE TABLE  WM_STANDARD_UOM_PRE
( DC_NBR                              SMALLINT                 not null

, STANDARD_UOM                        SMALLINT                 not null

, STANDARD_UOM_TYPE                   SMALLINT 

, ABBREVIATION                        STRING 

, DESCRIPTION                         STRING 

, UOM_SYSTEM                          STRING 

, IS_TYPE_SYS_DFLT                    SMALLINT 

, UNITS_IN_TYPE_SYS_DFLT              DECIMAL(16,4) 

, IS_DB_UOM                           SMALLINT 

, IS_SYSTEM_DEFINED                   SMALLINT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_standard_uom_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_STOP_PRE" , ***** Creating table: "WM_STOP_PRE"


CREATE TABLE  WM_STOP_PRE
( DC_NBR                              SMALLINT                 not null

, SHIPMENT_ID                         BIGINT                not null

, STOP_SEQ                            INT                 not null

, TC_COMPANY_ID                       INT 

, WAVE_ID                             BIGINT 

, WAVE_OPTION_ID                      BIGINT 

, WAVE_OPTION_LIST_ID                 INT 

, ARRIVAL_LATE_REASON_ID              INT 

, DEPARTURE_LATE_REASON_ID            INT 

, SIZE_UOM_ID                         INT 

, DOCK_DISPATCH_CAP_ID                BIGINT 

, IS_APPT_REQD                        TINYINT 

, POS_ARR_START_DTTM                  TIMESTAMP 

, POS_ARR_END_DTTM                    TIMESTAMP 

, POS_DEPART_START_DTTM               TIMESTAMP 

, POS_DEPART_END_DTTM                 TIMESTAMP 

, ARRIVAL_START_DTTM                  TIMESTAMP 

, ARRIVAL_END_DTTM                    TIMESTAMP 

, EST_ARRIVAL_DTTM                    TIMESTAMP 

, ARRIVAL_COMMIT_DTTM                 TIMESTAMP 

, ACTUAL_ARRIVAL_DTTM                 TIMESTAMP 

, APPT_DTTM                           TIMESTAMP 

, DEPARTURE_START_DTTM                TIMESTAMP 

, DEPARTURE_END_DTTM                  TIMESTAMP 

, EST_DEPARTURE_DTTM                  TIMESTAMP 

, ACTUAL_DEPARTURE_DTTM               TIMESTAMP 

, START_SHIPMENT_DTTM                 TIMESTAMP 

, END_SHIPMENT_DTTM                   TIMESTAMP 

, PAPERWORK_START_DTTM                TIMESTAMP 

, PICKUP_START_DTTM                   TIMESTAMP 

, EST_DISPATCH_DTTM                   TIMESTAMP 

, SHIPMENT_START_DTTM                 TIMESTAMP 

, SHIPMENT_END_DTTM                   TIMESTAMP 

, CHECKIN_TIME                        TIMESTAMP 

, CHECKOUT_TIME                       TIMESTAMP 

, ARRIVAL_NET_DTTM                    TIMESTAMP 

, ARRIVAL_NLT_DTTM                    TIMESTAMP 

, DEPARTURE_NET_DTTM                  TIMESTAMP 

, DEPARTURE_NLT_DTTM                  TIMESTAMP 

, REC_ARRIVAL_DTTM                    TIMESTAMP 

, REC_DEPARTURE_DTTM                  TIMESTAMP 

, BREAK_TIME                          INT 

, WAITING_TIME                        INT 

, STOP_LOCATION_NAME                  STRING 

, CONTACT_FIRST_NAME                  STRING 

, CONTACT_SURNAME                     STRING 

, CONTACT_PHONE_NUMBER                STRING 

, FACILITY_ALIAS_ID                   STRING 

, FACILITY_ID                         INT 

, ADDRESS_1                           STRING 

, ADDRESS_2                           STRING 

, CITY                                STRING 

, STATE_PROV                          STRING 

, POSTAL_CODE                         STRING 

, COUNTY                              STRING 

, COUNTRY_CODE                        STRING 

, HANDLER                             TINYINT 

, DISTANCE                            DECIMAL(9,2) 

, DISTANCE_UOM                        STRING 

, STOP_TZ                             SMALLINT 

, BILL_OF_LADING_NUMBER               STRING 

, DROP_HOOK_INDICATOR                 TINYINT 

, HANDLING_TIME                       DECIMAL(10,2) 

, REPORTED_HANDLER                    TINYINT 

, SEGMENT_ID                          BIGINT 

, SEGMENT_STOP_SEQ                    SMALLINT 

, CAPACITY                            INT 

, HAS_NOTES                           TINYINT 

, IS_ADDR_OVERRIDE                    TINYINT 

, IS_RECONSIGNED                      TINYINT 

, IS_EST_ARR_USER_DEFINED             TINYINT 

, IS_EST_DEP_USER_DEFINED             TINYINT 

, IS_WAVE_MAN_CHANGED                 TINYINT 

, DELAY_TYPE                          INT 

, STOP_STATUS                         SMALLINT 

, SEAL_NUMBER                         STRING 

, APPOINTMENT_DURATION                INT 

, BOL_TYPE                            TINYINT 

, PRO_NBR                             STRING 

, AUTH_NBR                            STRING 

, MANIF_TYPE                          STRING 

, CURR_PLT_ID                         STRING 

, CONS_LOCN_ID                        STRING 

, RTE_ID                              STRING 

, AES_ITN                             STRING 

, BOL_BREAK_ATTR                      STRING 

, FREIGHT_TERMS                       STRING 

, ADDRESS_3                           STRING 

, DOCK_DOOR_LOCN_ID                   STRING 

, FLOOR_SHIPMENT                      STRING 

, HIBERNATE_VERSION                   BIGINT 

, HITCH_TIME                          INT 

, LOAD_CLOSED_INDICATOR               TINYINT 

, STOP_TIME_MODIFIED                  TINYINT 

, DRIVE_TIME                          INT 

, REF_FIELD_1                         STRING 

, REF_FIELD_2                         STRING 

, REF_FIELD_3                         STRING 

, REF_FIELD_4                         STRING 

, REF_FIELD_5                         STRING 

, REF_FIELD_6                         STRING 

, REF_FIELD_7                         STRING 

, REF_FIELD_8                         STRING 

, REF_FIELD_9                         STRING 

, REF_FIELD_10                        STRING 

, ORDER_WINDOW_VIOLATION              BIGINT 

, DOCK_VIOLATION                      BIGINT 

, WAVE_OPTION_VIOLATION               BIGINT 

, STAGING_LOCN_ID                     STRING 

, CALC_EST_ARRIVAL_DTTM               TIMESTAMP 

, DEGREE_LATE_EARLY                   INT 

, ON_STATIC_ROUTE                     TINYINT 

, AUTO_APPOINTMENT                    TINYINT 

, STOP_REVENUE                        DECIMAL(13,4) 

, STOP_REVENUE_CURRENCY_CODE          STRING 

, STOP_MARGIN                         DECIMAL(13,4) 

, STOP_MARGIN_CURRENCY_CODE           STRING 

, STOP_COST                           DECIMAL(13,4) 

, STOP_COST_CURRENCY_CODE             STRING 

, STOP_LINEHAUL_REVENUE               DECIMAL(13,4) 

, ACCESSORIAL_REVENUE                 DECIMAL(13,4) 

, STOPOFF_REVENUE                     DECIMAL(13,4) 

, CM_DISCOUNT_REVENUE                 DECIMAL(13,3) 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_stop_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (SHIPMENT_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_STOP_STATUS_PRE" , ***** Creating table: "WM_STOP_STATUS_PRE"


CREATE TABLE  WM_STOP_STATUS_PRE
( DC_NBR                              SMALLINT                 not null

, STOP_STATUS                         SMALLINT                 not null

, DESCRIPTION                         STRING 

, SHORT_DESC                          STRING 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_stop_status_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_SYS_CODE_PRE" , ***** Creating table: "WM_SYS_CODE_PRE"


CREATE TABLE  WM_SYS_CODE_PRE
( DC_NBR                              SMALLINT                 not null

, REC_TYPE                            STRING         not null

, CODE_TYPE                           STRING         not null

, CODE_ID                             STRING        not null

, CODE_DESC                           STRING 

, SHORT_DESC                          STRING 

, MISC_FLAGS                          STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, WM_VERSION_ID                       INT 

, SYS_CODE_ID                         INT 

, SYS_CODE_TYPE_ID                    INT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_sys_code_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_TASK_DTL_PRE" , ***** Creating table: "WM_TASK_DTL_PRE"


CREATE TABLE  WM_TASK_DTL_PRE
( DC_NBR                              SMALLINT                 not null

, TASK_DTL_ID                         INT                 not null

, TASK_ID                             INT 

, TASK_SEQ_NBR                        SMALLINT 

, CNTR_NBR                            STRING 

, INVN_TYPE                           STRING 

, PROD_STAT                           STRING 

, BATCH_NBR                           STRING 

, SKU_ATTR_1                          STRING 

, SKU_ATTR_2                          STRING 

, SKU_ATTR_3                          STRING 

, SKU_ATTR_4                          STRING 

, SKU_ATTR_5                          STRING 

, CNTRY_OF_ORGN                       STRING 

, ALLOC_INVN_CODE                     SMALLINT 

, PULL_LOCN_ID                        STRING 

, ALLOC_UOM_QTY                       DECIMAL(9,2) 

, FULL_CNTR_ALLOCD                    STRING 

, INVN_NEED_TYPE                      SMALLINT 

, TASK_PRTY                           INT 

, TASK_GENRTN_REF_CODE                STRING 

, TASK_GENRTN_REF_NBR                 STRING 

, TASK_CMPL_REF_CODE                  STRING 

, TASK_CMPL_REF_NBR                   STRING 

, ERLST_START_DATE_TIME               TIMESTAMP 

, LTST_START_DATE_TIME                TIMESTAMP 

, LTST_CMPL_DATE_TIME                 TIMESTAMP 

, ALLOC_UOM                           STRING 

, ORIG_REQMT                          DECIMAL(13,5) 

, QTY_ALLOC                           DECIMAL(13,5) 

, QTY_PULLD                           DECIMAL(13,5) 

, DEST_LOCN_ID                        STRING 

, DEST_LOCN_SEQ                       INT 

, STAT_CODE                           SMALLINT 

, TASK_TYPE                           STRING 

, CURR_WORK_GRP                       STRING 

, CURR_WORK_AREA                      STRING 

, REPL_DIVRT_LOCN                     STRING 

, PICK_SEQ_CODE                       STRING 

, TASK_LOCN_SEQ                       SMALLINT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, PKT_CTRL_NBR                        STRING 

, REQD_INVN_TYPE                      STRING 

, REQD_PROD_STAT                      STRING 

, REQD_BATCH_NBR                      STRING 

, REQD_SKU_ATTR_1                     STRING 

, REQD_SKU_ATTR_2                     STRING 

, REQD_SKU_ATTR_3                     STRING 

, REQD_SKU_ATTR_4                     STRING 

, REQD_SKU_ATTR_5                     STRING 

, REQD_CNTRY_OF_ORGN                  STRING 

, PKT_SEQ_NBR                         INT 

, CARTON_NBR                          STRING 

, CARTON_SEQ_NBR                      INT 

, PIKR_NBR                            SMALLINT 

, TASK_CMPL_REF_NBR_SEQ               INT 

, ALLOC_INVN_DTL_ID                   INT 

, SUBSTITUTION_FLAG                   SMALLINT 

, NEXT_TASK_ID                        INT 

, NEXT_TASK_SEQ_NBR                   SMALLINT 

, NEXT_TASK_DESC                      STRING 

, NEXT_TASK_TYPE                      STRING 

, MISC_ALPHA_FIELD_1                  STRING 

, MISC_ALPHA_FIELD_2                  STRING 

, MISC_ALPHA_FIELD_3                  STRING 

, VOCOLLECT_POSN                      SMALLINT 

, VOCOLLECT_SHORT_FLAG                STRING 

, PAGE_NBR                            INT 

, RSN_CODE                            STRING 

, SLOT_NBR                            INT 

, SUBSLOT_NBR                         INT 

, CD_MASTER_ID                        INT 

, TASK_HDR_ID                         INT 

, WM_VERSION_ID                       INT 

, ITEM_ID                             BIGINT 

, TC_ORDER_ID                         STRING 

, LINE_ITEM_ID                        BIGINT 

, TRANS_INVN_TYPE                     SMALLINT 

, TOTE_NBR                            STRING 

, IS_CHASE_CREATED                    TINYINT 

, RESOURCE_GROUP_ID                   STRING 

, WORK_RESOURCE_ID                    STRING 

, WORK_RELEASE_BATCH_NUMBER           STRING 

, ALLOCATION_KEY                      STRING 

, DISTRIBUTION_KEY                    STRING 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_task_dtl_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (TASK_DTL_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_TASK_HDR_PRE" , ***** Creating table: "WM_TASK_HDR_PRE"


CREATE TABLE  WM_TASK_HDR_PRE
( DC_NBR                              SMALLINT                 not null

, TASK_HDR_ID                         INT                 not null

, TASK_ID                             INT 

, WHSE                                STRING 

, TASK_DESC                           STRING 

, INVN_TYPE                           STRING 

, INVN_NEED_TYPE                      SMALLINT 

, DFLT_TASK_PRTY                      INT 

, CURR_TASK_PRTY                      INT 

, XPECTD_DURTN                        INT 

, ACTL_DURTN                          INT 

, ERLST_START_DATE_TIME               TIMESTAMP 

, LTST_START_DATE_TIME                TIMESTAMP 

, LTST_CMPL_DATE_TIME                 TIMESTAMP 

, BEGIN_AREA                          STRING 

, BEGIN_ZONE                          STRING 

, BEGIN_AISLE                         STRING 

, END_AREA                            STRING 

, END_ZONE                            STRING 

, END_AISLE                           STRING 

, START_CURR_WORK_GRP                 STRING 

, START_CURR_WORK_AREA                STRING 

, END_CURR_WORK_GRP                   STRING 

, END_CURR_WORK_AREA                  STRING 

, START_DEST_WORK_GRP                 STRING 

, START_DEST_WORK_AREA                STRING 

, END_DEST_WORK_GRP                   STRING 

, END_DEST_WORK_AREA                  STRING 

, TASK_TYPE                           STRING 

, TASK_GENRTN_REF_CODE                STRING 

, TASK_GENRTN_REF_NBR                 STRING 

, NEED_ID                             STRING 

, TASK_BATCH                          STRING 

, STAT_CODE                           SMALLINT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, RLS_DATE_TIME                       TIMESTAMP 

, TASK_CMPL_REF_CODE                  STRING 

, TASK_CMPL_REF_NBR                   STRING 

, OWNER_USER_ID                       STRING 

, ONE_USER_PER_GRP                    STRING 

, NEXT_TASK_ID                        INT 

, EXCEPTION_CODE                      STRING 

, CURR_LOCN_ID                        STRING 

, TASK_PARM_ID                        INT 

, RULE_ID                             INT 

, VOCOLLECT_ASSIGN_ID                 INT 

, CURR_USER_ID                        STRING 

, MHE_FLAG                            STRING 

, PICK_TO_TOTE_FLAG                   STRING 

, MHE_ORD_STATE                       STRING 

, PRT_TASK_LIST_FLAG                  STRING 

, RPT_PRTR_REQSTR                     STRING 

, ORIG_TASK_ID                        INT 

, DOC_ID                              STRING 

, VOCO_INTRNL_REVERSE_ID              STRING 

, WM_VERSION_ID                       INT 

, ITEM_ID                             BIGINT 

, LOCN_HDR_ID                         INT 

, TASK_RULE_PARM_ID                   INT 

, PICK_CART_TYPE                      STRING 

, REPRINT_COUNT                       TINYINT 

, ESTIMATED_TIME                      DECIMAL(16,4) 

, ESTIMATED_DISTANCE                  DECIMAL(16,4) 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_task_hdr_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (TASK_HDR_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_TRAILER_CONTENTS_PRE" , ***** Creating table: "WM_TRAILER_CONTENTS_PRE"


CREATE TABLE  WM_TRAILER_CONTENTS_PRE
( DC_NBR                              SMALLINT                 not null

, TRAILER_CONTENTS_ID                 INT                 not null

, VISIT_DETAIL_ID                     INT 

, IS_PLANNED                          TINYINT 

, SHIPMENT_ID                         BIGINT 

, ASN_ID                              BIGINT 

, PO_ID                               BIGINT 

, CREATED_DTTM                        TIMESTAMP 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_trailer_contents_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (TRAILER_CONTENTS_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_TRAILER_REF_PRE" , ***** Creating table: "WM_TRAILER_REF_PRE"


CREATE TABLE  WM_TRAILER_REF_PRE
( DC_NBR                              SMALLINT                 not null

, TRAILER_ID                          BIGINT                not null

, TRAILER_STATUS                      SMALLINT 

, CURRENT_LOCATION_ID                 STRING 

, ASSIGNED_LOCATION_ID                STRING 

, ACTIVE_VISIT_ID                     INT 

, ACTIVE_VISIT_DETAIL_ID              INT 

, CREATED_DTTM                        TIMESTAMP 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, TRAILER_LOCATION_STATUS             SMALLINT 

, CONVEYABLE                          TINYINT 

, PROTECTION_LEVEL                    INT 

, PRODUCT_CLASS                       INT 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_trailer_ref_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_TRAILER_TYPE_PRE" , ***** Creating table: "WM_TRAILER_TYPE_PRE"


CREATE TABLE  WM_TRAILER_TYPE_PRE
( DC_NBR                              SMALLINT                 not null

, TRAILER_TYPE                        TINYINT                 not null

, DESCRIPTION                         STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_trailer_type_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_TRAILER_VISIT_DETAIL_PRE" , ***** Creating table: "WM_TRAILER_VISIT_DETAIL_PRE"


CREATE TABLE  WM_TRAILER_VISIT_DETAIL_PRE
( DC_NBR                              SMALLINT                 not null

, VISIT_DETAIL_ID                     INT                 not null

, VISIT_ID                            INT 

, TYPE                                STRING 

, APPOINTMENT_ID                      INT 

, DRIVER_ID                           BIGINT 

, TRACTOR_ID                          BIGINT 

, SEAL_NUMBER                         STRING 

, FOB_INDICATOR                       SMALLINT 

, START_DTTM                          TIMESTAMP 

, END_DTTM                            TIMESTAMP 

, CREATED_DTTM                        TIMESTAMP 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, PROTECTION_LEVEL                    STRING 

, PRODUCT_CLASS                       STRING 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_trailer_visit_detail_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (VISIT_DETAIL_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_TRAILER_VISIT_PRE" , ***** Creating table: "WM_TRAILER_VISIT_PRE"


CREATE TABLE  WM_TRAILER_VISIT_PRE
( DC_NBR                              SMALLINT                 not null

, VISIT_ID                            INT                 not null

, FACILITY_ID                         INT 

, TRAILER_ID                          BIGINT 

, CHECKIN_DTTM                        TIMESTAMP 

, CHECKOUT_DTTM                       TIMESTAMP 

, CREATED_DTTM                        TIMESTAMP 

, CREATED_SOURCE_TYPE                 SMALLINT 

, CREATED_SOURCE                      STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            SMALLINT 

, LAST_UPDATED_SOURCE                 STRING 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_trailer_visit_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (VISIT_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_UN_NUMBER_PRE" , ***** Creating table: "WM_UN_NUMBER_PRE"


CREATE TABLE  WM_UN_NUMBER_PRE
( DC_NBR                              SMALLINT                 not null

, UN_NUMBER_ID                        BIGINT                not null

, TC_COMPANY_ID                       INT 

, UN_NUMBER_VALUE                     STRING 

, UN_NUMBER_DESCRIPTION               STRING 

, UNN_CLASS_DIV                       STRING 

, UNN_SUB_RISK                        STRING 

, UNN_HAZARD_LABEL                    STRING 

, UNN_PG                              STRING 

, UNN_SP                              STRING 

, UNN_AIR_BOTH_INST_LTD               STRING 

, UNN_AIR_BOTH_QTY_LTD                STRING 

, UNN_AIR_BOTH_INST                   STRING 

, UNN_AIR_BOTH_QTY                    STRING 

, UNN_AIR_CARGO_INST                  STRING 

, UNN_AIR_CARGO_QTY                   STRING 

, UN_NUMBER                           BIGINT 

, RQ_FIELD_1                          STRING 

, RQ_FIELD_2                          STRING 

, RQ_FIELD_3                          STRING 

, RQ_FIELD_4                          STRING 

, RQ_FIELD_5                          STRING 

, HZC_FIELD_1                         STRING 

, HZC_FIELD_2                         STRING 

, HZC_FIELD_3                         STRING 

, HZC_FIELD_4                         STRING 

, HZC_FIELD_5                         STRING 

, MARK_FOR_DELETION                   TINYINT 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, UN_NUMBER_VERSION                   STRING 

, HAZMAT_UOM                          INT 

, MAX_HAZMAT_QTY                      DECIMAL(13,4) 

, REGULATION_SET                      STRING 

, TRANS_MODE                          STRING 

, REPORTABLE_QTY                      TINYINT 

, TECHNICAL_NAME                      STRING 

, ADDL_DESC                           STRING 

, PHONE_NUMBER                        STRING 

, CONTACT_NAME                        STRING 

, IS_ACCESSIBLE                       TINYINT 

, RESIDUE_INDICATOR_CODE              STRING 

, HAZMAT_CLASS_QUAL                   STRING 

, NOS_INDICATOR_CODE                  TINYINT 

, HAZMAT_MATERIAL_QUAL                STRING 

, FLASH_POINT_TEMPERATURE             DECIMAL(13,4) 

, FLASH_POINT_TEMPERATURE_UOM         STRING 

, HAZARD_ZONE_CODE                    STRING 

, RADIOACTIVE_QTY_CALC_FACTOR         DECIMAL(13,4) 

, RADIOACTIVE_QUANTITY_UOM            STRING 

, EPAWASTESTREAM_NUMBER               STRING 

, WASTE_CHARACTERISTIC_CODE           STRING 

, NET_EXPLOSIVE_QUANTITY_FACTOR       DECIMAL(13,4) 

, HAZMAT_EXEMPTION_REF_NUMBR          STRING 

, RQ_FIELD_6                          STRING 

, RQ_FIELD_7                          STRING 

, RQ_FIELD_8                          STRING 

, RQ_FIELD_9                          STRING 

, RQ_FIELD_10                         STRING 

, RQ_FIELD_11                         STRING 

, RQ_FIELD_12                         STRING 

, RQ_FIELD_13                         STRING 

, RQ_FIELD_14                         STRING 

, RQ_FIELD_15                         STRING 

, RQ_FIELD_16                         STRING 

, RQ_FIELD_17                         STRING 

, RQ_FIELD_18                         STRING 

, RQ_FIELD_19                         STRING 

, RQ_FIELD_20                         STRING 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_un_number_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_USER_PROFILE_PRE" , ***** Creating table: "WM_USER_PROFILE_PRE"


CREATE TABLE  WM_USER_PROFILE_PRE
( DC_NBR                              SMALLINT                 not null

, USER_PROFILE_ID                     INT                 not null

, LOGIN_USER_ID                       STRING 

, MENU_ID                             INT 

, EMPLYE_ID                           STRING 

, RESTR_TASK_GRP_TO_DFLT              STRING 

, RESTR_MENU_MODE_TO_DFLT             STRING 

, DFLT_RF_MENU_MODE                   STRING 

, LANG_ID                             STRING 

, DATE_MASK                           STRING 

, LAST_TASK                           STRING 

, LAST_LOCN                           STRING 

, LAST_WORK_GRP                       STRING 

, LAST_WORK_AREA                      STRING 

, ALLOW_TASK_INT_CHG                  STRING 

, NBR_OF_TASK_TO_DSP                  TINYINT 

, TASK_DSP_MODE                       STRING 

, DB_USER_ID                          STRING 

, DB_PSWD                             STRING 

, DB_CONNECT_STRING                   STRING 

, PRTR_REQSTR                         STRING 

, IDLE_TIME_BEF_SHTDWN                INT 

, RF_MENU_ID                          INT 

, USER_ID                             STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, PAGE_SIZE                           INT 

, VOCOLLECT_WORK_TYPE                 TINYINT 

, CURR_TASK_GRP                       STRING 

, CURR_VOCOLLECT_PTS_CASE             STRING 

, CURR_VOCOLLECT_REASON_CODE          STRING 

, TASK_GRP_JUMP_FLAG                  STRING 

, AUTO_3PL_LOGIN_FLAG                 STRING 

, SECURITY_CONTEXT_ID                 INT 

, SEC_USER_NAME                       STRING 

, VOCOLLECT_PUTAWAY_FLAG              TINYINT 

, VOCOLLECT_REPLEN_FLAG               TINYINT 

, VOCOLLECT_PACKING_FLAG              TINYINT 

, CLS_TIMEZONE_ID                     INT 

, DAL_CONNECTION_STRING               STRING 

, WM_VERSION_ID                       INT 

, USER_SECURITY_CONTEXT_ID            INT 

, DFLT_TASK_INT                       SMALLINT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, MOBILE_HELP_TEXT                    STRING 

, MOB_SPLASH_SCREEN_FLAG              STRING 

, SCREEN_TYPE_ID                      SMALLINT 

, MOBILE_MSG_SPEECH_LEVEL             STRING 

, MOBILE_MSG_SPEECH_RATE              STRING 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_user_profile_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (USER_PROFILE_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_VEND_PERF_TRAN_PRE" , ***** Creating table: "WM_VEND_PERF_TRAN_PRE"


CREATE TABLE  WM_VEND_PERF_TRAN_PRE
( DC_NBR                              SMALLINT                 not null

, VEND_PERF_TRAN_ID                   INT                 not null

, PERF_CODE                           STRING 

, WHSE                                STRING 

, SHPMT_NBR                           STRING 

, PO_NBR                              STRING 

, CASE_NBR                            STRING 

, UOM                                 STRING 

, QTY                                 DECIMAL(13,5) 

, SAMS                                DECIMAL(9,4) 

, STAT_CODE                           TINYINT 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, CHRG_AMT                            DECIMAL(9,2) 

, BILL_FLAG                           STRING 

, LOAD_NBR                            STRING 

, ILM_APPT_NBR                        STRING 

, VENDOR_MASTER_ID                    INT 

, CD_MASTER_ID                        INT 

, CMNT                                STRING 

, CREATED_BY_USER_ID                  STRING 

, WM_VERSION_ID                       INT 

, PO_HDR_ID                           INT 

, ASN_HDR_ID                          INT 

, CASE_HDR_ID                         INT 

, ITEM_ID                             BIGINT 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_vend_perf_tran_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (VEND_PERF_TRAN_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_WAVE_PARM_PRE" , ***** Creating table: "WM_WAVE_PARM_PRE"


CREATE TABLE  WM_WAVE_PARM_PRE
( DC_NBR                              SMALLINT                 not null

, WAVE_PARM_ID                        INT                 not null

, WHSE                                STRING 

, DAY_OF_WEEK                         SMALLINT 

, SHIFT_NBR                           STRING 

, REC_TYPE                            STRING 

, WAVE_NBR                            STRING 

, WAVE_DESC                           STRING 

, MAX_UNITS                           INT 

, MAX_ORDERS                          INT 

, WAVE_CMPL_STAT_CODE                 SMALLINT 

, WAVE_STAT_CODE                      SMALLINT 

, WAVE_STAT_DATE_TIME                 TIMESTAMP 

, PICK_STAT_CODE                      SMALLINT 

, PACK_STAT_CODE                      SMALLINT 

, REPL_STAT_CODE                      SMALLINT 

, INDUC_STAT_CODE                     SMALLINT 

, MISC_STAT_CODE_1                    SMALLINT 

, MISC_STAT_CODE_2                    SMALLINT 

, SCHED_DLVRY_DATE                    TIMESTAMP 

, SCHED_WAVE                          STRING 

, NBR_OF_PKTS                         INT 

, NBR_OF_CARTONS                      INT 

, NBR_OF_LOCN                         INT 

, NBR_OF_CHUTE                        INT 

, NBR_OF_PLT_POSN                     INT 

, ASSIGN_PLT_POSN                     SMALLINT 

, WAVE_PROC_TYPE                      SMALLINT 

, WAVE_PARM_TEMPLATE_ID               INT 

, FORCE_WPT                           STRING 

, REPL_TRIG                           STRING 

, RTE_W_WAVE                          STRING 

, CHG_PKT_QTY                         STRING 

, LABEL_PRTR_REQSTR                   STRING 

, RPT_PRTR_REQSTR                     STRING 

, CARTON_BREAK_ON_AREA_CHG            STRING 

, CARTON_BREAK_ON_ZONE_CHG            STRING 

, CARTON_BREAK_ON_AISLE_CHG           STRING 

, PKT_CONSOL_REJECT_RULE              STRING 

, ASSIGN_PRO_NBR                      STRING 

, SHIP_VIA                            STRING 

, SWC_NBR                             STRING 

, MANIF_NBR                           STRING 

, BOL                                 STRING 

, SHPMT_NBR                           STRING 

, SWC_NBR_FLAG                        STRING 

, MANIF_FLAG                          STRING 

, BOL_FLAG                            STRING 

, SHPMT_FLAG                          STRING 

, CLEAR_SPUR_LOCN                     STRING 

, SPUR                                STRING 

, XCEED_CAPCTY_FLAG                   STRING 

, FILL_CAPCTY_STRK                    SMALLINT 

, CONSL_LOCN_CLEAR_METHD              STRING 

, PERP_PLT_POSN                       STRING 

, NBR_OF_LABELS_PER_SPOOL             INT 

, NBR_OF_LABELS_PER_ROW               SMALLINT 

, PKT_CONSOL_PROF                     STRING 

, PKT_CONSOL_DETRM_TYPE               STRING 

, SKU_CNSTR                           STRING 

, SKU_SUB                             STRING 

, USE_INBD_LPN_AS_OUTBD_LPN           STRING 

, CARTON_TYPE                         STRING 

, NBR_OF_PIKRS_PAKRS                  SMALLINT 

, AVG_PIKS_PER_PIKR                   DECIMAL(5,1) 

, AVG_TMU_PER_PIKR                    DECIMAL(6,2) 

, PICK_LOCN_ASSIGN_TYPE               STRING 

, DIFF_PIKR_ON_AREA_CHG               STRING 

, DIFF_PIKR_ON_ZONE_CHG               STRING 

, DIFF_PIKR_ON_AISLE_CHG              STRING 

, DIFF_PIKR_ON_BAY_CHG                STRING 

, DIFF_PIKR_ON_LVL_CHG                STRING 

, DIFF_PIKR_ON_POSN_CHG               STRING 

, SAME_PIKR_FOR_AREA                  STRING 

, SAME_PIKR_FOR_ZONE                  STRING 

, SAME_PIKR_FOR_AISLE                 STRING 

, SAME_PIKR_FOR_BAY                   STRING 

, SAME_PIKR_FOR_LVL                   STRING 

, SAME_PIKR_FOR_POSN                  STRING 

, OK_TO_PICK                          STRING 

, OK_TO_PACK                          STRING 

, OK_TO_REPL                          STRING 

, OK_TO_INDUC                         STRING 

, OK_TO_MISC_1                        STRING 

, OK_TO_MISC_2                        STRING 

, START_PICK_DATE_TIME                TIMESTAMP 

, END_PICK_DATE_TIME                  TIMESTAMP 

, START_PACK_DATE_TIME                TIMESTAMP 

, END_PACK_DATE_TIME                  TIMESTAMP 

, START_REPL_DATE_TIME                TIMESTAMP 

, END_REPL_DATE_TIME                  TIMESTAMP 

, START_INDUC_DATE_TIME               TIMESTAMP 

, END_INDUC_DATE_TIME                 TIMESTAMP 

, START_MISC_1_DATE_TIME              TIMESTAMP 

, END_MISC_1_DATE_TIME                TIMESTAMP 

, START_MISC_2_DATE_TIME              TIMESTAMP 

, END_MISC_2_DATE_TIME                TIMESTAMP 

, CMPLTD_DATE_TIME                    TIMESTAMP 

, GRP_PKT_BY_SWC_NBR                  STRING 

, PULL_ALL_SWC                        STRING 

, ZONE_PICK_METHOD                    STRING 

, WAVE_TYPE_INDIC                     STRING 

, REJECT_DISTRO_RULE                  STRING 

, RETAIL_MAX_UNITS                    INT 

, RETAIL_UNIT_PCNT_OVER               DECIMAL(5,2) 

, RETAIL_MAX_SKUS                     INT 

, RETAIL_MAX_STORES                   INT 

, RETAIL_MAX_ROUTES                   INT 

, BULK_WAVE_BAL_FROM_ACTV             STRING 

, PROC_ATTR_1                         STRING 

, PROC_ATTR_2                         STRING 

, PROC_ATTR_3                         STRING 

, PROC_ATTR_4                         STRING 

, PROC_ATTR_5                         STRING 

, SPL_INSTR_CODE_1                    STRING 

, SPL_INSTR_CODE_2                    STRING 

, SPL_INSTR_CODE_3                    STRING 

, SPL_INSTR_CODE_4                    STRING 

, SPL_INSTR_CODE_5                    STRING 

, CREATE_DATE_TIME                    TIMESTAMP 

, MOD_DATE_TIME                       TIMESTAMP 

, USER_ID                             STRING 

, PRT_CS_LABELS_W_WAVE                STRING 

, CREATE_SHPMT                        STRING 

, CREATE_LOAD                         STRING 

, CHG_WO_QTY                          STRING 

, PACK_CMPL_W_WAVE                    STRING 

, DLVRY_WIN_RANGE_CODE                STRING 

, DFLT_PLT_TYPE                       STRING 

, PACK_WAVE_PARM_ID                   INT 

, CAT_PARM_ID                         INT 

, DESEL_SNGL_FLAG                     STRING 

, ALLOC_TYPE                          STRING 

, FORCE_ALLOC_TYPE                    STRING 

, FORCE_PICK_LOCN_ASSIGN_TYPE         STRING 

, BULK_PICK_ZONE                      STRING 

, CONSOL_PLTZ_CARTONS_FLAG            SMALLINT 

, ACTV_REPL_ORGN                      STRING 

, MAX_CARTONS                         INT 

, MAX_PICK_WAVES                      INT 

, HOLD_PRINT                          SMALLINT 

, SUPPR_PR40_REPL                     SMALLINT 

, COMB_4050_REPL                      SMALLINT 

, SUPPR_PR60_REPL                     SMALLINT 

, LOAD_PLAN                           STRING 

, ASSIGN_NEW_LOAD                     STRING 

, RESV_BAL                            STRING 

, RESV_BALANCE_INT                    SMALLINT 

, ORDER_RND_FLAG                      STRING 

, ALLOC_REMAIN                        STRING 

, MHE_FLAG                            STRING 

, MHE_ONE_SEND_FLAG                   STRING 

, MHE_PICK_DEST_LEVEL                 STRING 

, SHIP_DATE_TIME                      TIMESTAMP 

, COLLATE_PRTR_REQSTR                 STRING 

, MAX_DYNAMIC_LOCN_OPTN               SMALLINT 

, FORCE_SHIP_DATE_FLAG                SMALLINT 

, MAX_WT                              DECIMAL(13,4) 

, MAX_VOL                             DECIMAL(13,4) 

, BULK_PICK_ASSIGN_TYPE               STRING 

, REPL_WAVE                           STRING 

, RESEQ_ACTIV_LOC                     STRING 

, VOCO_GRP_CARTONS_TO_TASK            SMALLINT 

, VOCO_MAX_CARTON_ON_TASK             SMALLINT 

, WM_VERSION_ID                       INT 

, TC_COMPANY_ID                       INT 

, MAX_ORDER_LINES                     INT 

, CHASE_WAVE                          STRING 

, DEFAULT_QUEUE_PRTY                  SMALLINT 

, ALLOC_LOGGING                       TINYINT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, CARTON_BREAK_ON_LOCN_CLASS          STRING 

, DESELECT_UNALLOCATED_SINGLES        TINYINT 

, CARTON_TYPE_EVENT_ID                INT 

, CHUTE_ASSIGN_TYPE_EVENT_ID          INT 

, LPN_BRK_ATTRIB_EVENT_ID             INT 

, IGNORE_SINGLES_PROCESSING           TINYINT 

, HLD_RLS_FLAG                        TINYINT 

, MAX_NBR_PACK_WAVES_TO_RLS           SMALLINT 

, REPLEN_BUMP_UP                      TINYINT 

, RETAIN_PALLET_ID                    TINYINT 

, ORDER_SEL_LOGGING                   TINYINT 

, AGG_LOGGING                         TINYINT 

, CUB_LOGGING                         TINYINT 

, PCK_WAVE_LOGGING                    TINYINT 

, TSK_GEN_LOGGING                     TINYINT 

, LABEL_PRNT_LOGGING                  TINYINT 

, ORDER_SEQ_LOGGING                   TINYINT 

, ORDER_CNSOL_LOGGING                 TINYINT 

, RELEASE_ALL_TASK                    TINYINT 

, FORCE_ORDER_STREAMING               STRING 

, CUBING_ACTION_FOR_FAILED_ORDER      TINYINT 

, NBR_ALLOCATION_THREAD               SMALLINT 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_wave_parm_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (WAVE_PARM_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_YARD_PRE" , ***** Creating table: "WM_YARD_PRE"


CREATE TABLE  WM_YARD_PRE
( DC_NBR                              SMALLINT                 not null

, YARD_ID                             BIGINT                not null

, TC_COMPANY_ID                       INT 

, YARD_NAME                           STRING 

, CREATED_SOURCE_TYPE                 TINYINT 

, CREATED_SOURCE                      STRING 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, DO_GENERATE_MOVE_TASK               TINYINT 

, DO_GENERATE_NEXT_EQUIP              TINYINT 

, IS_RANGE_TASKS                      TINYINT 

, IS_SEAL_TASK_TRGD                   TINYINT 

, DO_OVERRIDE_SYSTEM_TASKS            TINYINT 

, IS_TASKING_ALLOWED                  TINYINT 

, ADDRESS                             STRING 

, CITY                                STRING 

, STATE_PROV                          STRING 

, POSTAL_CODE                         STRING 

, COUNTY                              STRING 

, COUNTRY_CODE                        STRING 

, TIME_ZONE_ID                        SMALLINT 

, MAX_EQUIPMENT_ALLOWED               INT 

, UPPER_CHECKIN_TIME_MINS             SMALLINT 

, LOWER_CHECKIN_TIME_MINS             SMALLINT 

, FIXED_TIME_MINS                     SMALLINT 

, MARK_FOR_DELETION                   TINYINT 

, LOCK_TRAILER_ON_MOVE_TO_DOOR        TINYINT 

, YARD_SVG_FILE                       STRING 

, LOCATION_ID                         BIGINT 

, THRESHOLD_PERCENT                   INT 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_yard_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON (YARD_ID)

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_YARD_ZONE_PRE" , ***** Creating table: "WM_YARD_ZONE_PRE"


CREATE TABLE  WM_YARD_ZONE_PRE
( DC_NBR                              SMALLINT                 not null

, YARD_ID                             BIGINT                not null

, YARD_ZONE_ID                        BIGINT                not null

, YARD_ZONE_NAME                      STRING 

, MARK_FOR_DELETION                   TINYINT 

, PUTAWAY_ELIGIBLE                    TINYINT 

, LOCATION_ID                         BIGINT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, CREATED_SOURCE                      STRING 

, CREATED_SOURCE_TYPE                 TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_yard_zone_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)







--*****  Creating table:  "WM_YARD_ZONE_SLOT_PRE" , ***** Creating table: "WM_YARD_ZONE_SLOT_PRE"


CREATE TABLE  WM_YARD_ZONE_SLOT_PRE
( DC_NBR                              SMALLINT                 not null

, YARD_ID                             BIGINT                not null

, YARD_ZONE_ID                        BIGINT                not null

, YARD_ZONE_SLOT_ID                   BIGINT                not null

, YARD_ZONE_SLOT_NAME                 STRING 

, YARD_ZONE_SLOT_STATUS               SMALLINT 

, X_COORDINATE                        DECIMAL(14,3) 

, Y_COORDINATE                        DECIMAL(14,3) 

, Z_COORDINATE                        DECIMAL(14,3) 

, MAX_CAPACITY                        INT 

, USED_CAPACITY                       INT 

, MARK_FOR_DELETION                   TINYINT 

, IS_GUARD_HOUSE                      TINYINT 

, IS_THRESHOLD_GUARD_HOUSE            SMALLINT 

, CREATED_DTTM                        TIMESTAMP 

, LAST_UPDATED_DTTM                   TIMESTAMP 

, LOCN_ID                             STRING 

, CREATED_SOURCE                      STRING 

, CREATED_SOURCE_TYPE                 TINYINT 

, LAST_UPDATED_SOURCE                 STRING 

, LAST_UPDATED_SOURCE_TYPE            TINYINT 

, LOAD_TSTMP                          TIMESTAMP                    

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/wms/wm_yard_zone_slot_pre' 
PARTITIONED BY (DC_NBR) 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (DC_NBR)


