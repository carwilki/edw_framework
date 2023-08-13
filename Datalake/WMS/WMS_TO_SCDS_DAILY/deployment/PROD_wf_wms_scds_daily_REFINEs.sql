

--*****  Creating table:  "WM_ASN" , ***** Creating table: "WM_ASN"

use refine;
CREATE TABLE  WM_ASN
( LOCATION_ID INT not null

, WM_ASN_ID                              BIGINT              not null

, WM_TC_ASN_ID                           STRING 

, WM_TC_ASN_ID_UPPER_CASE                STRING 

, WM_TC_COMPANY_ID                       INT 

, WM_ASN_TYPE                            TINYINT 

, WM_ASN_LEVEL                           SMALLINT 

, WM_ASN_PRIORITY                        TINYINT 

, WM_ASN_SHPMT_TYPE                      STRING 

, WM_ASN_STATUS                          SMALLINT 

, WM_APPOINTMENT_ID                      STRING 

, WM_APPOINTMENT_TSTMP                   TIMESTAMP 

, APPOINTMENT_DURATION                   INT 

, WM_SHIPMENT_ID                         BIGINT 

, WM_TC_SHIPMENT_ID                      STRING 

, WM_INVOICE_NBR                         STRING 

, WM_INVOICE_TSTMP                       TIMESTAMP 

, WM_DC_ORD_NBR                          STRING 

, WM_WORK_ORD_NBR                        STRING 

, WM_ORIGINAL_ASN_ID                     BIGINT 

, WM_ASN_ORGN_TYPE                       STRING 

, WM_ORIGINAL_ORDER_NBR                  STRING 

, WM_ORIGIN_FACILITY_ALIAS_ID            STRING 

, WM_ORIGIN_FACILITY_ID                  INT 

, WM_DESTINATION_FACILITY_ALIAS_ID       STRING 

, WM_DESTINATION_FACILITY_ID             INT 

, WM_RETURN_REFERENCE_NBR                STRING 

, WM_DELIVERY_STOP_SEQ                   SMALLINT 

, WM_RECEIPT_TYPE                        TINYINT 

, WM_RECEIPT_VARIANCE                    TINYINT 

, WM_PRE_RECEIPT_STATUS                  STRING 

, WM_RECEIPT_TSTMP                       TIMESTAMP 

, WM_FIRST_RECEIPT_TSTMP                 TIMESTAMP 

, WM_LAST_RECEIPT_TSTMP                  TIMESTAMP 

, VERIFICATION_ATTEMPTED_FLAG            TINYINT 

, WM_VERIFIED_TSTMP                      TIMESTAMP 

, WM_PICKUP_END_TSTMP                    TIMESTAMP 

, WM_ACTUAL_DEPARTURE_TSTMP              TIMESTAMP 

, WM_ACTUAL_ARRIVAL_TSTMP                TIMESTAMP 

, WM_ACTUAL_SHIPPED_TSTMP                TIMESTAMP 

, WM_DELIVERY_START_TSTMP                TIMESTAMP 

, WM_DELIVERY_END_TSTMP                  TIMESTAMP 

, WM_LAST_TRANSMITTED_TSTMP              TIMESTAMP 

, FLOW_THRU_ALLOC_IN_PROG_FLAG           TINYINT 

, WM_FLOW_THROUGH_ALLOCATION_METHOD      STRING 

, WM_FLOW_THROUGH_ALLOCATION_STATUS      TINYINT 

, WM_BUSINESS_PARTNER_ID                 STRING 

, WM_BILL_OF_LADING_NBR                  STRING 

, WM_MANIF_NBR                           STRING 

, WM_MANIF_TYPE                          STRING 

, WM_PRO_NBR                             STRING 

, WM_EQUIPMENT_CD_ID                     INT 

, WM_EQUIPMENT_CD                        STRING 

, WM_EQUIPMENT_TYPE                      STRING 

, WM_TRACTOR_NBR                         STRING 

, WM_TRACTOR_CARRIER_CD_ID               INT 

, WM_TRACTOR_CARRIER_CD                  STRING 

, WM_REGION_ID                           INT 

, WM_INBOUND_REGION_ID                   INT 

, WM_INBOUND_REGION_NAME                 STRING 

, ASSOCIATED_TO_OUTBOUND_FLAG            TINYINT 

, WM_OUTBOUND_REGION_ID                  INT 

, WM_OUTBOUND_REGION_NAME                STRING 

, WM_ASSIGNED_CARRIER_CD_ID              INT 

, WM_ASSIGNED_CARRIER_CD                 STRING 

, WM_CUT_NBR                             STRING 

, WM_MFG_PLNT                            STRING 

, WM_DOCK_DOOR_ID                        BIGINT 

, WM_MODE_ID                             INT 

, WM_TRAILER_NBR                         STRING 

, TRAILER_CLOSED_FLAG                    TINYINT 

, WM_DESTINATION_TYPE                    STRING 

, WM_VARIANCE_TYPE                       SMALLINT 

, WM_CONTRAC_LOCN                        STRING 

, WM_MHE_SENT                            STRING 

, WM_BUYER_CD                            STRING 

, WM_REP_NAME                            STRING 

, DRIVER_NAME                            STRING 

, FIRM_APPT_FLAG                         TINYINT 

, ALERTS_FLAG                            TINYINT 

, SYSTEM_ALLOCATED_FLAG                  TINYINT 

, COGI_GENERATED_FLAG                    TINYINT 

, WM_SCHEDULE_APPT_IND                   TINYINT 

, WHSE_TRANSFER_FLAG                     TINYINT 

, HAS_NOTES_FLAG                         TINYINT 

, GIFT_FLAG                              TINYINT 

, QUALITY_CHECK_HOLD_UPON_RCPT_FLAG      TINYINT 

, LABEL_PRINT_REQD_FLAG                  TINYINT 

, INITIATE_IND                           STRING 

, ALLOCATION_COMPLETED_FLAG              TINYINT 

, WM_PALLET_FOOTPRINT                    STRING 

, PRE_ALLOCATION_FIT_PCT                 SMALLINT 

, QUALITY_AUDIT_PCT                      DECIMAL(5,2) 

, WM_SHIPPED_LPN_CNT                     INT 

, WM_RECEIVED_LPN_CNT                    INT 

, TOTAL_SHIPPED_QTY                      DECIMAL(16,4) 

, TOTAL_RECEIVED_QTY                     DECIMAL(16,4) 

, TOTAL_WEIGHT                           DECIMAL(13,4) 

, TOTAL_VOLUME                           DECIMAL(13,4) 

, WM_QTY_UOM_ID_BASE                     INT 

, WM_WEIGHT_UOM_ID_BASE                  INT 

, WM_VOLUME_UOM_ID_BASE                  INT 

, SHIPPING_COST                          DECIMAL(13,4) 

, SHIPPING_COST_CURRENCY_CD              STRING 

, WM_CONTACT_NBR                         STRING 

, CONTACT_ADDR_1                         STRING 

, CONTACT_ADDR_2                         STRING 

, CONTACT_ADDR_3                         STRING 

, CONTACT_CITY                           STRING 

, CONTACT_STATE_PROV                     STRING 

, CONTACT_ZIP                            STRING 

, CONTACT_COUNTY                         STRING 

, CONTACT_COUNTRY_CD                     STRING 

, IMPORT_ERROR_FLAG                      TINYINT 

, SOFT_CHECK_ERROR_FLAG                  TINYINT 

, CANCELLED_FLAG                         TINYINT 

, CLOSED_FLAG                            TINYINT 

, REF_FIELD_1                            STRING 

, REF_FIELD_2                            STRING 

, REF_FIELD_3                            STRING 

, REF_FIELD_4                            STRING 

, REF_FIELD_5                            STRING 

, REF_FIELD_6                            STRING 

, REF_FIELD_7                            STRING 

, REF_FIELD_8                            STRING 

, REF_FIELD_9                            STRING 

, REF_FIELD_10                           STRING 

, REF_NUM_1                              DECIMAL(13,5) 

, REF_NUM_2                              DECIMAL(13,5) 

, REF_NUM_3                              DECIMAL(13,5) 

, REF_NUM_4                              DECIMAL(13,5) 

, REF_NUM_5                              DECIMAL(13,5) 

, MISC_INSTR_CD_1                        STRING 

, MISC_INSTR_CD_2                        STRING 

, WM_HIBERNATE_VERSION                   BIGINT 

, WM_EXT_CREATED_TSTMP                   TIMESTAMP 

, WM_CREATED_SOURCE_TYPE                 TINYINT 

, WM_CREATED_SOURCE                      STRING 

, WM_CREATED_TSTMP                       TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE            TINYINT 

, WM_LAST_UPDATED_SOURCE                 STRING 

, WM_LAST_UPDATED_TSTMP                  TIMESTAMP 

, UPDATE_TSTMP                           TIMESTAMP                  not null

, LOAD_TSTMP                             TIMESTAMP                  not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_asn' 
;

--DISTRIBUTE ON (WM_ASN_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ASN_DETAIL" , ***** Creating table: "WM_ASN_DETAIL"


CREATE TABLE  WM_ASN_DETAIL
( LOCATION_ID INT not null

, WM_ASN_DETAIL_ID                       BIGINT              not null

, WM_ASN_ID                              BIGINT 

, WM_ASN_DETAIL_STATUS                   SMALLINT 

, WM_TC_COMPANY_ID                       INT 

, WM_TC_ORDER_ID                         STRING 

, WM_TC_ORDER_LINE_ID                    STRING 

, WM_TC_PURCHASE_ORDERS_ID               STRING 

, WM_TC_PO_LINE_ID                       STRING 

, WM_ORDER_ID                            BIGINT 

, WM_ORDER_LINE_ITEM_ID                  BIGINT 

, WM_PURCHASE_ORDERS_ID                  BIGINT 

, WM_PURCHASE_ORDERS_LINE_ITEM_ID        BIGINT 

, WM_CUT_NBR                             STRING 

, WM_BATCH_NBR                           STRING 

, WM_SEQ_NBR                             STRING 

, WM_ORDER_TYPE_DESC                     STRING 

, WM_REFERENCE_ORDER_NBR                 STRING 

, WM_BUSINESS_PARTNER_ID                 STRING 

, WM_PACKAGE_TYPE_ID                     INT 

, WM_PACKAGE_TYPE_DESC                   STRING 

, WM_PACKAGE_TYPE_INSTANCE               STRING 

, WM_EPC_TRACKING_RFID_VALUE             STRING 

, WM_PRE_RECEIPT_STATUS                  STRING 

, WM_INVN_TYPE                           STRING 

, WM_PUTWY_TYPE                          STRING 

, WM_PROD_STAT                           STRING 

, MFG_PLNT                               STRING 

, WM_REGION_ID                           INT 

, COUNTRY_OF_ORIGIN                      STRING 

, WM_INVENTORY_SEGMENT_ID                BIGINT 

, WM_INV_DISPOSITION                     STRING 

, WM_PPACK_GRP_CD                        STRING 

, WM_EXP_RECEIVE_CONDITION_CD            STRING 

, ASN_RECV_RULES                         STRING 

, WM_DISPOSITION_TYPE                    STRING 

, WM_EXT_PLAN_ID                         STRING 

, MANUFACTURING_TSTMP                    TIMESTAMP 

, SHIP_BY_TSTMP                          TIMESTAMP 

, EXPIRE_TSTMP                           TIMESTAMP 

, INCUBATION_TSTMP                       TIMESTAMP 

, EPC_REQ_ON_ALL_CASES_FLAG              TINYINT 

, ASSOCIATED_TO_OUTBOUND_FLAG            TINYINT 

, WM_PROCESSED_FOR_TRLR_MOVES_FLAG       TINYINT 

, PROCESS_IMMEDIATE_NEEDS_FLAG           TINYINT 

, QUALITY_CHECK_HOLD_UPON_RCPT_FLAG      TINYINT 

, PRICE_TIX_AVAIL_FLAG                   TINYINT 

, WM_SKU_ID                              INT 

, WM_SKU_NAME                            STRING 

, GTIN                                   STRING 

, WM_SKU_ATTR_1                          STRING 

, WM_SKU_ATTR_2                          STRING 

, WM_SKU_ATTR_3                          STRING 

, WM_SKU_ATTR_4                          STRING 

, WM_SKU_ATTR_5                          STRING 

, RETAIL_PRICE                           DECIMAL(16,4) 

, UNITS_ASSIGNED_TO_LPN                  DECIMAL(16,4) 

, SHIPPED_LPN_CNT                        INT 

, RECEIVED_LPN_CNT                       INT 

, SHIPPED_QTY                            DECIMAL(16,4) 

, RECEIVED_QTY                           DECIMAL(16,4) 

, STD_PACK_QTY                           DECIMAL(13,4) 

, STD_CASE_QTY                           DECIMAL(16,4) 

, STD_SUB_PACK_QTY                       DECIMAL(13,4) 

, QTY_CONV_FACTOR                        DECIMAL(17,8) 

, WM_QTY_UOM_ID                          BIGINT 

, WM_QTY_UOM_ID_BASE                     INT 

, ACTUAL_WEIGHT                          DECIMAL(13,4) 

, ACTUAL_WEIGHT_RECEIVED                 DECIMAL(16,4) 

, ACTUAL_WEIGHT_PACK_CNT                 DECIMAL(13,4) 

, NBR_OF_PACK_FOR_CATCH_WT               DECIMAL(13,4) 

, WM_WEIGHT_UOM_ID                       INT 

, WM_WEIGHT_UOM_ID_BASE                  INT 

, LPN_PER_TIER                           INT 

, TIER_PER_PALLET                        INT 

, CHECKSUM                               STRING 

, CANCELLED_FLAG                         TINYINT 

, CLOSED_FLAG                            TINYINT 

, REF_FIELD_1                            STRING 

, REF_FIELD_2                            STRING 

, REF_FIELD_3                            STRING 

, REF_FIELD_4                            STRING 

, REF_FIELD_5                            STRING 

, REF_FIELD_6                            STRING 

, REF_FIELD_7                            STRING 

, REF_FIELD_8                            STRING 

, REF_FIELD_9                            STRING 

, REF_FIELD_10                           STRING 

, REF_NUM_1                              DECIMAL(13,5) 

, REF_NUM_2                              DECIMAL(13,5) 

, REF_NUM_3                              DECIMAL(13,5) 

, REF_NUM_4                              DECIMAL(13,5) 

, REF_NUM_5                              DECIMAL(13,5) 

, WM_HIBERNATE_VERSION                   BIGINT 

, WM_CREATED_SOURCE_TYPE                 TINYINT 

, WM_CREATED_SOURCE                      STRING 

, WM_CREATED_TSTMP                       TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE            TINYINT 

, WM_LAST_UPDATED_SOURCE                 STRING 

, WM_LAST_UPDATED_TSTMP                  TIMESTAMP 

, UPDATE_TSTMP                           TIMESTAMP                  not null

, LOAD_TSTMP                             TIMESTAMP                  not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_asn_detail' 
;

--DISTRIBUTE ON (WM_ASN_DETAIL_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ASN_DETAIL_STATUS" , ***** Creating table: "WM_ASN_DETAIL_STATUS"


CREATE TABLE  WM_ASN_DETAIL_STATUS
( LOCATION_ID INT not null

, WM_ASN_DETAIL_STATUS                   SMALLINT               not null

, WM_ASN_DETAIL_STATUS_DESC              STRING 

, UPDATE_TSTMP                           TIMESTAMP                  not null

, LOAD_TSTMP                             TIMESTAMP                  not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_asn_detail_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ASN_STATUS" , ***** Creating table: "WM_ASN_STATUS"


CREATE TABLE  WM_ASN_STATUS
( LOCATION_ID INT not null

, WM_ASN_STATUS                          SMALLINT               not null

, WM_ASN_STATUS_DESC                     STRING 

, WM_CREATED_TSTMP                       TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                  TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_asn_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_BUSINESS_PARTNER" , ***** Creating table: "WM_BUSINESS_PARTNER"


CREATE TABLE  WM_BUSINESS_PARTNER
( LOCATION_ID INT not null

, WM_TC_COMPANY_ID                        INT                not null

, WM_BUSINESS_PARTNER_ID                  STRING       not null

, WM_BUSINESSS_PARTNER_DESC               STRING 

, WM_BP_ID                                BIGINT 

, WM_BP_COMPANY_ID                        INT 

, WM_BUSINESS_NBR                         STRING 

, ACCREDITED_BP_FLAG                      TINYINT 

, PREFIX                                  STRING 

, ADDR_1                                  STRING 

, ADDR_2                                  STRING 

, ADDR_3                                  STRING 

, CITY                                    STRING 

, STATE_PROV                              STRING 

, POSTAL_CD                               STRING 

, COUNTY                                  STRING 

, COUNTRY_CD                              STRING 

, PHONE_NBR                               STRING 

, ATTR_1                                  TINYINT 

, ATTR_2                                  STRING 

, ATTR_3                                  STRING 

, ATTR_4                                  STRING 

, ATTR_5                                  STRING 

, WM_COMMENT                              STRING 

, MARK_FOR_DELETION_FLAG                  TINYINT 

, WM_HIBERNATE_VERSION                    BIGINT 

, WM_CREATED_SOURCE                       STRING 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_SOURCE                  STRING 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_business_partner' 
;

--DISTRIBUTE ON (WM_TC_COMPANY_ID, WM_BUSINESS_PARTNER_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_CARRIER_CODE" , ***** Creating table: "WM_CARRIER_CODE"


CREATE TABLE  WM_CARRIER_CODE
( LOCATION_ID INT not null

, WM_CARRIER_ID                           BIGINT               not null

, WM_CARRIER_CD                           STRING 

, WM_CARRIER_CD_NAME                      STRING 

, WM_CARRIER_CD_STATUS                    TINYINT 

, WM_TC_COMPANY_ID                        INT 

, WM_TP_COMPANY_ID                        INT 

, WM_TP_COMPANY_NAME                      STRING 

, ADDRESS                                 STRING 

, CITY                                    STRING 

, STATE_PROV                              STRING 

, POSTAL_CD                               STRING 

, COUNTRY_CD                              STRING 

, CARRIER_ADDR_FOR_CL                     STRING 

, WM_CARRIER_TYPE_ID                      SMALLINT 

, WM_EPI_CARRIER_CD                       STRING 

, WM_SCAC_CD                              STRING 

, PAYMENT_METHOD                          STRING 

, PAYMENT_REFERENCE_NBR                   STRING 

, PREPAY_FLAG                             TINYINT 

, AUTO_CREATE_INVOICE_FLAG                TINYINT 

, INVOICE_PAYMENT_TERMS                   STRING 

, VALIDATE_INVOICE                        INT 

, MATCH_PAY_EMAIL                         STRING 

, PAYEE_FLAG                              TINYINT 

, WM_TARIFF_ID                            STRING 

, WM_TAX_ID                               STRING 

, HAZMAT_FLAG                             TINYINT 

, HAZMAT_REG_NBR                          STRING 

, EXPIRY_DT                               TIMESTAMP 

, DAYS_TO_DISPUTE_CLAIM                   INT 

, COMM_METHOD                             STRING 

, MOTOR_CARRIER_NBR                       INT 

, FREIGHT_FRWRDR_NBR                      INT 

, PRO_NBR_LEVEL                           STRING 

, DEFAULT_PALLET_TYPE                     STRING 

, AWB_UOM                                 STRING 

, TRANS_EXCEL                             SMALLINT 

, DOT_NBR                                 STRING 

, BENCHMARK_FLAG                          TINYINT 

, UNION_FLAG                              TINYINT 

, AUTO_DELIVER_FLAG                       TINYINT 

, AUTO_ACCEPT_TYPE                        TINYINT 

, ACCEPT_MSG_REQD_FLAG                    TINYINT 

, APPT_MSG_REQD_FLAG                      TINYINT 

, DEPART_MSG_REQD_FLAG                    TINYINT 

, ARRIVE_MSG_REQD_FLAG                    TINYINT 

, CONFIRM_RECALL_REQD_FLAG                TINYINT 

, CONFIRM_UPDATE_REQD_FLAG                TINYINT 

, BROKER_FLAG                             TINYINT 

, BROKER_RECEIVE_UPDATES_FLAG             TINYINT 

, ALLOW_BOL_ENTRY_FLAG                    TINYINT 

, AUTO_DELIVER_DELAY_HOURS                INT 

, DOCK_SCHEDULE_PERMISSION                TINYINT 

, ALLOW_LOAD_COMPLETION_FLAG              TINYINT 

, RECEIVES_DOCK_INFO_FLAG                 TINYINT 

, ALLOW_TRAILER_ENTRY_FLAG                TINYINT 

, ALLOW_TRACTOR_ENTRY_FLAG                TINYINT 

, ALLOW_CARRIER_CHARGES_VIEW_FLAG         TINYINT 

, SUPPORTS_PARCEL                         TINYINT 

, RECEIVE_FACILITY_INFO_FLAG              TINYINT 

, PRIVATE_FLEET_FLAG                      TINYINT 

, PREFERRED_FLAG                          TINYINT 

, CUT_PARCEL_INVOICE_FLAG                 TINYINT 

, USE_CARRIER_ADDR_FLAG                   TINYINT 

, CLAIM_ON_DETENTION_APPROVAL             TINYINT 

, RES_BASED_COSTING_ALLOWED_FLAG          TINYINT 

, ACTIVITY_BASED_COSTING_FLAG             TINYINT 

, PCT_OVER_CAPACITY                       DECIMAL(13,4) 

, ALLOW_CARRIER_BOOKING_REF_FLAG          TINYINT 

, ALLOW_FORWARDER_AWB_FLAG                TINYINT 

, ALLOW_MASTER_AWB_FLAG                   TINYINT 

, MAX_FLEET_APPROVED_AMT                  DECIMAL(13,2) 

, CHECK_NON_MACHINABLE_FLAG               TINYINT 

, INV_ON_CARR_SIZES_FLAG                  TINYINT 

, PRO_NUMBER_REQD_FLAG                    TINYINT 

, ALLOW_TENDER_REJECT_FLAG                TINYINT 

, ALLOW_COUNTER_OFFER_FLAG                TINYINT 

, DEPART_MSG_REQD_AT_FIRST_STOP_FLAG      TINYINT 

, EPI_SUPPORTS_HOLD_PROCESS_FLAG          TINYINT 

, EPI_SUPPORTS_EOD_FLAG                   TINYINT 

, LOAD_PRO_NEXT_UP_COUNTER                STRING 

, STOP_PRO_NEXT_UP_COUNTER                STRING 

, MOBILE_SHIP_TRACK_FLAG                  TINYINT 

, WM_LANGUAGE_ID                          STRING 

, REF_FIELD_1                             STRING 

, REF_FIELD_2                             STRING 

, REF_FIELD_3                             STRING 

, REF_FIELD_4                             STRING 

, REF_FIELD_5                             STRING 

, REF_NUM_1                               DECIMAL(13,5) 

, REF_NUM_2                               DECIMAL(13,5) 

, REF_NUM_3                               DECIMAL(13,5) 

, REF_NUM_4                               DECIMAL(13,5) 

, REF_NUM_5                               DECIMAL(13,5) 

, MARK_FOR_DELETION_FLAG                  TINYINT 

, WM_CREATED_SOURCE_TYPE                  TINYINT 

, WM_CREATED_SOURCE                       STRING 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE             TINYINT 

, WM_LAST_UPDATED_SOURCE                  STRING 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_carrier_code' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_COMMODITY_CODE" , ***** Creating table: "WM_COMMODITY_CODE"


CREATE TABLE  WM_COMMODITY_CODE
( LOCATION_ID INT not null

, WM_COMMODITY_CD_ID                      BIGINT               not null

, WM_TC_COMPANY_ID                        INT 

, WM_COMMODITY_CD_SHORT_DESC              STRING 

, WM_COMMODITY_CD_LONG_DESC               STRING 

, WM_COMMODITY_CD_SECTION                 SMALLINT 

, WM_COMMODITY_CD_CHAPTER                 SMALLINT 

, MARK_FOR_DELETION_FLAG                  TINYINT 

, WM_CREATED_SOURCE_TYPE                  TINYINT 

, WM_CREATED_SOURCE                       STRING 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE             TINYINT 

, WM_LAST_UPDATED_SOURCE                  STRING 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_commodity_code' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_C_LEADER_AUDIT" , ***** Creating table: "WM_C_LEADER_AUDIT"


CREATE TABLE  WM_C_LEADER_AUDIT
( LOCATION_ID INT not null

, WM_C_LEADER_AUDIT_ID                    INT                not null

, WM_LEADER_USER_ID                       STRING 

, WM_PICKER_USER_ID                       STRING 

, WM_STATUS                               TINYINT 

, WM_ITEM_NAME                            STRING 

, WM_LPN                                  STRING 

, EXPECTED_QTY                            INT 

, ACTUAL_QTY                              INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_c_leader_audit' 
;

--DISTRIBUTE ON (WM_C_LEADER_AUDIT_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_C_TMS_PLAN" , ***** Creating table: "WM_C_TMS_PLAN"


CREATE TABLE  WM_C_TMS_PLAN
( LOCATION_ID INT not null

, WM_C_TMS_PLAN_ID                        INT                not null

, WM_PLAN_ID                              STRING 

, WM_STO_NBR                              STRING 

, WM_ROUTE_ID                             STRING 

, WM_STOP_ID                              INT 

, WM_STOP_LOC                             STRING 

, SPLIT_ROUTE                             STRING 

, WM_SHIP_VIA                             STRING 

, WM_EQUIP_TYPE                           STRING 

, WM_DEL_TYPE                             STRING 

, WM_TMS_CARRIER_ID                       STRING 

, WM_TMS_CARRIER_NAME                     STRING 

, WM_CARRIER_REF_NBR                      STRING 

, WM_TMS_TRAILER_ID                       STRING 

, WM_LOAD_STAT_CD                         STRING 

, WM_STAT_CD                              TINYINT 

, MILES                                   INT 

, DRIVE_TIME                              STRING 

, ERROR_SEQ_NBR                           INT 

, WM_TMS_PLAN_TSTMP                       TIMESTAMP 

, DROP_DEAD_TSTMP                         TIMESTAMP 

, ETA_TSTMP                               TIMESTAMP 

, ORIGIN                                  STRING 

, DESTINATION                             STRING 

, CONSOLIDATOR                            STRING 

, CONS_ADDR_1                             STRING 

, CONS_ADDR_2                             STRING 

, CONS_ADDR_3                             STRING 

, CITY                                    STRING 

, STATE                                   STRING 

, ZIP                                     STRING 

, COUNTRY_CD                              STRING 

, CONTACT_PERSON                          STRING 

, PHONE                                   STRING 

, COMMODITY                               STRING 

, NUM_PALLETS                             INT 

, PLT_WEIGHT                              DECIMAL(13,4) 

, WEIGHT                                  DECIMAL(13,4) 

, VOLUME                                  DECIMAL(13,4) 

, WM_USER_ID                              STRING 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_c_tms_plan' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_DOCK_DOOR" , ***** Creating table: "WM_DOCK_DOOR"


CREATE TABLE  WM_DOCK_DOOR
( LOCATION_ID INT not null

, WM_DOCK_DOOR_ID                         BIGINT               not null

, WM_TC_COMPANY_ID                        INT 

, WM_FACILITY_ID                          INT 

, WM_DOCK_ID                              STRING 

, WM_DOCK_DOOR_NAME                       STRING 

, WM_DOCK_DOOR_DESC                       STRING 

, WM_DOCK_DOOR_STATUS                     SMALLINT 

, WM_OLD_DOCK_DOOR_STATUS                 SMALLINT 

, WM_DOCK_DOOR_BARCODE                    STRING 

, WM_DOCK_DOOR_LOCN_ID                    STRING 

, WM_LOCN_HDR_ID                          INT 

, WM_OUTBD_STAGING_LOCN_ID                STRING 

, WM_FLOWTHRU_ALLOC_SORT_PRTY             STRING 

, WM_SORT_ZONE                            STRING 

, WM_ILM_APPOINTMENT_NBR                  STRING 

, WM_ACTIVITY_TYPE                        STRING 

, WM_APPOINTMENT_TYPE                     STRING 

, TIME_FROM_INDUCTION                     DECIMAL(5,2) 

, WM_PALLETIZATION_SPUR                   STRING 

, MARK_FOR_DELETION_FLAG                  SMALLINT 

, WM_CREATED_SOURCE_TYPE                  SMALLINT 

, WM_CREATED_SOURCE                       STRING 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE             SMALLINT 

, WM_LAST_UPDATED_SOURCE                  STRING 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_dock_door' 
;

--DISTRIBUTE ON (WM_DOCK_DOOR_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_DO_STATUS" , ***** Creating table: "WM_DO_STATUS"


CREATE TABLE  WM_DO_STATUS
( LOCATION_ID INT not null

, WM_ORDER_STATUS_ID                      SMALLINT                not null

, WM_ORDER_STATUS_DESC                    STRING 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_do_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_EQUIPMENT" , ***** Creating table: "WM_EQUIPMENT"


CREATE TABLE  WM_EQUIPMENT
( LOCATION_ID INT not null

, WM_EQUIPMENT_ID                         BIGINT               not null

, WM_TC_COMPANY_ID                        INT 

, WM_EQUIPMENT_CD                         STRING 

, WM_EQUIPMENT_DESC                       STRING 

, WM_MASTER_EQUIPMENT_ID                  BIGINT 

, WM_EQUIPMENT_TYPE_ID                    SMALLINT 

, WM_SHAPE_TYPE                           SMALLINT 

, WM_TRAILER_TYPE_ID                      TINYINT 

, WM_OWNERSHIP_TYPE_ID                    SMALLINT 

, TAX_BAND_NAME                           STRING 

, BACK_IN_TIME                            SMALLINT 

, BACK_OUT_TIME                           SMALLINT 

, NUMBER_OF_AXLES                         SMALLINT 

, PER_USAGE_COST                          DECIMAL(13,2) 

, PER_USAGE_COST_CURRENCY_CD              STRING 

, ALLOW_TRAILER_SWAPPING_FLAG             TINYINT 

, TANDEM_CAPABLE_FLAG                     TINYINT 

, WM_VOLUME_CALC_VALUE                    DECIMAL(16,4) 

, WM_VOLUME_CALC_STANDARD_UOM_ID          INT 

, WM_WEIGHT_EMPTY_VALUE                   DECIMAL(16,4) 

, WM_WEIGHT_EMPTY_STANDARD_UOM_ID         INT 

, WM_WEIGHT_EMPTY_SIZE_UOM_ID             INT 

, WM_PLATED_WEIGHT                        DECIMAL(13,2) 

, WM_PLATED_WEIGHT_SIZE_UOM_ID            INT 

, WM_WEIGHT_VALUE                         DECIMAL(13,4) 

, WM_WEIGHT_STANDARD_UOM_ID               INT 

, WM_EQUIP_DW_HEIGHT_VALUE                DECIMAL(16,4) 

, WM_EQUIP_DW_HEIGHT_STANDARD_UOM_ID      INT 

, WM_EQUIP_HEIGHT_VALUE                   INT 

, WM_EQUIP_HEIGHT_STANDARD_UOM_ID         INT 

, WM_FLOOR_SPACE_VALUE                    BIGINT 

, WM_FLOOR_SPACE_SIZE_UOM_ID              INT 

, WM_DIM_01_VALUE                         DECIMAL(16,4) 

, WM_DIM_01_STANDARD_UOM_ID               INT 

, WM_DIM_02_VALUE                         DECIMAL(16,4) 

, WM_DIM_02_STANDARD_UOM_ID               INT 

, WM_DIM_03_VALUE                         DECIMAL(16,4) 

, WM_DIM_03_STANDARD_UOM_ID               INT 

, MARK_FOR_DELETION                       SMALLINT 

, WM_CREATED_SOURCE_TYPE                  SMALLINT 

, WM_CREATED_SOURCE                       STRING 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE             SMALLINT 

, WM_LAST_UPDATED_SOURCE                  STRING 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_equipment' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_EQUIPMENT_INSTANCE" , ***** Creating table: "WM_EQUIPMENT_INSTANCE"


CREATE TABLE  WM_EQUIPMENT_INSTANCE
( LOCATION_ID INT not null

, WM_EQUIPMENT_INSTANCE_ID                BIGINT               not null

, WM_TC_COMPANY_ID                        INT 

, WM_EQUIP_INST_DESC                      STRING 

, WM_EQUIP_INST_STATUS                    SMALLINT 

, WM_REASON_ID                            INT 

, WM_EQUIPMENT_ID                         BIGINT 

, WM_CONDITION_TYPE                       SMALLINT 

, WM_EQUIP_DOOR_TYPE                      SMALLINT 

, WM_TP_COMPANY_ID                        INT 

, WM_CARRIER_ID                           BIGINT 

, EQUIP_INST_REF_CARRIER                  STRING 

, RIGID_FLAG                              TINYINT 

, WM_RIGID_EQUIPMENT_ID                   BIGINT 

, MAKE                                    STRING 

, MODEL                                   STRING 

, EQUIPMENT_BRCD                          STRING 

, LICENSE_TAG_NBR                         STRING 

, LICENSE_STATE_PROV                      STRING 

, LICENSE_COUNTRY_CD                      STRING 

, WM_OWNERSHIP_TYPE                       SMALLINT 

, PLATED_WEIGHT                           DECIMAL(13,2) 

, WM_PLATED_WEIGHT_SIZE_UOM_ID            INT 

, WM_SLEEPER_TYPE                         SMALLINT 

, TAX_BAND_NAME                           STRING 

, SEAL_NBR                                STRING 

, LOCKED_FLAG                             SMALLINT 

, LOCK_REASON_CD                          STRING 

, COMMENTS                                STRING 

, ALERTS_FLAG                             TINYINT 

, IMPORT_ERROR_FLAG                       TINYINT 

, SOFT_CHECK_ERROR_FLAG                   TINYINT 

, FOB_FLAG                                TINYINT 

, OBC_ONBOARD_FLAG                        TINYINT 

, NON_SMOKING_FLAG                        TINYINT 

, NIGHT_HEATER_FLAG                       TINYINT 

, LABOR_FLAG                              TINYINT 

, PER_USAGE_COST                          DECIMAL(13,2) 

, PER_USAGE_COST_CURRENCY_CD              STRING 

, TANDEM_CAPABLE_FLAG                     TINYINT 

, TANDEM_LEAD_FLAG                        TINYINT 

, WM_ZONE_FACILITY_ID                     BIGINT 

, WM_ZONE_FACILITY_ALIAS_ID               STRING 

, WM_ZONE_TYPE                            SMALLINT 

, CONTRACT_START_TSTMP                    TIMESTAMP 

, CONTRACT_END_TSTMP                      TIMESTAMP 

, WM_ASSIGNED_LOCATION_ID                 BIGINT 

, WM_CURRENT_APPOINTMENT_ID               INT 

, WM_DISPATCH_STATUS                      SMALLINT 

, WM_HOME_DOMICILE_FACILITY_ID            INT 

, WM_HOME_DOMICILE_FAC_ALIAS_ID           STRING 

, WM_HOME_DSP_REGION_ID                   BIGINT 

, WM_CURRENT_FACILITY_ID                  INT 

, WM_CURRENT_FACILITY_ALIAS_ID            STRING 

, WM_CURRENT_LOCATION_ID                  BIGINT 

, LAST_MAINTENANCE_TSTMP                  TIMESTAMP 

, LAST_KNOWN_TSTMP                        TIMESTAMP 

, LAST_KNOWN_TERM                         STRING 

, WM_LAST_KNOWN_FACILITY_ID               INT 

, WM_LAST_KNOWN_FACILITY_ALIAS_ID         STRING 

, WM_LAST_KNOWN_DSP_REGION_ID             BIGINT 

, CUST_LK_LOCATION_NAME                   STRING 

, CUST_LK_ADDRESS                         STRING 

, CUST_LK_CITY                            STRING 

, CUST_LK_STATE                           STRING 

, CUST_LK_POSTAL_CD                       STRING 

, CUST_LK_COUNTRY_CD                      STRING 

, NEXT_AVAILABLE_TSTMP                    TIMESTAMP 

, WM_NEXT_AVAIL_FACILITY_ID               INT 

, NEXT_AVAILABLE_ZIP                      STRING 

, NEXT_AVAILABLE_TERM                     STRING 

, USR_ENT_NEXT_AVAILABLE_TSTMP            TIMESTAMP 

, USR_ENT_NEXT_AVAILABLE_TERM             STRING 

, WM_USR_ENT_NEXT_AVAIL_FACILITY_ID       INT 

, CUST_NA_LOCATION_NAME                   STRING 

, CUST_NA_ADDRESS                         STRING 

, CUST_NA_CITY                            STRING 

, CUST_NA_STATE                           STRING 

, CUST_NA_POSTAL_CD                       STRING 

, CUST_NA_COUNTRY_CD                      STRING 

, WM_MAINTENANCE_FACILITY_ID              BIGINT 

, WM_MAINTENANCE_FACILITY_ALIAS_ID        STRING 

, MAINTENANCE_START_TSTMP                 TIMESTAMP 

, MAINTENANCE_FREQ_WEEKS                  INT 

, MAINTENANCE_DURATION                    DECIMAL(12,4) 

, NEXT_MAINTENANCE_TSTMP                  TIMESTAMP 

, MILEAGE_SCHEDULE                        BIGINT 

, MILEAGE_UOM                             STRING 

, CURRENT_MILEAGE                         DECIMAL(13,2) 

, LAST_MNT_MILEAGE                        DECIMAL(13,2) 

, NEXT_MNT_MILEAGE                        DECIMAL(13,2) 

, WM_HIBERNATE_VERSION                    BIGINT 

, MARK_FOR_DELETION_FLAG                  SMALLINT 

, WM_CREATED_SOURCE_TYPE                  SMALLINT 

, WM_CREATED_SOURCE                       STRING 

, WM_CREATED_DTTM                         TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE             SMALLINT 

, WM_LAST_UPDATED_SOURCE                  STRING 

, WM_LAST_UPDATED_DTTM                    TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_equipment_instance' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_ACT" , ***** Creating table: "WM_E_ACT"


CREATE TABLE  WM_E_ACT
( LOCATION_ID INT not null

, WM_ACT_ID                               INT                not null

, WM_WHSE                                 STRING 

, WM_JOB_FUNC_ID                          INT 

, WM_LABOR_TYPE_ID                        INT 

, WM_LABOR_ACTIVITY_ID                    INT 

, WM_PROC_ZONE_LOCN_IND                   INT 

, WM_OVERRIDE_PROC_ZONE_ID                INT 

, WM_VHCL_TYPE_ID                         INT 

, WM_UNQ_SEED_ID                          INT 

, LM_MAN_EVNT_REQ_APRV_FLAG               TINYINT 

, LM_KIOSK_REQ_APRV_FLAG                  TINYINT 

, REQ_APRV_FLAG                           TINYINT 

, MONITOR_UOM                             STRING 

, WM_DISPLAY_UOM                          STRING 

, WM_THRUPUT_GOAL                         DECIMAL(20,7) 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_act' 
;

--DISTRIBUTE ON (WM_ACT_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_ACT_ELM" , ***** Creating table: "WM_E_ACT_ELM"


CREATE TABLE  WM_E_ACT_ELM
( LOCATION_ID INT not null

, WM_ACT_ID                               INT                not null

, WM_ELM_ID                               INT                not null

, WM_SEQ_NBR                              INT 

, TIME_ALLOW                              DECIMAL(9,4) 

, WM_THRUPUT_MSRMNT                       STRING 

, WM_AVG_ACT_ID                           INT 

, WM_AVG_BY                               STRING 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_act_elm' 
;

--DISTRIBUTE ON (WM_ACT_ID, WM_ELM_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_ACT_ELM_CRIT" , ***** Creating table: "WM_E_ACT_ELM_CRIT"


CREATE TABLE  WM_E_ACT_ELM_CRIT
( LOCATION_ID INT not null

, WM_ACT_ID                               INT                not null

, WM_ELM_ID                               INT                not null

, WM_CRIT_ID                              INT                not null

, WM_CRIT_VAL_ID                          INT                not null

, TIME_ALLOW                              DECIMAL(9,4) 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_act_elm_crit' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_AUD_LOG" , ***** Creating table: "WM_E_AUD_LOG"


CREATE TABLE  WM_E_AUD_LOG
( LOCATION_ID INT not null

, WM_AUD_ID                               DECIMAL(20,0)               not null

, WM_WHSE                                 STRING 

, WM_TRAN_NBR                             INT 

, WM_SKU_ID                               STRING 

, WM_SKU_HNDL_ATTR                        STRING 

, WM_CRITERIA                             STRING 

, WM_SLOT_ATTR                            STRING 

, WM_ACT_ID                               INT 

, WM_LPN                                  STRING 

, WM_COMPONENT_BK                         STRING 

, WM_ELM_ID                               INT 

, WM_FACTOR                               STRING 

, WM_THRUPUT_MSRMNT                       STRING 

, WM_MODULE_TYPE                          STRING 

, WM_TA_MULTIPLIER                        DECIMAL(13,7) 

, WM_ELS_TRAN_ID                          DECIMAL(20,0) 

, WM_CRIT_VAL                             STRING 

, WM_CRIT_TIME                            DECIMAL(13,5) 

, WM_CURR_SLOT                            STRING 

, WM_NEXT_SLOT                            STRING 

, WM_CURR_LOCN_CLASS                      STRING 

, WM_NEXT_LOCN_CLASS                      STRING 

, PFD_TIME                                DECIMAL(20,7) 

, FACTOR_TIME                             DECIMAL(20,7) 

, DISTANCE                                DECIMAL(20,7) 

, HEIGHT                                  DECIMAL(20,7) 

, TRVL_DIR                                STRING 

, ELEM_DESC                               STRING 

, ELEM_TIME                               DECIMAL(13,5) 

, UOM                                     STRING 

, TOT_TIME                                DECIMAL(15,7) 

, TOT_UNITS                               DECIMAL(13,5) 

, UNITS_PER_GRAB                          DECIMAL(13,5) 

, UNIT_PICK_TIME_ALLOW                    DECIMAL(9,4) 

, SLOT_TYPE_TIME_ALLOW                    DECIMAL(9,4) 

, ADDTL_TIME_ALLOW                        DECIMAL(9,4) 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_aud_log' 
;

--DISTRIBUTE ON (WM_AUD_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_CRIT_VAL" , ***** Creating table: "WM_E_CRIT_VAL"


CREATE TABLE  WM_E_CRIT_VAL
( LOCATION_ID INT not null

, WM_CRIT_VAL_ID                          INT                not null

, WM_CRIT_ID                              INT                not null

, WM_CRIT_VAL                             STRING 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_crit_val' 
;

--DISTRIBUTE ON (WM_CRIT_VAL_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_ELM" , ***** Creating table: "WM_E_ELM"


CREATE TABLE  WM_E_ELM
( LOCATION_ID INT not null

, WM_ELM_ID                               INT                not null

, WM_ELM_NAME                             STRING 

, WM_ELM_DESC                             STRING 

, ORIG_NAME                               STRING 

, CORE_FLAG                               TINYINT 

, WM_MSRMNT_ID                            INT 

, TIME_ALLOW                              DECIMAL(9,4) 

, WM_ELM_GRP_ID                           INT 

, WM_UNQ_SEED_ID                          INT 

, WM_SIM_WHSE                             STRING 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_elm' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_ELM_CRIT" , ***** Creating table: "WM_E_ELM_CRIT"


CREATE TABLE  WM_E_ELM_CRIT
( LOCATION_ID INT not null

, WM_ELM_ID                               INT                not null

, WM_CRIT_ID                              INT                not null

, WM_CRIT_VAL_ID                          INT                not null

, TIME_ALLOW                              DECIMAL(9,4) 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_elm_crit' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_EMP_DTL" , ***** Creating table: "WM_E_EMP_DTL"


CREATE TABLE  WM_E_EMP_DTL
( LOCATION_ID INT not null

, WM_EMP_DTL_ID                           INT                not null

, WM_WHSE                                 STRING 

, WM_EMP_ID                               BIGINT 

, EFF_TSTMP                               TIMESTAMP 

, WM_EMP_STAT_ID                          INT 

, WM_SUPERVISOR_EMP_ID                    BIGINT 

, SUPERVISOR_FLAG                         TINYINT 

, WM_ROLE_ID                              INT 

, WM_JOB_FUNC_ID                          INT 

, WM_DEPT_ID                              INT 

, PAY_RATE                                DECIMAL(9,2) 

, WM_PAY_SCALE_ID                         INT 

, WM_SHIFT_ID                             INT 

, STARTUP_TIME                            DECIMAL(20,7) 

, CLEANUP_TIME                            DECIMAL(20,7) 

, DFLT_PERF_GOAL                          DECIMAL(5,2) 

, EXCLUDE_AUTO_CICO_FLAG                  TINYINT 

, USER_DEF_FIELD_1                        STRING 

, USER_DEF_FIELD_2                        STRING 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_COMMENT                              STRING 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_emp_dtl' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_E_EMP_STAT_CODE" , ***** Creating table: "WM_E_EMP_STAT_CODE"


CREATE TABLE  WM_E_EMP_STAT_CODE
( LOCATION_ID INT not null

, WM_EMP_STAT_ID                          INT                not null

, WM_EMP_STAT_CODE                        STRING 

, WM_EMP_STAT_DESC                        STRING 

, WM_UNQ_SEED_ID                          INT 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_emp_stat_code' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_EVNT_SMRY_HDR" , ***** Creating table: "WM_E_EVNT_SMRY_HDR"


CREATE TABLE  WM_E_EVNT_SMRY_HDR
( LOCATION_ID INT not null

, WM_ELS_TRAN_ID                          DECIMAL(20,0)               not null

, WM_WHSE                                 STRING 

, WM_LOGIN_USER_ID                        STRING 

, WM_SHIFT_CD                             STRING 

, WM_ORIG_EVNT_START_TSTMP                TIMESTAMP 

, WM_ORIG_EVNT_END_TSTMP                  TIMESTAMP 

, WM_SCHED_START_TSTMP                    TIMESTAMP 

, WM_ACTUAL_END_TSTMP                     TIMESTAMP 

, WM_EVNT_STAT_CD                         STRING 

, WM_LABOR_TYPE_ID                        INT 

, WM_JOB_FUNC_ID                          INT 

, WM_DEPT_CD                              STRING 

, WM_RESOURCE_GROUP_ID                    STRING 

, WM_LOCN_GRP_ATTR                        STRING 

, WM_EMP_PERF_SMRY_ID                     DECIMAL(20,0) 

, WM_HDR_MSG_ID                           DECIMAL(20,0) 

, WM_ACT_ID                               INT 

, WM_PERF_SMRY_TRAN_ID                    DECIMAL(20,0) 

, WM_SPVSR_LOGIN_USER_ID                  STRING 

, WM_ORIG_LOGIN_USER_ID                   STRING 

, WM_TRAN_NBR                             INT 

, WM_COMP_EMPLOYEE_DAY_ID                 STRING 

, WM_COMP_EVENT_SUMMARY_HEADER_ID         STRING 

, ASSIGNMENT_START_TSTMP                  TIMESTAMP 

, ASSIGNMENT_END_TSTMP                    TIMESTAMP 

, WM_REFLECTIVE_CD                        STRING 

, WM_COMP_ASSIGNMENT_ID                   STRING 

, WM_ADJ_REASON_CD                        STRING 

, WM_ADJ_REF_TRAN_NBR                     DECIMAL(20,0) 

, WM_CO                                   STRING 

, WM_DIV                                  STRING 

, WM_REF_CD                               STRING 

, WM_REF_NBR                              STRING 

, WM_TEAM_STD_SMRY_ID                     INT 

, WM_TEAM_CD                              STRING 

, TEAM_BEGIN_TSTMP                        TIMESTAMP 

, WM_PROC_ZONE_ID                         DECIMAL(20,7) 

, WM_EVENT_TYPE                           STRING 

, WM_EVNT_CTGRY_1                         STRING 

, WM_EVNT_CTGRY_2                         STRING 

, WM_EVNT_CTGRY_3                         STRING 

, WM_EVNT_CTGRY_4                         STRING 

, WM_EVNT_CTGRY_5                         STRING 

, CONFLICT                                STRING 

, WM_APRV_SPVSR                           STRING 

, WM_APRV_SPVSR_TSTMP                     TIMESTAMP 

, WM_TEAM_CHG_ID                          DECIMAL(20,7) 

, VHCL_TYPE                               STRING 

, CONSOL_EVNT_TIME                        DECIMAL(20,7) 

, ACTL_TIME                               DECIMAL(20,7) 

, STD_TIME_ALLOW                          DECIMAL(13,5) 

, EMP_PERF_ALLOW                          DECIMAL(13,5) 

, PERF_ADJ_AMT                            DECIMAL(13,5) 

, SCHED_ADJ_AMT                           DECIMAL(13,5) 

, SCHED_UNITS_QTY                         DECIMAL(20,7) 

, TMU_RECALC_CNT                          INT 

, TOTAL_WEIGHT                            DECIMAL(20,7) 

, PAID_BRK_OVERLAP                        DECIMAL(20,7) 

, UNPAID_BRK_OVERLAP                      DECIMAL(20,7) 

, THRUPUT_GOAL                            DECIMAL(20,7) 

, THRUPUT_MIN                             DECIMAL(20,7) 

, DISPLAY_UOM                             STRING 

, DISPLAY_UOM_QTY                         DECIMAL(20,7) 

, UNIT_MIN                                DECIMAL(20,7) 

, ERROR_CNT                               INT 

, USER_DEF_FIELD_1                        STRING 

, USER_DEF_FIELD_2                        STRING 

, MISC                                    STRING 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, SOURCE                                  STRING 

, WM_SYS_UPDATED_FLAG                     STRING 

, LAST_MOD_BY                             STRING 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_evnt_smry_hdr' 
;

--DISTRIBUTE ON (WM_ELS_TRAN_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_JOB_FUNCTION" , ***** Creating table: "WM_E_JOB_FUNCTION"


CREATE TABLE  WM_E_JOB_FUNCTION
( LOCATION_ID INT not null

, WM_JOB_FUNCTION_ID                      INT                not null

, WM_JOB_FUNCTION_NAME                    STRING 

, WM_JOB_FUNCTION_DESC                    STRING 

, WM_WHSE                                 STRING 

, WM_DEPT_CD                              STRING 

, WM_JF_TYPE                              STRING 

, WM_UNQ_SEED_ID                          INT 

, WM_DEFAULT_ACT_ID                       INT 

, STARTUP_TIME                            DECIMAL(20,7) 

, CLEANUP_TIME                            DECIMAL(20,7) 

, TRANSITION_START_TIME                   DECIMAL(20,7) 

, TRANSITION_END_TIME                     DECIMAL(20,7) 

, APPLY_TEAM_SETUP_TIME_FLAG              TINYINT 

, TEAM_STARTUP_TIME                       DECIMAL(20,7) 

, TEAM_CLEANUP_TIME                       DECIMAL(20,7) 

, TEAM_TRANSITION_START_TIME              DECIMAL(20,7) 

, TEAM_TRANSITION_END_TIME                DECIMAL(20,7) 

, WM_OPS_CD_ID                            INT 

, WM_DFLT_PROC_ZONE_ID                    INT 

, WM_PROC_ZONE_TEMPL_ID                   INT 

, WM_MSRMNT_ID                            INT 

, OBS_THRESHOLD_EP                        DECIMAL(20,7) 

, TRACK_HIST_TIME_FLAG                    TINYINT 

, TRAIN_PERIOD                            INT 

, RETRAIN_PERIOD                          DECIMAL(7,2) 

, OS_ONLY_FLAG                            TINYINT 

, RETRAIN_REQ_DURATION                    DECIMAL(7,2) 

, PERF_GOAL_FLAG                          TINYINT 

, TRAINING_REQD_FLAG                      TINYINT 

, USE_JF_TRAIN_FLAG                       TINYINT 

, WM_PERF_EVAL_PERIOD_ID                  STRING 

, DFLT_MAX_OCCUPANCY                      DECIMAL(20,7) 

, PLAN_EP                                 DECIMAL(13,7) 

, WM_WHSE_VISIBILITY_GROUP                STRING 

, LEVEL_1                                 STRING 

, LEVEL_2                                 STRING 

, LEVEL_3                                 STRING 

, LEVEL_4                                 STRING 

, LEVEL_5                                 STRING 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_job_function' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_E_LABOR_TYPE_CODE" , ***** Creating table: "WM_E_LABOR_TYPE_CODE"


CREATE TABLE  WM_E_LABOR_TYPE_CODE
( LOCATION_ID INT not null

, WM_LABOR_TYPE_ID                        INT                not null

, WM_LABOR_TYPE_CD                        STRING 

, WM_LABOR_TYPE_DESC                      STRING 

, SPVSR_AUTH_REQUIRED_FLAG                TINYINT 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_labor_type_code' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_MSRMNT" , ***** Creating table: "WM_E_MSRMNT"


CREATE TABLE  WM_E_MSRMNT
( LOCATION_ID INT not null

, WM_MSRMNT_ID                            INT                not null

, WM_MSRMNT_CD                            STRING 

, WM_MSRMNT_NAME                          STRING 

, WM_ORIG_MSRMNT_CD                       STRING 

, WM_ORIG_MSRMNT_NAME                     STRING 

, WM_MSRMNT_STATUS_CD                     STRING 

, SYS_CREATED_FLAG                        TINYINT 

, WM_UNIQUE_SEED_ID                       INT 

, SIMULATION_DC_NAME                      STRING 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_msrmnt' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_MSRMNT_RULE" , ***** Creating table: "WM_E_MSRMNT_RULE"


CREATE TABLE  WM_E_MSRMNT_RULE
( LOCATION_ID INT not null

, WM_MSRMNT_ID                            INT                not null

, WM_RULE_NBR                             INT                not null

, WM_MSRMNT_RULE_DESC                     STRING 

, STATUS_FLAG                             STRING 

, THEN_STATEMENT                          STRING 

, ELSE_STATEMENT                          STRING 

, NOTE                                    STRING 

, MISC                                    STRING 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_msrmnt_rule' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_MSRMNT_RULE_CALC" , ***** Creating table: "WM_E_MSRMNT_RULE_CALC"


CREATE TABLE  WM_E_MSRMNT_RULE_CALC
( LOCATION_ID INT not null

, WM_MSRMNT_ID                            INT                not null

, WM_RULE_NBR                             INT                not null

, CALC_SEQ_NBR                            INT                not null

, WM_CALC_TYPE_CD                         STRING 

, CALC_FIELD                              STRING 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_msrmnt_rule_calc' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_MSRMNT_RULE_CONDITION" , ***** Creating table: "WM_E_MSRMNT_RULE_CONDITION"


CREATE TABLE  WM_E_MSRMNT_RULE_CONDITION
( LOCATION_ID INT not null

, WM_MSRMNT_ID                            INT                not null

, WM_RULE_NBR                             INT                not null

, RULE_SEQ_NBR                            INT                not null

, WM_MSRMNT_RULE_CONDITION_DESC           STRING 

, OPEN_PARAN                              STRING 

, AND_OR                                  STRING 

, FIELD_NAME                              STRING 

, OPERATOR                                STRING 

, RULE_COMPARE_VALUE                      STRING 

, CLOSE_PARAN                             STRING 

, FREE_FORM_TEXT                          STRING 

, MISC                                    STRING 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_msrmnt_rule_condition' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_E_SHIFT" , ***** Creating table: "WM_E_SHIFT"


CREATE TABLE  WM_E_SHIFT
( LOCATION_ID INT not null

, WM_SHIFT_ID                             INT                not null

, WM_SHIFT_CD                             STRING 

, WM_SHIFT_DESC                           STRING 

, WM_WHSE                                 STRING 

, WM_SHIFT_EFF_DT DATE

, OT_FLAG                                 TINYINT 

, OT_PAY_FCT                              DECIMAL(5,2) 

, WEEK_MIN_OT_HRS                         DECIMAL(5,2) 

, SCHED_SHIFT                             STRING 

, REPORT_SHIFT                            STRING 

, ROT_SHIFT_FLAG                          TINYINT 

, ROT_SHIFT_NUM_DAYS                      INT 

, ROT_SHIFT_START_TSTMP                   TIMESTAMP 

, MISC_TXT_1                              STRING 

, MISC_TXT_2                              STRING 

, MISC_NUM_1                              DECIMAL(20,7) 

, MISC_NUM_2                              DECIMAL(20,7) 

, WM_USER_ID                              STRING 

, WM_VERSION_ID                           INT 

, WM_CREATE_TSTMP                         TIMESTAMP 

, WM_MOD_TSTMP                            TIMESTAMP 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, DELETE_FLAG                             TINYINT 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_e_shift' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_FACILITY" , ***** Creating table: "WM_FACILITY"


CREATE TABLE  WM_FACILITY
( LOCATION_ID INT not null

, WM_FACILITY_ID                          INT                not null

, WM_WHSE                                 STRING 

, WM_TC_COMPANY_ID                        INT 

, WM_BUSINESS_PARTNER_ID                  STRING 

, WM_FACILITY_NAME                        STRING 

, WM_FACILITY_DESC                        STRING 

, FACILITY_TYPE_BITS                      SMALLINT 

, WM_GROUP_ID                             STRING 

, WM_BUSINESS_GROUP_ID_1                  BIGINT 

, WM_BUSINESS_GROUP_ID_2                  BIGINT 

, FULFILLMENT_GROUP                       STRING 

, STORE_TYPE                              TINYINT 

, STORE_TYPE_GROUPING                     STRING 

, EIN_NBR                                 STRING 

, IATA_CD                                 STRING 

, GLOBAL_LOCN_NBR                         STRING 

, WM_TAX_ID                               STRING 

, DUNS_NBR                                STRING 

, ACCOUNT_CD_NBR                          STRING 

, ON_TIME_PERF_METHOD                     STRING 

, STOP_CONSTRAINT_BITS                    SMALLINT 

, ASN_COMMUNICATION_METHOD                SMALLINT 

, WM_DSP_DR_CONFIGURATION_ID              BIGINT 

, VISIT_GAP                               SMALLINT 

, RECOMMENDATION_CRITERIA                 STRING 

, NBR_OF_SLOTS_TO_SHOW                    SMALLINT 

, AUTO_CREATE_SHIPMENT                    INT 

, LOGO_IMAGE_PATH                         STRING 

, GEN_LAST_SHIPMENT_LEG                   TINYINT 

, WM_DEF_DRIVER_TYPE_ID                   BIGINT 

, WM_DEF_TRACTOR_ID                       BIGINT 

, WM_DEF_EQUIPMENT_ID                     BIGINT 

, DRIVER_AVAIL_HRS                        DECIMAL(5,2) 

, GRP                                     STRING 

, CHAIN                                   STRING 

, ZONE                                    STRING 

, SHIP_VIA                                STRING 

, WAVE_LABEL_TYPE                         STRING 

, PKG_SLIP_TYPE                           STRING 

, PRINT_CD                                STRING 

, CARTON_CNT_TYPE                         STRING 

, CARTON_LABEL_TYPE                       STRING 

, ASSIGN_MERCH_TYPE                       STRING 

, ASSIGN_MERCH_GROUP                      STRING 

, ASSIGN_STORE_DEPT                       STRING 

, WM_INBOUND_REGION_ID                    INT 

, WM_OUTBOUND_REGION_ID                   INT 

, WM_INBOUNDS_RS_AREA_ID                  INT 

, WM_OUTBOUND_RS_AREA_ID                  INT 

, CARTON_CUBNG_INDIC                      TINYINT 

, BUSN_UNIT_CD                            STRING 

, AUDIT_TRANSACTION                       STRING 

, WM_AUDIT_PARTY_ID                       INT 

, CAPTURE_OTHER_MA                        SMALLINT 

, METER_NBR                               BIGINT 

, DIRECT_DELIVERY_ALLOWED                 SMALLINT 

, TRACK_EQUIP_ID_FLAG                     TINYINT 

, STAT_CD                                 TINYINT 

, FLOWTHRU_ALLOC_SORT_PRTY                STRING 

, RLS_HOLD_DT DATE

, RE_COMPUTE_FEASIBLE_EQUIPMENT           SMALLINT 

, SMARTPOST_DC_NBR                        STRING 

, SPLC_CD                                 STRING 

, ERPC_CD                                 STRING 

, R260_CD                                 STRING 

, IATA_SCR_NBR                            STRING 

, TRANS_PLAN_FLOW                         SMALLINT 

, GENERATE_PERPETUAL_EVENT                SMALLINT 

, GENERATE_SEGMENT_EVENT                  SMALLINT 

, GEOFENCE_RADIUS                         DECIMAL(13,5) 

, DROP_IND                                TINYINT 

, HOOK_IND                                TINYINT 

, HANDLER                                 TINYINT 

, IS_CREDIT_AVAILABLE                     TINYINT 

, IS_SHIP_APPT_REQD                       TINYINT 

, IS_RCV_APPT_REQD                        TINYINT 

, IS_DOCK_SCHED_FAC                       TINYINT 

, IS_OPERATIONAL                          TINYINT 

, IS_MAINTENANCE_FACILITY                 TINYINT 

, TRACK_ON_TIME_INDICATOR                 TINYINT 

, ALLOW_OVERLAPPING_APPS                  TINYINT 

, HANDLES_NON_MACHINEABLE                 TINYINT 

, MAINTAINS_PERPETUAL_INVTY               TINYINT 

, IS_CUST_OWNED_FACILITY                  TINYINT 

, IS_VTBP                                 INT 

, USE_INBD_LPN_AS_OUTBD_LPN               STRING 

, PRINT_COO                               STRING 

, PRINT_INV                               STRING 

, PRINT_SED                               STRING 

, PRINT_CANADIAN_CUST_INVC_FLAG           STRING 

, PRINT_DOCK_RCPT_FLAG                    STRING 

, PRINT_NAFTA_COO_FLAG                    STRING 

, PRINT_OCEAN_BOL_FLAG                    STRING 

, PRINT_PKG_LIST_FLAG                     STRING 

, PRINT_SHPR_LTR_OF_INSTR_FLAG            STRING 

, OPEN_DT DATE

, CLOSE_DT DATE

, HOLD_DT DATE

, CUTOFF_TSTMP                            TIMESTAMP 

, DISPATCH_TSTMP                          TIMESTAMP 

, WM_FACILITY_TZ                          SMALLINT 

, DRIVE_IN_TIME                           INT 

, DRIVE_OUT_TIME                          INT 

, CHECK_IN_TIME                           INT 

, CHECK_OUT_TIME                          INT 

, DRIVER_CHECK_IN_TIME                    INT 

, DRIVER_DEBRIEF_TIME                     INT 

, PAPERWORK_TIME                          INT 

, VEHICLE_CHECK_TIME                      INT 

, DISPATCH_INITIATION_TIME                INT 

, PP_HANDLING_TIME                        INT 

, MIN_HANDLING_TIME                       DECIMAL(31,0) 

, DWELL_TIME                              INT 

, PICK_TIME                               INT 

, DROP_HOOK_TIME                          INT 

, TANDEM_DROP_HOOK_TIME                   INT 

, MAX_TRAILER_YARD_TIME                   INT 

, FREE_WAIT_TIME                          INT 

, MAX_WAIT_TIME                           INT 

, LOADING_END_TIME                        INT 

, HAND_OVER_TIME                          INT 

, RESTRICT_FLEET_ASMT_TIME                SMALLINT 

, LOAD_RATE                               DECIMAL(16,4) 

, UNLOAD_RATE                             DECIMAL(16,4) 

, HANDLING_RATE                           DECIMAL(16,4) 

, LOAD_FACTOR_SIZE_VALUE                  DECIMAL(13,2) 

, LOAD_FACTOR_MOT_ID                      BIGINT 

, LOAD_UNLOAD_MOT_ID                      BIGINT 

, LOAD_UNLOAD_SIZE_UOM_ID                 BIGINT 

, LOAD_FACTOR_SIZE_UOM_ID                 BIGINT 

, MAX_CTN                                 INT 

, MAX_PLT                                 INT 

, PARCEL_LENGTH_RATIO                     DECIMAL(16,4) 

, PARCEL_WIDTH_RATIO                      DECIMAL(16,4) 

, PARCEL_HEIGHT_RATIO                     DECIMAL(16,4) 

, MIN_TRAIL_FILL                          SMALLINT 

, OVER_BOOK_PCT                           SMALLINT 

, AVG_HNDLG_COST_PER_LN                   DECIMAL(16,4) 

, AVG_HNDLG_COST_PER_LN_CUR_CD            STRING 

, PENALTY_COST                            DECIMAL(13,4) 

, PENALTY_COST_CURRENCY_CD                STRING 

, EARLY_TOLERANCE_POINT                   INT 

, LATE_TOLERANCE_POINT                    INT 

, EARLY_TOLERANCE_WINDOW                  INT 

, LATE_TOLERANCE_WINDOW                   INT 

, ADDR_KEY_1                              STRING 

, ADDRESS_TYPE                            STRING 

, ADDR_1                                  STRING 

, ADDR_2                                  STRING 

, ADDR_3                                  STRING 

, CITY                                    STRING 

, STATE_PROV                              STRING 

, POSTAL_CD                               STRING 

, COUNTY                                  STRING 

, COUNTRY_CD                              STRING 

, TERRITORY                               STRING 

, DISTRICT                                STRING 

, REGION                                  STRING 

, WHSE_REGION                             INT 

, LONGITUDE                               DECIMAL(12,6) 

, LATITUDE                                DECIMAL(12,6) 

, RTE_NBR                                 STRING 

, RTE_TO                                  STRING 

, RTE_ZIP                                 STRING 

, RTE_ATTR                                STRING 

, RTE_TYPE_1                              STRING 

, RTE_TYPE_2                              STRING 

, PICKUP_AT_STORE                         TINYINT 

, SHIP_TO_STORE                           TINYINT 

, SHIP_FROM_FACILITY                      TINYINT 

, SHIP_MON                                STRING 

, SHIP_TUE                                STRING 

, SHIP_WED                                STRING 

, SHIP_THU                                STRING 

, SHIP_FRI                                STRING 

, SHIP_SAT                                STRING 

, SHIP_SU                                 STRING 

, ACCEPT_IRREG                            STRING 

, MAINTAIN_ON_HAND                        SMALLINT 

, MAINTAIN_IN_TRANSIT                     SMALLINT 

, MAINTAIN_ON_ORDER                       SMALLINT 

, MAINTAIN_DISPOSITION                    SMALLINT 

, MAINTAIN_SUBLOCATION                    SMALLINT 

, MAINTAIN_SEGMENT                        SMALLINT 

, MAINTAIN_CNTRY_OF_ORGN                  SMALLINT 

, MAINTAIN_BATCH_NBR                      SMALLINT 

, MAINTAIN_PROD_STAT                      SMALLINT 

, MAINTAIN_INV_TYPE                       SMALLINT 

, MAINTAIN_ITEM_ATTR_1                    SMALLINT 

, MAINTAIN_ITEM_ATTR_2                    SMALLINT 

, MAINTAIN_ITEM_ATTR_3                    SMALLINT 

, MAINTAIN_ITEM_ATTR_4                    SMALLINT 

, MAINTAIN_ITEM_ATTR_5                    SMALLINT 

, REF_FIELD_1                             STRING 

, REF_FIELD_2                             STRING 

, REF_FIELD_3                             STRING 

, REF_FIELD_4                             STRING 

, REF_FIELD_5                             STRING 

, REF_FIELD_6                             STRING 

, REF_FIELD_7                             STRING 

, REF_FIELD_8                             STRING 

, REF_FIELD_9                             STRING 

, REF_FIELD_10                            STRING 

, REF_NUM_1                               DECIMAL(13,5) 

, REF_NUM_2                               DECIMAL(13,5) 

, REF_NUM_3                               DECIMAL(13,5) 

, REF_NUM_4                               DECIMAL(13,5) 

, REF_NUM_5                               DECIMAL(13,5) 

, SPL_INSTR_CD_1                          STRING 

, SPL_INSTR_CD_2                          STRING 

, SPL_INSTR_CD_3                          STRING 

, SPL_INSTR_CD_4                          STRING 

, SPL_INSTR_CD_5                          STRING 

, SPL_INSTR_CD_6                          STRING 

, SPL_INSTR_CD_7                          STRING 

, SPL_INSTR_CD_8                          STRING 

, SPL_INSTR_CD_9                          STRING 

, SPL_INSTR_CD_10                         STRING 

, MARK_FOR_DELETION_FLAG                  TINYINT 

, WM_HIBERNATE_VERSION                    BIGINT 

, WM_CREATED_SOURCE_TYPE                  TINYINT 

, WM_CREATED_SOURCE                       STRING 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE             TINYINT 

, WM_LAST_UPDATED_SOURCE                  STRING 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_facility' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_ILM_APPOINTMENTS" , ***** Creating table: "WM_ILM_APPOINTMENTS"


CREATE TABLE  WM_ILM_APPOINTMENTS
( LOCATION_ID INT not null

, WM_APPOINTMENT_ID                       INT                not null

, WM_TC_APPOINTMENT_ID                    STRING 

, WM_COMPANY_ID                           INT 

, WM_FACILITY_ID                          INT 

, WM_APPT_TYPE                            SMALLINT 

, WM_APPT_STATUS                          SMALLINT 

, SCHED_DEPARTURE_TSTMP                   TIMESTAMP 

, EST_DEPARTURE_TSTMP                     TIMESTAMP 

, SCHEDULED_TSTMP                         TIMESTAMP 

, CHECK_IN_TSTMP                          TIMESTAMP 

, CHECK_OUT_TSTMP                         TIMESTAMP 

, ACTUAL_CHECK_IN_TSTMP                   TIMESTAMP 

, LOAD_UNLOAD_START_TSTMP                 TIMESTAMP 

, LOAD_UNLOAD_END_TSTMP                   TIMESTAMP 

, LATEST_DELIVERY_TSTMP                   TIMESTAMP 

, WM_CURRENT_LOCATION_ID                  INT 

, APPT_PRIORITY                           INT 

, WM_APPT_CREATION_TYPE                   SMALLINT 

, WM_APPT_REASON_CD                       INT 

, REQUESTED_TSTMP                         TIMESTAMP 

, REQUESTOR_NAME                          STRING 

, WM_REQUEST_COMM_TYPE                    STRING 

, WM_REQUESTOR_TYPE_ID                    SMALLINT 

, WM_RESERVE_TYPE                         TINYINT 

, WM_RESERVED_APPT_ID                     INT 

, WM_REASON_ID                            INT 

, WM_APPT_REASON_ID                       INT 

, WM_CANCELLED_REASON_CD                  INT 

, WM_BUSINESS_PARTNER_ID                  STRING 

, WM_BP_ID                                BIGINT 

, WM_BP_COMPANY_ID                        INT 

, WM_YARD_ID                              INT 

, WM_CONTROL_NO                           STRING 

, WM_DOCK_DOOR_ID                         STRING 

, WM_DOOR_TYPE_ID                         STRING 

, WM_TP_COMPANY_ID                        INT 

, WM_LOADING_TYPE                         STRING 

, WM_ILM_LOAD_POSITION                    SMALLINT 

, WM_SEAL_NBR                             STRING 

, SEAL_NBR_VERIFIED_FLAG                  TINYINT 

, TOP_FRIEGHT_FLAG                        TINYINT 

, TEMPLATE_FLAG                           TINYINT 

, WM_TEMPLATE_ID                          INT 

, HAS_SOFT_CHECK_ERRORS_FLAG              TINYINT 

, HAS_IMPORT_ERROR_FLAG                   TINYINT 

, HAS_ALERTS_FLAG                         TINYINT 

, WM_ORIGIN_FACILITY                      INT 

, WM_EQUIPMENT_ID                         INT 

, WM_CREATED_COMPANY_TYPE                 STRING 

, WM_CARRIER_ID                           INT 

, WM_CARRIER_CD                           STRING 

, WM_DRIVER_ID                            INT 

, DRIVER_NAME                             STRING 

, DRIVER_LICENSE                          STRING 

, DRIVER_LICENSE_STATE                    STRING 

, BEEPER_NBR                              STRING 

, DRIVER_DURATION_ON_YARD                 INT 

, TRAILER_DURATION                        INT 

, WM_COMMENT                              STRING 

, WM_CREATED_TSTMP                        TIMESTAMP 

, WM_CREATED_SOURCE_TYPE                  SMALLINT 

, WM_CREATED_SOURCE                       STRING 

, WM_CREATED_SOURCE_TSTMP                 TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE             SMALLINT 

, WM_LAST_UPDATED_SOURCE                  STRING 

, WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, UPDATE_TSTMP                            TIMESTAMP                   not null

, LOAD_TSTMP                              TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_ilm_appointments' 
;

--DISTRIBUTE ON (WM_APPOINTMENT_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ILM_APPOINTMENT_OBJECTS" , ***** Creating table: "WM_ILM_APPOINTMENT_OBJECTS"


CREATE TABLE  WM_ILM_APPOINTMENT_OBJECTS
( LOCATION_ID INT not null

, WM_ILM_APPOINTMENT_OBJECTS_ID           INT                not null

, WM_COMPANY_ID                           INT 

, WM_APPOINTMENT_ID                       INT 

, WM_STOP_SEQ                             SMALLINT 

, WM_APPT_OBJ_ID                             INT 

, WM_APPT_OBJ_TYPE                           SMALLINT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_ilm_appointment_objects' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ILM_APPOINTMENT_STATUS" , ***** Creating table: "WM_ILM_APPOINTMENT_STATUS"


CREATE TABLE  WM_ILM_APPOINTMENT_STATUS
( LOCATION_ID INT not null

, WM_ILM_APPT_STATUS_CD                      SMALLINT                not null

, WM_ILM_APPT_STATUS_DESC                    STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_ilm_appointment_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ILM_APPOINTMENT_TYPE" , ***** Creating table: "WM_ILM_APPOINTMENT_TYPE"


CREATE TABLE  WM_ILM_APPOINTMENT_TYPE
( LOCATION_ID INT not null

, WM_APPT_TYPE_ID                            SMALLINT                not null

, WM_APPT_TYPE_DESC                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_ilm_appointment_type' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ILM_APPT_EQUIPMENTS" , ***** Creating table: "WM_ILM_APPT_EQUIPMENTS"


CREATE TABLE  WM_ILM_APPT_EQUIPMENTS
( LOCATION_ID INT not null

, WM_APPOINTMENT_ID                          INT                not null

, WM_COMPANY_ID                              INT                not null

, WM_EQUIPMENT_INSTANCE_ID                   INT                not null

, WM_EQUIPMENT_NBR                           STRING 

, WM_EQUIPMENT_LICENSE_NBR                   STRING 

, WM_EQUIPMENT_LICENSE_STATE                 STRING 

, WM_EQUIPMENT_TYPE                          SMALLINT 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_ilm_appt_equipments' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ILM_TASK_STATUS" , ***** Creating table: "WM_ILM_TASK_STATUS"


CREATE TABLE  WM_ILM_TASK_STATUS
( LOCATION_ID INT not null

, WM_ILM_TASK_STATUS                         INT                not null

, WM_ILM_TASK_STATUS_DESC                    STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_ilm_task_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ILM_YARD_ACTIVITY" , ***** Creating table: "WM_ILM_YARD_ACTIVITY"


CREATE TABLE  WM_ILM_YARD_ACTIVITY
( LOCATION_ID INT not null

, WM_ACTIVITY_ID                             INT                not null

, WM_COMPANY_ID                              INT 

, WM_FACILITY_ID                             INT 

, WM_LOCN_ID                                 STRING 

, WM_LOCATION_ID                             INT 

, WM_ACTIVITY_TYPE_ID                        SMALLINT 

, WM_ACTIVITY_SOURCE                         STRING 

, WM_ACTIVITY_TSTMP                          TIMESTAMP 

, WM_APPOINTMENT_ID                          INT 

, WM_TASK_ID                                 INT 

, WM_VISIT_DETAIL_ID                         INT 

, WM_EQUIPMENT_ID_1                          INT 

, WM_EQUIPMENT_ID_2                          INT 

, WM_EQUIP_INS_STATUS_ID                     SMALLINT 

, WM_DRIVER_ID                               INT 

, NO_OF_PALLETS                              INT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_ilm_yard_activity' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ITEM_CBO" , ***** Creating table: "WM_ITEM_CBO"


CREATE TABLE  WM_ITEM_CBO
( LOCATION_ID INT not null

, WM_ITEM_ID                                 INT                not null

, WM_COMPANY_ID                              INT 

, WM_ITEM_NAME                               STRING 

, WM_ITEM_DESC                               STRING 

, WM_ITEM_SHORT_DESC                         STRING 

, WM_ITEM_LONG_DESC                          STRING 

, BRAND                                      STRING 

, ITEM_UPC_GTIN                              STRING 

, ITEM_BAR_CD                                STRING 

, ITEM_ORIENTATION                           STRING 

, ITEM_DISPOSITION                           STRING 

, ITEM_SEASON                                STRING 

, ITEM_SEASON_YEAR                           STRING 

, ITEM_STYLE                                 STRING 

, ITEM_STYLE_SFX                             STRING 

, ITEM_COLOR                                 STRING 

, ITEM_COLOR_SFX                             STRING 

, ITEM_SECOND_DIM                            STRING 

, ITEM_QUALITY                               STRING 

, ITEM_SIZE_DESC                             STRING 

, SIZE_SEQ                                   SMALLINT 

, COLOR_SEQ                                  SMALLINT 

, COLOR_DESC                                 STRING 

, WM_ITEM_QUALITY_CD                         INT 

, STD_BUNDL_QTY                              DECIMAL(9,2) 

, MAX_NEST_NBR                               SMALLINT 

, SELL_THROUGH                               DECIMAL(13,4) 

, UNIT_HEIGHT                                DECIMAL(16,4) 

, UNIT_WIDTH                                 DECIMAL(16,4) 

, UNIT_LENGTH                                DECIMAL(16,4) 

, VARIABLE_WEIGHT                            TINYINT 

, UNIT_WEIGHT                                DECIMAL(16,4) 

, UNIT_VOLUME                                DECIMAL(16,4) 

, CAVITY_LEN                                 DECIMAL(7,2) 

, CAVITY_WD                                  DECIMAL(7,2) 

, CAVITY_HT                                  DECIMAL(7,2) 

, INCREMENTAL_LEN                            DECIMAL(16,4) 

, INCREMENTAL_WD                             DECIMAL(16,4) 

, INCREMENTAL_HT                             DECIMAL(16,4) 

, WM_DIMENSION_UOM_ID                        INT 

, WM_WEIGHT_UOM_ID                           INT 

, WM_VOLUME_UOM_ID                           INT 

, WM_DATABASE_QTY_UOM_ID                     INT 

, WM_DISPLAY_QTY_UOM_ID                      INT 

, WM_MIN_SHIP_INNER_UOM_ID                   STRING 

, WM_BASE_STORAGE_UOM_ID                     INT 

, WM_ITEM_STATUS_CD                          TINYINT 

, WM_PRICE_STATUS                            STRING 

, CATCH_WEIGHT_ITEM_FLAG                     TINYINT 

, COMMODITY_LEVEL_DESC                       STRING 

, WM_CHANNEL_TYPE_ID                         INT 

, WM_PROD_TYPE                               STRING 

, WM_GIFT_CARD_TYPE                          STRING 

, WM_STAB_CD                                 STRING 

, PROTN_FACTOR                               STRING 

, WM_PROTECTION_LEVEL_ID                     INT 

, WM_COMMODITY_CLASS_ID                      INT 

, WM_COMMODITY_CD_ID                         BIGINT 

, WM_PRODUCT_CLASS_ID                        INT 

, WM_UN_NBR_ID                               BIGINT 

, WM_SCHEDULE_B_CD_ID                        INT 

, WM_STCC_CD_ID                              INT 

, WM_SITC_CD_ID                              INT 

, WM_URL                                     STRING 

, WM_ITEM_IMAGE_FILE_NAME                    STRING 

, STACKABLE_ITEM_FLAG                        TINYINT 

, SOLD_ONLINE_FLAG                           TINYINT 

, SOLD_IN_STORES_FLAG                        TINYINT 

, PICKUP_AT_STORE_FLAG                       TINYINT 

, SHIP_TO_STORE_FLAG                         TINYINT 

, RETURNABLE_FLAG                            TINYINT 

, EXCHANGEABLE_FLAG                          TINYINT 

, RETURNABLE_AT_STORE_FLAG                   TINYINT 

, COMMERCE_ATTR_1                            STRING 

, COMMERCE_ATTR_2                            STRING 

, COMMERCE_NUM_ATTR_1                        DECIMAL(13,4) 

, COMMERCE_NUM_ATTR_2                        DECIMAL(13,4) 

, COMMERCE_DATE_ATTR_1                       TIMESTAMP 

, REF_FIELD_1                                STRING 

, REF_FIELD_2                                STRING 

, REF_FIELD_3                                STRING 

, REF_FIELD_4                                STRING 

, REF_FIELD_5                                STRING 

, REF_NUM_1                                  DECIMAL(13,5) 

, REF_NUM_2                                  DECIMAL(13,5) 

, REF_NUM_3                                  DECIMAL(13,5) 

, REF_NUM_4                                  DECIMAL(13,5) 

, REF_NUM_5                                  DECIMAL(13,5) 

, REF_FIELD_6                                STRING 

, REF_FIELD_7                                STRING 

, REF_FIELD_8                                STRING 

, REF_FIELD_9                                STRING 

, REF_FIELD_10                               STRING 

, CUBISCAN_LAST_UPDATED_TSTMP                TIMESTAMP 

, WM_VERSION                                 BIGINT 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_AUDIT_CREATED_SOURCE_TYPE               TINYINT 

, WM_AUDIT_CREATED_SOURCE                    STRING 

, WM_AUDIT_CREATED_TSTMP                     TIMESTAMP 

, WM_AUDIT_LAST_UPDATED_SOURCE_TYPE          TINYINT 

, WM_AUDIT_LAST_UPDATED_SOURCE               STRING 

, WM_AUDIT_LAST_UPDATED_TSTMP                TIMESTAMP 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_item_cbo' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_ITEM_FACILITY_MAPPING_WMS" , ***** Creating table: "WM_ITEM_FACILITY_MAPPING_WMS"


CREATE TABLE  WM_ITEM_FACILITY_MAPPING_WMS
( LOCATION_ID INT not null

, WM_ITEM_FACILITY_MAPPING_ID                DECIMAL(19,0)               not null

, WM_FACILITY_ID                             INT 

, WM_ITEM_ID                                 INT 

, WM_BUSINESS_PARTNER_ID                     STRING 

, WM_PUTWY_TYPE                              STRING 

, WM_ALLOC_TYPE                              STRING 

, WM_AUTO_SUB_CASE                           STRING 

, WM_PICK_LOCN_ASSIGN_TYPE                   STRING 

, WM_DFLT_WAVE_PROC_TYPE                     SMALLINT 

, WM_XCESS_WAVE_NEED_PROC_TYPE               TINYINT 

, WM_VIOLATE_FIFO_ALLOC_QTY_MATCH_FLAG       TINYINT 

, WM_QV_ITEM_GRP                             STRING 

, WM_QUAL_INSPCT_ITEM_GRP                    STRING 

, WM_DFLT_CATCH_WT_METHOD                    STRING 

, WM_DFLT_DATE_MASK                          STRING 

, WM_CARTON_BREAK_ATTR                       STRING 

, WM_CHUTE_ASSIGN_TYPE                       STRING 

, WM_ACTV_REPL_ORGN                          STRING 

, WM_UIN_NBR                                 STRING 

, WM_FIFO_RANGE                              SMALLINT 

, WM_PRTL_CASE_ALLOC_THRESH_UNITS            DECIMAL(13,4) 

, WM_PRTL_CASE_PUTWY_THRESH_UNITS            DECIMAL(13,4) 

, WM_ASSIGN_DYNAMIC_ACTV_PICK_SITE_FLAG      TINYINT 

, WM_ASSIGN_DYNAMIC_CASE_PICK_SITE_FLAG      TINYINT 

, WM_VENDOR_TAGGED_EPC_FLAG                  TINYINT 

, WM_DFLT_MIN_FROM_PREV_LOCN_FLAG            TINYINT 

, WM_LPN_PER_TIER                            INT 

, WM_TIER_PER_PLT                            INT 

, WM_VENDOR_CARTON_PER_TIER                  INT 

, WM_VENDOR_TIER_PER_PLT                     INT 

, WM_ORD_CARTON_PER_TIER                     INT 

, WM_ORD_TIER_PER_PLT                        INT 

, WM_MAX_UNITS_IN_DYNAMIC_ACTV               DECIMAL(13,5) 

, WM_MAX_CASES_IN_DYNAMIC_ACTV               INT 

, WM_MAX_UNITS_IN_DYNAMIC_CASE_PICK          DECIMAL(13,5) 

, WM_MAX_CASES_IN_DYNAMIC_CASE_PICK          INT 

, WM_NBR_OF_DYN_ACTV_PICK_PER_ITEM           SMALLINT 

, WM_NBR_OF_DYN_CASE_PICK_PER_ITEM           SMALLINT 

, WM_CASE_CNT_TSTMP                          TIMESTAMP 

, WM_ACTV_CNT_TSTMP                          TIMESTAMP 

, WM_CASE_PICK_CNT_TSTMP                     TIMESTAMP 

, WM_AVERAGE_MOVEMENT                        DECIMAL(9,2) 

, WM_CASE_SIZE_TYPE                          STRING 

, WM_ITEM_AVG_WT                             DECIMAL(13,4) 

, WM_PICK_RATE                               DECIMAL(9,4) 

, WM_WAGE_VALUE                              DECIMAL(9,4) 

, WM_PACK_RATE                               DECIMAL(13,4) 

, WM_SPL_PROC_RATE                           DECIMAL(9,4) 

, WM_SHELF_DAYS                              INT 

, SLOT_INCR_HT                               DECIMAL(7,2) 

, SLOT_INCR_LEN                              DECIMAL(7,2) 

, SLOT_INCR_WIDTH                            DECIMAL(7,2) 

, WM_SLOT_ROTATE_EACHES_FLAG                 TINYINT 

, WM_SLOT_ROTATE_INNERS_FLAG                 TINYINT 

, WM_SLOT_ROTATE_BINS_FLAG                   TINYINT 

, WM_SLOT_ROTATE_CASES_FLAG                  TINYINT 

, WM_SLOT_3D_SLOTTING_FLAG                   TINYINT 

, WM_SLOT_NEST_EACHES_FLAG                   TINYINT 

, SLOT_MISC_1                                STRING 

, SLOT_MISC_2                                STRING 

, SLOT_MISC_3                                STRING 

, SLOT_MISC_4                                STRING 

, SLOT_MISC_5                                STRING 

, SLOT_MISC_6                                STRING 

, MISC_SHORT_ALPHA_1                         STRING 

, MISC_SHORT_ALPHA_2                         STRING 

, MISC_ALPHA_1                               STRING 

, MISC_ALPHA_2                               STRING 

, MISC_ALPHA_3                               STRING 

, MISC_NUM_1                                 DECIMAL(9,3) 

, MISC_NUM_2                                 DECIMAL(9,3) 

, MISC_NUM_3                                 DECIMAL(9,3) 

, MISC_DATE_1                                TIMESTAMP 

, MISC_DATE_2                                TIMESTAMP 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_AUDIT_CREATED_SOURCE_TYPE               TINYINT 

, WM_AUDIT_CREATED_SOURCE                    STRING 

, WM_AUDIT_CREATED_TSTMP                     TIMESTAMP 

, WM_AUDIT_LAST_UPDATED_SOURCE_TYPE          TINYINT 

, WM_AUDIT_LAST_UPDATED_SOURCE               STRING 

, WM_AUDIT_LAST_UPDATED_TSTMP                TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_item_facility_mapping_wms' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_ITEM_FACILITY_SLOTTING" , ***** Creating table: "WM_ITEM_FACILITY_SLOTTING"


CREATE TABLE  WM_ITEM_FACILITY_SLOTTING
( LOCATION_ID INT not null

, WM_ITEM_FACILITY_MAPPING_ID                INT                not null

, WM_ITEM_ID                                 INT 

, WM_ITEM_NAME                               STRING 

, WM_WHSE_CD                                 STRING 

, WM_SLOTTING_GROUP                          STRING 

, DISC_TRANS_TSTMP                           TIMESTAMP 

, EX_RECEIPT_TSTMP                           TIMESTAMP 

, UNIT_MEASURE                               BIGINT 

, EACH_PER_BIN                               BIGINT 

, NBR_UNITS_PER_BIN                          BIGINT 

, INN_PER_CS                                 BIGINT 

, MAX_SLOTS                                  BIGINT 

, MAX_PALLET_STACKING                        BIGINT 

, MAX_STACKING                               BIGINT 

, MAX_LANES                                  BIGINT 

, WM_RESERVE_RACK_TYPE_ID                    BIGINT 

, AFRAME_HT                                  DECIMAL(9,4) 

, AFRAME_LEN                                 DECIMAL(9,4) 

, AFRAME_WID                                 DECIMAL(9,4) 

, AFRAME_WT                                  DECIMAL(9,4) 

, AFRAME_ALLOW_FLAG                          TINYINT 

, ALLOW_SLU                                  BIGINT 

, ALLOW_SU                                   BIGINT 

, ALLOW_RESERVE                              BIGINT 

, PALPAT_RESERVE                             SMALLINT 

, THREE_D_CALC_DONE                          TINYINT 

, PROP_BORROWING_OBJECT                      BIGINT 

, PROP_BORROWING_SPECIFIC                    DECIMAL(19,0) 

, HEIGHT_CAN_BORROW_FLAG                     TINYINT 

, LENGTH_CAN_BORROW_FLAG                     TINYINT 

, WIDTH_CAN_BORROW_FLAG                      TINYINT 

, WEIGHT_CAN_BORROW_FLAG                     TINYINT 

, VEND_PACK_CAN_BORROW_FLAG                  TINYINT 

, INNER_PACK_CAN_BORROW_FLAG                 TINYINT 

, TI_CAN_BORROW_FLAG                         TINYINT 

, HI_CAN_BORROW_FLAG                         TINYINT 

, QTY_PER_GRAB_CAN_BORROW_FLAG               TINYINT 

, HAND_ATTR_CAN_BORROW_FLAG                  TINYINT 

, FAM_GRP_CAN_BORROW_FLAG                    TINYINT 

, COMMODITY_CAN_BORROW_FLAG                  TINYINT 

, CRUSHABILITY_CAN_BORROW_FLAG               TINYINT 

, HAZARD_CAN_BORROW_FLAG                     TINYINT 

, VEND_CODE_CAN_BORROW_FLAG                  TINYINT 

, HEIGHT_BORROWED_FLAG                       TINYINT 

, LENGTH_BORROWED_FLAG                       TINYINT 

, WIDTH_BORROWED_FLAG                        TINYINT 

, WEIGHT_BORROWED_FLAG                       TINYINT 

, VEND_PACK_BORROWED_FLAG                    TINYINT 

, INNER_PACK_BORROWED_FLAG                   TINYINT 

, TI_BORROWED_FLAG                           TINYINT 

, HI_BORROWED_FLAG                           TINYINT 

, QTY_PER_GRAB_BORROWED_FLAG                 TINYINT 

, HAND_ATTR_BORROWED_FLAG                    TINYINT 

, FAM_GRP_BORROWED_FLAG                      TINYINT 

, COMMODITY_BORROWED_FLAG                    TINYINT 

, CRUSHABILITY_BORROWED_FLAG                 TINYINT 

, HAZARD_BORROWED_FLAG                       TINYINT 

, VEND_CODE_BORROWED_FLAG                    TINYINT 

, ITEM_NUM_1                                 DECIMAL(13,4) 

, ITEM_NUM_2                                 DECIMAL(13,4) 

, ITEM_NUM_3                                 DECIMAL(13,4) 

, ITEM_NUM_4                                 DECIMAL(13,4) 

, ITEM_NUM_5                                 DECIMAL(13,4) 

, ITEM_CHAR_1                                STRING 

, ITEM_CHAR_2                                STRING 

, ITEM_CHAR_3                                STRING 

, ITEM_CHAR_4                                STRING 

, ITEM_CHAR_5                                STRING 

, RESERVED_1                                 STRING 

, RESERVED_2                                 STRING 

, RESERVED_3                                 BIGINT 

, RESERVED_4                                 BIGINT 

, MISC_1_BORROWED_FLAG                       TINYINT 

, MISC_2_BORROWED_FLAG                       TINYINT 

, MISC_3_BORROWED_FLAG                       TINYINT 

, MISC_4_BORROWED_FLAG                       TINYINT 

, MISC_5_BORROWED_FLAG                       TINYINT 

, MISC_6_BORROWED_FLAG                       TINYINT 

, MISC_1_CAN_BORROW_FLAG                     TINYINT 

, MISC_2_CAN_BORROW_FLAG                     TINYINT 

, MISC_3_CAN_BORROW_FLAG                     TINYINT 

, MISC_4_CAN_BORROW_FLAG                     TINYINT 

, MISC_5_CAN_BORROW_FLAG                     TINYINT 

, MISC_6_CAN_BORROW_FLAG                     TINYINT 

, DISCONTINUED_FLAG                          TINYINT 

, MANUAL_FLAG                                TINYINT 

, NEW_FLAG                                   TINYINT 

, PROCESSED_FLAG                             TINYINT 

, WM_MOD_USER                                STRING 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_item_facility_slotting' 
;

--DISTRIBUTE ON (WM_ITEM_FACILITY_MAPPING_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ITEM_GROUP_WMS" , ***** Creating table: "WM_ITEM_GROUP_WMS"


CREATE TABLE  WM_ITEM_GROUP_WMS
( LOCATION_ID INT not null

, WM_ITEM_GROUP_ID                           INT                not null

, WM_ITEM_ID                                 INT 

, WM_GROUP_TYPE                              STRING 

, WM_GROUP_CD                                STRING 

, WM_GROUP_ATTRIBUTE                         STRING 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_item_group_wms' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_ITEM_PACKAGE_CBO" , ***** Creating table: "WM_ITEM_PACKAGE_CBO"


CREATE TABLE  WM_ITEM_PACKAGE_CBO
( LOCATION_ID INT not null

, WM_ITEM_PACKAGE_ID                         INT                not null

, WM_ITEM_ID                                 INT 

, WM_PACKAGE_UOM_ID                          INT 

, GTIN                                       STRING 

, WM_BUSINESS_PARTNER_ID                     STRING 

, STANDARD_UOM_FLAG                          TINYINT 

, ITEM_PACKAGE_QTY                           DECIMAL(16,4) 

, ITEM_PACKAGE_WEIGHT                        DECIMAL(16,4) 

, WM_WEIGHT_UOM_ID                           INT 

, ITEM_PACKAGE_VOLUME                        DECIMAL(16,4) 

, WM_VOLUME_UOM_ID                           DECIMAL(16,4) 

, ITEM_PACKAGE_LENGTH                        DECIMAL(16,4) 

, ITEM_PACKAGE_HEIGHT                        DECIMAL(16,4) 

, ITEM_PACKAGE_WIDTH                         DECIMAL(16,4) 

, WM_DIMENSION_UOM_ID                        INT 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_item_package_cbo' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_ITEM_WMS" , ***** Creating table: "WM_ITEM_WMS"


CREATE TABLE  WM_ITEM_WMS
( LOCATION_ID INT not null

, WM_ITEM_ID                                 INT                not null

, WM_VENDOR_MASTER_ID                        INT 

, WM_VENDOR_ITEM_NBR                         STRING 

, WM_PROD_TYPE                               STRING 

, WM_PROD_CATGRY                             STRING 

, WM_PROD_LINE                               STRING 

, WM_PROD_SUB_GRP                            STRING 

, WM_MERCH_TYPE                              STRING 

, WM_MERCH_GROUP                             STRING 

, WM_CARTON_TYPE                             STRING 

, WM_STORE_DEPT                              STRING 

, WM_SRL_NBR_BRCD_TYPE                       STRING 

, WM_SALE_GRP                                STRING 

, WM_SIZE_RANGE_CD                           STRING 

, WM_SIZE_REL_POSN_IN_TABLE                  STRING 

, WM_VOLTY_CD                                STRING 

, WM_PKG_TYPE                                STRING 

, WM_PKT_CONSOL_ATTR                         STRING 

, WM_BUYER_DISP_CD                           STRING 

, WM_SLOTTING_OPT_STAT_CD                    SMALLINT 

, WM_OPER_CD                                 STRING 

, WM_CRUSH_CD                                STRING 

, ACTIVATION_DT DATE

, LOAD_ATTR                                  STRING 

, TEMP_ZONE                                  STRING 

, TRLR_TEMP_ZONE                             STRING 

, ECCN_NBR                                   STRING 

, EXP_LICN_NBR                               STRING 

, EXP_LICN_XP_DT DATE

, EXP_LICN_SYMBOL                            STRING 

, EXP_LICN_DESC                              STRING 

, WM_ORGN_CERT_CD                            STRING 

, ITAR_EXEMPT_NBR                            STRING 

, NMFC_CD                                    STRING 

, FRT_CLASS                                  STRING 

, CARTON_CNT_TSTMP                           TIMESTAMP 

, TRANS_INVN_CNT_TSTMP                       TIMESTAMP 

, WORK_ORD_CNT_TSTMP                         TIMESTAMP 

, PROMPT_FOR_VENDOR_ITEM_NBR_FLAG            TINYINT 

, PROMPT_PACK_QTY_FLAG                       TINYINT 

, CONVEY_FLAG                                TINYINT 

, MINOR_SRL_NBR_REQ_FLAG                     TINYINT 

, DUP_SRL_NBR_FLAG                           TINYINT 

, ALLOW_RCPT_OLDER_ITEM_FLAG                 TINYINT 

, BASE_INCUB_FLAG                            TINYINT 

, CD_DT_PROMPT_METHOD_FLAG                   STRING 

, TOP_SHELF_ELIGIBLE_FLAG                    TINYINT 

, PREF_CRITERIA_FLAG                         STRING 

, PRODUCER_FLAG                              STRING 

, SRL_NBR_REQD_FLAG                          TINYINT 

, MFG_DATE_REQD_FLAG                         TINYINT 

, XPIRE_DATE_REQD_FLAG                       TINYINT 

, SHIP_BY_DATE_REQD_FLAG                     TINYINT 

, ITEM_ATTR_REQD_FLAG                        TINYINT 

, BATCH_REQD_FLAG                            TINYINT 

, PROD_STAT_REQD_FLAG                        TINYINT 

, COUNTRY_OF_ORIGIN_REQD_FLAG                TINYINT 

, NET_COST_FLAG                              STRING 

, PRICE_TIX_AVAIL_FLAG                       TINYINT 

, WM_UNIT_PRICE                              DECIMAL(16,4) 

, WM_RETAIL_PRICE                            DECIMAL(16,4) 

, WM_PRICE_TKT_TYPE                          STRING 

, MONETARY_VALUE                             DECIMAL(16,4) 

, MV_CURRENCY_CD                             STRING 

, AVG_DLY_DMND                               DECIMAL(13,5) 

, COORD_1                                    INT 

, COORD_2                                    INT 

, WM_DISPOSITION_TYPE                        STRING 

, SHELF_DAYS                                 INT 

, DFLT_INCUB_LOCK                            STRING 

, INCUB_DAYS                                 SMALLINT 

, INCUB_HOURS                                DECIMAL(4,2) 

, LET_UP_PRTY                                INT 

, MARKS_NBRS                                 STRING 

, UNITS_PER_GRAB_PLT                         DECIMAL(13,4) 

, HNDL_ATTR_PLT                              STRING 

, MAX_CASE_QTY                               DECIMAL(9,2) 

, MAX_RCPT_QTY                               DECIMAL(9,2) 

, VOCOLLECT_BASE_WT                          DECIMAL(13,4) 

, VOCOLLECT_BASE_QTY                         DECIMAL(9,2) 

, VOCOLLECT_BASE_ITEM                        STRING 

, CUBE_MULT_QTY                              DECIMAL(13,4) 

, NEST_VOL                                   DECIMAL(13,4) 

, NEST_CNT                                   INT 

, PICK_WT_TOL_AMT                            SMALLINT 

, PICK_WT_TOL_AMNT_ERROR                     SMALLINT 

, WM_PICK_WT_TOL_TYPE                        STRING 

, MHE_WT_TOL_AMT                             SMALLINT 

, WM_MHE_WT_TOL_TYPE                         STRING 

, NBR_OF_DYN_ACTV_PICK_PER_SKU               SMALLINT 

, NBR_OF_DYN_CASE_PICK_PER_SKU               SMALLINT 

, PROD_LIFE_IN_DAY                           INT 

, MIN_RECV_TO_XPIRE_DAYS                     INT 

, MAX_RECV_TO_XPIRE_DAYS                     INT 

, WT_TOL_PCNT                                DECIMAL(5,2) 

, MIN_PCNT_FOR_LPN_SPLIT                     SMALLINT 

, MIN_LPN_QTY_FOR_SPLIT                      DECIMAL(13,5) 

, VOCO_ABS_PICK_TOL_AMT                      INT 

, WM_CONS_PRTY_DATE_CD                       STRING 

, CONS_PRTY_DATE_WINDOW                      STRING 

, CONS_PRTY_DATE_WINDOW_INCR                 SMALLINT 

, CC_UNIT_TOLER_VALUE                        BIGINT 

, CC_WGT_TOLER_VALUE                         DECIMAL(13,4) 

, CC_DLR_TOLER_VALUE                         DECIMAL(13,4) 

, CC_PCNT_TOLER_VALUE                        DECIMAL(13,4) 

, UNITS_PER_PICK_ACTIVE                      DECIMAL(9,2) 

, HNDL_ATTR_ACTIVE                           STRING 

, UNITS_PER_PICK_CASE_PICK                   DECIMAL(9,2) 

, HNDL_ATTR_CASE_PICK                        STRING 

, UNITS_PER_PICK_RESV                        DECIMAL(9,2) 

, HNDL_ATTR_RESV                             STRING 

, WM_MV_SIZE_UOM_ID                          INT 

, WM_HNDL_ATTR_ACT_UOM_ID                    INT 

, WM_HNDL_ATTR_CASE_PICK_UOM_ID              INT 

, CRITCL_DIM_1                               DECIMAL(7,2) 

, CRITCL_DIM_2                               DECIMAL(7,2) 

, CRITCL_DIM_3                               DECIMAL(7,2) 

, SPL_INSTR_1                                STRING 

, SPL_INSTR_2                                STRING 

, SPL_INSTR_CD_1                             STRING 

, SPL_INSTR_CD_2                             STRING 

, SPL_INSTR_CD_3                             STRING 

, SPL_INSTR_CD_4                             STRING 

, SPL_INSTR_CD_5                             STRING 

, SPL_INSTR_CD_6                             STRING 

, SPL_INSTR_CD_7                             STRING 

, SPL_INSTR_CD_8                             STRING 

, SPL_INSTR_CD_9                             STRING 

, SPL_INSTR_CD_10                            STRING 

, WM_DFLT_BATCH_STAT                         STRING 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_AUDIT_CREATED_SOURCE_TYPE               TINYINT 

, WM_AUDIT_CREATED_SOURCE                    STRING 

, WM_AUDIT_CREATED_TSTMP                     TIMESTAMP 

, WM_AUDIT_LAST_UPDATED_SOURCE_TYPE          TINYINT 

, WM_AUDIT_LAST_UPDATED_SOURCE               STRING 

, WM_AUDIT_LAST_UPDATED_TSTMP                TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_item_wms' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_LABOR_ACTIVITY" , ***** Creating table: "WM_LABOR_ACTIVITY"


CREATE TABLE  WM_LABOR_ACTIVITY
( LOCATION_ID INT not null

, WM_LABOR_ACTIVITY_ID                       INT                not null

, WM_LABOR_ACTIVITY_NAME                     STRING 

, WM_LABOR_ACTIVITY_DESC                     STRING 

, WM_ACTIVITY_TYPE                           STRING 

, WM_CRIT_RULE_TYPE                          STRING 

, WM_PERMISSION_ID                           INT 

, WM_COMPANY_ID                              INT 

, WM_AIL_ACTIVITY_FLAG                       STRING 

, WM_PROMPT_LOCN_FLAG                        TINYINT 

, WM_DISPLAY_EPP_FLAG                        TINYINT 

, WM_INCLUDE_TRAVEL_FLAG                     TINYINT 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, LAST_UPDATED_SOURCE_TYPE                   TINYINT 

, LAST_UPDATED_SOURCE                        STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_labor_activity' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LABOR_CRITERIA" , ***** Creating table: "WM_LABOR_CRITERIA"


CREATE TABLE  WM_LABOR_CRITERIA
( LOCATION_ID INT not null

, WM_CRIT_ID                                 INT                not null

, WM_CRIT_CD                                 STRING 

, WM_CRIT_DESC                               STRING 

, RULE_FILTER_FLAG                           TINYINT 

, DATA_TYPE                                  STRING 

, DATA_SIZE                                  INT 

, WM_COMPANY_ID                              INT 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_labor_criteria' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LABOR_MSG" , ***** Creating table: "WM_LABOR_MSG"


CREATE TABLE  WM_LABOR_MSG
( LOCATION_ID INT not null

, WM_LABOR_MSG_ID                            DECIMAL(20,0)               not null

, WM_TC_COMPANY_ID                           INT 

, WM_WHSE                                    STRING 

, WM_LABOR_MSG_STATUS                        INT 

, WM_MSG_STAT_CD                             SMALLINT 

, WM_TRAN_NBR                                INT 

, WM_LOGIN_USER_ID                           STRING 

, WM_ACCOUNT_NAME                            STRING 

, WM_ORIG_ACCOUNT_NAME                       STRING 

, WM_DEPT                                    STRING 

, WM_DIVISION                                STRING 

, WM_JOB_FUNCTION                            STRING 

, WM_SHIFT                                   STRING 

, WM_WHSE_TSTMP                              TIMESTAMP 

, WM_ORIG_EVNT_START_TSTMP                   TIMESTAMP 

, WM_ORIG_EVNT_END_TSTMP                     TIMESTAMP 

, WM_SCHED_START_TIME                        TIMESTAMP 

, WM_SCHED_START_TSTMP                       TIMESTAMP 

, WM_ACTUAL_END_TIME                         TIMESTAMP 

, WM_ACTUAL_END_TSTMP                        TIMESTAMP 

, WM_TASK_NBR                                STRING 

, WM_CASE_NBR                                STRING 

, WM_CARTON_NBR                              STRING 

, WM_WAVE_NBR                                STRING 

, WM_TRAN_DATA_ELS_TRAN_ID                   DECIMAL(20,0) 

, WM_LOCN_GRP_ATTR                           STRING 

, WM_VEHICLE_TYPE                            STRING 

, WM_ENGINE_CD                               STRING 

, WM_ENGINE_GROUP                            STRING 

, WM_TEAM_STD_GRP                            STRING 

, WM_REF_CD                                  STRING 

, WM_REF_NBR                                 STRING 

, WM_MONITOR_SMRY_ID                         INT 

, HAS_MONITOR_MSG_FLAG                       TINYINT 

, RESEND_TRAN_FLAG                           STRING 

, REQ_SAM_REPLY_FLAG                         STRING 

, CONFLICT_FLAG                              TINYINT 

, PRIORITY                                   INT 

, WM_PLAN_ID                                 STRING 

, WM_RESOURCE_GROUP_ID                       STRING 

, WM_ADJ_REASON_CD                           STRING 

, WM_ADJ_REF_TRAN_NBR                        DECIMAL(20,0) 

, WM_INV_NEED_TYPE                           STRING 

, WM_TRANS_TYPE                              STRING 

, WM_TRANS_VALUE                             STRING 

, WM_DISPLAY_UOM                             STRING 

, UOM_QTY                                    DECIMAL(20,7) 

, THRUPUT_MIN                                DECIMAL(20,7) 

, TOTAL_QTY                                  DECIMAL(20,7) 

, ORIG_COMPLETED_WORK                        DECIMAL(20,7) 

, COMPLETED_WORK                             DECIMAL(20,7) 

, USER_DEF_VAL_1                             STRING 

, USER_DEF_VAL_2                             STRING 

, MISC                                       STRING 

, MISC_2                                     STRING 

, MISC_TXT_1                                 STRING 

, MISC_TXT_2                                 STRING 

, MISC_NUM_1                                 DECIMAL(20,7) 

, MISC_NUM_2                                 DECIMAL(20,7) 

, EVNT_CTGRY_1                               STRING 

, EVNT_CTGRY_2                               STRING 

, EVNT_CTGRY_3                               STRING 

, EVNT_CTGRY_4                               STRING 

, EVNT_CTGRY_5                               STRING 

, LEVEL_1                                    STRING 

, LEVEL_2                                    STRING 

, LEVEL_3                                    STRING 

, LEVEL_4                                    STRING 

, LEVEL_5                                    STRING 

, WM_SOURCE                                  STRING 

, WM_MOD_USER_ID                             STRING 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_DTTM                       TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_labor_msg' 
;

--DISTRIBUTE ON (WM_LABOR_MSG_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LABOR_MSG_CRIT" , ***** Creating table: "WM_LABOR_MSG_CRIT"


CREATE TABLE  WM_LABOR_MSG_CRIT
( LOCATION_ID INT not null

, WM_LABOR_MSG_CRIT_ID                       DECIMAL(20,0)               not null

, WM_WHSE                                    STRING 

, WM_LABOR_MSG_ID                            DECIMAL(20,0) 

, WM_TRAN_NBR                                INT 

, CRIT_SEQ_NBR                               INT 

, WM_MSG_STAT_CD                             STRING 

, WM_CRIT_TYPE                               STRING 

, WM_CRIT_VALUE                              STRING 

, MISC_TXT_1                                 STRING 

, MISC_TXT_2                                 STRING 

, MISC_NUM_1                                 DECIMAL(20,7) 

, MISC_NUM_2                                 DECIMAL(20,7) 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, LAST_UPDATED_SOURCE_TYPE                   SMALLINT 

, LAST_UPDATED_SOURCE                        STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_labor_msg_crit' 
;

--DISTRIBUTE ON (WM_LABOR_MSG_CRIT_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LABOR_MSG_DTL" , ***** Creating table: "WM_LABOR_MSG_DTL"


CREATE TABLE  WM_LABOR_MSG_DTL
( LOCATION_ID INT not null

, WM_LABOR_MSG_DTL_ID                        DECIMAL(20,0)               not null

, WM_LABOR_MSG_ID                            DECIMAL(20,0) 

, WM_TRAN_NBR                                INT 

, WM_TRAN_SEQ_NBR                            INT 

, WM_MSG_STAT_CD                             STRING 

, SEQ_NBR                                    STRING 

, WM_COMPANY                                 STRING 

, WM_DIVISION                                STRING 

, WM_START_TSTMP                             TIMESTAMP 

, WM_TC_ILPN_ID                              STRING 

, WM_TC_OLPN_ID                              STRING 

, WM_PUTAWAY_ZONE                            STRING 

, WM_PICK_DETERMINATION_ZONE                 STRING 

, WM_WORK_GROUP                              STRING 

, WM_WORK_AREA                               STRING 

, WM_PULL_ZONE                               STRING 

, WM_ASSIGNMENT_ZONE                         STRING 

, WM_RESOURCE_GROUP_ID                       STRING 

, LOADED_FLAG                                TINYINT 

, WM_ITEM_BAR_CD                             STRING 

, WM_ITEM_NAME                               STRING 

, SEC_DIM                                    STRING 

, WEIGHT                                     DECIMAL(20,7) 

, VOLUME                                     DECIMAL(20,7) 

, SEASON                                     STRING 

, SEASON_YR                                  STRING 

, STYLE                                      STRING 

, STYLE_SFX                                  STRING 

, COLOR                                      STRING 

, COLOR_SFX                                  STRING 

, WM_PALLET_ID                               STRING 

, WM_SIZE_RANGE_CD                           STRING 

, SIZE_REL_POSN_IN_TABLE                     STRING 

, WM_LOCN_SLOT_TYPE                          STRING 

, WM_LOCN_CLASS                              STRING 

, WM_LOCN_GRP_ATTR                           STRING 

, DSP_LOCN                                   STRING 

, LOCN_X_COORD                               DECIMAL(13,5) 

, LOCN_Y_COORD                               DECIMAL(13,5) 

, LOCN_Z_COORD                               DECIMAL(13,5) 

, LOCN_TRAV_AISLE                            STRING 

, LOCN_TRAV_ZONE                             STRING 

, WM_HANDLING_ATTR                           STRING 

, WM_HANDLING_UOM                            STRING 

, QUAL                                       STRING 

, QTY_PER_GRAB                               DECIMAL(13,5) 

, QTY                                        DECIMAL(13,5) 

, BOX_QTY                                    DECIMAL(13,5) 

, INNERPACK_QTY                              DECIMAL(13,5) 

, PACK_QTY                                   DECIMAL(13,5) 

, CASE_QTY                                   DECIMAL(13,5) 

, TIER_QTY                                   DECIMAL(13,5) 

, PALLET_QTY                                 DECIMAL(13,5) 

, CRIT_DIM1                                  DECIMAL(7,2) 

, CRIT_DIM2                                  DECIMAL(7,2) 

, CRIT_DIM3                                  DECIMAL(7,2) 

, MISC                                       STRING 

, MISC_2                                     STRING 

, MISC_NUM_1                                 DECIMAL(13,5) 

, MISC_NUM_2                                 DECIMAL(13,5) 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_labor_msg_dtl' 
;

--DISTRIBUTE ON (WM_LABOR_MSG_DTL_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LABOR_MSG_DTL_CRIT" , ***** Creating table: "WM_LABOR_MSG_DTL_CRIT"


CREATE TABLE  WM_LABOR_MSG_DTL_CRIT
( LOCATION_ID INT not null

, WM_LABOR_MSG_DTL_CRIT_ID                   DECIMAL(20,0)               not null

, WM_WHSE                                    STRING 

, WM_LABOR_MSG_DTL_ID                        DECIMAL(20,0) 

, CRIT_SEQ_NBR                               INT 

, WM_TRAN_NBR                                INT 

, WM_MSG_STAT_CD                             STRING 

, WM_CRIT_TYPE                               STRING 

, WM_CRIT_VALUE                              STRING 

, MISC_TXT_1                                 STRING 

, MISC_TXT_2                                 STRING 

, MISC_NUM_1                                 DECIMAL(20,7) 

, MISC_NUM_2                                 DECIMAL(20,7) 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_labor_msg_dtl_crit' 
;

--DISTRIBUTE ON (WM_LABOR_MSG_DTL_CRIT_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LABOR_TRAN_DTL_CRIT" , ***** Creating table: "WM_LABOR_TRAN_DTL_CRIT"


CREATE TABLE  WM_LABOR_TRAN_DTL_CRIT
( LOCATION_ID INT not null

, WM_LABOR_TRAN_DTL_CRIT_ID                  DECIMAL(20,0)               not null

, WM_WHSE                                    STRING 

, WM_LABOR_TRAN_DTL_ID                       DECIMAL(20,0) 

, WM_TRAN_NBR                                INT 

, CRIT_SEQ_NBR                               INT 

, WM_CRIT_TYPE                               STRING 

, WM_CRIT_VALUE                              STRING 

, MISC_TXT_1                                 STRING 

, MISC_TXT_2                                 STRING 

, MISC_NUM_1                                 DECIMAL(20,7) 

, MISC_NUM_2                                 DECIMAL(20,7) 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_labor_tran_dtl_crit' 
;

--DISTRIBUTE ON (WM_LABOR_TRAN_DTL_CRIT_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LOCN_GRP" , ***** Creating table: "WM_LOCN_GRP"


CREATE TABLE  WM_LOCN_GRP
( LOCATION_ID INT not null

, WM_LOCN_GRP_ID                             INT                not null

, WM_GRP_TYPE                                SMALLINT 

, WM_LOCN_ID                                 STRING 

, WM_GRP_ATTR                                STRING 

, WM_LOCN_HDR_ID                             INT 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_locn_grp' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_LOCN_HDR" , ***** Creating table: "WM_LOCN_HDR"


CREATE TABLE  WM_LOCN_HDR
( LOCATION_ID INT not null

, WM_LOCN_HDR_ID                             INT                not null

, WM_LOCN_ID                                 STRING 

, WM_WHSE                                    STRING 

, WM_FACILITY_ID                             INT 

, WM_LOCN_CLASS                              STRING 

, WM_LOCN_BRCD                               STRING 

, WM_DSP_LOCN                                STRING 

, WM_SKU_DEDCTN_TYPE                         STRING 

, WM_SLOT_TYPE                               STRING 

, WM_PUTWY_ZONE                              STRING 

, WM_PULL_ZONE                               STRING 

, WM_PICK_DETRM_ZONE                         STRING 

, WM_LOCN_PICK_SEQ                           STRING 

, WM_LOCN_PUTWY_SEQ                          STRING 

, WM_LOCN_DYN_ASSGN_SEQ                      STRING 

, WM_WORK_GRP                                STRING 

, WM_WORK_AREA                               STRING 

, WM_VOCO_INTRNL_REVERSE_BRCD                STRING 

, WM_CHECK_DIGIT                             STRING 

, WM_LAST_FROZN_TSTMP                        TIMESTAMP 

, WM_LAST_CNT_TSTMP                          TIMESTAMP 

, CYCLE_CNT_PENDING_FLAG                     TINYINT 

, PRT_LABEL_FLAG                             TINYINT 

, SLOT_UNUSABLE_FLAG                         TINYINT 

, LEN                                        DECIMAL(16,4) 

, WIDTH                                      DECIMAL(16,4) 

, HT     DECIMAL(16,4) 

, X_COORD                                    DECIMAL(13,5) 

, Y_COORD                                    DECIMAL(13,5) 

, Z_COORD                                    DECIMAL(13,5) 

, AREA                                       STRING 

, ZONE                                       STRING 

, AISLE                                      STRING 

, BAY                                        STRING 

, LVL                                        STRING 

, POSN                                       STRING 

, TRAVEL_AISLE                               STRING 

, TRAVEL_ZONE                                STRING 

, WM_STORAGE_UOM                             STRING 

, WM_PICK_UOM                                STRING 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_locn_hdr' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_LPN" , ***** Creating table: "WM_LPN"


CREATE TABLE  WM_LPN
( LOCATION_ID INT not null

, WM_LPN_ID                                  BIGINT               not null

, WM_TC_LPN_ID                               STRING 

, WM_PARENT_LPN_ID                           BIGINT 

, WM_TC_PARENT_LPN_ID                        STRING 

, WM_TC_REFERENCE_LPN_ID                     STRING 

, WM_SPLIT_LPN_ID                            BIGINT 

, WM_VENDOR_LPN_NBR                          STRING 

, WM_TC_COMPANY_ID                           INT 

, WM_BUSINESS_PARTNER_ID                     STRING 

, WM_ORDER_ID                                BIGINT 

, WM_INTERNAL_ORDER_ID                       STRING 

, WM_TC_ORDER_ID                             STRING 

, WM_ORDER_SPLIT_ID                          BIGINT 

, WM_CROSSDOCK_TC_ORDER_ID                   STRING 

, WM_WORK_ORD_NBR                            STRING 

, WM_PURCHASE_ORDERS_ID                      BIGINT 

, WM_TC_PURCHASE_ORDERS_ID                   STRING 

, WM_SHIPMENT_ID                             BIGINT 

, WM_TC_SHIPMENT_ID                          STRING 

, WM_ORIGINAL_TC_SHIPMENT_ID                 STRING 

, WM_RECEIVED_TC_SHIPMENT_ID                 STRING 

, WM_ASN_ID                                  BIGINT 

, WM_TC_ASN_ID                               STRING 

, WM_PLANNED_TC_ASN_ID                       STRING 

, WM_MASTER_BOL_NBR                          STRING 

, WM_BOL_NBR                                 STRING 

, WM_LPN_TYPE                                SMALLINT 

, WM_LPN_LABEL_TYPE                          STRING 

, WM_LPN_STATUS                              SMALLINT 

, WM_LPN_STATUS_UPDATED_TSTMP                TIMESTAMP 

, WM_LPN_FACILITY_STATUS                     SMALLINT 

, WM_LPN_FACILITY_STAT_UPDATED_TSTMP         TIMESTAMP 

, WM_LPN_DISP_STATUS                         STRING 

, WM_WAVE_NBR                                STRING 

, WM_WAVE_SEQ_NBR                            INT 

, WM_WAVE_STAT_CD                            TINYINT 

, WM_BUILD_LPN_BATCH_ID                      BIGINT 

, WM_PACK_WAVE_NBR                           STRING 

, WM_PARCEL_SHIPMENT_NBR                     STRING 

, WM_REF_SHIPMENT_NBR                        STRING 

, WM_TRACKING_NBR                            STRING 

, WM_ALT_TRACKING_NBR                        STRING 

, WM_RETURN_TRACKING_NBR                     STRING 

, WM_RETURN_TRACKING_NBR_2                   STRING 

, WM_RETURN_REFERENCE_NBR                    STRING 

, WM_MANIFEST_NBR                            STRING 

, WM_MASTER_PACK_ID                          STRING 

, WM_EPI_PACKAGE_ID                          STRING 

, WM_EPI_PACKAGE_STATUS                      STRING 

, WM_EPI_SHIPMENT_ID                         STRING 

, WM_EPI_MANIFEST_ID                         STRING 

, WM_EPI_DOC_OUTPUT_TYPE                     STRING 

, WM_SHIP_VIA                                STRING 

, WM_INIT_SHIP_VIA                           STRING 

, WM_ORIGINAL_ASSIGNED_SHIP_VIA              STRING 

, WM_PATH_ID                                 BIGINT 

, WM_ROUTING_LANE_ID                         INT 

, WM_ROUTING_LANE_DETAIL_ID                  BIGINT 

, WM_RATING_LANE_ID                          INT 

, WM_RATING_LANE_DETAIL_ID                   BIGINT 

, WM_ROUTE_RULE_ID                           INT 

, WM_STATIC_ROUTE_ID                         INT 

, WM_RTS_ID                                  BIGINT 

, TRAILER_STOP_SEQ_NBR                       INT 

, WM_IDEAL_PICK_TSTMP                        TIMESTAMP 

, WM_LAST_FROZEN_TSTMP                       TIMESTAMP 

, WM_LAST_COUNTED_TSTMP                      TIMESTAMP 

, WM_CALCULATED_CUT_OFF_TSTMP                TIMESTAMP 

, WM_MANUFACTURED_TSTMP                      TIMESTAMP 

, WM_MANUFACT_PLANT_FACILITY_ALIAS           STRING 

, WM_EXPIRATION_DT DATE

, WM_RCVD_TSTMP                              TIMESTAMP 

, WM_INCUBATION_DT DATE

, WM_SHIP_BY_DT DATE

, WM_SCHED_SHIP_TSTMP                        TIMESTAMP 

, WM_SHIPPED_TSTMP                           TIMESTAMP 

, WM_SCHEDULED_DELIVERY_TSTMP                TIMESTAMP 

, WM_PLAN_LOAD_ID                            BIGINT 

, WM_LOADED_TSTMP                            TIMESTAMP 

, WM_LOADER_USER_ID                          STRING 

, WM_LOADED_POSN                             STRING 

, WM_PICKER_USER_ID                          STRING 

, WM_PACKER_USER_ID                          STRING 

, WM_DELV_RECIPIENT_NAME                     STRING 

, WM_DELV_RECEIPT_TSTMP                      TIMESTAMP 

, DELV_ON_TIME_FLAG                          TINYINT 

, DELV_COMPL_FLAG                            TINYINT 

, WM_DISTRIBUTION_LEG_CARRIER_ID             BIGINT 

, WM_DISTRIBUTION_LEG_MODE_ID                INT 

, WM_DISTRIBUTION_LEV_SVCE_LEVEL_ID          INT 

, WM_VOCOLLECT_ASSIGN_ID                     INT 

, WM_VOCO_INTRN_REVERSE_ID                   STRING 

, WM_VOCO_INTRN_REVERSE_PALLET_ID            STRING 

, WM_PICK_SUB_LOCN_ID                        STRING 

, WM_CURR_SUB_LOCN_ID                        STRING 

, WM_PREV_SUB_LOCN_ID                        STRING 

, WM_DEST_SUB_LOCN_ID                        STRING 

, WM_C_FACILITY_ID                           BIGINT 

, WM_C_FACILITY_ALIAS_ID                     STRING 

, WM_O_FACILITY_ID                           BIGINT 

, WM_O_FACILITY_ALIAS_ID                     STRING 

, WM_D_FACILITY_ID                           BIGINT 

, WM_D_FACILITY_ALIAS_ID                     STRING 

, WM_ZONE_SKIP_HUB_FACILITY_ALIAS_ID         STRING 

, WM_ZONE_SKIP_HUB_FACILITY_ID               INT 

, WM_PLANING_DEST_FACILITY_ALIAS_ID          INT 

, WM_PLANING_DEST_FACILITY_ID                INT 

, WM_FINAL_DEST_FACILITY_ALIAS_ID            STRING 

, WM_FINAL_DEST_FACILITY_ID                  BIGINT 

, WM_CHUTE_ID                                STRING 

, WM_CHUTE_ASSIGN_TYPE                       STRING 

, WM_DISPOSITION_TYPE                        STRING 

, WM_DISPOSITION_SOURCE_ID                   INT 

, WM_TRANSITIONAL_INVENTORY_TYPE             SMALLINT 

, WM_CONSUMPTION_SEQUENCE                    STRING 

, WM_CONSUMPTION_PRIORITY                    STRING 

, WM_CONSUMPTION_PRIORITY_TSTMP              TIMESTAMP 

, WM_RETURN_DISPOSITION_CD                   STRING 

, WM_FINAL_DISPOSITION_CD                    STRING 

, WM_PACKAGE_TYPE_ID                         INT 

, WM_LPN_SIZE_TYPE_ID                        INT 

, WM_DELIVERY_TYPE                           STRING 

, WM_PUTAWAY_TYPE                            STRING 

, WM_VARIANCE_TYPE                           SMALLINT 

, WM_BILLING_METHOD                          INT 

, WM_RECEIPT_TYPE                            TINYINT 

, WM_PRE_RECEIPT_STATUS                      STRING 

, WM_EXPORT_LICENSE_NBR                      STRING 

, WM_EXPORT_LICENSE_SYMBOL                   STRING 

, WM_EXPORT_INFO_CD                          STRING 

, WM_PACKWAVE_RULE_ID                        INT 

, WM_CONS_RUN_ID                             BIGINT 

, SPECIAL_PERMITS                            STRING 

, CN22_NBR                                   STRING 

, END_OLPN_TSTMP                             TIMESTAMP 

, XREF_OLPN                                  STRING 

, REGULATION_SET                             STRING 

, LPN_BREAK_ATTR                             STRING 

, WM_LPN_PRINT_GROUP_CD                      STRING 

, SEQ_RULE_PRIORITY                          INT 

, WM_LPN_DIVERT_CD                           STRING 

, INTERNAL_ORDER_CONSOL_ATTR                 STRING 

, WM_WHSE_INTERNAL_EVENT_CD                  STRING 

, WM_SELECTION_RULE_ID                       INT 

, WM_LPN_CREATION_CD                         TINYINT 

, WM_LOAD_SEQUENCE                           INT 

, RATE_ZONE                                  STRING 

, WM_FRT_FORWARDER_ACCT_NBR                  STRING 

, INTL_GOODS_DESC                            STRING 

, SHIPMENT_PRINT_SED                         SMALLINT 

, REPRINT_SHIPPING_LABEL                     SMALLINT 

, WM_PRINTING_RULE_ID                        INT 

, WM_PHYSICAL_ENTITY_CD                      STRING 

, WM_QUAL_AUD_STAT_CD                        TINYINT 

, WM_AUDITOR_USER_ID                         STRING 

, LPN_LEVEL_ROUTING                          TINYINT 

, PENDING_CANCELLATION_FLAG                  TINYINT 

, PALLET_OPEN_FLAG                           TINYINT 

, MASTER_LPN_FLAG                            TINYINT 

, PALLET_MASTER_LPN_FLAG                     TINYINT 

, ACTIVE_LPN_FLAG                            TINYINT 

, LPN_SHIPPED_FLAG                           TINYINT 

, SINGLE_LINE_LPN_FLAG                       TINYINT 

, ALL_PAKD_IN_ONE_FLAG                       TINYINT 

, MHE_LOADED_FLAG                            TINYINT 

, RECEIPT_VARIANCE_FLAG                      TINYINT 

, HAS_ALERTS_FLAG                            TINYINT 

, ERROR_FLAG                                 TINYINT 

, WARNING_FLAG                               TINYINT 

, QA_FLAG                                    TINYINT 

, CONVEYABLE_LPN_FLAG                        TINYINT 

, STD_QTY_FLAG                               TINYINT 

, OVERPACK_ALLOWED_FLAG                      TINYINT 

, STAGE_INDICATOR                            TINYINT 

, INBOUND_OUTBOUND_INDICATOR                 STRING 

, OUT_OF_ZONE_INDICATOR                      STRING 

, LABEL_PRINT_REQD_FLAG                      TINYINT 

, NON_INVENTORY_LPN_FLAG                     TINYINT 

, EPC_MATCH_FLAG                             TINYINT 

, ZONE_SKIPPED_FLAG                          TINYINT 

, SERVICE_LEVEL_INDICATOR                    STRING 

, DOCUMENTS_ONLY                             SMALLINT 

, NON_MACHINEABLE_FLAG                       TINYINT 

, PROCESS_IMMEDIATE_NEEDS_FLAG               TINYINT 

, FAILED_GUARANTEED_DELIVERY                 STRING 

, CHASE_CREATED_FLAG                         TINYINT 

, PROCESSED_FOR_TRLR_MOVES_FLAG              TINYINT 

, EXCEPTION_OLPN_FLAG                        TINYINT 

, HAS_NOTES_FLAG                             TINYINT 

, WM_CUSTOMER_ID                             BIGINT 

, WM_ITEM_ID                                 INT 

, WM_ITEM_NAME                               STRING 

, WM_PALLET_CONTENT_CD                       STRING 

, LPN_NBR_X_OF_Y                             INT 

, PALLET_X_OF_Y                              INT 

, CONTAINER_TYPE                             STRING 

, CONTAINER_SIZE                             STRING 

, SPUR_LANE                                  STRING 

, SPUR_POSITION                              STRING 

, FIRST_ZONE                                 STRING 

, LAST_ZONE                                  STRING 

, NBR_OF_ZONES                               SMALLINT 

, WM_QTY_UOM_ID_BASE                         INT 

, REPRINT_CNT                                TINYINT 

, CASH_ON_DELIVERY_FLAG                      TINYINT 

, CASH_ON_DELIVERY_AMT                       DECIMAL(9,2) 

, CASH_ON_DELIVERY_PAYMENT_METHOD            INT 

, RATED_WEIGHT                               DECIMAL(13,4) 

, RATE_WEIGHT_TYPE                           SMALLINT 

, ESTIMATED_WEIGHT                           DECIMAL(13,4) 

, WEIGHT                                     DECIMAL(16,4) 

, CONTAINS_DRY_ICE_FLAG                      TINYINT 

, DRY_ICE_WT                                 DECIMAL(13,4) 

, WM_WEIGHT_UOM_ID_BASE                      INT 

, ESTIMATED_VOLUME                           DECIMAL(13,4) 

, ACTUAL_VOLUME                              DECIMAL(13,4) 

, WM_VOLUME_UOM_ID_BASE                      INT 

, LENGTH                                     DECIMAL(16,4) 

, WIDTH                                      DECIMAL(16,4) 

, HEIGHT                                     DECIMAL(16,4) 

, OVERSIZE_LENGTH                            SMALLINT 

, WM_CUBE_UOM                                STRING 

, STD_LPN_QTY                                DECIMAL(13,4) 

, TOTAL_LPN_QTY                              DECIMAL(13,4) 

, TIER_QTY                                   DECIMAL(9,2) 

, DIRECTED_QTY                               DECIMAL(9,2) 

, SALVAGE_PACK                               TINYINT 

, SALVAGE_PACK_QTY                           DECIMAL(13,4) 

, PICK_DELIVERY_DURATION                     DECIMAL(5,2) 

, Q_VALUE                                    DECIMAL(2,1) 

, LPN_MONETARY_VALUE                         DECIMAL(11,2) 

, LPN_MONETARY_VALUE_CURR_CD                 STRING 

, BASE_CHARGE                                DECIMAL(9,2) 

, BASE_CHARGE_CURR_CD                        STRING 

, ADDNL_OPTION_CHARGE                        DECIMAL(9,2) 

, ADDNL_OPTION_CHARGE_CURR_CD                STRING 

, INSUR_CHARGE                               DECIMAL(9,2) 

, INSUR_CHARGE_CURR_CD                       STRING 

, ACTUAL_CHARGE                              DECIMAL(9,2) 

, ACTUAL_CHARGE_CURR_CD                      STRING 

, PRE_BULK_BASE_CHARGE                       DECIMAL(9,2) 

, PRE_BULK_BASE_CHARGE_CURR_CD               STRING 

, PRE_BULK_ADD_OPT_CHR                       DECIMAL(9,2) 

, PRE_BULK_ADD_OPT_CHR_CURR_CD               STRING 

, DIST_CHARGE                                DECIMAL(11,2) 

, DIST_CHARGE_CURR_CD                        STRING 

, FREIGHT_CHARGE                             DECIMAL(11,2) 

, FREIGHT_CHARGE_CURR_CD                     STRING 

, REF_FIELD_1                                STRING 

, REF_FIELD_2                                STRING 

, REF_FIELD_3                                STRING 

, REF_FIELD_4                                STRING 

, REF_FIELD_5                                STRING 

, REF_FIELD_6                                STRING 

, REF_FIELD_7                                STRING 

, REF_FIELD_8                                STRING 

, REF_FIELD_9                                STRING 

, REF_FIELD_10                               STRING 

, REF_NUM_1                                  DECIMAL(13,5) 

, REF_NUM_2                                  DECIMAL(13,5) 

, REF_NUM_3                                  DECIMAL(13,5) 

, REF_NUM_4                                  DECIMAL(13,5) 

, REF_NUM_5                                  DECIMAL(13,5) 

, MISC_INSTR_CD_1                            STRING 

, MISC_INSTR_CD_2                            STRING 

, MISC_INSTR_CD_3                            STRING 

, MISC_INSTR_CD_4                            STRING 

, MISC_INSTR_CD_5                            STRING 

, MISC_NUM_1                                 DECIMAL(9,2) 

, MISC_NUM_2                                 DECIMAL(9,2) 

, MARK_FOR                                   STRING 

, WM_HIBERNATE_VERSION                       INT 

, WM_VERSION_NBR                             BIGINT 

, WM_EXT_CREATED_TSTMP                       TIMESTAMP 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_lpn' 
;

--DISTRIBUTE ON (WM_LPN_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LPN_AUDIT_RESULTS" , ***** Creating table: "WM_LPN_AUDIT_RESULTS"


CREATE TABLE  WM_LPN_AUDIT_RESULTS
( LOCATION_ID INT not null

, WM_LPN_AUDIT_RESULTS_ID                    INT                not null

, WM_TC_COMPANY_ID                           INT 

, WM_FACILITY_ID                             BIGINT 

, WM_FACILITY_ALIAS_ID                       STRING 

, WM_DEST_FACILITY_ALIAS_ID                  STRING 

, WM_AUDIT_TRANSACTION_ID                    INT 

, AUDIT_CNT                                  INT 

, WM_QUAL_AUD_STAT_CD                        INT 

, VALIDATION_LEVEL                           STRING 

, WM_TRAN_NAME                               STRING 

, INBOUND_OUTBOUND_IND                       STRING 

, WM_TC_ORDER_ID                             STRING 

, WM_TC_PARENT_LPN_ID                        STRING 

, WM_TC_LPN_ID                               STRING 

, WM_TC_SHIPMENT_ID                          STRING 

, WM_STOP_SEQ                                INT 

, WM_STATIC_ROUTE_ID                         INT 

, WM_INVENTORY_TYPE                          STRING 

, WM_ITEM_ID                                 INT 

, WM_GTIN                                    STRING 

, COUNTRY_OF_ORIGIN                          STRING 

, WM_PRODUCT_STATUS                          STRING 

, QA_FLAG                                    STRING 

, CNT_QTY                                    DECIMAL(13,5) 

, EXPECTED_QTY                               DECIMAL(13,5) 

, WM_BATCH_NBR                               STRING 

, WM_AUDITOR_USER_ID                         STRING 

, WM_PICKER_USER_ID                          STRING 

, WM_PACKER_USER_ID                          STRING 

, ITEM_ATTR_1                                STRING 

, ITEM_ATTR_2                                STRING 

, ITEM_ATTR_3                                STRING 

, ITEM_ATTR_4                                STRING 

, ITEM_ATTR_5                                STRING 

, WM_CREATED_SOURCE_TYPE                     INT 

, WM_CREATED_SOURCE                          STRING 

, WM_WM_CREATED_TSTMP                        TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                INT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_WM_LAST_UPDATED_TSTMP                   TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_lpn_audit_results' 
;

--DISTRIBUTE ON (WM_LPN_AUDIT_RESULTS_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LPN_DETAIL" , ***** Creating table: "WM_LPN_DETAIL"


CREATE TABLE  WM_LPN_DETAIL
( LOCATION_ID INT not null

, WM_LPN_ID                                  BIGINT               not null

, WM_LPN_DETAIL_ID                           BIGINT               not null

, WM_TC_COMPANY_ID                           INT 

, WM_BUSINESS_PARTNER_ID                     STRING 

, WM_LPN_DETAIL_STATUS                       SMALLINT 

, WM_PURCHASE_ORDERS_ID                      BIGINT 

, WM_TC_PURCHASE_ORDERS_ID                   STRING 

, WM_PURCHASE_ORDERS_LINE_ID                 BIGINT 

, WM_TC_PURCHASE_ORDERS_LINE_ID              STRING 

, WM_INTERNAL_ORDER_ID                       STRING 

, WM_INTERNAL_ORDER_DTL_ID                   BIGINT 

, WM_TC_ORDER_LINE_ID                        STRING 

, WM_DISTRIBUTION_ORDER_DTL_ID               BIGINT 

, WM_ASN_DTL_ID                              BIGINT 

, WM_ITEM_ID                                 INT 

, WM_ITEM_NAME                               STRING 

, WM_GTIN                                    STRING 

, WM_VENDOR_ITEM_NBR                         STRING 

, WM_INVENTORY_TYPE                          STRING 

, WM_PRODUCT_STATUS                          STRING 

, COUNTRY_OF_ORIGIN                          STRING 

, MANUFACTURED_PLANT                         STRING 

, WM_BATCH_NBR                               STRING 

, WM_CHASE_WAVE_NBR                          STRING 

, WM_ASSORT_NBR                              STRING 

, WM_CUT_NBR                                 STRING 

, WM_PREPACK_GROUP_CD                        STRING 

, WM_PACK_CD                                 TINYINT 

, WM_RECEIVED_QTY                            DECIMAL(13,4) 

, WM_STD_PACK_QTY                            DECIMAL(13,4) 

, WM_STD_SUB_PACK_QTY                        DECIMAL(13,4) 

, WM_STD_BUNDLE_QTY                          DECIMAL(13,4) 

, WM_INCUBATION_DT DATE

, WM_EXPIRATION_DT DATE

, WM_SHIP_BY_DT DATE

, WM_SELL_BY_TSTMP                           TIMESTAMP 

, WM_CONSUMPTION_PRIORITY_TSTMP              TIMESTAMP 

, WM_MANUFACTURED_TSTMP                      TIMESTAMP 

, PACK_WEIGHT                                DECIMAL(13,4) 

, ESTIMATED_WEIGHT                           DECIMAL(13,4) 

, ESTIMATED_VOLUME                           DECIMAL(13,4) 

, SIZE_VALUE                                 DECIMAL(16,4) 

, WEIGHT                                     DECIMAL(16,4) 

, WM_QTY_UOM_ID                              BIGINT 

, WM_WEIGHT_UOM_ID                           INT 

, WM_VOLUME_UOM_ID                           INT 

, ASSIGNED_QTY                               DECIMAL(13,4) 

, SHIPPED_QTY                                DECIMAL(13,4) 

, INITIAL_QTY                                DECIMAL(13,4) 

, QTY_CONV_FACTOR                            DECIMAL(17,8) 

, WM_QTY_UOM_ID_BASE                         INT 

, WM_WEIGHT_UOM_ID_BASE                      INT 

, WM_VOLUME_UOM_ID_BASE                      INT 

, WM_HAZMAT_UOM_ID                           INT 

, HAZMAT_QTY                                 DECIMAL(13,4) 

, ITEM_ATTR_1                                STRING 

, ITEM_ATTR_2                                STRING 

, ITEM_ATTR_3                                STRING 

, ITEM_ATTR_4                                STRING 

, ITEM_ATTR_5                                STRING 

, INSTRTN_CD_1                               STRING 

, INSTRTN_CD_2                               STRING 

, INSTRTN_CD_3                               STRING 

, INSTRTN_CD_4                               STRING 

, INSTRTN_CD_5                               STRING 

, REF_FIELD_1                                STRING 

, REF_FIELD_2                                STRING 

, REF_FIELD_3                                STRING 

, REF_FIELD_4                                STRING 

, REF_FIELD_5                                STRING 

, REF_FIELD_6                                STRING 

, REF_FIELD_7                                STRING 

, REF_FIELD_8                                STRING 

, REF_FIELD_9                                STRING 

, REF_FIELD_10                               STRING 

, REF_NUM_1                                  DECIMAL(13,5) 

, REF_NUM_2                                  DECIMAL(13,5) 

, REF_NUM_3                                  DECIMAL(13,5) 

, REF_NUM_4                                  DECIMAL(13,5) 

, REF_NUM_5                                  DECIMAL(13,5) 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_lpn_detail' 
;

--DISTRIBUTE ON (WM_LPN_ID, WM_LPN_DETAIL_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LPN_FACILITY_STATUS" , ***** Creating table: "WM_LPN_FACILITY_STATUS"


CREATE TABLE  WM_LPN_FACILITY_STATUS
( LOCATION_ID INT not null

, WM_LPN_FACILITY_STATUS                     SMALLINT                not null

, INBOUND_OUTBOUND_IND                       STRING        not null

, WM_LPN_FACILITY_STATUS_DESC                STRING 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_lpn_facility_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LPN_LOCK" , ***** Creating table: "WM_LPN_LOCK"


CREATE TABLE  WM_LPN_LOCK
( LOCATION_ID INT not null

, WM_LPN_LOCK_ID                             BIGINT               not null

, WM_LPN_ID                                  BIGINT 

, WM_TC_LPN_ID                               STRING 

, WM_INVENTORY_LOCK_CD                       STRING 

, WM_REASON_CD                               STRING 

, LOCK_CNT                                   TINYINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_lpn_lock' 
;

--DISTRIBUTE ON (WM_LPN_LOCK_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LPN_SIZE_TYPE" , ***** Creating table: "WM_LPN_SIZE_TYPE"


CREATE TABLE  WM_LPN_SIZE_TYPE
( LOCATION_ID INT not null

, WM_LPN_SIZE_TYPE_ID                        INT                not null

, WM_LPN_TYPE_ID                             INT 

, WM_LPN_SIZE_TYPE                           STRING 

, WM_LPN_SIZE_DESC                           STRING 

, WM_LPN_DESC                                STRING 

, WM_TC_COMPANY_ID                           INT 

, WM_FACILITY_ID                             INT 

, WM_PKG_DESC                                STRING 

, OVERSIZE_FLAG                              STRING 

, STACKABLE_FLAG                             TINYINT 

, STACK_RANK                                 SMALLINT 

, STACK_POSITION                             SMALLINT 

, LPN_PER_TIER                               INT 

, TIER_PER_PALLET                            INT 

, WM_SHAPE_TYPE                              SMALLINT 

, LOAD_BEARING_STRENGTH                      BIGINT 

, WM_LPN_DIM_UOM                             STRING 

, LENGTH                                     DECIMAL(16,4) 

, WIDTH                                      DECIMAL(16,4) 

, HEIGHT                                     DECIMAL(16,4) 

, PROCESS_ATTR_1                             STRING 

, PROCESS_ATTR_2                             STRING 

, PROCESS_ATTR_3                             STRING 

, PROCESS_ATTR_4                             STRING 

, PROCESS_ATTR_5                             STRING 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_HIBERNATE_VERSION                       BIGINT 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_lpn_size_type' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LPN_STATUS" , ***** Creating table: "WM_LPN_STATUS"


CREATE TABLE  WM_LPN_STATUS
( LOCATION_ID INT not null

, WM_LPN_STATUS                              SMALLINT                not null

, WM_LPN_STATUS_DESC                         STRING 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_lpn_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_LPN_TYPE" , ***** Creating table: "WM_LPN_TYPE"


CREATE TABLE  WM_LPN_TYPE
( LOCATION_ID INT not null

, WM_LPN_TYPE                                SMALLINT                not null

, WM_LPN_TYPE_DESC                           STRING 

, WM_PHYSICAL_ENTITY_CD                      STRING 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_lpn_type' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ORDERS" , ***** Creating table: "WM_ORDERS"


CREATE TABLE  WM_ORDERS
( LOCATION_ID INT not null

, WM_ORDER_ID                                BIGINT               not null

, WM_TC_ORDER_ID                             STRING 

, WM_TC_ORDER_ID_U                           STRING 

, WM_TC_COMPANY_ID                           INT 

, WM_BUSINESS_PARTNER_ID                     STRING 

, WM_PARENT_ORDER_ID                         BIGINT 

, WM_C_TMS_PLAN_ID                           INT 

, WM_EXT_PURCHASE_ORDER                      STRING 

, WM_ORDER_TYPE                              STRING 

, WM_ORDER_STATUS                            SMALLINT 

, WM_VAS_ORDER_STATUS                        SMALLINT 

, WM_PURCHASE_ORDER_ID                       BIGINT 

, WM_PURCHASE_ORDER_NBR                      STRING 

, WM_DO_STATUS                               SMALLINT 

, WM_DO_TYPE                                 TINYINT 

, WM_SHIPMENT_ID                             BIGINT 

, WM_TC_SHIPMENT_ID                          STRING 

, WM_REF_SHIPMENT_NBR                        STRING 

, WM_ORDER_SHIPMENT_SEQ                      BIGINT 

, WM_SHIP_GROUP_ID                           STRING 

, WM_SHIP_GROUP_SEQ                          SMALLINT 

, WM_REPL_WAVE_NBR                           STRING 

, WM_PACK_WAVE_NBR                           STRING 

, WM_RTE_WAVE_NBR                            STRING 

, WM_REF_STOP_SEQ                            INT 

, WM_MOVE_TYPE                               TINYINT 

, WM_MOVEMENT_TYPE                           STRING 

, WM_MOVEMENT_OPTION                         TINYINT 

, WM_EST_LPN                                 BIGINT 

, WM_EST_LPN_BRIDGED                         BIGINT 

, WM_CUSTOMER_ID                             BIGINT 

, WM_CUST_BROKER_ACCT_NBR                    STRING 

, WM_ACCT_RCVBL_CD                           STRING 

, WM_ACCT_RCVBL_ACCT_NBR                     STRING 

, WM_FREIGHT_FORWARDER_ACCT_NBR              STRING 

, WM_BILL_OF_LADING_NBR                      STRING 

, WM_BOL_BREAK_ATTR                          STRING 

, WM_ORDER_DATE_TSTMP                        TIMESTAMP 

, WM_ORDER_RECON_TSTMP                       TIMESTAMP 

, WM_ACTUAL_SHIPPED_TSTMP                    TIMESTAMP 

, WM_SCHED_PICKUP_TSTMP                      TIMESTAMP 

, WM_PICKUP_START_TSTMP                      TIMESTAMP 

, WM_PICKUP_END_TSTMP                        TIMESTAMP 

, WM_PICKUP_TZ                               SMALLINT 

, WM_SCHED_DOW                               TINYINT 

, WM_SCHED_DELIVERY_TSTMP                    TIMESTAMP 

, WM_DELIVERY_START_TSTMP                    TIMESTAMP 

, WM_DELIVERY_END_TSTMP                      TIMESTAMP 

, WM_DELIVERY_TZ                             SMALLINT 

, WM_ADVERTIZING_DT DATE

, WM_ORDER_PRINT_TSTMP                       TIMESTAMP 

, WM_MUST_RELEASE_BY_TSTMP                   TIMESTAMP 

, WM_PLAN_DUE_TSTMP                          TIMESTAMP 

, BASELINE_SHIP_VIA                          STRING 

, ORIGINAL_ASSIGNED_SHIP_VIA                 STRING 

, DISTRIBUTION_SHIP_VIA                      STRING 

, DSG_SHIP_VIA                               STRING 

, LINE_HAUL_SHIP_VIA                         STRING 

, WM_BASELINE_CARRIER_ID                     BIGINT 

, WM_ASSIGNED_CARRIER_CD                     STRING 

, WM_DSG_CARRIER_CD                          STRING 

, WM_INBOUND_REGION_ID                       INT 

, WM_OUTBOUND_REGION_ID                      INT 

, WM_DSG_SERVICE_LEVEL_ID                    BIGINT 

, WM_DSG_CARRIER_ID                          BIGINT 

, WM_DSG_EQUIPMENT_ID                        BIGINT 

, WM_DSG_TRACTOR_EQUIPMENT_ID                BIGINT 

, WM_DSG_MOT_ID                              BIGINT 

, WM_BASELINE_MOT_ID                         BIGINT 

, WM_BASELINE_SERVICE_LEVEL_ID               BIGINT 

, WM_ASSIGNED_MOT_ID                         BIGINT 

, WM_ASSIGNED_CARRIER_ID                     BIGINT 

, WM_ASSIGNED_SERVICE_LEVEL_ID               BIGINT 

, WM_ASSIGNED_EQUIPMENT_ID                   BIGINT 

, WM_WAVE_ID                                 BIGINT 

, WM_WAVE_OPTION_ID                          BIGINT 

, WM_PATH_ID                                 BIGINT 

, WM_PATH_SET_ID                             BIGINT 

, WM_DRIVER_TYPE_ID                          BIGINT 

, WM_UN_NUMBER_ID                            BIGINT 

, WM_PRODUCT_CLASS_ID                        INT 

, WM_PROTECTION_LEVEL_ID                     INT 

, WM_MERCHANDIZING_DEPT_ID                   BIGINT 

, WM_FREIGHT_CLASS                           DECIMAL(5,1) 

, WM_COMMODITY_CD_ID                         BIGINT 

, WM_INTL_GOODS_DESC                         STRING 

, WM_SOURCE_ORDER                            STRING 

, WM_CREATION_TYPE                           TINYINT 

, ORDER_STREAM_FLAG                          TINYINT 

, WM_MAJOR_ORDER_CTRL_NBR                    STRING 

, WM_MAJOR_ORDER_GRP_ATTR                    STRING 

, WM_ORDER_CONSOL_PROFILE                    STRING 

, WM_PRIORITY                                SMALLINT 

, WM_EFFECTIVE_RANK                          STRING 

, WM_BATCH_ID                                BIGINT 

, WM_ORDER_LOADING_SEQ                       INT 

, WM_PARTIAL_SHIP_CONFIRM_STATUS             TINYINT 

, WM_CHANNEL_TYPE                            SMALLINT 

, WM_DELIVERY_CHANNEL_ID                     SMALLINT 

, WM_SHIPPING_CHANNEL                        STRING 

, WM_PO_TYPE_ATTR                            STRING 

, WM_LPN_LABEL_TYPE                          STRING 

, WM_PACK_SLIP_TYPE                          STRING 

, WM_BOL_TYPE                                STRING 

, WM_MANIF_TYPE                              STRING 

, WM_EQUIPMENT_TYPE                          SMALLINT 

, WM_CONS_RUN_ID                             BIGINT 

, WM_TRANS_RESP_CD                           STRING 

, WM_BILLING_METHOD                          TINYINT 

, DROP_OFF_PICKUP                            STRING 

, WM_COMPARTMENT_NO                          TINYINT 

, WM_PACKAGING                               STRING 

, WM_CHUTE_ID                                STRING 

, DOCS_ONLY_SHPMT                            STRING 

, FTSR_NBR                                   STRING 

, AES_ITN                                    STRING 

, WM_PROD_SCHED_REF_NBR                      STRING 

, WM_CUBING_STATUS                           SMALLINT 

, NON_MACHINEABLE_FLAG                       TINYINT 

, DYNAMIC_REQUEST_SENT                       BIGINT 

, WM_RELEASE_DESTINATION                     SMALLINT 

, WM_TEMPLATE_ID                             BIGINT 

, OVERRIDE_BILLING_METHOD_FLAG               TINYINT 

, WM_PARENT_TYPE                             TINYINT 

, WM_ADVT_CD                                 STRING 

, WM_LAST_RUN_ID                             BIGINT 

, LANE_NAME                                  STRING 

, WM_GLOBAL_LOCN_NBR                         STRING 

, IMPORTER_DEFN                              STRING 

, WM_LANG_ID                                 STRING 

, MAJOR_MINOR_ORDER                          STRING 

, WM_MHE_ORD_STATE                           STRING 

, PARTIAL_LPN_OPTION                         STRING 

, PARTIES_RELATED                            STRING 

, WM_PRE_STICKER_CD                          STRING 

, WM_MANIFEST_NBR                            STRING 

, DESTINATION_ACTION                         STRING 

, TRANS_PLAN_OWNER                           SMALLINT 

, DELIVERY_OPTIONS                           STRING 

, WM_PRE_BILL_STATUS                         STRING 

, WM_PALLET_CONTENT_LABEL_TYPE               STRING 

, TRANS_PLAN_DIRECTION                       STRING 

, HAZ_OFFER_OR_NAME                          STRING 

, WM_DISTRO_NBR                              STRING 

, WM_INCOTERM_ID                             INT 

, DIRECTION                                  STRING 

, NBR_OF_ZONES                               SMALLINT 

, FIRST_ZONE                                 STRING 

, LAST_ZONE                                  STRING 

, WM_DSG_VOYAGE_FLIGHT                       STRING 

, WM_DSG_TRAILER_NBR                         STRING 

, UPSMI_COST_CENTER                          STRING 

, WM_PICKLIST_ID                             STRING 

, APPLY_LPN_TYPE_FOR_ORDER_FLAG              TINYINT 

, WM_EPI_SERVICE_GROUP                       STRING 

, WM_IMPORTER_OF_RECORD_NBR                  STRING 

, WM_B13A_EXPORT_DECL_NBR                    STRING 

, CANCELLED_FLAG                             TINYINT 

, WM_CANCEL_TSTMP                            TIMESTAMP 

, ORDER_RECEIVED_FLAG                        TINYINT 

, ORIGINAL_ORDER_FLAG                        TINYINT 

, AUTO_APPOINTMENT_FLAG                      TINYINT 

, CUSTOMER_PICKUP_FLAG                       TINYINT 

, D_PO_BOX_FLAG                              TINYINT 

, DIRECT_ALLOWED_FLAG                        TINYINT 

, ALLOW_PRE_BILLING_FLAG                     TINYINT 

, ORDER_RECONCILED_FLAG                      TINYINT 

, SINGLE_UNIT_FLAG                           TINYINT 

, PARTIAL_SHIP_CONFIRM_FLAG                  TINYINT 

, CHASE_ELIGIBLE_FLAG                        TINYINT 

, GUARANTEED_DELIVERY_FLAG                   TINYINT 

, LPN_CUBING_IND                             TINYINT 

, BLOCK_AUTO_CREATE_FLAG                     TINYINT 

, BLOCK_AUTO_CONSOLIDATE_FLAG                TINYINT 

, HAS_ALERTS_FLAG                            TINYINT 

, HAS_EM_NOTIFY_FLAG                         TINYINT 

, HAS_IMPORT_ERROR_FLAG                      TINYINT 

, HAS_NOTES_FLAG                             TINYINT 

, HAS_SOFT_CHECK_ERRORS_FLAG                 TINYINT 

, HAS_SPLIT_FLAG                             TINYINT 

, BOOKING_REQD_FLAG                          TINYINT 

, BACK_ORDERED_IND                           STRING 

, STAGE_IND                                  TINYINT 

, ROUTED_FLAG                                TINYINT 

, DYNAMIC_ROUTING_REQD_FLAG                  TINYINT 

, HAZMAT_FLAG                                TINYINT 

, IMPORTED_FLAG                              TINYINT 

, PARTIALLY_PLANNED_FLAG                     TINYINT 

, PERISHABLE_FLAG                            TINYINT 

, SUSPENDED_FLAG                             TINYINT 

, PALLET_CUBING_FLAG                         TINYINT 

, PRE_PACK_FLAG                              TINYINT 

, IN_TRANSIT_ALLOCATION_FLAG                 TINYINT 

, DELIVERY_REQUIREMENT                       STRING 

, FAILED_GUARANTEED_DELIVERY                 STRING 

, MHE_FLAG                                   TINYINT 

, PNH_FLAG                                   TINYINT 

, PRINT_CANADIAN_CUST_INVOICE_FLAG           TINYINT 

, PRINT_COO_FLAG                             TINYINT 

, PRINT_DOCK_RCPT_FLAG                       TINYINT 

, PRINT_INVOICE_FLAG                         TINYINT 

, PRINT_NAFTA_COO_FLAG                       TINYINT 

, PRINT_OCEAN_BOL_FLAG                       TINYINT 

, PRINT_SED_FLAG                             TINYINT 

, PRINT_SHIPPER_LIST_OF_INSTR_FLAG           TINYINT 

, PRINT_PKG_LIST_FLAG                        TINYINT 

, STORE_NBR                                  STRING 

, WM_DC_CTR_NBR                              STRING 

, WM_ORDER_CONSOL_LOCN_ID                    STRING 

, WM_DSG_HUB_LOCATION_ID                     INT 

, WM_ZONE_SKIP_HUB_LOCATION_ID               INT 

, WM_STAGING_LOCN_ID                         STRING 

, WM_PLAN_O_FACILITY_ID                      BIGINT 

, WM_PLAN_O_FACILITY_ALIAS_ID                STRING 

, WM_O_FACILITY_ID                           INT 

, WM_O_FACILITY_ALIAS_ID                     STRING 

, WM_O_FACILITY_NAME                         STRING 

, WM_O_DOCK_ID                               STRING 

, WM_O_DOCK_DOOR_ID                          BIGINT 

, O_CONTACT                                  STRING 

, O_ADDR_1                                   STRING 

, O_ADDR_2                                   STRING 

, O_ADDR_3                                   STRING 

, O_CITY                                     STRING 

, O_STATE_PROV                               STRING 

, O_POSTAL_CD                                STRING 

, O_COUNTY                                   STRING 

, O_COUNTRY_CD                               STRING 

, O_PHONE_NBR                                STRING 

, O_FAX_NBR                                  STRING 

, O_EMAIL                                    STRING 

, WM_PLAN_D_FACILITY_ID                      INT 

, WM_PLAN_D_FACILITY_ALIAS_ID                STRING 

, WM_D_FACILITY_ID                           INT 

, WM_D_FACILITY_ALIAS_ID                     STRING 

, WM_D_FACILITY_NAME                         STRING 

, WM_D_DOCK_ID                               STRING 

, WM_D_DOCK_DOOR_ID                          BIGINT 

, D_CONTACT                                  STRING 

, D_NAME                                     STRING 

, D_ADDR_1                                   STRING 

, D_ADDR_2                                   STRING 

, D_ADDR_3                                   STRING 

, D_CITY                                     STRING 

, D_STATE_PROV                               STRING 

, D_POSTAL_CD                                STRING 

, D_COUNTY                                   STRING 

, D_COUNTRY_CD                               STRING 

, D_PHONE_NBR                                STRING 

, D_FAX_NBR                                  STRING 

, D_EMAIL                                    STRING 

, WM_PLAN_LEG_D_ALIAS_ID                     STRING 

, WM_PLAN_LEG_O_ALIAS_ID                     STRING 

, WM_BILL_ACCT_NBR                           STRING 

, WM_BILL_FACILITY_ID                        INT 

, WM_BILL_FACILITY_ALIAS_ID                  STRING 

, WM_BILL_TO_FACILITY_NAME                   STRING 

, BILL_TO_TITLE                              STRING 

, BILL_TO_CONTACT_NAME                       STRING 

, BILL_TO_NAME                               STRING 

, BILL_TO_ADDR_1                             STRING 

, BILL_TO_ADDR_2                             STRING 

, BILL_TO_ADDR_3                             STRING 

, BILL_TO_CITY                               STRING 

, BILL_TO_STATE_PROV                         STRING 

, BILL_TO_COUNTY                             STRING 

, BILL_TO_POSTAL_CD                          STRING 

, BILL_TO_COUNTRY_CD                         STRING 

, BILL_TO_PHONE_NBR                          STRING 

, BILL_TO_FAX_NBR                            STRING 

, BILL_TO_EMAIL                              STRING 

, WM_ADDR_CD                                 STRING 

, ADDR_VALID_FLAG                            TINYINT 

, WM_PRIMARY_MAXI_ADDR_NBR                   STRING 

, WM_SECONDARY_MAXI_ADDR_NBR                 STRING 

, WM_RETURN_ADDR_CD                          STRING 

, WM_COD_RETURN_COMPANY_NAME                 STRING 

, WM_RTE_TO                                  STRING 

, WM_RTE_SWC_NBR                             STRING 

, WM_RTE_TYPE_1                              STRING 

, WM_RTE_TYPE_2                              STRING 

, WM_RTE_ATTR                                STRING 

, WM_ORIGIN_SHIP_THRU_FACILITY_ID            INT 

, WM_ORIGIN_SHIP_THRU_FAC_ALIAS_ID           STRING 

, WM_DEST_SHIP_THRU_FACILITY_ID              INT 

, WM_DEST_SHIP_THRU_FAC_ALIAS_ID             STRING 

, WM_INCOTERM_FACILITY_ID                    INT 

, WM_INCOTERM_FACILITY_ALIAS_ID              STRING 

, WM_INCOTERM_LOC_AVA_TSTMP                  TIMESTAMP 

, WM_INCOTERM_LOC_AVA_TIME_ZONE_ID           INT 

, WM_ASSIGNED_STATIC_ROUTE_ID                STRING 

, WM_DSG_STATIC_ROUTE_ID                     STRING 

, WM_STATIC_ROUTE_DELIVERY_TYPE              STRING 

, WM_CONTNT_LABEL_TYPE                       STRING 

, NBR_OF_CONTNT_LABEL                        INT 

, NBR_OF_LABEL                               INT 

, NBR_OF_PAKNG_SLIPS                         INT 

, PACK_SLIP_PRT_CNT                          TINYINT 

, TOTAL_NBR_OF_UNITS                         DECIMAL(15,4) 

, TOTAL_NBR_OF_LPN                           INT 

, TOTAL_NBR_OF_PLT                           INT 

, EST_PALLET_BRIDGED                         BIGINT 

, EST_PALLET                                 BIGINT 

, COD_AMT                                    DECIMAL(13,4) 

, COD_CURRENCY_CD                            STRING 

, COD_FUNDS                                  STRING 

, MONETARY_VALUE                             DECIMAL(16,4) 

, MV_CURRENCY_CD                             STRING 

, MARGIN                                     DECIMAL(13,4) 

, MARGIN_CURRENCY_CD                         STRING 

, DECLARED_VALUE                             DECIMAL(16,4) 

, DV_CURRENCY_CD                             STRING 

, DUTY_AND_TAX                               DECIMAL(13,4) 

, WM_TAX_ID                                  STRING 

, WM_DUTY_TAX_ACCT_NBR                       STRING 

, WM_DUTY_TAX_PAYMENT_TYPE                   TINYINT 

, SHPNG_CHRG                                 DECIMAL(11,2) 

, ORIG_BUDG_COST                             DECIMAL(13,4) 

, BUDG_COST                                  DECIMAL(13,4) 

, BUDG_COST_CURRENCY_CD                      STRING 

, ACTUAL_COST                                DECIMAL(13,4) 

, ACTUAL_COST_CURRENCY_CD                    STRING 

, BASELINE_COST                              DECIMAL(13,4) 

, NORMALIZED_BASELINE_COST                   DECIMAL(13,4) 

, BASELINE_LINEHAUL_COST                     DECIMAL(13,4) 

, BASELINE_ACCESSORIAL_COST                  DECIMAL(13,4) 

, BASELINE_COST_CURRENCY_CD                  STRING 

, WM_REVENUE_LANE_ID                         DECIMAL(13,4) 

, WM_REVENUE_LANE_DETAIL_ID                  DECIMAL(13,4) 

, LINEHAUL_REVENUE                           DECIMAL(13,4) 

, ACCESSORIAL_REVENUE                        DECIMAL(13,4) 

, STOP_OFF_REVENUE                           DECIMAL(13,4) 

, CM_DISCOUNT_REVENUE                        DECIMAL(13,3) 

, FREIGHT_REVENUE                            DECIMAL(13,4) 

, FREIGHT_REVENUE_CURRENCY_CD                STRING 

, WM_WEIGHT_UOM_ID_BASE                      INT 

, REF_FIELD_1                                STRING 

, REF_FIELD_2                                STRING 

, REF_FIELD_3                                STRING 

, REF_FIELD_4                                STRING 

, REF_FIELD_5                                STRING 

, REF_FIELD_6                                STRING 

, REF_FIELD_7                                STRING 

, REF_FIELD_8                                STRING 

, REF_FIELD_9                                STRING 

, REF_FIELD_10                               STRING 

, REF_NUM_1                                  DECIMAL(13,5) 

, REF_NUM_2                                  DECIMAL(13,5) 

, REF_NUM_3                                  DECIMAL(13,5) 

, REF_NUM_4                                  DECIMAL(13,5) 

, REF_NUM_5                                  DECIMAL(13,5) 

, SPL_INSTR_CD_1                             STRING 

, SPL_INSTR_CD_2                             STRING 

, SPL_INSTR_CD_3                             STRING 

, SPL_INSTR_CD_4                             STRING 

, SPL_INSTR_CD_5                             STRING 

, SPL_INSTR_CD_6                             STRING 

, SPL_INSTR_CD_7                             STRING 

, SPL_INSTR_CD_8                             STRING 

, SPL_INSTR_CD_9                             STRING 

, SPL_INSTR_CD_10                            STRING 

, MARK_FOR                                   STRING 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_orders' 
;

--DISTRIBUTE ON (WM_ORDER_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ORDER_LINE_ITEM" , ***** Creating table: "WM_ORDER_LINE_ITEM"


CREATE TABLE  WM_ORDER_LINE_ITEM
( LOCATION_ID INT not null

, WM_ORDER_ID                                BIGINT               not null

, WM_LINE_ITEM_ID                            BIGINT               not null

, WM_DESC                                    STRING 

, WM_MASTER_ORDER_ID                         BIGINT 

, WM_MO_LINE_ITEM_ID                         BIGINT 

, WM_ORDER_LINE_ID                           BIGINT 

, WM_PARENT_LINE_ITEM_ID                     BIGINT 

, WM_ITEM_ID                                 INT 

, WM_ITEM_NAME                               STRING 

, WM_ORIGINAL_ORDERED_ITEM_ID                INT 

, WM_TC_COMPANY_ID                           INT 

, WM_TC_ORDER_LINE_ID                        STRING 

, WM_MINOR_ORDER_NBR                         STRING 

, WM_INTERNAL_ORDER_ID                       STRING 

, WM_INTERNAL_ORDER_SEQ_NBR                  INT 

, WM_REF_ORDER_ID                            BIGINT 

, WM_REF_LINE_ITEM_ID                        BIGINT 

, WM_PURCHASE_ORDER_NBR                      STRING 

, WM_PURCHASE_ORDER_LINE_NBR                 STRING 

, WM_EXT_PURCHASE_ORDER                      STRING 

, WM_EXT_SYS_LINE_ITEM_ID                    STRING 

, WM_MERCHANDIZING_DEPT_ID                   BIGINT 

, WM_UN_NBR_ID                               BIGINT 

, WM_PICKUP_REF_NBR                          STRING 

, WM_DELIVERY_REF_NBR                        STRING 

, WM_RTS_ID                                  BIGINT 

, WM_RTS_LINE_ITEM_ID                        BIGINT 

, WM_PROTECTION_LEVEL_ID                     INT 

, WM_PACKAGE_TYPE_ID                         INT 

, WM_COMMODITY_CD_ID                         BIGINT 

, WM_EVENT_CD                                STRING 

, WM_WAVE_NBR                                STRING 

, WM_SHIP_WAVE_NBR                           STRING 

, WM_REPL_WAVE_NBR                           STRING 

, WM_LPN_TYPE                                STRING 

, WM_PRE_RECEIPT_STATUS                      STRING 

, WM_DO_DTL_STATUS                           SMALLINT 

, WM_PROD_STAT                               STRING 

, WM_HARD_ALLOC_REASON_CD                    STRING 

, WM_REASON_CD                               STRING 

, BACK_ORD_REASON                            STRING 

, WM_EXP_INFO_CD                             STRING 

, WM_WAVE_PROC_TYPE                          SMALLINT 

, LINE_TYPE                                  STRING 

, REPL_PROC_TYPE                             SMALLINT 

, REPL_WAVE_RUN_FLAG                         TINYINT 

, WM_ALLOC_TYPE                              STRING 

, WM_BATCH_NBR                               STRING 

, WM_BATCH_REQUIREMENT_TYPE                  STRING 

, WM_CHUTE_ASSIGN_TYPE                       STRING 

, WM_INVN_TYPE                               STRING 

, LPN_BRK_ATTR                               STRING 

, PACK_ZONE                                  STRING 

, WM_PALLET_TYPE                             STRING 

, WM_PICK_LOCN_ASSIGN_TYPE                   STRING 

, WM_PICK_LOCN_ID                            STRING 

, WM_PPACK_GRP_CD                            STRING 

, WM_PRICE_TKT_TYPE                          STRING 

, WM_SHELF_DAYS                              INT 

, WM_FULFILLMENT_TYPE                        STRING 

, WM_ORDER_CONSOL_ATTR                       STRING 

, VAS_PROCESS_TYPE                           STRING 

, WM_SEGMENT_NAME                            STRING 

, WM_EFFECTIVE_RANK                          STRING 

, PRIORITY                                   BIGINT 

, ALLOW_REVIVAL_RECEIPT_ALLOC                STRING 

, RECEIPT_ALLOCATED_FLAG                     TINYINT 

, HAZMAT_FLAG                                TINYINT 

, STACKABLE_FLAG                             TINYINT 

, HAS_ERRORS_FLAG                            TINYINT 

, EMERGENCY_FLAG                             TINYINT 

, SINGLE_UNIT_FLAG                           TINYINT 

, SERIAL_NBR_REQD_FLAG                       TINYINT 

, CHASE_CREATED_LINE_FLAG                    TINYINT 

, GIFT_FLAG                                  TINYINT 

, PARTIAL_FILL_FLAG                          TINYINT 

, LOCKED_FLAG                                TINYINT 

, CANCELLED_FLAG                             TINYINT 

, SKU_GTIN                                   STRING 

, COUNTRY_OF_ORIGIN                          STRING 

, COMMODITY_CLASS                            STRING 

, WM_PRODUCT_CLASS_ID                        INT 

, WM_STORE_DEPT                              STRING 

, WM_MERCH_GRP                               STRING 

, WM_MERCH_TYPE                              STRING 

, CUSTOM_TAG                                 STRING 

, CUSTOMER_ITEM                              STRING 

, WM_SKU_SUB_CD_ID                           STRING 

, WM_SKU_SUB_CD_VALUE                        STRING 

, WM_SUBSTITUTED_PARENT_LINE_ID              BIGINT 

, WM_ALLOCATION_SOURCE                       TINYINT 

, WM_ALLOC_LINE_ID                           INT 

, WM_ALLOCATION_SOURCE_ID                    STRING 

, WM_ALLOCATION_SOURCE_LINE_ID               STRING 

, SKU_BREAK_ATTR                             STRING 

, WM_ASSORT_NBR                              STRING 

, SUBSTITUTION_TYPE                          STRING 

, MANUFACTURING_TSTMP                        TIMESTAMP 

, PICKUP_START_TSTMP                         TIMESTAMP 

, PICKUP_END_TSTMP                           TIMESTAMP 

, DELIVERY_START_TSTMP                       TIMESTAMP 

, DELIVERY_END_TSTMP                         TIMESTAMP 

, PLANNED_SHIP_TSTMP                         TIMESTAMP 

, ACTUAL_SHIPPED_TSTMP                       TIMESTAMP 

, PACK_RATE                                  DECIMAL(13,4) 

, PPACK_QTY                                  DECIMAL(13,4) 

, USER_CANCELED_QTY                          DECIMAL(13,4) 

, ACTUAL_COST                                DECIMAL(13,4) 

, ORIG_BUDG_COST                             DECIMAL(13,4) 

, BUDG_COST                                  DECIMAL(13,4) 

, BUDG_COST_CURRENCY_CD                      STRING 

, ACTUAL_COST_CURRENCY_CD                    STRING 

, TOTAL_MONETARY_VALUE                       DECIMAL(13,4) 

, PRICE                                      DECIMAL(13,4) 

, RETAIL_PRICE                               DECIMAL(16,4) 

, GIFT_CARD_VALUE                            DECIMAL(13,4) 

, UNIT_COST                                  DECIMAL(16,4) 

, UNIT_PRICE_AMT                             DECIMAL(13,4) 

, UNIT_VOL                                   DECIMAL(13,4) 

, UNIT_WT                                    DECIMAL(13,4) 

, UNIT_TAX_AMT                               DECIMAL(16,4) 

, UNIT_MONETARY_VALUE                        DECIMAL(13,4) 

, MV_CURRENCY_CD                             STRING 

, ORIG_ORDER_QTY                             DECIMAL(13,4) 

, ORDER_QTY                                  DECIMAL(13,4) 

, ADJUSTED_ORDER_QTY                         DECIMAL(13,4) 

, ALLOCATED_QTY                              DECIMAL(13,4) 

, SHIPPED_QTY                                DECIMAL(13,4) 

, RECEIVED_QTY                               DECIMAL(13,4) 

, RTL_TO_BE_DISTROED_QTY                     DECIMAL(9,2) 

, UNITS_PAKD                                 DECIMAL(13,4) 

, CUBE_MULTIPLE_QTY                          DECIMAL(13,4) 

, STD_PACK_QTY                               DECIMAL(13,4) 

, STD_PALLET_QTY                             DECIMAL(13,4) 

, STD_SUB_PACK_QTY                           DECIMAL(13,4) 

, STD_BUNDLE_QTY                             DECIMAL(13,4) 

, STD_LPN_QTY                                DECIMAL(13,4) 

, STD_LPN_VOL                                DECIMAL(13,4) 

, STD_LPN_WT                                 DECIMAL(13,4) 

, LPN_SIZE                                   DECIMAL(16,4) 

, QTY_CONV_FACTOR                            DECIMAL(17,8) 

, WM_QTY_UOM_ID                              BIGINT 

, WM_QTY_UOM_ID_BASE                         INT 

, WM_MV_SIZE_UOM_ID                          INT 

, PLANNED_WEIGHT                             DECIMAL(13,4) 

, RECEIVED_WEIGHT                            DECIMAL(13,4) 

, SHIPPED_WEIGHT                             DECIMAL(13,4) 

, WM_WEIGHT_UOM_ID_BASE                      INT 

, WM_WEIGHT_UOM_ID                           INT 

, PLANNED_VOLUME                             DECIMAL(13,4) 

, RECEIVED_VOLUME                            DECIMAL(13,4) 

, SHIPPED_VOLUME                             DECIMAL(13,4) 

, WM_VOLUME_UOM_ID_BASE                      INT 

, WM_VOLUME_UOM_ID                           INT 

, SIZE_1_VALUE                               DECIMAL(13,4) 

, RECEIVED_SIZE_1                            DECIMAL(13,4) 

, SHIPPED_SIZE_1                             DECIMAL(13,4) 

, WM_SIZE_1_UOM_ID                           INT 

, SIZE_2_VALUE                               DECIMAL(13,4) 

, RECEIVED_SIZE_2                            DECIMAL(13,4) 

, SHIPPED_SIZE_2                             DECIMAL(13,4) 

, WM_SIZE_2_UOM_ID                           INT 

, STACK_RANK                                 SMALLINT 

, STACK_LENGTH_VALUE                         DECIMAL(16,4) 

, WM_STACK_LENGTH_STANDARD_UOM               INT 

, STACK_WIDTH_VALUE                          DECIMAL(16,4) 

, WM_STACK_WIDTH_STANDARD_UOM                INT 

, STACK_HEIGHT_VALUE                         DECIMAL(16,4) 

, WM_STACK_HEIGHT_STANDARD_UOM               INT 

, STACK_DIAMETER_VALUE                       DECIMAL(16,4) 

, WM_STACK_DIAMETER_STANDARD_UOM             INT 

, FREIGHT_REVENUE                            DECIMAL(13,4) 

, FREIGHT_REVENUE_CURRENCY_CD                STRING 

, ITEM_ATTR_1                                STRING 

, ITEM_ATTR_2                                STRING 

, ITEM_ATTR_3                                STRING 

, ITEM_ATTR_4                                STRING 

, ITEM_ATTR_5                                STRING 

, CRITCL_DIM_1                               DECIMAL(7,2) 

, CRITCL_DIM_2                               DECIMAL(7,2) 

, CRITCL_DIM_3                               DECIMAL(7,2) 

, REF_BOOLEAN_1                              TINYINT 

, REF_BOOLEAN_2                              TINYINT 

, REF_SYS_CD_1                               STRING 

, REF_SYS_CD_2                               STRING 

, REF_SYS_CD_3                               STRING 

, REF_FIELD_1                                STRING 

, REF_FIELD_2                                STRING 

, REF_FIELD_3                                STRING 

, REF_FIELD_4                                STRING 

, REF_FIELD_5                                STRING 

, REF_FIELD_6                                STRING 

, REF_FIELD_7                                STRING 

, REF_FIELD_8                                STRING 

, REF_FIELD_9                                STRING 

, REF_FIELD_10                               STRING 

, REF_NUM_1                                  DECIMAL(13,5) 

, REF_NUM_2                                  DECIMAL(13,5) 

, REF_NUM_3                                  DECIMAL(13,5) 

, REF_NUM_4                                  DECIMAL(13,5) 

, REF_NUM_5                                  DECIMAL(13,5) 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_order_line_item' 
;

--DISTRIBUTE ON (WM_ORDER_ID, WM_LINE_ITEM_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_ORDER_STATUS" , ***** Creating table: "WM_ORDER_STATUS"


CREATE TABLE  WM_ORDER_STATUS
( LOCATION_ID INT not null

, WM_ORDER_STATUS_ID                         SMALLINT                not null

, WM_ORDER_STATUS_DESC                       STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_order_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_OUTPT_LPN" , ***** Creating table: "WM_OUTPT_LPN"


CREATE TABLE  WM_OUTPT_LPN
( LOCATION_ID INT not null

, WM_OUTPT_LPN_ID                            BIGINT               not null

, WM_TC_COMPANY_ID                           INT 

, WM_TC_LPN_ID                               STRING 

, WM_TC_ORDER_ID                             STRING 

, WM_TC_PURCHASE_ORDER_ID                    STRING 

, WM_TC_PARENT_LPN_ID                        STRING 

, WM_TC_SHIPMENT_ID                          STRING 

, WM_EPI_PACKAGE_ID                          STRING 

, WM_EPI_SHIPMENT_ID                         STRING 

, WM_INVC_BATCH_NBR                          INT 

, WM_C_FACILITY_ALIAS_ID                     STRING 

, WM_FINAL_DEST_FACILITY_ALIAS_ID            STRING 

, WM_RETURN_TRACKING_NBR                     STRING 

, WM_RETURN_REFERENCE_NBR                    STRING 

, WM_XREF_OLPN                               STRING 

, WM_BILL_OF_LADING_NBR                      STRING 

, WM_MASTER_BOL_NBR                          STRING 

, WM_MANIFEST_NBR                            STRING 

, WM_STATIC_ROUTE_ID                         INT 

, PROC_TSTMP                                 TIMESTAMP 

, WM_PROC_STAT_CD                            SMALLINT 

, LOAD_SEQ                                   INT 

, LOADED_TSTMP                               TIMESTAMP 

, SHIPPED_TSTMP                              TIMESTAMP 

, SHIP_VIA                                   STRING 

, TRACKING_NBR                               STRING 

, ALT_TRACKING_NBR                           STRING 

, WM_CONTAINER_SIZE                          STRING 

, WM_CONTAINER_TYPE                          STRING 

, WM_PACKAGE_DESC                            STRING 

, WM_PACKER_USER_ID                          STRING 

, WM_LOADED_POSN                             STRING 

, WM_LPN_EPC                                 STRING 

, WM_LPN_NBR_X_OF_Y                          INT 

, PALLET_EPC                                 STRING 

, PALLET_MASTER_LPN_FLAG                     TINYINT 

, PARCEL_SERVICE_MODE                        STRING 

, WM_PARCEL_SHIPMENT_NBR                     STRING 

, WM_PICKER_USER_ID                          STRING 

, SERVICE_LEVEL                              STRING 

, PRE_BULK_ADD_OPT_CHR                       DECIMAL(9,2) 

, PRE_BULK_ADD_OPT_CHR_CURRENCY              STRING 

, PRE_BULK_BASE_CHARGE                       DECIMAL(9,2) 

, ACTUAL_CHARGE                              DECIMAL(9,2) 

, ACTUAL_CHARGE_CURRENCY                     STRING 

, ACTUAL_MONETARY_VALUE                      DECIMAL(13,4) 

, ACTUAL_VOLUME                              DECIMAL(13,4) 

, ADDNL_OPTION_CHARGE                        DECIMAL(9,2) 

, ADDNL_OPTION_CHARGE_CURRENCY               STRING 

, ADF_TRANSMIT_FLAG                          TINYINT 

, BASE_CHARGE                                DECIMAL(13,4) 

, BASE_CHARGE_CURRENCY                       STRING 

, COD_AMOUNT                                 DECIMAL(13,4) 

, WM_COD_PAYMENT_METHOD                      INT 

, DECLARED_MONETARY_VALUE                    DECIMAL(13,4) 

, DIST_CHARGE                                DECIMAL(11,2) 

, DIST_CHARGE_CURRENCY                       STRING 

, FREIGHT_CHARGE                             DECIMAL(11,2) 

, FREIGHT_CHARGE_CURRENCY                    STRING 

, INSUR_CHARGE                               DECIMAL(9,2) 

, INSUR_CHARGE_CURRENCY                      STRING 

, TOTAL_LPN_QTY                              DECIMAL(13,4) 

, QTY_UOM                                    STRING 

, RATE_ZONE                                  STRING 

, RATED_WEIGHT                               DECIMAL(13,4) 

, RATED_WEIGHT_UOM                           STRING 

, ESTIMATED_WEIGHT                           DECIMAL(13,4) 

, WEIGHT                                     DECIMAL(16,4) 

, WEIGHT_UOM                                 STRING 

, VOLUME_UOM                                 STRING 

, EPC_MATCH_FLAG                             TINYINT 

, NON_INVENTORY_LPN_FLAG                     TINYINT 

, NON_MACHINEABLE                            INT 

, REF_FIELD_1                                STRING 

, REF_FIELD_2                                STRING 

, REF_FIELD_3                                STRING 

, REF_FIELD_4                                STRING 

, REF_FIELD_5                                STRING 

, REF_FIELD_6                                STRING 

, REF_FIELD_7                                STRING 

, REF_FIELD_8                                STRING 

, REF_FIELD_9                                STRING 

, REF_FIELD_10                               STRING 

, MISC_INSTR_CD_1                            STRING 

, MISC_INSTR_CD_2                            STRING 

, MISC_INSTR_CD_3                            STRING 

, MISC_INSTR_CD_4                            STRING 

, MISC_INSTR_CD_5                            STRING 

, MISC_NUM_1                                 DECIMAL(9,2) 

, MISC_NUM_2                                 DECIMAL(9,2) 

, WM_AUDITOR_USER_ID                         STRING 

, WM_VERSION_NBR                             BIGINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_outpt_lpn' 
;

--DISTRIBUTE ON (WM_OUTPT_LPN_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_OUTPT_LPN_DETAIL" , ***** Creating table: "WM_OUTPT_LPN_DETAIL"


CREATE TABLE  WM_OUTPT_LPN_DETAIL
( LOCATION_ID INT not null

, WM_OUTPT_LPN_DETAIL_ID                     BIGINT               not null

, WM_LPN_DETAIL_ID                           BIGINT 

, WM_TC_LPN_ID                               STRING 

, WM_TC_COMPANY_ID                           INT 

, WM_INVC_BATCH_NBR                          INT 

, WM_ASSORT_NBR                              STRING 

, WM_BUSINESS_PARTNER                        STRING 

, WM_MINOR_ORDER_NBR                         STRING 

, WM_MINOR_PO_NBR                            STRING 

, WM_TC_ORDER_LINE_ID                        STRING 

, WM_DISTRIBUTION_ORDER_DTL_ID               BIGINT 

, WM_INVENTORY_TYPE                          STRING 

, WM_CONSUMPTION_PRIORITY_TSTMP              TIMESTAMP 

, WM_GTIN                                    STRING 

, WM_ITEM_ID                                 INT 

, WM_ITEM_NAME                               STRING 

, WM_VENDOR_ITEM_NBR                         STRING 

, WM_BATCH_NBR                               STRING 

, WM_MANUFACTURED_TSTMP                      TIMESTAMP 

, WM_MANUFACTURED_PLANT                      STRING 

, WM_DISTRO_NBR                              STRING 

, COUNTRY_OF_ORIGIN                          STRING 

, WM_PRODUCT_STATUS                          STRING 

, REC_PROC_INDICATOR                         STRING 

, WM_PROC_TSTMP                              TIMESTAMP 

, WM_PROC_STAT_CD                            SMALLINT 

, WM_SIZE_RANGE_CD                           STRING 

, WM_SIZE_REL_POSN_IN_TABLE                  STRING 

, SIZE_VALUE                                 DECIMAL(16,4) 

, ITEM_COLOR                                 STRING 

, ITEM_COLOR_SFX                             STRING 

, ITEM_SEASON                                STRING 

, ITEM_SEASON_YEAR                           STRING 

, ITEM_SECOND_DIM                            STRING 

, ITEM_SIZE_DESC                             STRING 

, ITEM_QUALITY                               STRING 

, ITEM_STYLE                                 STRING 

, ITEM_STYLE_SFX                             STRING 

, WM_QTY_UOM                                 STRING 

, ITEM_ATTR_1                                STRING 

, ITEM_ATTR_2                                STRING 

, ITEM_ATTR_3                                STRING 

, ITEM_ATTR_4                                STRING 

, ITEM_ATTR_5                                STRING 

, WM_VERSION_NBR                             BIGINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_outpt_lpn_detail' 
;

--DISTRIBUTE ON (WM_OUTPT_LPN_DETAIL_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_OUTPT_ORDERS" , ***** Creating table: "WM_OUTPT_ORDERS"


CREATE TABLE  WM_OUTPT_ORDERS
( LOCATION_ID INT not null

, WM_OUTPT_ORDERS_ID                         BIGINT               not null

, WM_TC_COMPANY_ID                           INT 

, WM_TC_ORDER_ID                             STRING 

, WM_ORDER_STATUS                            SMALLINT 

, WM_DO_TYPE                                 SMALLINT 

, WM_PO_TYPE_ATTR                            STRING 

, WM_DO_STATUS                               SMALLINT 

, WM_TC_SHIPMENT_ID                          STRING 

, WM_PURCHASE_ORDER_NBR                      STRING 

, WM_EXT_PURCHASE_ORDER                      STRING 

, WM_SHIPMENT_ID                             BIGINT 

, WM_REF_SHIPMENT_NBR                        STRING 

, WM_SHIP_GROUP_ID                           STRING 

, SHIP_GROUP_SEQ                             SMALLINT 

, WM_INVC_BATCH_NBR                          INT 

, WM_BATCH_CTRL_NBR                          STRING 

, BILL_OF_LADING_NBR                         STRING 

, BOL_BREAK_ATTR                             STRING 

, WM_BOL_TYPE                                STRING 

, BUSINESS_PARTNER                           STRING 

, WM_PROC_TSTMP                              TIMESTAMP 

, WM_PROC_STAT_CD                            SMALLINT 

, BACK_ORDERED_IND                           STRING 

, WM_RTE_ID                                  STRING 

, RTE_SHIPMENT_SEQ                           INT 

, WM_DISTRO_NBR                              STRING 

, REF_STOP_SEQ                               INT 

, WM_ACCT_RCVBL_ACCT_NBR                     STRING 

, WM_ACCT_RCVBL_CD                           STRING 

, WM_FREIGHT_FORWARDER_ACCT_NBR              STRING 

, WM_GLOBAL_LOCN_NBR                         STRING 

, WM_ADDR_CD                                 STRING 

, WM_TAX_ID                                  STRING 

, WM_BILLING_METHOD                          SMALLINT 

, ALLOW_PRE_BILLING_FLAG                     TINYINT 

, PRE_BILLED_FLAG                            TINYINT 

, WM_PRE_BILL_STATUS                         SMALLINT 

, PRTL_SHIP_CONF_FLAG                        TINYINT 

, WM_PRTL_SHIP_CONF_STATUS                   INT 

, MARK_FOR                                   STRING 

, ORIGINAL_SHIP_VIA                          STRING 

, DISTRIBUTION_SHIP_VIA                      STRING 

, LINE_HAUL_SHIP_VIA                         STRING 

, FREIGHT_CLASS                              DECIMAL(5,1) 

, WM_INCOTERM_ID                             INT 

, INCOTERM_NAME                              STRING 

, WM_INCOTERM_FACILITY_ID                    INT 

, WM_INCOTERM_FACILITY_ALIAS_ID              STRING 

, INCOTERM_LOC_AVA_TSTMP                     TIMESTAMP 

, WM_INCOTERM_LOC_AVA_TIME_ZONE_ID           INT 

, WM_MAJOR_MINOR_ORDER                       STRING 

, WM_MAJOR_ORDER_CTRL_NBR                    STRING 

, WM_MAJOR_ORDER_GRP_ATTR                    STRING 

, WM_PRO_NBR                                 STRING 

, WM_MANIFEST_ID                             STRING 

, ORDER_DT_TSTMP                             TIMESTAMP 

, PICKUP_START_TSTMP                         TIMESTAMP 

, PICKUP_END_TSTMP                           TIMESTAMP 

, SCHED_DELIVERY_TSTMP                       TIMESTAMP 

, DELIVERY_START_TSTMP                       TIMESTAMP 

, PLANNED_SHIP_TSTMP                         TIMESTAMP 

, SHIP_TSTMP                                 TIMESTAMP 

, PRE_PACK_FLAG                              TINYINT 

, PRIORITY                                   SMALLINT 

, REC_XPANS_FIELD                            STRING 

, DC_CTR_NBR                                 STRING 

, STORE_NBR                                  STRING 

, STORE_TYPE                                 STRING 

, MERCHANDIZING_DEPT                         STRING 

, MOVEMENT_OPTION                            SMALLINT 

, NON_MACHINEABLE                            SMALLINT 

, DSG_SHIP_VIA                               STRING 

, WM_ORG_SHIP_FACILITY_ALIAS_ID              STRING 

, WM_ORG_SHIP_FACILITY_ID                    INT 

, ORIGINAL_ASSIGNED_SHIP_VIA                 STRING 

, WM_ORIGIN_SHIP_THRU_FACILITY_ID            INT 

, WM_ORIGIN_SHIP_THRU_FAC_ALIAS_ID           STRING 

, WM_PLAN_D_FACILITY_ALIAS_ID                STRING 

, WM_PLAN_D_FACILITY_ID                      INT 

, WM_PLAN_LEG_D_ALIAS_ID                     STRING 

, WM_PLAN_LEG_D_ID                           INT 

, WM_PLAN_LEG_O_ALIAS_ID                     STRING 

, WM_PLAN_LEG_O_ID                           INT 

, WM_PLAN_O_FACILITY_ALIAS_ID                STRING 

, WM_PLAN_O_FACILITY_ID                      BIGINT 

, WM_BILL_ACCT_NBR                           STRING 

, WM_BILL_FACILITY_ID                        INT 

, WM_BILL_FACILITY_ALIAS_ID                  STRING 

, BILL_TO_NAME                               STRING 

, BILL_TO_TITLE                              STRING 

, BILL_TO_CONTACT                            STRING 

, BILL_TO_CONTACT_NAME                       STRING 

, BILL_TO_FACILITY_NAME                      STRING 

, BILL_TO_ADDR_1                             STRING 

, BILL_TO_ADDR_2                             STRING 

, BILL_TO_ADDR_3                             STRING 

, BILL_TO_CITY                               STRING 

, BILL_TO_STATE_PROV                         STRING 

, BILL_TO_POSTAL_CD                          STRING 

, BILL_TO_COUNTY                             STRING 

, BILL_TO_COUNTRY_CD                         STRING 

, BILL_TO_PHONE_NBR                          STRING 

, BILL_TO_FAX_NBR                            STRING 

, BILL_TO_EMAIL                              STRING 

, D_NAME                                     STRING 

, D_CONTACT                                  STRING 

, WM_D_FACILITY_ID                           INT 

, WM_D_FACILITY_ALIAS_ID                     STRING 

, D_FACILITY_NAME                            STRING 

, D_ADDR_1                                   STRING 

, D_ADDR_2                                   STRING 

, D_ADDR_3                                   STRING 

, D_CITY                                     STRING 

, D_STATE_PROV                               STRING 

, D_POSTAL_CD                                STRING 

, D_COUNTY                                   STRING 

, D_COUNTRY_CD                               STRING 

, D_PHONE_NBR                                STRING 

, D_FAX_NBR                                  STRING 

, D_EMAIL                                    STRING 

, WM_D_DOCK_ID                               STRING 

, WM_D_DOCK_DOOR_ID                          BIGINT 

, O_CONTACT                                  STRING 

, WM_O_FACILITY_ID                           INT 

, WM_O_FACILITY_ALIAS_ID                     STRING 

, O_FACILITY_NAME                            STRING 

, O_ADDR_1                                   STRING 

, O_ADDR_2                                   STRING 

, O_ADDR_3                                   STRING 

, O_CITY                                     STRING 

, O_STATE_PROV                               STRING 

, O_POSTAL_CD                                STRING 

, O_COUNTY                                   STRING 

, O_COUNTRY_CD                               STRING 

, O_PHONE_NBR                                STRING 

, O_FAX_NBR                                  STRING 

, O_EMAIL                                    STRING 

, WM_O_DOCK_ID                               STRING 

, WM_O_DOCK_DOOR_ID                          BIGINT 

, TOTAL_NBR_OF_LPN                           INT 

, TOTAL_NBR_OF_PLT                           INT 

, TOTAL_NBR_OF_UNITS                         DECIMAL(15,4) 

, BACK_ORDERED_QTY                           DECIMAL(13,2) 

, ORDER_WEIGHT                               DECIMAL(13,4) 

, MONETARY_VALUE                             DECIMAL(16,4) 

, TAX_CHARGES_AMT                            DECIMAL(13,4) 

, SHIPPING_CHARGES_AMT                       DECIMAL(11,2) 

, HANDLING_CHARGES_AMT                       DECIMAL(13,4) 

, INSURANCE_CHARGES_AMT                      DECIMAL(11,2) 

, MISC_CHARGES_AMT                           DECIMAL(13,4) 

, CURRENCY                                   STRING 

, MV_CURRENCY                                STRING 

, SPL_INSTR_CD_1                             STRING 

, SPL_INSTR_CD_2                             STRING 

, SPL_INSTR_CD_3                             STRING 

, SPL_INSTR_CD_4                             STRING 

, SPL_INSTR_CD_5                             STRING 

, SPL_INSTR_CD_6                             STRING 

, SPL_INSTR_CD_7                             STRING 

, SPL_INSTR_CD_8                             STRING 

, SPL_INSTR_CD_9                             STRING 

, SPL_INSTR_CD_10                            STRING 

, WM_VERSION_NBR                             BIGINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_outpt_orders' 
;

--DISTRIBUTE ON (WM_OUTPT_ORDERS_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_OUTPT_ORDER_LINE_ITEM" , ***** Creating table: "WM_OUTPT_ORDER_LINE_ITEM"


CREATE TABLE  WM_OUTPT_ORDER_LINE_ITEM
( LOCATION_ID INT not null

, WM_OUTPT_ORDER_LINE_ITEM_ID                BIGINT               not null

, WM_TC_COMPANY_ID                           INT 

, WM_TC_ORDER_ID                             STRING 

, WM_TC_ORDER_LINE_ID                        STRING 

, WM_TC_PURCHASE_ORDERS_ID                   STRING 

, WM_PURCHASE_ORDER_NBR                      STRING 

, WM_PURCHASE_ORDER_LINE_NBR                 STRING 

, WM_EXT_PURCHASE_ORDER                      STRING 

, WM_LINE_ITEM_ID                            BIGINT 

, WM_PARENT_LINE_ITEM_ID                     BIGINT 

, WM_INVC_BATCH_NBR                          INT 

, WM_BATCH_CTRL_NBR                          STRING 

, WM_BATCH_NBR                               STRING 

, WM_PROC_TSTMP                              TIMESTAMP 

, WM_PROC_STAT_CD                            SMALLINT 

, WM_ITEM_ID                                 INT 

, WM_ITEM_NAME                               STRING 

, WM_GTIN                                    STRING 

, WM_ASSORT_NBR                              STRING 

, WM_PRODUCT_STATUS                          STRING 

, WM_REASON_CD                               STRING 

, WM_LINE_TYPE                               STRING 

, WM_INVN_TYPE                               STRING 

, WM_COMMODITY_CLASS                         STRING 

, WM_CUSTOMER_ITEM                           STRING 

, WM_EXP_INFO_CD                             STRING 

, COUNTRY_OF_ORIGIN                          STRING 

, WM_MANUFACTURING_TSTMP                     TIMESTAMP 

, WM_PICKUP_REFERENCE_NBR                    STRING 

, WM_PICKUP_START_TSTMP                      TIMESTAMP 

, WM_PICKUP_END_TSTMP                        TIMESTAMP 

, WM_PLANNED_SHIP_TSTMP                      TIMESTAMP 

, WM_ACTUAL_SHIPPED_TSTMP                    TIMESTAMP 

, WM_REC_PROC_FLAG                           TINYINT 

, WM_REC_XPANS_FIELD                         STRING 

, PLANNED_QTY                                DECIMAL(13,4) 

, WM_PLANNED_QTY_UOM                         STRING 

, ORDER_QTY                                  DECIMAL(13,4) 

, WM_ORDER_QTY_UOM                           STRING 

, BACK_ORDERED_QTY                           DECIMAL(13,2) 

, USER_CANCELED_QTY                          DECIMAL(13,4) 

, WM_PPACK_QTY                               DECIMAL(13,4) 

, WM_PPACK_GRP_CD                            STRING 

, WM_SHIPPED_QTY                             DECIMAL(13,4) 

, WM_PRICE                                   DECIMAL(13,4) 

, WM_PRICE_TKT_TYPE                          STRING 

, WM_RETAIL_PRICE                            DECIMAL(16,4) 

, WM_ACTUAL_COST                             DECIMAL(13,4) 

, WM_ACTUAL_COST_CURRENCY_CD                 STRING 

, UNIT_VOL                                   DECIMAL(16,4) 

, UNIT_WT                                    DECIMAL(16,4) 

, UOM                                        STRING 

, AISLE                                      STRING 

, AREA                                       STRING 

, BAY                                        STRING 

, LVL                                        STRING 

, POSN                                       STRING 

, ZONE                                       STRING 

, TEMP_ZONE                                  STRING 

, WM_SIZE_RANGE_CD                           STRING 

, SIZE_REL_POSN_IN_TABLE                     STRING 

, SHELF_DAYS                                 INT 

, ITEM_COLOR                                 STRING 

, ITEM_COLOR_SFX                             STRING 

, ITEM_SEASON                                STRING 

, ITEM_SEASON_YEAR                           STRING 

, ITEM_SECOND_DIM                            STRING 

, ITEM_SIZE_DESC                             STRING 

, ITEM_QUALITY                               STRING 

, ITEM_STYLE                                 STRING 

, ITEM_STYLE_SFX                             STRING 

, ITEM_ATTR_1                                STRING 

, ITEM_ATTR_2                                STRING 

, ITEM_ATTR_3                                STRING 

, ITEM_ATTR_4                                STRING 

, ITEM_ATTR_5                                STRING 

, MISC_INSTR_10_BYTE_1                       STRING 

, MISC_INSTR_10_BYTE_2                       STRING 

, WM_ORIG_ORDER_LINE_ITEM_ID                 BIGINT 

, WM_ORIG_ITEM_ID                            INT 

, WM_ORIG_ORDER_QTY                          DECIMAL(13,4) 

, WM_ORIG_ORDER_QTY_UOM                      STRING 

, ORIG_ITEM_COLOR                            STRING 

, ORIG_ITEM_COLOR_SFX                        STRING 

, ORIG_ITEM_SEASON                           STRING 

, ORIG_ITEM_SEASON_YEAR                      STRING 

, ORIG_ITEM_SECOND_DIM                       STRING 

, ORIG_ITEM_SIZE_DESC                        STRING 

, ORIG_ITEM_QUALITY                          STRING 

, ORIG_ITEM_STYLE                            STRING 

, ORIG_ITEM_STYLE_SFX                        STRING 

, WM_VERSION_NBR                             BIGINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_outpt_order_line_item' 
;

--DISTRIBUTE ON (WM_OUTPT_ORDER_LINE_ITEM_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_PICKING_SHORT_ITEM" , ***** Creating table: "WM_PICKING_SHORT_ITEM"


CREATE TABLE  WM_PICKING_SHORT_ITEM
( LOCATION_ID INT not null

, WM_PICKING_SHORT_ITEM_ID                   INT                not null

, WM_ITEM_ID                                 INT 

, WM_LOCN_ID                                 STRING 

, WM_LINE_ITEM_ID                            BIGINT 

, WM_TC_ORDER_ID                             STRING 

, WM_WAVE_NBR                                STRING 

, WM_SHORT_QTY                               DECIMAL(13,5) 

, WM_STAT_CODE                               SMALLINT 

, WM_TC_COMPANY_ID                           INT 

, WM_SHORT_TYPE                              STRING 

, WM_TC_LPN_ID                               STRING 

, WM_LPN_DETAIL_ID                           BIGINT 

, REQD_INVN_TYPE                             STRING 

, REQD_PROD_STAT                             STRING 

, REQD_BATCH_NBR                             STRING 

, REQD_SKU_ATTR_1                            STRING 

, REQD_SKU_ATTR_2                            STRING 

, REQD_SKU_ATTR_3                            STRING 

, REQD_SKU_ATTR_4                            STRING 

, REQD_SKU_ATTR_5                            STRING 

, REQD_CNTRY_OF_ORGN                         STRING 

, WM_SHIPMENT_ID                             BIGINT 

, WM_TC_SHIPMENT_ID                          STRING 

, WM_CREATED_TSTMP                          date
, WM_CREATED_SOURCE                          STRING 

, WM_LAST_UPDATED_TSTMP DATE

, WM_LAST_UPDATED_SOURCE                     STRING 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_picking_short_item' 
;

--DISTRIBUTE ON (WM_PICKING_SHORT_ITEM_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_PICK_LOCN_DTL" , ***** Creating table: "WM_PICK_LOCN_DTL"


CREATE TABLE  WM_PICK_LOCN_DTL
( LOCATION_ID INT not null

, WM_PICK_LOCN_DTL_ID                        BIGINT               not null

, WM_PICK_LOCN_HDR_ID                        INT 

, WM_LOCN_ID                                 STRING 

, WM_LOCN_SEQ_NBR                            INT 

, WM_ITEM_MASTER_ID                          INT 

, WM_ITEM_ID                                 INT 

, WM_BATCH_NBR                               STRING 

, WM_INVN_TYPE                               STRING 

, WM_PROD_STAT                               STRING 

, WM_FIRST_WAVE_NBR                          STRING 

, WM_LAST_WAVE_NBR                           STRING 

, WM_LTST_SKU_ASSIGN_FLAG                    TINYINT 

, WM_LTST_PICK_ASSIGN_TSTMP                  TIMESTAMP 

, WM_PIKNG_LOCK_CD                           STRING 

, TRIG_REPL_FOR_SKU_FLAG                     TINYINT 

, PRIM_LOCN_FOR_SKU_FLAG                     TINYINT 

, WM_LANES                                   STRING 

, WM_STACKING                                STRING 

, WM_TASK_RELEASED_FLAG                      TINYINT 

, WM_REPLEN_CONTROL                          STRING 

, WM_PICK_TO_ZERO_ACTION                     STRING 

, COUNTRY_OF_ORIGIN                          STRING 

, PRE_ALLOCATED_QTY                          DECIMAL(13,4) 

, PACK_QTY                                   DECIMAL(9,2) 

, TO_BE_FILLD_CASES                          INT 

, MAX_INVN_QTY                               DECIMAL(13,5) 

, MIN_INVN_QTY                               DECIMAL(13,5) 

, ACTL_INVN_CASES                            BIGINT 

, MIN_INVN_CASES                             BIGINT 

, MAX_INVN_CASES                             BIGINT 

, MIN_QTY_TO_RLS_HELD_RPLN                   DECIMAL(13,5) 

, MIN_CASES_TO_RLS_HELD_RPLN                 INT 

, STOP_QTY                                   DECIMAL(13,4) 

, STOP_TSTMP                                 TIMESTAMP 

, UTIL_PERCENT                               DECIMAL(3,2) 

, WM_SKU_ATTR_1                              STRING 

, WM_SKU_ATTR_2                              STRING 

, WM_SKU_ATTR_3                              STRING 

, WM_SKU_ATTR_4                              STRING 

, WM_SKU_ATTR_5                              STRING 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_pick_locn_dtl' 
;

--DISTRIBUTE ON (WM_PICK_LOCN_DTL_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_PICK_LOCN_DTL_SLOTTING" , ***** Creating table: "WM_PICK_LOCN_DTL_SLOTTING"


CREATE TABLE  WM_PICK_LOCN_DTL_SLOTTING
( LOCATION_ID INT not null

, WM_PICK_LOCN_DTL_ID                        INT                not null

, REPLEN_GROUP                               STRING 

, CUR_ORIENTATION                            STRING 

, REC_LANES                                  BIGINT 

, REC_STACKING                               BIGINT 

, MULT_LOC_GRP                               STRING 

, HIST_MATCH                                 STRING 

, IGN_FOR_RESLOT_FLAG                        TINYINT 

, OPT_PALLET_PATTERN                         STRING 

, HI_RESIDUAL_1                              TINYINT 

, SI_NUM_1                                   DECIMAL(13,4) 

, SI_NUM_2                                   DECIMAL(13,4) 

, SI_NUM_3                                   DECIMAL(13,4) 

, SI_NUM_4                                   DECIMAL(13,4) 

, SI_NUM_5                                   DECIMAL(13,4) 

, SI_NUM_6                                   DECIMAL(13,4) 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_pick_locn_dtl_slotting' 
;

--DISTRIBUTE ON (WM_PICK_LOCN_DTL_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_PICK_LOCN_HDR" , ***** Creating table: "WM_PICK_LOCN_HDR"


CREATE TABLE  WM_PICK_LOCN_HDR
( LOCATION_ID INT not null

, WM_PICK_LOCN_HDR_ID                        INT                not null

, WM_LOCN_ID                                 STRING 

, WM_LOCN_HDR_ID                             INT 

, WM_PICK_LOCN_ASSIGN_TYPE                   STRING 

, WM_LOCN_PUTAWAY_LOCK                       STRING 

, WM_INVN_LOCK_CD                            STRING 

, WM_XCESS_WAVE_NEED_PROC_TYPE               TINYINT 

, WM_PUTWY_TYPE                              STRING 

, WM_PICK_LOCN_ASSIGN_ZONE                   STRING 

, PICK_TO_LIGHT_FLAG                         TINYINT 

, PICK_TO_LIGHT_REPL_FLAG                    TINYINT 

, WM_REPL_FLAG                               TINYINT 

, WM_REPL_LOCN_BRCD                          STRING 

, WM_REPL_TRAVEL_AISLE                       STRING 

, WM_REPL_TRAVEL_ZONE                        STRING 

, WM_SUPPR_PR40_REPL                         SMALLINT 

, WM_COMB_4050_REPL                          SMALLINT 

, WM_REPL_CHECK_DIGIT                        STRING 

, REPL_X_COORD                               DECIMAL(13,5) 

, REPL_Y_COORD                               DECIMAL(13,5) 

, REPL_Z_COORD                               DECIMAL(13,5) 

, MAX_NBR_OF_SKU                             SMALLINT 

, MAX_VOL                                    DECIMAL(13,4) 

, MAX_WT                                     DECIMAL(13,4) 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_pick_locn_hdr' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_PICK_LOCN_HDR_SLOTTING" , ***** Creating table: "WM_PICK_LOCN_HDR_SLOTTING"


CREATE TABLE  WM_PICK_LOCN_HDR_SLOTTING
( LOCATION_ID INT not null

, WM_PICK_LOCN_HDR_ID                        DECIMAL(19,0)               not null

, WM_SLOTTING_GROUP                          STRING 

, WM_SLOT_PRIORITY                           STRING 

, WM_RACK_LEVEL_ID                           BIGINT 

, WM_RACK_TYPE                               BIGINT 

, MY_RANGE                                   BIGINT 

, MY_SNS                                     BIGINT 

, SIDE_OF_AISLE                              SMALLINT 

, LABEL_POS                                  DECIMAL(9,4) 

, LABEL_POS_OVR                              TINYINT 

, DEPTH_OVERRIDE                             TINYINT 

, HT_OVERRIDE                                TINYINT 

, WIDTH_OVERRIDE                             TINYINT 

, WT_LIMIT_OVERRIDE                          TINYINT 

, OLD_REC_SLOT_WIDTH                         DECIMAL(13,4) 

, RIGHT_SLOT                                 DECIMAL(19,0) 

, LEFT_SLOT                                  DECIMAL(19,0) 

, REACH_DIST                                 DECIMAL(13,4) 

, REACH_DIST_OVERRIDE                        TINYINT 

, ALLOW_EXPAND_FLAG                          TINYINT 

, ALLOW_EXPAND_LFT_FLAG                      TINYINT 

, ALLOW_EXPAND_LFT_OVR_FLAG                  TINYINT 

, ALLOW_EXPAND_OVR_FLAG                      TINYINT 

, ALLOW_EXPAND_RGT_FLAG                      TINYINT 

, ALLOW_EXPAND_RGT_OVR_FLAG                  TINYINT 

, MAX_HC_OVR                                 TINYINT 

, MAX_HT_CLEAR                               DECIMAL(9,4) 

, MAX_LANE_WT                                DECIMAL(13,4) 

, MAX_LANES                                  BIGINT 

, MAX_LN_OVR                                 TINYINT 

, MAX_LW_OVR                                 DECIMAL(9,4) 

, MAX_SC_OVR                                 TINYINT 

, MAX_SIDE_CLEAR                             DECIMAL(9,4) 

, MAX_ST_OVR                                 TINYINT 

, MAX_STACK                                  BIGINT 

, LOCKED_FLAG                                TINYINT 

, PROCESSED_FLAG                             TINYINT 

, RESERVED_1                                 STRING 

, RESERVED_2                                 STRING 

, RESERVED_3                                 BIGINT 

, RESERVED_4                                 BIGINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_pick_locn_hdr_slotting' 
;

--DISTRIBUTE ON (WM_PICK_LOCN_HDR_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_PIX_TRAN" , ***** Creating table: "WM_PIX_TRAN"


CREATE TABLE  WM_PIX_TRAN
( LOCATION_ID INT not null

, WM_PIX_TRAN_ID                             INT                not null

, WM_TRAN_NBR                                INT 

, WM_PIX_SEQ_NBR                             INT 

, WM_TRAN_TYPE                               STRING 

, WM_TRAN_CD                                 STRING 

, WM_PROC_STAT_CD                            TINYINT 

, WM_WHSE                                    STRING 

, WM_REF_WHSE                                STRING 

, WM_TC_COMPANY_ID                           INT 

, WM_COMPANY_CD                              STRING 

, WM_CASE_NBR                                STRING 

, WM_FACILITY_ID                             INT 

, WM_XML_GROUP_ID                            STRING 

, WM_ITEM_ID                                 BIGINT 

, WM_ITEM_NAME                               STRING 

, WM_ESIGN_USER_NAME                         STRING 

, SEASON                                     STRING 

, SEASON_YR                                  STRING 

, STYLE                                      STRING 

, STYLE_SFX                                  STRING 

, COLOR                                      STRING 

, COLOR_SFX                                  STRING 

, SEC_DIM                                    STRING 

, QUAL                                       STRING 

, SIZE_DESC                                  STRING 

, WM_SIZE_RANGE_CD                           STRING 

, WM_SIZE_REL_POSN_IN_TABLE                  STRING 

, WM_INVN_TYPE                               STRING 

, WM_PROD_STAT                               STRING 

, WM_BATCH_NBR                               STRING 

, COUNTRY_OF_ORIGIN                          STRING 

, INVN_ADJMT_QTY                             DECIMAL(13,5) 

, WM_INVN_ADJMT_TYPE                         STRING 

, WT_ADJMT_QTY                               DECIMAL(13,5) 

, WM_WT_ADJMT_TYPE                           STRING 

, UOM                                        STRING 

, WM_RSN_CD                                  STRING 

, WM_RCPT_VARI_FLAG                          TINYINT 

, WM_RCPT_CMPL_FLAG                          TINYINT 

, CASES_SHPD                                 INT 

, UNITS_SHPD                                 DECIMAL(15,5) 

, CASES_RCVD                                 INT 

, UNITS_RCVD                                 DECIMAL(15,5) 

, WM_ACTN_CD                                 STRING 

, WM_CUSTOM_REF                              STRING 

, WM_PROC_TSTMP                              TIMESTAMP 

, WM_ERROR_COMMENT                           STRING 

, WM_SKU_ATTR_1                              STRING 

, WM_SKU_ATTR_2                              STRING 

, WM_SKU_ATTR_3                              STRING 

, WM_SKU_ATTR_4                              STRING 

, WM_SKU_ATTR_5                              STRING 

, WM_REF_CODE_ID_1                           STRING 

, WM_REF_FIELD_1                             STRING 

, WM_REF_CODE_ID_2                           STRING 

, WM_REF_FIELD_2                             STRING 

, WM_REF_CODE_ID_3                           STRING 

, WM_REF_FIELD_3                             STRING 

, WM_REF_CODE_ID_4                           STRING 

, WM_REF_FIELD_4                             STRING 

, WM_REF_CODE_ID_5                           STRING 

, WM_REF_FIELD_5                             STRING 

, WM_REF_CODE_ID_6                           STRING 

, WM_REF_FIELD_6                             STRING 

, WM_REF_CODE_ID_7                           STRING 

, WM_REF_FIELD_7                             STRING 

, WM_REF_CODE_ID_8                           STRING 

, WM_REF_FIELD_8                             STRING 

, WM_REF_CODE_ID_9                           STRING 

, WM_REF_FIELD_9                             STRING 

, WM_REF_CODE_ID_10                          STRING 

, WM_REF_FIELD_10                            STRING 

, WM_SYS_USER_ID                             STRING 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_pix_tran' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_PRODUCT_CLASS" , ***** Creating table: "WM_PRODUCT_CLASS"


CREATE TABLE  WM_PRODUCT_CLASS
( LOCATION_ID INT not null

, WM_PRODUCT_CLASS_ID                        INT                not null

, WM_PRODUCT_CLASS                           STRING 

, WM_TC_COMPANY_ID                           INT 

, WM_PRODUCT_CLASS_DESC                      STRING 

, SPLIT_FLAG                                 TINYINT 

, RANK                                       SMALLINT 

, MIN_THRESHOLD                              SMALLINT 

, STACKING_FACTOR                            INT 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_product_class' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_PURCHASE_ORDERS" , ***** Creating table: "WM_PURCHASE_ORDERS"


CREATE TABLE  WM_PURCHASE_ORDERS
( LOCATION_ID INT not null

, WM_PURCHASE_ORDERS_ID                      BIGINT               not null

, WM_TC_COMPANY_ID                           INT 

, WM_BUSINESS_PARTNER_ID                     STRING 

, WM_PURCHASE_ORDERS_TYPE                    TINYINT 

, WM_PURCHASE_ORDERS_STATUS                  SMALLINT 

, WM_TC_PURCHASE_ORDERS_ID                   STRING 

, WM_TC_PURCHASE_ORDERS_ID_U                 STRING 

, WM_PARENT_PURCHASE_ORDERS_ID               BIGINT 

, WM_EXT_PURCHASE_ORDERS_ID                  STRING 

, WM_WORK_ORD_NBR                            STRING 

, WM_MANUFACTURING_ORDER_NBR                 STRING 

, WM_DC_ORDER_NBR                            STRING 

, WM_RETURN_REFERENCE_NBR                    STRING 

, WM_ORDER_CATEGORY                          INT 

, WM_CREATION_TYPE                           TINYINT 

, WM_MO_AUTO_CREATE_METHOD                   TINYINT 

, WM_DECLINE_REASON_ID                       INT 

, WM_REASON_ID                               INT 

, WM_REASON_CODES_GROUP_ID                   BIGINT 

, WM_SITE_ID                                 BIGINT 

, WM_STORE_NBR                               STRING 

, WM_STORE_TYPE                              STRING 

, WM_DOM_STORE_TYPE                          BIGINT 

, WM_ENTRY_CD_FACILITY_ID                    SMALLINT 

, WM_ORG_SHIP_FACILITY_ID                    INT 

, WM_ORG_SHIP_VIA_FACILITY_ALIAS_ID          STRING 

, WM_ORG_SHIP_VIA_FACILITY_ID                INT 

, WM_DES_SHIP_VIA_FACILITY_ALIAS_ID          STRING 

, WM_DES_SHIP_VIA_FACILITY_ID                INT 

, WM_INCOTERM_ID                             INT 

, WM_INCOTERM_FACILITY_ID                    INT 

, WM_INCOTERM_FACILITY_ALIAS_ID              STRING 

, WM_INCOTERM_LOC_AVA_TSTMP                  TIMESTAMP 

, WM_INCOTERM_LOC_AVA_TIME_ZONE_ID           SMALLINT 

, WM_PICKUP_TZ                               SMALLINT 

, WM_DELIVERY_TZ                             SMALLINT 

, WM_PICKUP_START_TSTMP                      TIMESTAMP 

, WM_PICKUP_END_TSTMP                        TIMESTAMP 

, WM_DELIVERY_START_TSTMP                    TIMESTAMP 

, WM_DELIVERY_END_TSTMP                      TIMESTAMP 

, WM_REQUESTED_DLVR_TSTMP                    TIMESTAMP 

, WM_MUST_DLVR_TSTMP                         TIMESTAMP 

, WM_PROMISED_DLVR_TSTMP                     TIMESTAMP 

, WM_COMMIMTED_DLVR_TSTMP                    TIMESTAMP 

, WM_DUE_DT                                  TIMESTAMP 

, WM_CANCEL_DT                               TIMESTAMP 

, WM_ACCEPTED_TSTMP                          TIMESTAMP 

, WM_RELEASE_TSTMP                           TIMESTAMP 

, WM_FIRST_RCPT_TSTMP                        TIMESTAMP 

, WM_LAST_RCPT_TSTMP                         TIMESTAMP 

, WM_PURCHASE_ORDERS_DT_TSTMP                TIMESTAMP 

, WM_ORDER_CONFIRMATION_TSTMP                TIMESTAMP 

, WM_UN_NBR_ID                               BIGINT 

, WM_BILL_OF_LADING_NBR                      STRING 

, WM_BOL_TYPE                                STRING 

, WM_TRANS_RESP_CD                           STRING 

, WM_PRIORITY_TYPE                           TINYINT 

, WM_PRO_NBR                                 STRING 

, WM_PRIORITY                                SMALLINT 

, WM_EVENT_IND_TYPE_ID                       BIGINT 

, WM_COMMODITY_CD_ID                         BIGINT 

, WM_CARRIER_ID                              BIGINT 

, WM_PRODUCT_CLASS_ID                        INT 

, WM_PROTECTION_LEVEL_ID                     INT 

, WM_DSG_CARRIER_ID                          BIGINT 

, WM_DSG_MOT_ID                              BIGINT 

, WM_DSG_SERVICE_LEVEL_ID                    BIGINT 

, WM_MERCHANDIZING_DEPARTMENT_ID             BIGINT 

, WM_BUYER_CD                                STRING 

, REP_NAME                                   STRING 

, WM_REGION_ID                               INT 

, WM_INBOUND_REGION_ID                       INT 

, WM_OUTBOUND_REGION_ID                      INT 

, WM_PLAN_SHPMT_NBR                          STRING 

, WM_ASN_SHPMT_TYPE                          STRING 

, WM_PROD_SCHED_REF_NBR                      STRING 

, WM_DELAY_TYPE                              SMALLINT 

, WM_EST_OUTBD_LPN                           BIGINT 

, WM_EST_PALLET_BRIDGED                      BIGINT 

, WM_MAJOR_PKT_GRP_ATTR                      STRING 

, WM_ON_HOLD_EVENT_ID                        BIGINT 

, WM_UN_HOLD_EVENT_ID                        BIGINT 

, WM_OUTBD_LPN_EPC_TYPE                      TINYINT 

, WM_PKT_CONSOL_PROF                         STRING 

, WM_PKT_PROFILE_ID                          STRING 

, WM_PLT_CONTNT_LABEL_TYPE                   STRING 

, WM_PRE_STIKR_CD                            STRING 

, WM_SWC_NBR_SEQ                             INT 

, WM_BATCH_ID                                BIGINT 

, DROP_OFF_PICKUP                            STRING 

, MFG_PLNT                                   STRING 

, WM_OUTBD_LPN_LABEL_TYPE                    STRING 

, WM_PACKAGING                               STRING 

, WM_RTN_MISC_NBR                            STRING 

, WM_DSG_CARRIER_CD                          STRING 

, WM_FRT_CLASS                               STRING 

, WM_PLAN_ID                                 STRING 

, WM_CUT_NBR                                 STRING 

, WM_CHANNEL_ID                              BIGINT 

, WM_CHANNEL_TYPE                            SMALLINT 

, WM_SHIPPING_CHANNEL                        STRING 

, WM_DELIVERY_CHANNEL_ID                     SMALLINT 

, WM_DEPARTMENT_ID                           INT 

, WM_FULFILL_MODE                            SMALLINT 

, WM_CONS_RUN_ID                             BIGINT 

, WM_LAST_RUN_ID                             BIGINT 

, ENTERED_BY                                 STRING 

, WM_ENTRY_TYPE                              STRING 

, WM_ENTRY_CD                                STRING 

, WM_PALLET_CONTENT_LABEL_TYPE               STRING 

, WM_MANIF_TYPE                              STRING 

, TRANS_PLAN_DIRECTION                       STRING 

, DSG_VOYAGE_FLIGHT                          STRING 

, DIRECTION                                  STRING 

, WM_RMA_STATUS                              SMALLINT 

, WM_EPI_SERVICE_GROUP                       STRING 

, WM_IMPORTER_OF_RECORD_NBR                  STRING 

, WM_B13A_EXPORT_DECL_NBR                    STRING 

, WM_STATIC_ROUTE_DELIVERY_TYPE              STRING 

, WM_SHIP_GROUP_ID                           STRING 

, ORDER_RECEIVED_FLAG                        TINYINT 

, AUTO_APPOINTMENT_FLAG                      TINYINT 

, ORG_APPT_EXISTS_FLAG                       TINYINT 

, DEST_APPT_EXISTS_FLAG                      TINYINT 

, DO_CREATED_FLAG                            TINYINT 

, CLOSED_FLAG                                TINYINT 

, CANCELLED_FLAG                             TINYINT 

, IMPORTED_FLAG                              TINYINT 

, UPDATE_SENT_FLAG                           TINYINT 

, HAZMAT_FLAG                                TINYINT 

, PERISHABLE_FLAG                            TINYINT 

, INFO_ONLY_FLAG                             TINYINT 

, HAS_IMPORT_ERROR_FLAG                      TINYINT 

, HAS_SOFT_CHECK_ERRORS_FLAG                 TINYINT 

, HAS_NOTES_FLAG                             TINYINT 

, HAS_ALERTS_FLAG                            TINYINT 

, PURCHASE_ORDERS_RECONCILED_FLAG            TINYINT 

, DELIVERY_REQ                               STRING 

, ACCEPTANCE_REQD_FLAG                       TINYINT 

, LEGAL_TERMS_REQD_FLAG                      TINYINT 

, READY_TO_SHIP_FLAG                         TINYINT 

, CUSTOMER_PICKUP_FLAG                       TINYINT 

, DIRECT_ALLOWED_FLAG                        TINYINT 

, PARTIALLY_PLANNED_FLAG                     TINYINT 

, COMPLETED_EXTERNALLY_FLAG                  TINYINT 

, PL_MANUALLY_SET_FLAG                       TINYINT 

, LOCKED_FLAG                                TINYINT 

, PURCHASE_ORDERS_CONFIRMED_FLAG             TINYINT 

, HAS_ROUTING_REQUEST_FLAG                   TINYINT 

, HAS_EM_NOTIFY_FLAG                         TINYINT 

, PUTAWAY_FLAG                               TINYINT 

, ALLOW_PARTIAL_SHIPPING_FLAG                TINYINT 

, ALLOW_ZONE_SKIPPING_FLAG                   TINYINT 

, SINGLE_UNIT_FLAG                           TINYINT 

, PRE_PACK_FLAG                              TINYINT 

, PACK_AND_HOLD_FLAG                         TINYINT 

, ON_HOLD_FLAG                               TINYINT 

, QUALITY_CHECK_HOLD_UPON_RCPT_FLAG          TINYINT 

, OUTBD_LPN_ASN_REQD_FLAG                    TINYINT 

, OUTBD_LPN_CUBING_FLAG                      TINYINT 

, PALLET_CUBING_FLAG                         TINYINT 

, PRINT_CANADIAN_CUSTOM_INVOICE_FLAG         TINYINT 

, PRINT_COO_FLAG                             TINYINT 

, PRINT_DOCK_RCPT_FLAG                       TINYINT 

, PRINT_INV_FLAG                             TINYINT 

, PRINT_NAFTA_COO_FLAG                       TINYINT 

, PRINT_OCEAN_BOL_FLAG                       TINYINT 

, PRINT_SED_FLAG                             TINYINT 

, PRINT_SHPR_LTR_OF_INSTR_FLAG               TINYINT 

, PRINT_PKG_LIST_FLAG                        TINYINT 

, WM_O_FACILITY_ID                           INT 

, WM_O_FACILITY_ALIAS_ID                     STRING 

, WM_O_DOCK_ID                               STRING 

, O_NAME                                     STRING 

, O_ADDR_1                                   STRING 

, O_ADDR_2                                   STRING 

, O_ADDR_3                                   STRING 

, O_CITY                                     STRING 

, O_STATE_PROV                               STRING 

, O_POSTAL_CD                                STRING 

, O_COUNTY                                   STRING 

, O_COUNTRY_CD                               STRING 

, O_PHONE_NBR                                STRING 

, O_FAX_NBR                                  STRING 

, O_EMAIL                                    STRING 

, WM_D_FACILITY_ID                           INT 

, WM_D_FACILITY_ALIAS_ID                     STRING 

, WM_D_DOCK_ID                               STRING 

, D_CONTACT_NAME                             STRING 

, D_NAME                                     STRING 

, D_LAST_NAME                                STRING 

, D_ADDR_1                                   STRING 

, D_ADDR_2                                   STRING 

, D_ADDR_3                                   STRING 

, D_CITY                                     STRING 

, D_STATE_PROV                               STRING 

, D_POSTAL_CD                                STRING 

, D_COUNTY                                   STRING 

, D_COUNTRY_CD                               STRING 

, D_PHONE_NBR                                STRING 

, D_FAX_NBR                                  STRING 

, D_EMAIL                                    STRING 

, WM_BILL_FACILITY_ID                        INT 

, WM_BILL_FACILITY_ALIAS_ID                  STRING 

, WM_BILL_TO_WHSE                            STRING 

, BILL_TO_CONTACT_NAME                       STRING 

, BILL_TO_NAME                               STRING 

, BILL_TO_LAST_NAME                          STRING 

, BILL_TO_ADDR_1                             STRING 

, BILL_TO_ADDR_2                             STRING 

, BILL_TO_ADDR_3                             STRING 

, BILL_TO_CITY                               STRING 

, BILL_TO_STATE_PROV                         STRING 

, BILL_TO_COUNTY                             STRING 

, BILL_TO_POSTAL_CD                          STRING 

, BILL_TO_COUNTRY_CD                         STRING 

, BILL_TO_PHONE_NBR                          STRING 

, BILL_TO_FAX_NBR                            STRING 

, BILL_TO_EMAIL                              STRING 

, BILL_ACCOUNT_NBR                           STRING 

, WM_CUSTOMER_ID                             BIGINT 

, WM_CUSTOMER_CD                             STRING 

, CUSTOMER_FIRST_NAME                        STRING 

, CUSTOMER_LAST_NAME                         STRING 

, WM_CUSTOMER_USER_ID                        STRING 

, CUSTOMER_EMAIL                             STRING 

, CUSTOMER_PHONE                             STRING 

, PRIMARY_CONTACT_NBR                        STRING 

, WM_CUSTOMER_TYPE                           STRING 

, ADDR_VALID_FLAG                            TINYINT 

, WM_RETURN_ADDR_CD                          STRING 

, PACK_SLIP_PRT_CNT                          TINYINT 

, MINIMIZE_SHIPMENTS                         SMALLINT 

, WM_CONTNT_LABEL_TYPE                       STRING 

, NBR_OF_CONTNT_LABEL                        INT 

, WM_LPN_LABEL_TYPE                          STRING 

, NBR_OF_LABEL                               INT 

, WM_PACK_SLIP_TYPE                          STRING 

, NBR_OF_PAKNG_SLIPS                         INT 

, WM_PURCHASE_ORDERS_WEIGHT                  DECIMAL(13,4) 

, WM_WEIGHT_UOM_ID_BASE                      INT 

, WM_BILLING_METHOD                          TINYINT 

, WM_PAYMENT_MODE                            SMALLINT 

, WM_PAYMENT_STATUS                          SMALLINT 

, WM_MONETARY_VALUE                          DECIMAL(19,4) 

, MV_CURRENCY_CD                             STRING 

, ORIG_BUDG_COST                             DECIMAL(13,4) 

, BUDG_COST                                  DECIMAL(13,4) 

, BUDG_COST_CURRENCY_CD                      STRING 

, FREIGHT_REVENUE                            DECIMAL(13,4) 

, FREIGHT_REVENUE_CURRENCY_CD                STRING 

, MISC_CHARGES                               DECIMAL(13,4) 

, TAX_CHARGES                                DECIMAL(13,4) 

, WM_TAX_ID                                  STRING 

, TOTAL_DLRS_DISC                            DECIMAL(19,4) 

, GRAND_TOTAL                                DECIMAL(13,4) 

, CURRENCY_CD                                STRING 

, MISC_INSTR_CD_1                            STRING 

, MISC_INSTR_CD_2                            STRING 

, REF_FIELD_1                                STRING 

, REF_FIELD_2                                STRING 

, REF_FIELD_3                                STRING 

, MARK_FOR                                   STRING 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_purchase_orders' 
;

--DISTRIBUTE ON (WM_PURCHASE_ORDERS_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_PURCHASE_ORDERS_LINE_ITEM" , ***** Creating table: "WM_PURCHASE_ORDERS_LINE_ITEM"


CREATE TABLE  WM_PURCHASE_ORDERS_LINE_ITEM
( LOCATION_ID INT not null

, WM_PURCHASE_ORDERS_ID                      BIGINT               not null

, WM_PURCHASE_ORDERS_LINE_ITEM_ID            BIGINT               not null

, WM_TC_COMPANY_ID                           INT 

, WM_BUSINESS_PARTNER_ID                     STRING 

, WM_TC_PO_LINE_ID                           STRING 

, WM_PARENT_PO_LINE_ITEM_ID                  BIGINT 

, WM_PARENT_TC_PO_LINE_ID                    STRING 

, WM_SUPPLY_CONS_PO_ID                       STRING 

, WM_SUPPLY_PO_LINE_ID                       STRING 

, WM_ROOT_LINE_ITEM_ID                       BIGINT 

, WM_EXT_SYS_LINE_ITEM_ID                    STRING 

, WM_DESC                                    STRING 

, WM_ORIGINAL_PO_LINE_ITEM_ID                BIGINT 

, WM_ORIG_ORD_LINE_NBR                       INT 

, WM_PURCHASE_ORDERS_LINE_STATUS             SMALLINT 

, WM_DOM_ORDER_LINE_STATUS                   SMALLINT 

, WM_RMA_LINE_STATUS                         SMALLINT 

, WM_REASON_ID                               INT 

, WM_BACK_ORD_REASON                         STRING 

, WM_REASON_CODES_GROUP_ID                   BIGINT 

, WM_STORE_FACILITY_ALIAS_ID                 STRING 

, WM_STORE_FACILITY_ID                       INT 

, WM_HUB1_FACILITY_ALIAS_ID                  STRING 

, WM_HUB1_FACILITY_ID                        INT 

, WM_HUB2_FACILITY_ALIAS_ID                  STRING 

, WM_HUB2_FACILITY_ID                        INT 

, WM_MERGE_FACILITY                          INT 

, WM_MERGE_FACILITY_ALIAS_ID                 STRING 

, WM_SKU_ID                                  INT 

, WM_DOM_SUB_SKU_ID                          BIGINT 

, WM_SKU                                     STRING 

, WM_CUST_SKU                                STRING 

, WM_SKU_GTIN                                STRING 

, WM_SKU_SUB_CD_ID                           INT 

, WM_SKU_SUB_CD_VALUE                        STRING 

, WM_COMMODITY_CD_ID                         BIGINT 

, WM_COMMODITY_CLASS                         INT 

, WM_ASSORT_NBR                              STRING 

, WM_MERCHANDIZING_DEPARTMENT_ID             BIGINT 

, WM_MERCH_TYPE                              STRING 

, WM_MERCH_GRP                               STRING 

, WM_STORE_DEPT                              STRING 

, WM_PROD_STAT                               STRING 

, COUNTRY_OF_ORIGIN                          STRING 

, MFG_PLANT                                  STRING 

, MFG_DT DATE

, EXPIRE_TSTMP                               TIMESTAMP 

, WM_UN_NBR_ID                               BIGINT 

, WM_EPC_TRACKING_RFID_VALUE                 STRING 

, WM_PROD_SCHED_REF_NBR                      STRING 

, WM_PICKUP_REFERENCE_NBR                    STRING 

, WM_DELIVERY_REFERENCE_NBR                  STRING 

, WM_PROTECTION_LEVEL_ID                     INT 

, WM_PACKAGE_TYPE_ID                         INT 

, WM_PRODUCT_CLASS_ID                        INT 

, WM_DSG_SERVICE_LEVEL_ID                    BIGINT 

, WM_ACCEPTED_TSTMP                          TIMESTAMP 

, WM_DELIVERY_TZ                             INT 

, WM_PICKUP_TZ                               INT 

, WM_PICKUP_START_TSTMP                      TIMESTAMP 

, WM_PICKUP_END_TSTMP                        TIMESTAMP 

, WM_DELIVERY_START_TSTMP                    TIMESTAMP 

, WM_DELIVERY_END_TSTMP                      TIMESTAMP 

, WM_PROMISED_DLVR_TSTMP                     TIMESTAMP 

, WM_REQ_DLVR_TSTMP                          TIMESTAMP 

, WM_MUST_DLVR_TSTMP                         TIMESTAMP 

, WM_COMM_DLVR_TSTMP                         TIMESTAMP 

, WM_COMM_SHIP_TSTMP                         TIMESTAMP 

, WM_SHIP_BY_DT DATE

, WM_INVN_TYPE                               STRING 

, WM_BATCH_NBR                               STRING 

, WM_PPACK_GRP_CD                            STRING 

, WM_SHELF_DAYS                              INT 

, WM_FRT_CLASS                               STRING 

, WM_GROUP_ID                                INT 

, WM_SUB_GROUP_ID                            INT 

, WM_ALLOC_TMPL_ID                           INT 

, WM_RELEASE_TMPL_ID                         INT 

, WM_WORKFLOW_ID                             INT 

, VARIANT_VALUE                              STRING 

, WM_PROCESS_TYPE                            STRING 

, WM_PRIORITY                                BIGINT 

, WM_OUTBD_LPN_BRK_ATTR                      STRING 

, WM_EFFECTIVE_RANK                          STRING 

, WM_SO_LN_FULFILL_OPTION                    TINYINT 

, WM_SUPPLY_ASN_DTL_ID                       BIGINT 

, WM_SUPPLY_ASN_ID                           STRING 

, WM_SUPPLY_PO_ID                            STRING 

, WM_DSG_MOT_ID                              BIGINT 

, WM_INBD_LPN_ID                             BIGINT 

, WM_OUTBD_LPN_EPC_TYPE                      TINYINT 

, WM_OUTBD_LPN_SIZE                          STRING 

, WM_OUTBD_LPN_TYPE                          STRING 

, WM_DSG_SHIP_VIA                            STRING 

, WM_DSG_CARRIER_ID                          BIGINT 

, WM_DSG_CARRIER_CD                          STRING 

, WM_ORDER_CONSOL_ATTR                       STRING 

, WM_WF_NODE_ID                              SMALLINT 

, WM_WF_PROCESS_STATE                        TINYINT 

, WM_ALLOC_FINALIZER                         STRING 

, WM_INVENTORY_SEGMENT_ID                    BIGINT 

, WM_PLT_TYPE                                STRING 

, WM_PACKAGE_TYPE_INSTANCE                   STRING 

, WM_NEW_LINE_TYPE                           TINYINT 

, ORDER_FULFILLMENT_OPTION                   STRING 

, WM_INV_DISPOSITION                         STRING 

, WM_EXT_PLAN_ID                             STRING 

, WM_RETURN_ACTION_TYPE                      INT 

, WM_PALLET_CONTENT_CD                       STRING 

, DO_CREATED_FLAG                            TINYINT 

, READY_TO_SHIP_FLAG                         TINYINT 

, HAZMAT_FLAG                                TINYINT 

, CANCELLED_FLAG                             TINYINT 

, CLOSED_FLAG                                TINYINT 

, BONDED_FLAG                                TINYINT 

, DELETED_FLAG                               TINYINT 

, LOCKED_FLAG                                TINYINT 

, HAS_ERRORS_FLAG                            TINYINT 

, FLOW_THROUGH_FLAG                          TINYINT 

, NEVER_OUT_FLAG                             TINYINT 

, QUANTITY_LOCKED_FLAG                       TINYINT 

, VARIABLE_WEIGHT_FLAG                       TINYINT 

, HAS_ROUTING_REQUEST_FLAG                   TINYINT 

, ALLOW_SUBSTITUTION_FLAG                    TINYINT 

, EXPIRE_DATE_REQD_FLAG                      SMALLINT 

, APPLY_PROMOTIONAL_FLAG                     TINYINT 

, PRE_PACK_FLAG                              TINYINT 

, PROCESS_FLAG                               TINYINT 

, ON_HOLD_FLAG                               TINYINT 

, ASSOCIATED_TO_OUTBOUND_FLAG                TINYINT 

, GIFT_FLAG                                  TINYINT 

, PROCESS_IMMEDIATE_NEEDS_FLAG               TINYINT 

, HAS_CHILD_FLAG                             TINYINT 

, RETURNABLE_FLAG                            TINYINT 

, EXCHANGEABLE_FLAG                          TINYINT 

, HAS_COMPONENT_ITEM_FLAG                    TINYINT 

, ALLOW_RESIDUAL_PACK_FLAG                   TINYINT 

, PRICE_OVERRIDE_FLAG                        TINYINT 

, TAX_INCLUDED_FLAG                          TINYINT 

, ADDR_VALID_FLAG                            TINYINT 

, WM_O_FACILITY_ID                           INT 

, WM_O_FACILITY_ALIAS_ID                     STRING 

, WM_D_FACILITY_ID                           INT 

, WM_D_FACILITY_ALIAS_ID                     STRING 

, D_NAME                                     STRING 

, D_LAST_NAME                                STRING 

, D_ADDR_1                                   STRING 

, D_ADDR_2                                   STRING 

, D_ADDR_3                                   STRING 

, D_CITY                                     STRING 

, D_STATE_PROV                               STRING 

, D_POSTAL_CD                                STRING 

, D_COUNTY                                   STRING 

, D_COUNTRY_CD                               STRING 

, D_PHONE_NBR                                STRING 

, D_FAX_NBR                                  STRING 

, D_EMAIL                                    STRING 

, WM_BILL_FACILITY_ALIAS_ID                  STRING 

, WM_BILL_FACILITY_ID                        INT 

, BILL_TO_ADDR_1                             STRING 

, BILL_TO_ADDR_2                             STRING 

, BILL_TO_ADDR_3                             STRING 

, BILL_TO_CITY                               STRING 

, BILL_TO_STATE_PROV                         STRING 

, BILL_TO_COUNTY                             STRING 

, BILL_TO_POSTAL_CD                          STRING 

, BILL_TO_COUNTRY_CD                         STRING 

, BILL_TO_PHONE_NBR                          STRING 

, BILL_TO_FAX_NBR                            STRING 

, BILL_TO_EMAIL                              STRING 

, STACK_LENGTH_VALUE                         DECIMAL(16,4) 

, WM_STACK_LENGTH_STANDARD_UOM               INT 

, STACK_WIDTH_VALUE                          DECIMAL(16,4) 

, WM_STACK_WIDTH_STANDARD_UOM                INT 

, STACK_HEIGHT_VALUE                         DECIMAL(16,4) 

, WM_STACK_HEIGHT_STANDARD_UOM               INT 

, STACK_DIAMETER_VALUE                       DECIMAL(16,4) 

, WM_STACK_DIAMETER_STANDARD_UOM             INT 

, STD_PACK_QTY                               DECIMAL(13,4) 

, STD_LPN_QTY                                DECIMAL(13,4) 

, STD_SUB_PACK_QTY                           DECIMAL(13,4) 

, STD_BUNDL_QTY                              DECIMAL(13,4) 

, STD_PLT_QTY                                DECIMAL(13,4) 

, STD_INBD_LPN_QTY                           DECIMAL(13,4) 

, STD_INBD_LPN_VOL                           DECIMAL(13,4) 

, STD_INBD_LPN_WT                            DECIMAL(13,4) 

, LPN_SIZE                                   DECIMAL(16,4) 

, LINE_TOTAL                                 DECIMAL(13,4) 

, NET_NEEDS                                  DECIMAL(13,4) 

, TOTAL_SIZE_ON_ORDERS                       DECIMAL(13,4) 

, REQ_CAPACITY_PER_UNIT                      INT 

, OVER_PACK_PCT                              INT 

, OVER_SHIP_PCT                              INT 

, ORDER_QTY                                  DECIMAL(13,4) 

, ORIG_ORDER_QTY                             DECIMAL(13,4) 

, ALLOCATED_QTY                              DECIMAL(13,4) 

, RELEASED_QTY                               DECIMAL(13,4) 

, CANCELLED_QTY                              DECIMAL(13,4) 

, PPACK_QTY                                  DECIMAL(13,4) 

, SHIPPED_QTY                                DECIMAL(16,4) 

, RECEIVED_QTY                               DECIMAL(16,4) 

, QTY_CONV_FACTOR                            DECIMAL(17,8) 

, WM_QTY_UOM_ID_BASE                         INT 

, WM_QTY_UOM_ID                              BIGINT 

, INBD_LPNS_RCVD                             INT 

, PLANNED_WEIGHT                             DECIMAL(13,4) 

, SHIPPED_WEIGHT                             DECIMAL(13,4) 

, RECEIVED_WEIGHT                            DECIMAL(13,4) 

, WM_WEIGHT_UOM_ID_BASE                      INT 

, WM_WEIGHT_UOM_ID                           INT 

, PLANNED_VOLUME                             DECIMAL(13,4) 

, RECEIVED_VOLUME                            DECIMAL(13,4) 

, SHIPPED_VOLUME                             DECIMAL(13,4) 

, WM_VOLUME_UOM_ID_BASE                      INT 

, WM_VOLUME_UOM_ID                           INT 

, PACKED_SIZE_VALUE                          DECIMAL(13,4) 

, SIZE1_VALUE                                DECIMAL(13,4) 

, RECEIVED_SIZE1                             DECIMAL(13,4) 

, SHIPPED_SIZE1                              DECIMAL(13,4) 

, WM_SIZE1_UOM_ID                            INT 

, SIZE2_VALUE                                DECIMAL(13,4) 

, RECEIVED_SIZE2                             DECIMAL(13,4) 

, SHIPPED_SIZE2                              DECIMAL(13,4) 

, WM_SIZE2_UOM_ID                            INT 

, GIFT_CARD_VALUE                            DECIMAL(13,4) 

, RETAIL_PRICE                               DECIMAL(16,4) 

, ORIGINAL_PRICE                             DECIMAL(13,4) 

, ORIG_BUDG_COST                             DECIMAL(13,4) 

, UNIT_MONETARY_VALUE                        DECIMAL(13,4) 

, TOTAL_MONETARY_VALUE                       DECIMAL(13,4) 

, WM_MV_SIZE_UOM_ID                          INT 

, MV_CURRENCY_CD                             STRING 

, UNIT_TAX_AMT                               DECIMAL(16,4) 

, BUDGETED_COST                              DECIMAL(13,4) 

, BUDGETED_COST_CURRENCY_CD                  STRING 

, ACTUAL_COST                                DECIMAL(13,4) 

, ACTUAL_CURRENCY_CD                         STRING 

, FREIGHT_REVENUE                            DECIMAL(13,4) 

, FREIGHT_REVENUE_CURRENCY_CD                STRING 

, SKU_ATTR_1                                 STRING 

, SKU_ATTR_2                                 STRING 

, SKU_ATTR_3                                 STRING 

, SKU_ATTR_4                                 STRING 

, SKU_ATTR_5                                 STRING 

, REF_FIELD_1                                STRING 

, REF_FIELD_2                                STRING 

, REF_FIELD_3                                STRING 

, REF_FIELD_4                                STRING 

, REF_FIELD_5                                STRING 

, REF_FIELD_6                                STRING 

, REF_FIELD_7                                STRING 

, REF_FIELD_8                                STRING 

, REF_FIELD_9                                STRING 

, REF_FIELD_10                               STRING 

, REF_NUM_1                                  DECIMAL(13,5) 

, REF_NUM_2                                  DECIMAL(13,5) 

, REF_NUM_3                                  DECIMAL(13,5) 

, REF_NUM_4                                  DECIMAL(13,5) 

, REF_NUM_5                                  DECIMAL(13,5) 

, REF_BOOLEAN_1                              TINYINT 

, REF_BOOLEAN_2                              TINYINT 

, REF_SYS_CD_1                               STRING 

, REF_SYS_CD_2                               STRING 

, REF_SYS_CD_3                               STRING 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_EXT_CREATED_TSTMP                       TIMESTAMP 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_purchase_orders_line_item' 
;

--DISTRIBUTE ON (WM_PURCHASE_ORDERS_ID, WM_PURCHASE_ORDERS_LINE_ITEM_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_PURCHASE_ORDERS_LINE_STATUS" , ***** Creating table: "WM_PURCHASE_ORDERS_LINE_STATUS"


CREATE TABLE  WM_PURCHASE_ORDERS_LINE_STATUS
( LOCATION_ID INT not null

, WM_PURCHASE_ORDERS_LINE_STATUS             SMALLINT                not null

, WM_PURCHASE_ORDERS_LINE_STATUS_DESC        STRING 

, WM_PURCHASE_ORDERS_LINE_STATUS_NOTE        STRING 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_purchase_orders_line_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_PURCHASE_ORDERS_STATUS" , ***** Creating table: "WM_PURCHASE_ORDERS_STATUS"


CREATE TABLE  WM_PURCHASE_ORDERS_STATUS
( LOCATION_ID INT not null

, WM_PURCHASE_ORDERS_STATUS                  SMALLINT                not null

, WM_PURCHASE_ORDERS_STATUS_DESC             STRING 

, WM_PURCHASE_ORDERS_STATUS_NOTE             STRING 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_purchase_orders_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_PUTAWAY_LOCK" , ***** Creating table: "WM_PUTAWAY_LOCK"


CREATE TABLE  WM_PUTAWAY_LOCK
( LOCATION_ID INT not null

, WM_LOCN_ID                                 STRING       not null

, LOCK_COUNTER                               SMALLINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_putaway_lock' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_RACK_TYPE" , ***** Creating table: "WM_RACK_TYPE"


CREATE TABLE  WM_RACK_TYPE
( LOCATION_ID INT not null

, WM_RACK_TYPE_ID                            BIGINT               not null

, WM_WHSE_CD                                 STRING 

, WM_RACK_TYPE_NAME                          STRING 

, WM_RACK_TYPE_DESC                          STRING 

, SEQ_NBR                                    BIGINT 

, WM_REF_LEVEL_ID                            BIGINT 

, RACK_CLASS                                 BIGINT 

, FIX_LABELS                                 TINYINT 

, MOVABLE_LABEL                              TINYINT 

, LABEL_WID                                  DECIMAL(13,4) 

, OVERLAP_DIST                               DECIMAL(13,4) 

, HN_CONSIDER                                TINYINT 

, UPRIGHT_THICKNESS                          DECIMAL(9,4) 

, FUNCTION_TYPE                              SMALLINT 

, OVERSTOCK_CONSIDER                         TINYINT 

, USE_3D_SLOT                                TINYINT 

, WEEKS_IN_SLOT_CONSTRAINT                   TINYINT 

, NUM_OF_BAYS_AVAIL                          DECIMAL(13,4) 

, WEEK_IN_SLOT                               DECIMAL(13,4) 

, PARENT_BT                                  BIGINT 

, WT_LIMIT                                   DECIMAL(13,4) 

, HT     DECIMAL(13,4) 

, WIDTH                                      DECIMAL(13,4) 

, DEPTH                                      DECIMAL(13,4) 

, CASE_HT_MIN                                DECIMAL(13,4) 

, CASE_HT_MAX                                DECIMAL(13,4) 

, CASE_LEN_MIN                               DECIMAL(13,4) 

, CASE_LEN_MAX                               DECIMAL(13,4) 

, CASE_WID_MIN                               DECIMAL(13,4) 

, CASE_WID_MAX                               DECIMAL(13,4) 

, CASE_WT_MIN                                DECIMAL(13,4) 

, CASE_WT_MAX                                DECIMAL(13,4) 

, CASE_CUBE_MIN                              DECIMAL(13,4) 

, CASE_CUBE_MAX                              DECIMAL(13,4) 

, CASE_INVEN_MIN                             DECIMAL(13,4) 

, CASE_INVEN_MAX                             DECIMAL(13,4) 

, CASE_MOVE_MIN                              DECIMAL(13,4) 

, CASE_MOVE_MAX                              DECIMAL(13,4) 

, CASE_HITS_MIN                              DECIMAL(13,4) 

, CASE_HITS_MAX                              DECIMAL(13,4) 

, INNER_HT_MIN                               DECIMAL(13,4) 

, INNER_HT_MAX                               DECIMAL(13,4) 

, INNER_LEN_MIN                              DECIMAL(13,4) 

, INNER_LEN_MAX                              DECIMAL(13,4) 

, INNER_WID_MIN                              DECIMAL(13,4) 

, INNER_WID_MAX                              DECIMAL(13,4) 

, INNER_WT_MIN                               DECIMAL(13,4) 

, INNER_WT_MAX                               DECIMAL(13,4) 

, INNER_CUBE_MIN                             DECIMAL(13,4) 

, INNER_CUBE_MAX                             DECIMAL(13,4) 

, EACH_HT_MIN                                DECIMAL(13,4) 

, EACH_HT_MAX                                DECIMAL(13,4) 

, EACH_LEN_MIN                               DECIMAL(13,4) 

, EACH_LEN_MAX                               DECIMAL(13,4) 

, EACH_WID_MIN                               DECIMAL(13,4) 

, EACH_WID_MAX                               DECIMAL(13,4) 

, EACH_WT_MIN                                DECIMAL(13,4) 

, EACH_WT_MAX                                DECIMAL(13,4) 

, EACH_CUBE_MIN                              DECIMAL(13,4) 

, EACH_CUBE_MAX                              DECIMAL(13,4) 

, PALLET_INVEN_MIN                           DECIMAL(13,4) 

, PALLET_INVEN_MAX                           DECIMAL(13,4) 

, CUBE_MOVE_MIN                              DECIMAL(13,4) 

, CUBE_MOVE_MAX                              DECIMAL(13,4) 

, CUBE_HITS_MIN                              DECIMAL(13,4) 

, CUBE_HITS_MAX                              DECIMAL(13,4) 

, CUBE_INVEN_MIN                             DECIMAL(13,4) 

, CUBE_INVEN_MAX                             DECIMAL(13,4) 

, VISC_MIN                                   DECIMAL(13,4) 

, VISC_MAX                                   DECIMAL(13,4) 

, FILL_PERCENT_MIN                           DECIMAL(5,2) 

, FILL_PERCENT_MAX                           DECIMAL(5,2) 

, RESERVED_1                                 STRING 

, RESERVED_2                                 STRING 

, RESERVED_3                                 BIGINT 

, RESERVED_4                                 BIGINT 

, WM_MOD_USER                                STRING 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_rack_type' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_RACK_TYPE_LEVEL" , ***** Creating table: "WM_RACK_TYPE_LEVEL"


CREATE TABLE  WM_RACK_TYPE_LEVEL
( LOCATION_ID INT not null

, WM_RACK_LEVEL_ID                           BIGINT               not null

, WM_RACK_TYPE_ID                            BIGINT 

, LEVEL_PRIORITY                             BIGINT 

, LEVEL_NBR                                  BIGINT 

, LEVEL_WT_LIMIT                             DECIMAL(13,4) 

, LEVEL_HT                                   DECIMAL(13,4) 

, LEVEL_WIDTH                                DECIMAL(13,4) 

, LEVEL_DEPTH                                DECIMAL(13,4) 

, USE_LEV_MAP_STR_FLAG                       TINYINT 

, LEV_MAP_STR                                STRING 

, USE_POS_MAP_STR_FLAG                       TINYINT 

, POS_MAP_STR                                STRING 

, NBR_OF_SLOTS                               BIGINT 

, SLOT_WT_LIMIT                              DECIMAL(13,4) 

, MIN_SL_WIDTH                               DECIMAL(13,4) 

, MIN_ITEM_WIDTH                             DECIMAL(13,4) 

, MAX_OVERHANG_PCT                           BIGINT 

, SLOT_UNITS                                 BIGINT 

, SHIP_UNITS                                 BIGINT 

, COST_TO_PICK                               DECIMAL(13,4) 

, MAX_LANE_WT                                DECIMAL(13,4) 

, HT_CLEARANCE                               DECIMAL(13,4) 

, SIDE_CLEAR                                 DECIMAL(13,4) 

, MAX_STACKING                               BIGINT 

, MAX_LANES                                  BIGINT 

, GUIDE_WIDTH                                DECIMAL(13,4) 

, LANE_GUIDES                                TINYINT 

, SLOT_GUIDES                                TINYINT 

, MOVE_GUIDES                                TINYINT 

, USE_FULL_WID_FLAG                          TINYINT 

, SHELF_THICKNESS                            DECIMAL(9,4) 

, LABEL_POS                                  DECIMAL(9,4) 

, MAX_PICK_HT                                DECIMAL(9,4) 

, MAX_SL_WID                                 DECIMAL(13,4) 

, ONE_FACING_PER_DSW_FLAG                    TINYINT 

, USE_INCREMENT_FLAG                         TINYINT 

, WIDTH_INCREMENT                            DECIMAL(13,4) 

, USE_VERT_DIV_FLAG                          TINYINT 

, VERT_DIV_HT                                DECIMAL(13,4) 

, MIN_CHANNEL_WID                            DECIMAL(13,4) 

, MAX_CHANNEL_WID                            DECIMAL(13,4) 

, MAX_CHANNEL_WT                             DECIMAL(13,4) 

, MAX_ADJ_SLOTS                              BIGINT 

, MAX_EJECTOR_WT                             DECIMAL(13,4) 

, AFRAME_ROTATE                              TINYINT 

, AFRAME_MIN_HT                              DECIMAL(13,4) 

, AFRAME_MAX_HT                              DECIMAL(13,4) 

, AFRAME_MIN_LEN                             DECIMAL(13,4) 

, AFRAME_MAX_LEN                             DECIMAL(13,4) 

, AFRAME_MIN_WID                             DECIMAL(13,4) 

, AFRAME_MAX_WID                             DECIMAL(13,4) 

, AFRAME_MIN_WT                              DECIMAL(13,4) 

, AFRAME_MAX_WT                              DECIMAL(13,4) 

, REACH_DIST                                 DECIMAL(13,4) 

, MOVE_GUIDES_RGT                            TINYINT 

, MOVE_GUIDES_LFT                            TINYINT 

, RESERVED_1                                 STRING 

, RESERVED_2                                 STRING 

, RESERVED_3                                 BIGINT 

, RESERVED_4                                 BIGINT 

, WM_MOD_USER                                STRING 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_rack_type_level' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_RESV_LOCN_HDR" , ***** Creating table: "WM_RESV_LOCN_HDR"


CREATE TABLE  WM_RESV_LOCN_HDR
( LOCATION_ID INT not null

, WM_RESV_LOCN_HDR_ID                        INT                not null

, WM_LOCN_ID                                 STRING 

, WM_LOCN_HDR_ID                             INT 

, WM_LOCN_PUTAWAY_LOCK                       STRING 

, WM_LOCN_SIZE_TYPE                          STRING 

, WM_INVN_LOCK_CD                            STRING 

, WM_PACK_ZONE                               STRING 

, SORT_LOCN_FLAG                             TINYINT 

, INBD_STAGING_FLAG                          TINYINT 

, WM_DEDCTN_BATCH_NBR                        STRING 

, WM_DEDCTN_ITEM_ID                          BIGINT 

, DEDCTN_PACK_QTY                            DECIMAL(9,2) 

, CURR_WT                                    DECIMAL(13,4) 

, CURR_VOL                                   DECIMAL(13,4) 

, CURR_UOM_QTY                               DECIMAL(9,2) 

, DIRCT_WT                                   DECIMAL(13,4) 

, DIRCT_VOL                                  DECIMAL(13,4) 

, DIRCT_UOM_QTY                              DECIMAL(9,2) 

, MAX_WT                                     DECIMAL(13,4) 

, MAX_VOL                                    DECIMAL(13,4) 

, MAX_UOM_QTY                                DECIMAL(15,5) 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_resv_locn_hdr' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_SEC_USER" , ***** Creating table: "WM_SEC_USER"


CREATE TABLE  WM_SEC_USER
( LOCATION_ID INT not null

, WM_SEC_USER_ID                             INT                not null

, WM_LOGIN_USER_ID                           STRING 

, WM_USER_NAME                               STRING 

, WM_USER_DESC                               STRING 

, WM_SEC_POLICY_SET_ID                       INT 

, WM_PSWD                                    STRING 

, WM_PSWD_EXP_DT DATE

, PSWD_CHANGE_AT_LOGIN_FLAG                  STRING 

, CAN_CHNG_PSWD_FLAG                         STRING 

, DISABLED_FLAG                              STRING 

, LOCKED_OUT_FLAG                            STRING 

, LAST_LOGIN_TSTMP                           TIMESTAMP 

, GRACE_LOGINS                               INT 

, LOCKED_OUT_EXP_TSTMP                       TIMESTAMP 

, FAILED_LOGIN_ATTEMPTS                      INT 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                   not null

, LOAD_TSTMP                                 TIMESTAMP                   not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_sec_user' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_SHIPMENT" , ***** Creating table: "WM_SHIPMENT"


CREATE TABLE  WM_SHIPMENT
( LOCATION_ID INT not null

, WM_SHIPMENT_ID                             BIGINT               not null

, WM_TC_COMPANY_ID                           INT 

, WM_BUSINESS_PARTNER_ID                     STRING 

, WM_RS_AREA_ID                              INT 

, WM_TC_SHIPMENT_ID                          STRING 

, WM_PP_SHIPMENT_ID                          BIGINT 

, WM_REF_SHIPMENT_NBR                        STRING 

, WM_SHIPMENT_TYPE                           STRING 

, WM_SHIPMENT_STATUS                         SMALLINT 

, WM_SHIPMENT_REF_ID                         STRING 

, WM_SHIP_GROUP_ID                           STRING 

, WM_CREATION_TYPE                           TINYINT 

, WM_EVENT_IND_TYPE_ID                       BIGINT 

, WM_ASSIGNED_CM_SHIPMENT_ID                 BIGINT 

, WM_EXT_SYS_SHIPMENT_ID                     STRING 

, WM_REC_CM_ID                               BIGINT 

, WM_REC_CM_SHIPMENT_ID                      BIGINT 

, WM_REC_LANE_ID                             BIGINT 

, WM_REC_LANE_DETAIL_ID                      BIGINT 

, WM_PURCHASE_ORDER                          STRING 

, WM_WMS_STATUS_CD                           SMALLINT 

, WM_GRS_MAX_SHIPMENT_STATUS                 SMALLINT 

, WM_BILL_OF_LADING_NBR                      STRING 

, WM_BOOKING_ID                              BIGINT 

, WM_CONS_LOCN_ID                            STRING 

, WM_CONS_ADDR_CD                            STRING 

, WM_CONS_RUN_ID                             BIGINT 

, WM_RS_TYPE                                 STRING 

, WM_RS_CONFIG_ID                            INT 

, WM_RS_CONFIG_CYCLE_ID                      INT 

, WM_CONFIG_CYCLE_SEQ                        SMALLINT 

, WM_CFMF_STATUS                             TINYINT 

, WM_CM_ID                                   BIGINT 

, WM_FACILITY_SCHEDULE_ID                    INT 

, WM_CUSTOMER_ID                             BIGINT 

, WM_ASSIGNED_CUSTOMER_ID                    BIGINT 

, WM_CUSTOMER_CREDIT_LIMIT_ID                BIGINT 

, WM_PRODUCT_CLASS_ID                        INT 

, WM_COMMODITY_CLASS                         INT 

, WM_COMMODITY_CD_ID                         BIGINT 

, WM_MERCHANDIZING_DEPARTMENT_ID             BIGINT 

, WM_PROD_SCHED_REF_NBR                      STRING 

, DELIVERY_REQUIREMENT                       STRING 

, DROP_OFF_PICKUP                            STRING 

, WM_PRIORITY_TYPE                           TINYINT 

, WM_BILLING_METHOD                          TINYINT 

, WM_EQUIPMENT_TYPE                          SMALLINT 

, WM_ASSIGNED_EQUIPMENT_ID                   BIGINT 

, WM_DSG_EQUIPMENT_ID                        BIGINT 

, WM_FEASIBLE_EQUIPMENT_ID                   BIGINT 

, WM_FEASIBLE_EQUIPMENT2_ID                  INT 

, WM_REC_EQUIPMENT_ID                        BIGINT 

, WM_PRO_NBR                                 STRING 

, WM_TRANS_RESP_CD                           STRING 

, WM_BROKER_REF                              STRING 

, WM_TRACTOR_NBR                             STRING 

, WM_PACKAGING                               STRING 

, WM_GRS_OPERATION                           TINYINT 

, WM_UN_NBR_ID                               BIGINT 

, WM_CONTRACT_NBR                            STRING 

, WM_SHIPMENT_LEG_TYPE                       BIGINT 

, WM_OCEAN_ROUTING_STAGE                     TINYINT 

, WM_NUM_CHARGE_LAYOVERS                     SMALLINT 

, WM_RATE_TYPE                               STRING 

, WM_RATE_UOM                                STRING 

, WM_FEASIBLE_DRIVER_TYPE                    BIGINT 

, WM_WAVE_ID                                 BIGINT 

, WM_LOC_REFERENCE                           STRING 

, WM_EQUIP_UTIL_PER                          DECIMAL(5,2) 

, WM_MOVE_TYPE                               TINYINT 

, WM_DELAY_TYPE                              INT 

, WM_DRIVER_TYPE_ID                          BIGINT 

, WM_DESIGNATED_TRACTOR_CD                   STRING 

, WM_DESIGNATED_DRIVER_TYPE                  BIGINT 

, WM_TANDEM_PATH_ID                          INT 

, WM_ASSIGNED_MOT_ID                         BIGINT 

, WM_DSG_MOT_ID                              BIGINT 

, WM_FEASIBLE_MOT_ID                         BIGINT 

, WM_REC_MOT_ID                              BIGINT 

, WM_PROTECTION_LEVEL_ID                     INT 

, WM_ASSIGNED_SERVICE_LEVEL_ID               BIGINT 

, WM_DSG_SERVICE_LEVEL_ID                    BIGINT 

, WM_FEASIBLE_SERVICE_LEVEL_ID               BIGINT 

, WM_REC_SERVICE_LEVEL_ID                    BIGINT 

, WM_DT_PARAM_SET_ID                         INT 

, WM_STATIC_ROUTE_ID                         INT 

, WM_AUTH_NBR                                STRING 

, WM_TRLR_TYPE                               STRING 

, WM_TRLR_SIZE                               STRING 

, WM_TRLR_GEN_CD                             STRING 

, WM_HUB_ID                                  BIGINT 

, WM_DOOR                                    STRING 

, WM_APPT_DOOR_SCHED_TYPE                    STRING 

, WM_SEAL_NBR                                STRING 

, WM_MANIFEST_ID                             INT 

, DAYS_TO_DELIVER                            TINYINT 

, WM_SERV_AREA_CD                            STRING 

, WM_PRINT_CONS_BOL                          STRING 

, WM_LANE_NAME                               STRING 

, WM_ASSIGNED_SHIP_VIA                       STRING 

, WM_INSURANCE_STATUS                        INT 

, WM_TRAILER_NBR                             STRING 

, WM_TRANS_PLAN_OWNER                        TINYINT 

, WM_DSG_VOYAGE_FLIGHT                       STRING 

, WM_FEASIBLE_VOYAGE_FLIGHT                  STRING 

, WM_INCOTERM_ID                             INT 

, WM_STAGING_LOCN_ID                         STRING 

, WM_LOADING_SEQ_ORD                         STRING 

, WM_HAZMAT_CERT_CONTACT                     STRING 

, WM_HAZMAT_CERT_DECLARATION                 STRING 

, WM_PLN_MIN_TEMPERATURE                     DECIMAL(8,2) 

, WM_PLN_MAX_TEMPERATURE                     DECIMAL(8,2) 

, WM_TEMPERATURE_UOM                         INT 

, WM_AVAILABLE_TSTMP                         TIMESTAMP 

, WM_RECEIVED_TSTMP                          TIMESTAMP 

, WM_PICK_START_TSTMP                        TIMESTAMP 

, WM_SCHEDULED_PICKUP_TSTMP                  TIMESTAMP 

, WM_PICKUP_START_TSTMP                      TIMESTAMP 

, WM_PICKUP_END_TSTMP                        TIMESTAMP 

, WM_PICKUP_TZ                               SMALLINT 

, WM_DELIVERY_START_TSTMP                    TIMESTAMP 

, WM_DELIVERY_END_TSTMP                      TIMESTAMP 

, WM_DELIVERY_TZ                             SMALLINT 

, WM_SHIPMENT_START_TSTMP                    TIMESTAMP 

, WM_SHIPMENT_END_TSTMP                      TIMESTAMP 

, WM_TENDER_TSTMP                            TIMESTAMP 

, WM_TENDER_RESP_DEADLINE_TSTMP              TIMESTAMP 

, WM_TENDER_RESP_DEADLINE_TZ                 SMALLINT 

, WM_CURRENCY_TSTMP                          TIMESTAMP 

, WM_CYCLE_DEADLINE_TSTMP                    TIMESTAMP 

, WM_CYCLE_EXECUTION_TSTMP                   TIMESTAMP 

, WM_CYCLE_RESP_DEADLINE_TZ                  SMALLINT 

, WM_LAST_RS_NOTIFICATION_TSTMP              TIMESTAMP 

, WM_PAPERWORK_START_TSTMP                   TIMESTAMP 

, WM_VEHICLE_CHECK_START_TSTMP               TIMESTAMP 

, WM_ESTIMATED_DISPATCH_TSTMP                TIMESTAMP 

, WM_LAST_SELECTOR_RUN_TSTMP                 TIMESTAMP 

, WM_LAST_CM_OPTION_GEN_TSTMP                TIMESTAMP 

, WM_EXTRACTION_TSTMP                        TIMESTAMP 

, WM_STATUS_CHANGE_TSTMP                     TIMESTAMP 

, WM_SENT_TO_PKMS_TSTMP                      TIMESTAMP 

, WM_SHIPMENT_RECON_TSTMP                    TIMESTAMP 

, WM_LAST_RUN_GRS_TSTMP                      TIMESTAMP 

, WM_BK_DEPARTURE_TSTMP                      TIMESTAMP 

, WM_BK_DEPARTURE_TZ                         SMALLINT 

, WM_BK_ARRIVAL_TSTMP                        TIMESTAMP 

, WM_BK_ARRIVAL_TZ                           SMALLINT 

, WM_BK_PICKUP_TSTMP                         TIMESTAMP 

, WM_BK_PICKUP_TZ                            SMALLINT 

, WM_BK_CUTOFF_TSTMP                         TIMESTAMP 

, WM_BK_CUTOFF_TZ                            SMALLINT 

, CM_OPTION_GEN_ACTIVE_FLAG                  TINYINT 

, RS_CYCLE_REMAINING_FLAG                    TINYINT 

, TIME_FEAS_ENABLED_FLAG                     TINYINT 

, ON_TIME_FLAG                               TINYINT 

, HAS_NOTES_FLAG                             TINYINT 

, HAS_ALERTS_FLAG                            TINYINT 

, HAS_IMPORT_ERROR_FLAG                      TINYINT 

, HAS_SOFT_CHECK_ERROR_FLAG                  TINYINT 

, HAS_TRACKING_MSG_FLAG                      TINYINT 

, TRACKING_MSG_PROBLEM_FLAG                  TINYINT 

, HAZMAT_FLAG                                TINYINT 

, PERISHABLE_FLAG                            TINYINT 

, CANCELLED_FLAG                             TINYINT 

, UPDATE_SENT_FLAG                           TINYINT 

, BUSINESS_PROCESS_FLAG                      TINYINT 

, AUTO_DELIVERED_FLAG                        TINYINT 

, SENT_TO_PKMS_FLAG                          TINYINT 

, SHIPMENT_RECONCILED_FLAG                   TINYINT 

, GRS_OPT_CYCLE_RUNNING_FLAG                 TINYINT 

, MANUAL_ASSIGN_FLAG                         TINYINT 

, FIRST_UPDATE_SENT_TO_PKMS_FLAG             TINYINT 

, SHIPMENT_CLOSED_FLAG                       TINYINT 

, WAVE_MAN_CHANGED_FLAG                      TINYINT 

, RETAIN_CONSOLIDATOR_TIMES_FLAG             TINYINT 

, FILO_FLAG                                  TINYINT 

, COOLER_AT_NOSE_FLAG                        TINYINT 

, SHIPMENT_WIN_ADJ_FLAG                      TINYINT 

, MISROUTED_FLAG                             TINYINT 

, ASSOCIATED_TO_OUTBOUND_FLAG                TINYINT 

, HAS_EM_NOTIFY_FLAG                         TINYINT 

, BOOKING_REQUIRED_FLAG                      TINYINT 

, LPN_ASSIGNMENT_STOPPED_FLAG                TINYINT 

, SED_GENERATED_FLAG                         TINYINT 

, USE_BROKER_AS_CARRIER_FLAG                 TINYINT 

, WM_O_FACILITY_NBR                          INT 

, WM_O_FACILITY_ID                           STRING 

, O_ADDRESS                                  STRING 

, O_CITY                                     STRING 

, O_STATE_PROV                               STRING 

, O_POSTAL_CD                                STRING 

, O_COUNTY                                   STRING 

, O_COUNTRY_CD                               STRING 

, WM_D_FACILITY_NBR                          INT 

, WM_D_FACILITY_ID                           STRING 

, D_ADDR                                     STRING 

, D_CITY                                     STRING 

, D_STATE_PROV                               STRING 

, D_POSTAL_CD                                STRING 

, D_COUNTY                                   STRING 

, D_COUNTRY_CD                               STRING 

, WM_BILL_TO_CD                              STRING 

, BILL_TO_TITLE                              STRING 

, BILL_TO_NAME                               STRING 

, BILL_TO_ADDR                               STRING 

, BILL_TO_CITY                               STRING 

, BILL_TO_STATE_PROV                         STRING 

, BILL_TO_POSTAL_CD                          STRING 

, BILL_TO_COUNTRY_CD                         STRING 

, BILL_TO_PHONE_NBR                          STRING 

, WM_O_STOP_LOCATION_NAME                    STRING 

, WM_D_STOP_LOCATION_NAME                    STRING 

, WM_BOOKING_REF_SHIPPER                     STRING 

, WM_BOOKING_REF_CARRIER                     STRING 

, WM_BK_RESOURCE_REF_EXTERNAL                STRING 

, WM_BK_RESOURCE_NAME_EXTERNAL               STRING 

, WM_BK_O_FACILITY_ID                        INT 

, WM_BK_O_FACILITY_ALIAS_ID                  STRING 

, WM_BK_D_FACILITY_ID                        INT 

, WM_BK_D_FACILITY_ALIAS_ID                  STRING 

, WM_BK_MASTER_AIRWAY_BILL                   STRING 

, WM_BK_FORWARDER_AIRWAY_BILL                STRING 

, WM_O_TANDEM_FACILITY                       INT 

, WM_O_TANDEM_FACILITY_ALIAS                 STRING 

, WM_D_TANDEM_FACILITY                       INT 

, WM_D_TANDEM_FACILITY_ALIAS                 STRING 

, WM_RTE_SWC_NBR                             STRING 

, WM_RTE_TO                                  STRING 

, WM_RTE_TYPE                                STRING 

, WM_RTE_TYPE_1                              STRING 

, WM_RTE_TYPE_2                              STRING 

, WM_REGION_ID                               INT 

, WM_INBOUND_REGION_ID                       INT 

, WM_OUTBOUND_REGION_ID                      INT 

, DISTANCE                                   DECIMAL(9,2) 

, DIRECT_DISTANCE                            DECIMAL(9,2) 

, OUT_OF_ROUTE_DISTANCE                      DECIMAL(9,2) 

, WM_DISTANCE_UOM                            STRING 

, RADIAL_DISTANCE                            DECIMAL(9,2) 

, WM_RADIAL_DISTANCE_UOM                     STRING 

, WM_ASSIGNED_CARRIER_CD                     STRING 

, WM_ASSIGNED_CARRIER_ID                     BIGINT 

, WM_ASSIGNED_BROKER_CARRIER_CD              STRING 

, WM_ASSIGNED_BROKER_CARRIER_ID              BIGINT 

, WM_ASSIGNED_SCNDR_CARRIER_CD               STRING 

, WM_ASSIGNED_SCNDR_CARRIER_ID               BIGINT 

, WM_DSG_CARRIER_CD                          STRING 

, WM_DSG_CARRIER_ID                          BIGINT 

, WM_DSG_SCNDR_CARRIER_CD                    STRING 

, WM_DSG_SCNDR_CARRIER_ID                    BIGINT 

, WM_REC_CARRIER_CD                          STRING 

, WM_REC_CARRIER_ID                          BIGINT 

, WM_REC_BROKER_CARRIER_CD                   STRING 

, WM_REC_BROKER_CARRIER_ID                   BIGINT 

, WM_FEASIBLE_CARRIER_CD                     STRING 

, WM_FEASIBLE_CARRIER_ID                     BIGINT 

, WM_PAYEE_CARRIER_ID                        BIGINT 

, WM_SCNDR_CARRIER_ID                        BIGINT 

, WM_BROKER_CARRIER_ID                       BIGINT 

, WM_LH_PAYEE_CARRIER_CD                     STRING 

, WM_LH_PAYEE_CARRIER_ID                     BIGINT 

, WM_HAULING_CARRIER                         STRING 

, WM_RATING_QUALIFIER                        TINYINT 

, WM_RATING_LANE_ID                          BIGINT 

, WM_RATING_LANE_DETAIL_ID                   BIGINT 

, WM_ASSIGNED_LANE_ID                        BIGINT 

, WM_ASSIGNED_LANE_DETAIL_ID                 BIGINT 

, WM_PLN_RATING_LANE_ID                      BIGINT 

, WM_PLN_RATING_LANE_DETAIL_ID               BIGINT 

, WM_FRT_REV_RATING_LANE_ID                  BIGINT 

, WM_FRT_REV_RATING_LANE_DETAIL_ID           BIGINT 

, WM_REC_RATING_LANE_ID                      BIGINT 

, WM_REC_BUDG_RATING_LANE_ID                 BIGINT 

, WM_REC_RATING_LANE_DETAIL_ID               BIGINT 

, WM_REC_BUDG_RATING_LANE_DETAIL_ID          BIGINT 

, WM_REVENUE_RATING_LEVEL                    TINYINT 

, TOTAL_TIME                                 INT 

, NUM_STOPS                                  SMALLINT 

, NUM_DOCKS                                  SMALLINT 

, MAX_NBR_OF_CTNS                            INT 

, LEFT_WT                                    DECIMAL(13,4) 

, RIGHT_WT                                   DECIMAL(13,4) 

, FINANCIAL_WT                               DECIMAL(13,2) 

, PLANNED_WEIGHT                             DECIMAL(13,4) 

, WM_WEIGHT_UOM_ID_BASE                      INT 

, PLANNED_VOLUME                             DECIMAL(13,4) 

, WM_VOLUME_UOM_ID_BASE                      INT 

, SIZE1_VALUE                                DECIMAL(13,4) 

, WM_SIZE1_UOM_ID                            INT 

, SIZE2_VALUE                                DECIMAL(13,4) 

, WM_SIZE2_UOM_ID                            INT 

, ORDER_QTY                                  DECIMAL(13,4) 

, WM_QTY_UOM_ID                              INT 

, BASELINE_COST                              DECIMAL(13,2) 

, BASELINE_COST_CURRENCY_CD                  STRING 

, ACTUAL_COST                                DECIMAL(13,2) 

, ACTUAL_COST_CURRENCY_CD                    STRING 

, ESTIMATED_COST                             DECIMAL(13,2) 

, LINEHAUL_COST                              DECIMAL(13,2) 

, ACCESSORIAL_COST                           DECIMAL(13,2) 

, STOP_COST                                  DECIMAL(13,2) 

, REPORTED_COST                              DECIMAL(13,2) 

, ACCESSORIAL_COST_TO_CARRIER                DECIMAL(13,2) 

, WAYPOINT_HANDLING_COST                     DECIMAL(10,2) 

, WAYPOINT_TOTAL_COST                        DECIMAL(10,2) 

, ORIG_BUDG_TOTAL_COST                       DECIMAL(13,2) 

, BUDG_TOTAL_COST                            DECIMAL(13,2) 

, BUDG_NORMALIZED_TOTAL_COST                 DECIMAL(13,2) 

, BUDG_CM_DISCOUNT                           DECIMAL(13,2) 

, BUDG_CURRENCY_CD                           STRING 

, REC_LINEHAUL_COST                          DECIMAL(13,2) 

, REC_ACCESSORIAL_COST                       DECIMAL(13,2) 

, REC_SPOT_CHARGE                            DECIMAL(13,2) 

, REC_CM_DISCOUNT                            DECIMAL(13,2) 

, REC_STOP_COST                              DECIMAL(13,2) 

, REC_TOTAL_COST                             DECIMAL(13,2) 

, REC_CURRENCY_CD                            STRING 

, REC_BUDG_LINEHAUL_COST                     DECIMAL(13,2) 

, REC_BUDG_STOP_COST                         DECIMAL(13,2) 

, REC_BUDG_CM_DISCOUNT                       DECIMAL(13,2) 

, REC_BUDG_ACCESSORIAL_COST                  DECIMAL(13,2) 

, REC_BUDG_TOTAL_COST                        DECIMAL(13,2) 

, REC_BUDG_NORMALIZED_TOTAL_COST             DECIMAL(13,2) 

, REC_BUDG_CURRENCY_CD                       STRING 

, PLN_LINEHAUL_COST                          DECIMAL(13,2) 

, PLN_TOTAL_ACCESSORIAL_COST                 DECIMAL(13,2) 

, PLN_STOP_OFF_COST                          DECIMAL(13,2) 

, PLN_NORMALIZED_TOTAL_COST                  DECIMAL(13,2) 

, PLN_ACCESSORL_COST_TO_CARRIER              DECIMAL(13,2) 

, PLN_CARRIER_CHARGE                         DECIMAL(13,2) 

, PLN_TOTAL_COST                             DECIMAL(13,2) 

, PLN_CURRENCY_CD                            STRING 

, FRT_REV_ACCESSORIAL_CHARGE                 DECIMAL(13,4) 

, FRT_REV_CM_DISCOUNT                        DECIMAL(13,4) 

, FRT_REV_LINEHAUL_CHARGE                    DECIMAL(13,4) 

, FRT_REV_SPOT_CHARGE                        DECIMAL(13,4) 

, FRT_REV_SPOT_CHARGE_CURR_CD                STRING 

, FRT_REV_STOP_CHARGE                        DECIMAL(13,4) 

, CUST_FRGT_CHARGE                           DECIMAL(13,2) 

, CARRIER_CHARGE                             DECIMAL(13,2) 

, SPOT_CHARGE                                DECIMAL(13,2) 

, SPOT_CHARGE_AND_PAYEE_ACC                  DECIMAL(13,2) 

, SPOT_CHARGE_AND_PAYEE_ACC_CC               STRING 

, SPOT_CHARGE_CURRENCY_CD                    STRING 

, REC_SPOT_CHARGE_CURRENCY_CD                STRING 

, NORM_SPOT_CHARGE_AND_PAYEE_ACC             DECIMAL(13,2) 

, NORMALIZED_MARGIN                          DECIMAL(13,2) 

, NORMALIZED_BASELINE_COST                   DECIMAL(13,2) 

, NORMALIZED_TOTAL_COST                      DECIMAL(13,2) 

, NORMALIZED_TOTAL_REVENUE                   DECIMAL(13,4) 

, REC_NORMALIZED_TOTAL_COST                  DECIMAL(13,2) 

, REC_MARGIN                                 DECIMAL(13,2) 

, REC_NORMALIZED_MARGIN                      DECIMAL(13,2) 

, TOTAL_TAX_AMOUNT                           DECIMAL(13,2) 

, TOTAL_COST                                 DECIMAL(15,4) 

, TOTAL_COST_EXCL_TAX                        DECIMAL(13,2) 

, TOTAL_REVENUE                              DECIMAL(13,4) 

, TOTAL_REVENUE_CURRENCY_CD                  STRING 

, EARNED_INCOME                              DECIMAL(13,2) 

, EARNED_INCOME_CURRENCY_CD                  STRING 

, DECLARED_VALUE                             DECIMAL(16,4) 

, DV_CURRENCY_CD                             STRING 

, COD_AMT                                    DECIMAL(13,4) 

, COD_CURRENCY_CD                            STRING 

, ESTIMATED_SAVINGS                          DECIMAL(13,2) 

, MARGIN                                     DECIMAL(13,4) 

, MONETARY_VALUE                             DECIMAL(16,4) 

, MV_CURRENCY_CD                             STRING 

, COST_BREAKUP                               STRING 

, WM_CM_DISCOUNT                             DECIMAL(13,3) 

, RATE                                       DECIMAL(16,4) 

, MIN_RATE                                   DECIMAL(15,4) 

, TARIFF                                     DECIMAL(15,4) 

, CURRENCY_CD                                STRING 

, WM_HIBERNATE_VERSION                       BIGINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_shipment' 
;

--DISTRIBUTE ON (WM_SHIPMENT_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_SHIPMENT_STATUS" , ***** Creating table: "WM_SHIPMENT_STATUS"


CREATE TABLE  WM_SHIPMENT_STATUS
( LOCATION_ID INT not null

, WM_SHIPMENT_STATUS                         SMALLINT                 not null

, WM_SHIPMENT_STATUS_DESC                    STRING 

, WM_SHIPMENT_STATUS_SHORT_DESC              STRING 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_shipment_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_SHIP_VIA" , ***** Creating table: "WM_SHIP_VIA"


CREATE TABLE  WM_SHIP_VIA
( LOCATION_ID INT not null

, WM_SHIP_VIA_ID                             INT                 not null

, WM_SHIP_VIA                                STRING 

, WM_SHIP_VIA_DESC                           STRING 

, WM_TC_COMPANY_ID                           INT 

, WM_CARRIER_ID                              BIGINT 

, WM_EXECUTION_LEVEL_ID                      INT 

, WM_MOT_ID                                  BIGINT 

, TRACKING_NBR_REQ_FLAG                      TINYINT 

, WM_SERVICE_LEVEL_ID                        BIGINT 

, WM_SERVICE_LEVEL_IND                       STRING 

, WM_SERVICE_LEVEL_ICON                      STRING 

, WM_LABEL_TYPE                              STRING 

, WM_INS_COVER_TYPE_ID                       TINYINT 

, WM_BILL_SHIP_VIA_ID                        INT 

, WM_CUSTOM_SHIP_VIA_ATTR                    STRING 

, ACCESSORIAL_SEARCH_STRING                  STRING 

, MIN_DECLARED_VALUE                         DECIMAL(14,3) 

, MAX_DECLARED_VALUE                         DECIMAL(14,3) 

, DECLARED_VALUE_CURRENCY                    STRING 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_ship_via' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_SIZE_UOM" , ***** Creating table: "WM_SIZE_UOM"


CREATE TABLE  WM_SIZE_UOM
( LOCATION_ID INT not null

, WM_SIZE_UOM_ID                             INT                 not null

, WM_TC_COMPANY_ID                           INT 

, WM_SIZE_UOM                                STRING 

, WM_SIZE_UOM_DESC                           STRING 

, WM_SIZE_MAPPING                            STRING 

, WM_STANDARD_UOM_ID                         SMALLINT 

, WM_STANDARD_UNITS                          DECIMAL(16,8) 

, WM_CONSOLIDATION_CD                        STRING 

, WM_SPLITTER_CONS_CD                        STRING 

, DO_NOT_INHERIT_TO_ORDER_FLAG               TINYINT 

, APPLY_TO_VENDOR_FLAG                       TINYINT 

, DISCRETE_FLAG                              TINYINT 

, WM_ADJUSTMENT_SIZE                         DECIMAL(8,4) 

, WM_ADJUSTMENT_SIZE_UOM_ID                  INT 

, WM_AUDIT_CREATED_SOURCE_TYPE               SMALLINT 

, WM_AUDIT_CREATED_SOURCE                    STRING 

, WM_AUDIT_CREATED_TSTMP                     TIMESTAMP 

, WM_AUDIT_LAST_UPDATED_SOURCE_TYPE          SMALLINT 

, WM_AUDIT_LAST_UPDATED_SOURCE               STRING 

, WM_AUDIT_LAST_UPDATED_TSTMP                TIMESTAMP 

, WM_HIBERNATE_VERSION                       BIGINT 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_size_uom' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_SLOT_ITEM" , ***** Creating table: "WM_SLOT_ITEM"


CREATE TABLE  WM_SLOT_ITEM
( LOCATION_ID INT not null

, WM_SLOT_ITEM_ID                            DECIMAL(19,0)                not null

, WM_SLOT_ID                                 DECIMAL(19,0) 

, WM_SKU_ID                                  DECIMAL(19,0) 

, WM_REPLEN_GROUP                            STRING 

, WM_ADJ_GRP_ID                              DECIMAL(19,0) 

, WM_MULT_LOC_GRP                            STRING 

, WM_SESSION_ID                              BIGINT 

, WM_RANK                                    BIGINT 

, MAX_LANES                                  BIGINT 

, DEEPS                                      BIGINT 

, SLOT_WIDTH                                 DECIMAL(13,4) 

, PALLET_HI                                  BIGINT 

, NUM_VERT_DIV                               BIGINT 

, REC_LANES                                  BIGINT 

, REC_STACKING                               BIGINT 

, WM_BORROWING_OBJECT                        BIGINT 

, WM_BORROWING_SPECIFIC                      DECIMAL(19,0) 

, HIST_MATCH                                 STRING 

, GAVE_HIST_TO                               DECIMAL(19,0) 

, USE_ESTIMATED_HIST_FLAG                    TINYINT 

, ABW_TEMP_TAG                               STRING 

, WM_NEEDED_RACK_TYPE                        STRING 

, WM_PALLETE_PATTERN                         BIGINT 

, WM_OPT_PALLET_PATTERN                      BIGINT 

, SLOT_LOCKED_FLAG                           TINYINT 

, PRIMARY_MOVE_FLAG                          TINYINT 

, IGN_FOR_RESLOT_FLAG                        TINYINT 

, ALLOW_EXPAND_FLAG                          TINYINT 

, LEGAL_FIT_FLAG                             TINYINT 

, WM_LEGAL_FIT_REASON                        BIGINT 

, ADDED_FOR_MLM_FLAG                         TINYINT 

, FORECAST_BORROWED_FLAG                     TINYINT 

, EST_MVMT_CAN_BORROW_FLAG                   TINYINT 

, EST_HITS_CAN_BORROW_FLAG                   TINYINT 

, DELETE_MULTIPLE_FLAG                       TINYINT 

, WM_SLOT_UNIT                               BIGINT 

, WM_BIN_UNIT                                BIGINT 

, WM_SHIP_UNIT                               BIGINT 

, CURRENT_ORIENTATION                        BIGINT 

, CURRENT_BIN                                BIGINT 

, CURRENT_PALLET                             BIGINT 

, ITEM_COST                                  DECIMAL(9,4) 

, SLOT_UNIT_WEIGHT                           DECIMAL(9,4) 

, TOTAL_ITEM_WT                              DECIMAL(9,4) 

, OPT_FLUID_VOL                              DECIMAL(19,7) 

, CALC_VISC                                  DECIMAL(9,2) 

, EST_VISC                                   DECIMAL(9,2) 

, USER_DEFINED                               DECIMAL(9,2) 

, EST_HITS                                   DECIMAL(13,4) 

, EST_INVENTORY                              DECIMAL(13,4) 

, EST_MOVEMENT                               DECIMAL(13,4) 

, PALLET_MOVEMENT                            DECIMAL(13,4) 

, CASE_MOVEMENT                              DECIMAL(13,4) 

, INNER_MOVEMENT                             DECIMAL(13,4) 

, EACH_MOVEMENT                              DECIMAL(13,4) 

, BIN_MOVEMENT                               DECIMAL(13,4) 

, PALLET_INVENTORY                           DECIMAL(13,4) 

, CASE_INVENTORY                             DECIMAL(13,4) 

, INNER_INVENTORY                            DECIMAL(13,4) 

, EACH_INVENTORY                             DECIMAL(13,4) 

, BIN_INVENTORY                              DECIMAL(13,4) 

, CALC_HITS                                  DECIMAL(13,4) 

, SCORE                                      DECIMAL(9,4) 

, PLB_SCORE                                  DECIMAL(9,4) 

, IGA_SCORE                                  DECIMAL(9,4) 

, SCORE_DIRTY_FLAG                           TINYINT 

, INFO_1                                     STRING 

, INFO_2                                     STRING 

, INFO_3                                     STRING 

, INFO_4                                     STRING 

, INFO_5                                     STRING 

, INFO_6                                     STRING 

, SI_NUM_1                                   DECIMAL(13,4) 

, SI_NUM_2                                   DECIMAL(13,4) 

, SI_NUM_3                                   DECIMAL(13,4) 

, SI_NUM_4                                   DECIMAL(13,4) 

, SI_NUM_5                                   DECIMAL(13,4) 

, SI_NUM_6                                   DECIMAL(13,4) 

, RESERVED_1                                 STRING 

, RESERVED_2                                 STRING 

, RESERVED_3                                 BIGINT 

, RESERVED_4                                 BIGINT 

, RESERVED_5                                 STRING 

, WM_MOD_USER                                STRING 

, WM_LAST_CHANGE_TSTMP                       TIMESTAMP 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_slot_item' 
;

--DISTRIBUTE ON (WM_SLOT_ITEM_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_SLOT_ITEM_SCORE" , ***** Creating table: "WM_SLOT_ITEM_SCORE"


CREATE TABLE  WM_SLOT_ITEM_SCORE
( LOCATION_ID INT not null

, WM_SLOT_ITEM_SCORE_ID                      DECIMAL(19,0)                not null

, WM_SLOT_ITEM_ID                            DECIMAL(19,0) 

, WM_CNSTR_ID                                BIGINT 

, WM_SEQ_CNSTR_VIOLATION                     TINYINT 

, SCORE                                      DECIMAL(9,4) 

, WM_MOD_USER                                STRING 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_slot_item_score' 
;

--DISTRIBUTE ON (WM_SLOT_ITEM_SCORE_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_STANDARD_UOM" , ***** Creating table: "WM_STANDARD_UOM"


CREATE TABLE  WM_STANDARD_UOM
( LOCATION_ID INT not null

, WM_STANDARD_UOM_ID                         SMALLINT                 not null

, WM_STANDARD_UOM_TYPE_ID                    SMALLINT 

, WM_UOM_SYSTEM                              STRING 

, WM_STANDARD_UOM_ABBREVIATION               STRING 

, WM_STANDARD_UOM_DESC                       STRING 

, WM_TYPE_SYSTEM_DEFAULT_FLAG                TINYINT 

, WM_UNITS_IN_TYPE_SYSTEM_DEFAULT            DECIMAL(16,4) 

, WM_DB_UOM_FLAG                             TINYINT 

, WM_SYSTEM_DEFINED_FLAG                     TINYINT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_standard_uom' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_STOP" , ***** Creating table: "WM_STOP"


CREATE TABLE  WM_STOP
( LOCATION_ID INT not null

, WM_SHIPMENT_ID                             BIGINT                not null

, WM_STOP_SEQ                                INT                 not null

, WM_TC_COMPANY_ID                           INT 

, STOP_LOCATION_NAME                         STRING 

, WM_FACILITY_ID                             INT 

, WM_FACILITY_ALIAS_ID                       STRING 

, WM_CONS_LOCN_ID                            STRING 

, WM_STAGING_LOCN_ID                         STRING 

, WM_DOCK_DOOR_LOCN_ID                       STRING 

, WM_WAVE_ID                                 BIGINT 

, WM_WAVE_OPTION_ID                          BIGINT 

, WM_WAVE_OPTION_LIST_ID                     INT 

, WM_DOCK_DISPATCH_CAP_ID                    BIGINT 

, WM_SEGMENT_ID                              BIGINT 

, SEGMENT_STOP_SEQ                           SMALLINT 

, HANDLER                                    TINYINT 

, REPORTED_HANDLER                           TINYINT 

, DELAY_TYPE                                 INT 

, STOP_STATUS                                SMALLINT 

, WM_SEAL_NBR                                STRING 

, BILL_OF_LADING_NBR                         STRING 

, BOL_TYPE                                   TINYINT 

, WM_PRO_NBR                                 STRING 

, WM_AUTH_NBR                                STRING 

, WM_MANIF_TYPE                              STRING 

, WM_CURR_PLT_ID                             STRING 

, WM_RTE_ID                                  STRING 

, WM_AES_ITN                                 STRING 

, BOL_BREAK_ATTR                             STRING 

, FREIGHT_TERMS                              STRING 

, FLOOR_SHIPMENT                             STRING 

, ORDER_WINDOW_VIOLATION                     BIGINT 

, DOCK_VIOLATION                             BIGINT 

, WAVE_OPTION_VIOLATION                      BIGINT 

, AUTO_APPOINTMENT_FLAG                      TINYINT 

, APPT_REQD_FLAG                             TINYINT 

, ADDR_OVERRIDE_FLAG                         TINYINT 

, LOAD_CLOSED_FLAG                           TINYINT 

, WAVE_MAN_CHANGED_FLAG                      TINYINT 

, ON_STATIC_ROUTE_FLAG                       TINYINT 

, HAS_NOTES_FLAG                             TINYINT 

, RECONSIGNED_FLAG                           TINYINT 

, EST_ARR_USER_DEFINED_FLAG                  TINYINT 

, EST_DEP_USER_DEFINED_FLAG                  TINYINT 

, DROP_HOOK_FLAG                             TINYINT 

, CONTACT_FIRST_NAME                         STRING 

, CONTACT_SURNAME                            STRING 

, ADDR_1                                     STRING 

, ADDR_2                                     STRING 

, ADDR_3                                     STRING 

, CITY                                       STRING 

, STATE_PROV                                 STRING 

, POSTAL_CD                                  STRING 

, COUNTY                                     STRING 

, COUNTRY_CD                                 STRING 

, CONTACT_PHONE_NBR                          STRING 

, APPT_TSTMP                                 TIMESTAMP 

, APPOINTMENT_DURATION                       INT 

, PAPERWORK_START_TSTMP                      TIMESTAMP 

, PICKUP_START_TSTMP                         TIMESTAMP 

, HANDLING_TIME                              DECIMAL(10,2) 

, BREAK_TIME                                 INT 

, WAITING_TIME                               INT 

, HITCH_TIME                                 INT 

, DRIVE_TIME                                 INT 

, STOP_TIME_MODIFIED                         TINYINT 

, DISTANCE                                   DECIMAL(9,2) 

, DISTANCE_UOM                               STRING 

, EST_DISPATCH_TSTMP                         TIMESTAMP 

, POS_ARR_START_TSTMP                        TIMESTAMP 

, POS_ARR_END_TSTMP                          TIMESTAMP 

, POS_DEPART_START_TSTMP                     TIMESTAMP 

, POS_DEPART_END_TSTMP                       TIMESTAMP 

, ARRIVAL_START_TSTMP                        TIMESTAMP 

, ARRIVAL_END_TSTMP                          TIMESTAMP 

, EST_ARRIVAL_TSTMP                          TIMESTAMP 

, CALC_EST_ARRIVAL_TSTMP                     TIMESTAMP 

, ARRIVAL_COMMIT_TSTMP                       TIMESTAMP 

, ACTUAL_ARRIVAL_TSTMP                       TIMESTAMP 

, DEPARTURE_START_TSTMP                      TIMESTAMP 

, DEPARTURE_END_TSTMP                        TIMESTAMP 

, EST_DEPARTURE_TSTMP                        TIMESTAMP 

, ACTUAL_DEPARTURE_TSTMP                     TIMESTAMP 

, START_SHIPMENT_TSTMP                       TIMESTAMP 

, END_SHIPMENT_TSTMP                         TIMESTAMP 

, SHIPMENT_START_TSTMP                       TIMESTAMP 

, SHIPMENT_END_TSTMP                         TIMESTAMP 

, CHECK_IN_TSTMP                             TIMESTAMP 

, CHECK_OUT_TSTMP                            TIMESTAMP 

, ARRIVAL_NET_TSTMP                          TIMESTAMP 

, ARRIVAL_NLT_TSTMP                          TIMESTAMP 

, DEPARTURE_NET_TSTMP                        TIMESTAMP 

, DEPARTURE_NLT_TSTMP                        TIMESTAMP 

, REC_ARRIVAL_TSTMP                          TIMESTAMP 

, REC_DEPARTURE_TSTMP                        TIMESTAMP 

, WM_STOP_TZ                                 SMALLINT 

, DEGREE_LATE_EARLY                          INT 

, WM_ARRIVAL_LATE_REASON_ID                  INT 

, WM_DEPARTURE_LATE_REASON_ID                INT 

, STOP_REVENUE                               DECIMAL(13,4) 

, STOP_REVENUE_CURRENCY_CD                   STRING 

, STOP_MARGIN                                DECIMAL(13,4) 

, STOP_MARGIN_CURRENCY_CD                    STRING 

, STOP_COST                                  DECIMAL(13,4) 

, STOP_COST_CURRENCY_CD                      STRING 

, STOP_LINEHAUL_REVENUE                      DECIMAL(13,4) 

, ACCESSORIAL_REVENUE                        DECIMAL(13,4) 

, STOP_OFF_REVENUE                           DECIMAL(13,4) 

, CM_DISCOUNT_REVENUE                        DECIMAL(13,3) 

, CAPACITY                                   INT 

, WM_SIZE_UOM_ID                             INT 

, REF_FIELD_1                                STRING 

, REF_FIELD_2                                STRING 

, REF_FIELD_3                                STRING 

, REF_FIELD_4                                STRING 

, REF_FIELD_5                                STRING 

, REF_FIELD_6                                STRING 

, REF_FIELD_7                                STRING 

, REF_FIELD_8                                STRING 

, REF_FIELD_9                                STRING 

, REF_FIELD_10                               STRING 

, WM_HIBERNATE_VERSION                       BIGINT 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_stop' 
;

--DISTRIBUTE ON (WM_SHIPMENT_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_STOP_STATUS" , ***** Creating table: "WM_STOP_STATUS"


CREATE TABLE  WM_STOP_STATUS
( LOCATION_ID INT not null

, WM_STOP_STATUS                             SMALLINT                 not null

, WM_STOP_STATUS_DESC                        STRING 

, WM_STOP_STATUS_SHORT_DESC                  STRING 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_stop_status' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_SYS_CODE" , ***** Creating table: "WM_SYS_CODE"


CREATE TABLE  WM_SYS_CODE
( LOCATION_ID INT not null

, WM_REC_TYPE                                STRING         not null

, WM_CD_TYPE                                 STRING         not null

, WM_CD_ID                                   STRING        not null

, WM_SYS_CD_TYPE_ID                          INT 

, WM_SYS_CD_ID                               INT 

, WM_CD_DESC                                 STRING 

, WM_CD_SHORT_DESC                           STRING 

, WM_MISC_FLAGS                              STRING 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_sys_code' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_TASK_DTL" , ***** Creating table: "WM_TASK_DTL"


CREATE TABLE  WM_TASK_DTL
( LOCATION_ID INT not null

, WM_TASK_DTL_ID                             INT                 not null

, WM_TASK_HDR_ID                             INT 

, WM_TASK_ID                                 INT 

, WM_TASK_SEQ_NBR                            SMALLINT 

, WM_TC_ORDER_ID                             STRING 

, WM_LINE_ITEM_ID                            BIGINT 

, WM_ITEM_ID                                 BIGINT 

, WM_TASK_TYPE                               STRING 

, WM_INVN_TYPE                               STRING 

, WM_TRANS_INVN_TYPE                         SMALLINT 

, WM_CNTR_NBR                                STRING 

, WM_STAT_CD                                 SMALLINT 

, WM_PROD_STAT                               STRING 

, WM_RSN_CD                                  STRING 

, WM_BATCH_NBR                               STRING 

, WM_CARTON_NBR                              STRING 

, WM_CARTON_SEQ_NBR                          INT 

, WM_PIKR_NBR                                SMALLINT 

, WM_PKT_CTRL_NBR                            STRING 

, WM_PKT_SEQ_NBR                             INT 

, WM_PICK_SEQ_CD                             STRING 

, COUNTRY_OF_ORIGIN                          STRING 

, WM_ALLOC_INVN_CD                           SMALLINT 

, CURR_WORK_AREA                             STRING 

, WM_CURR_WORK_GRP                           STRING 

, WM_TASK_LOCN_SEQ                           SMALLINT 

, SLOT_NBR                                   INT 

, SUB_SLOT_NBR                               INT 

, WM_CD_MASTER_ID                            INT 

, WM_TOTE_NBR                                STRING 

, WM_REPL_DIVRT_LOCN                         STRING 

, WM_PULL_LOCN_ID                            STRING 

, WM_DEST_LOCN_ID                            STRING 

, WM_DEST_LOCN_SEQ                           INT 

, FULL_CNTR_ALLOCD_FLAG                      TINYINT 

, WM_INVN_NEED_TYPE                          SMALLINT 

, TASK_PRTY                                  INT 

, WM_TASK_GENRTN_REF_CD                      STRING 

, WM_TASK_GENRTN_REF_NBR                     STRING 

, WM_TASK_CMPL_REF_CD                        STRING 

, WM_TASK_CMPL_REF_NBR                       STRING 

, WM_NEXT_TASK_ID                            INT 

, WM_NEXT_TASK_SEQ_NBR                       SMALLINT 

, NEXT_TASK_DESC                             STRING 

, WM_NEXT_TASK_TYPE                          STRING 

, WM_REQD_INVN_TYPE                          STRING 

, WM_REQD_PROD_STAT                          STRING 

, WM_REQD_BATCH_NBR                          STRING 

, REQD_COUNTRY_OF_ORIGIN                     STRING 

, WM_TASK_CMPL_REF_NBR_SEQ                   INT 

, WM_ALLOC_INVN_DTL_ID                       INT 

, SUBSTITUTION_FLAG                          TINYINT 

, VOCOLLECT_POSN                             SMALLINT 

, VOCOLLECT_SHORT_FLAG                       STRING 

, PAGE_NBR                                   INT 

, CHASE_CREATED_FLAG                         TINYINT 

, WM_RESOURCE_GROUP_ID                       STRING 

, WM_WORK_RESOURCE_ID                        STRING 

, WM_WORK_RELEASE_BATCH_NBR                  STRING 

, WM_ALLOCATION_KEY                          STRING 

, WM_DISTRIBUTION_KEY                        STRING 

, ERLST_START_TSTMP                          TIMESTAMP 

, LTST_START_TSTMP                           TIMESTAMP 

, LTST_CMPL_TSTMP                            TIMESTAMP 

, ORIG_REQMT                                 DECIMAL(13,5) 

, QTY_ALLOC                                  DECIMAL(13,5) 

, QTY_PULLD                                  DECIMAL(13,5) 

, ALLOC_UOM_QTY                              DECIMAL(9,2) 

, ALLOC_UOM                                  STRING 

, SKU_ATTR_1                                 STRING 

, SKU_ATTR_2                                 STRING 

, SKU_ATTR_3                                 STRING 

, SKU_ATTR_4                                 STRING 

, SKU_ATTR_5                                 STRING 

, REQD_SKU_ATTR_1                            STRING 

, REQD_SKU_ATTR_2                            STRING 

, REQD_SKU_ATTR_3                            STRING 

, REQD_SKU_ATTR_4                            STRING 

, REQD_SKU_ATTR_5                            STRING 

, MISC_ALPHA_FIELD_1                         STRING 

, MISC_ALPHA_FIELD_2                         STRING 

, MISC_ALPHA_FIELD_3                         STRING 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_task_dtl' 
;

--DISTRIBUTE ON (WM_TASK_DTL_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_TASK_HDR" , ***** Creating table: "WM_TASK_HDR"


CREATE TABLE  WM_TASK_HDR
( LOCATION_ID INT not null

, WM_TASK_HDR_ID                             INT                 not null

, WM_TASK_ID                                 INT 

, WM_ORIG_TASK_ID                            INT 

, WM_WHSE                                    STRING 

, WM_TASK_DESC                               STRING 

, WM_TASK_TYPE                               STRING 

, WM_TASK_BATCH                              STRING 

, WM_NEXT_TASK_ID                            INT 

, WM_STAT_CD                                 SMALLINT 

, WM_LOCN_HDR_ID                             INT 

, WM_ITEM_ID                                 BIGINT 

, WM_NEED_ID                                 STRING 

, WM_INVN_TYPE                               STRING 

, WM_INVN_NEED_TYPE                          SMALLINT 

, WM_TASK_RULE_PARM_ID                       INT 

, WM_TASK_PARM_ID                            INT 

, WM_RULE_ID                                 INT 

, WM_EXCEPTION_CD                            STRING 

, WM_PICK_CART_TYPE                          STRING 

, DFLT_TASK_PRTY                             INT 

, CURR_TASK_PRTY                             INT 

, WM_TASK_GENRTN_REF_CD                      STRING 

, WM_TASK_GENRTN_REF_NBR                     STRING 

, WM_TASK_CMPL_REF_CD                        STRING 

, WM_TASK_CMPL_REF_NBR                       STRING 

, ONE_USER_PER_GRP                           STRING 

, WM_VOCOLLECT_ASSIGN_ID                     INT 

, WM_MHE_FLAG                                TINYINT 

, PICK_TO_TOTE_FLAG                          TINYINT 

, WM_MHE_ORD_STATE                           STRING 

, PRT_TASK_LIST_FLAG                         TINYINT 

, RPT_PRTR_REQSTR                            STRING 

, WM_DOC_ID                                  STRING 

, WM_VOCO_INTRNL_REVERSE_ID                  STRING 

, WM_CURR_LOCN_ID                            STRING 

, START_CURR_WORK_GRP                        STRING 

, START_CURR_WORK_AREA                       STRING 

, END_CURR_WORK_GRP                          STRING 

, END_CURR_WORK_AREA                         STRING 

, START_DEST_WORK_GRP                        STRING 

, START_DEST_WORK_AREA                       STRING 

, END_DEST_WORK_GRP                          STRING 

, END_DEST_WORK_AREA                         STRING 

, WM_RLS_TSTMP                               TIMESTAMP 

, ERLST_START_TSTMP                          TIMESTAMP 

, LTST_START_TSTMP                           TIMESTAMP 

, LTST_CMPL_TSTMP                            TIMESTAMP 

, BEGIN_AREA                                 STRING 

, BEGIN_ZONE                                 STRING 

, BEGIN_AISLE                                STRING 

, END_AREA                                   STRING 

, END_ZONE                                   STRING 

, END_AISLE                                  STRING 

, REPRINT_CNT                                TINYINT 

, ESTIMATED_TIME                             DECIMAL(16,4) 

, ESTIMATED_DISTANCE                         DECIMAL(16,4) 

, XPECTD_DURTN                               INT 

, ACTL_DURTN                                 INT 

, WM_OWNER_USER_ID                           STRING 

, WM_CURR_USER_ID                            STRING 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_task_hdr' 
;

--DISTRIBUTE ON (WM_TASK_HDR_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_TRAILER_CONTENTS" , ***** Creating table: "WM_TRAILER_CONTENTS"


CREATE TABLE  WM_TRAILER_CONTENTS
( LOCATION_ID INT not null

, WM_TRAILER_CONTENTS_ID                     INT                 not null

, WM_VISIT_DETAIL_ID                         INT 

, WM_PLANNED_FLAG                            TINYINT 

, WM_SHIPMENT_ID                             BIGINT 

, WM_ASN_ID                                  BIGINT 

, WM_PO_ID                                   BIGINT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_trailer_contents' 
;

--DISTRIBUTE ON (WM_TRAILER_CONTENTS_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_TRAILER_REF" , ***** Creating table: "WM_TRAILER_REF"


CREATE TABLE  WM_TRAILER_REF
( LOCATION_ID INT not null

, WM_TRAILER_ID                              BIGINT                not null

, WM_TRAILER_STATUS                          SMALLINT 

, WM_CURRENT_LOCATION_ID                     STRING 

, WM_ASSIGNED_LOCATION_ID                    STRING 

, WM_TRAILER_LOCATION_STATUS                 SMALLINT 

, WM_ACTIVE_VISIT_ID                         INT 

, WM_ACTIVE_VISIT_DETAIL_ID                  INT 

, WM_PROTECTION_LEVEL                        INT 

, WM_PRODUCT_CLASS                           INT 

, CONVEYABLE_FLAG                            TINYINT 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_trailer_ref' 
;

--DISTRIBUTE ON RANDOM







--*****  Creating table:  "WM_TRAILER_TYPE" , ***** Creating table: "WM_TRAILER_TYPE"


CREATE TABLE  WM_TRAILER_TYPE
( LOCATION_ID INT not null

, WM_TRAILER_TYPE_ID                         TINYINT                 not null

, WM_TRAILER_TYPE_DESC                       STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_trailer_type' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_TRAILER_VISIT" , ***** Creating table: "WM_TRAILER_VISIT"


CREATE TABLE  WM_TRAILER_VISIT
( LOCATION_ID INT not null

, WM_VISIT_ID                                INT                 not null

, WM_FACILITY_ID                             INT 

, WM_TRAILER_ID                              BIGINT 

, WM_TRAILER_CHECKIN_TSTMP                   TIMESTAMP 

, WM_TRAILER_CHECKOUT_TSTMP                  TIMESTAMP 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_trailer_visit' 
;

--DISTRIBUTE ON (WM_VISIT_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_TRAILER_VISIT_DTL" , ***** Creating table: "WM_TRAILER_VISIT_DTL"


CREATE TABLE  WM_TRAILER_VISIT_DTL
( LOCATION_ID INT not null

, WM_VISIT_DETAIL_ID                         INT                 not null

, WM_VISIT_ID                                INT 

, WM_VISIT_TYPE                              STRING 

, WM_APPOINTMENT_ID                          INT 

, WM_DRIVER_ID                               BIGINT 

, WM_TRACTOR_ID                              BIGINT 

, WM_SEAL_NUMBER                             STRING 

, WM_FOB_IND                                 SMALLINT 

, WM_TRAILER_START_TSTMP                     TIMESTAMP 

, WM_TRAILER_END_TSTMP                       TIMESTAMP 

, WM_PROTECTION_LEVEL                        STRING 

, WM_PRODUCT_CLASS                           STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_CREATED_SOURCE_TYPE                     SMALLINT 

, WM_CREATED_SOURCE                          STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                SMALLINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_trailer_visit_dtl' 
;

--DISTRIBUTE ON (WM_VISIT_DETAIL_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_UN_NUMBER" , ***** Creating table: "WM_UN_NUMBER"


CREATE TABLE  WM_UN_NUMBER
( LOCATION_ID INT not null

, WM_UN_NBR_ID                               BIGINT                not null

, WM_TC_COMPANY_ID                           INT 

, WM_UN_NBR_VALUE                            STRING 

, WM_UN_NBR_DESC                             STRING 

, ADDL_DESC                                  STRING 

, TECHNICAL_NAME                             STRING 

, WM_UN_NBR                                  BIGINT 

, WM_UN_NBR_VERSION                          STRING 

, WM_HAZMAT_UOM                              INT 

, MAX_HAZMAT_QTY                             DECIMAL(13,4) 

, REGULATION_SET                             STRING 

, WM_TRANS_MODE                              STRING 

, REPORTABLE_QTY_FLAG                        TINYINT 

, ACCESSIBLE_FLAG                            TINYINT 

, RESIDUE_INDICATOR_CD                       STRING 

, HAZMAT_CLASS_QUAL                          STRING 

, NOS_INDICATOR_CD                           TINYINT 

, HAZMAT_MATERIAL_QUAL                       STRING 

, FLASH_POINT_TEMPERATURE                    DECIMAL(13,4) 

, FLASH_POINT_TEMPERATURE_UOM                STRING 

, HAZARD_ZONE_CD                             STRING 

, RADIOACTIVE_QTY_CALC_FACTOR                DECIMAL(13,4) 

, RADIOACTIVE_QUANTITY_UOM                   STRING 

, EPA_WASTE_STREAM_NBR                       STRING 

, WASTE_CHARACTERISTIC_CODE                  STRING 

, NET_EXPLOSIVE_QUANTITY_FACTOR              DECIMAL(13,4) 

, HAZMAT_EXEMPTION_REF_NBR                   STRING 

, UNN_CLASS_DIV                              STRING 

, UNN_SUB_RISK                               STRING 

, UNN_HAZARD_LABEL                           STRING 

, UNN_PG                                     STRING 

, UNN_SP                                     STRING 

, UNN_AIR_BOTH_LTD_INSTRUCTIONS              STRING 

, UNN_AIR_BOTH_LTD_QTY                       STRING 

, UNN_AIR_BOTH_INSTRUCTIONS                  STRING 

, UNN_AIR_BOTH_QTY                           STRING 

, UNN_AIR_CARGO_INSTRUCTIONS                 STRING 

, UNN_AIR_CARGO_QTY                          STRING 

, CONTACT_NAME                               STRING 

, PHONE_NBR                                  STRING 

, HZC_FIELD_1                                STRING 

, HZC_FIELD_2                                STRING 

, HZC_FIELD_3                                STRING 

, HZC_FIELD_4                                STRING 

, HZC_FIELD_5                                STRING 

, RQ_FIELD_1                                 STRING 

, RQ_FIELD_2                                 STRING 

, RQ_FIELD_3                                 STRING 

, RQ_FIELD_4                                 STRING 

, RQ_FIELD_5                                 STRING 

, RQ_FIELD_6                                 STRING 

, RQ_FIELD_7                                 STRING 

, RQ_FIELD_8                                 STRING 

, RQ_FIELD_9                                 STRING 

, RQ_FIELD_10                                STRING 

, RQ_FIELD_11                                STRING 

, RQ_FIELD_12                                STRING 

, RQ_FIELD_13                                STRING 

, RQ_FIELD_14                                STRING 

, RQ_FIELD_15                                STRING 

, RQ_FIELD_16                                STRING 

, RQ_FIELD_17                                STRING 

, RQ_FIELD_18                                STRING 

, RQ_FIELD_19                                STRING 

, RQ_FIELD_20                                STRING 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_un_number' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_USER_PROFILE" , ***** Creating table: "WM_USER_PROFILE"


CREATE TABLE  WM_USER_PROFILE
( LOCATION_ID INT not null

, WM_USER_PROFILE_ID                         INT                 not null

, WM_LOGIN_USER_ID                           STRING 

, WM_MENU_ID                                 INT 

, WM_EMPLOYEE_ID                             STRING 

, RESTR_TASK_GRP_TO_DFLT_FLAG                TINYINT 

, RESTR_MENU_MODE_TO_DFLT_FLAG               TINYINT 

, WM_DFLT_RF_MENU_MODE                       STRING 

, WM_LANG_ID                                 STRING 

, WM_DATE_MASK                               STRING 

, WM_LAST_TASK_ID                            STRING 

, WM_LAST_LOCN_ID                            STRING 

, WM_LAST_WORK_GRP                           STRING 

, WM_LAST_WORK_AREA                          STRING 

, ALLOW_TASK_INT_CHG_FLAG                    TINYINT 

, NBR_OF_TASK_TO_DSP                         TINYINT 

, WM_TASK_DSP_MODE                           STRING 

, DB_USER_ID                                 STRING 

, DB_PSWD                                    STRING 

, DB_CONNECT_STRING                          STRING 

, PRTR_REQSTR                                STRING 

, IDLE_TIME_BEF_SHTDWN                       INT 

, WM_RF_MENU_ID                              INT 

, PAGE_SIZE                                  INT 

, WM_VOCOLLECT_WORK_TYPE                     TINYINT 

, WM_CURR_TASK_GRP                           STRING 

, WM_CURR_VOCOLLECT_PTS_CASE                 STRING 

, WM_CURR_VOCOLLECT_REASON_CD                STRING 

, TASK_GRP_JUMP_FLAG                         TINYINT 

, AUTO_3PL_LOGIN_FLAG                        TINYINT 

, WM_SECURITY_CONTEXT_ID                     INT 

, SEC_USER_NAME                              STRING 

, VOCOLLECT_PUTAWAY_FLAG                     TINYINT 

, VOCOLLECT_REPLEN_FLAG                      TINYINT 

, VOCOLLECT_PACKING_FLAG                     TINYINT 

, WM_CLS_TIMEZONE_ID                         INT 

, DAL_CONNECTION_STRING                      STRING 

, WM_USER_SECURITY_CONTEXT_ID                INT 

, DFLT_TASK_INT                              SMALLINT 

, MOBILE_HELP_TEXT_FLAG                      TINYINT 

, MOB_SPLASH_SCREEN_FLAG                     TINYINT 

, WM_SCREEN_TYPE_ID                          SMALLINT 

, MOBILE_MSG_SPEECH_LEVEL                    STRING 

, MOBILE_MSG_SPEECH_RATE                     STRING 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_user_profile' 
;

--DISTRIBUTE ON (WM_USER_PROFILE_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_VEND_PERF_TRAN" , ***** Creating table: "WM_VEND_PERF_TRAN"


CREATE TABLE  WM_VEND_PERF_TRAN
( LOCATION_ID INT not null

, WM_VEND_PERF_TRAN_ID                       INT                 not null

, WM_WHSE                                    STRING 

, WM_PERF_CD                                 STRING 

, WM_VENDOR_MASTER_ID                        INT 

, WM_CD_MASTER_ID                            INT 

, WM_SHPMT_NBR                               STRING 

, WM_PO_HDR_ID                               INT 

, WM_PO_NBR                                  STRING 

, WM_CASE_HDR_ID                             INT 

, WM_CASE_NBR                                STRING 

, WM_ASN_HDR_ID                              INT 

, WM_ILM_APPT_NBR                            STRING 

, WM_ITEM_ID                                 BIGINT 

, WM_LOAD_NBR                                STRING 

, WM_STAT_CD                                 TINYINT 

, BILL_FLAG                                  STRING 

, QTY                                        DECIMAL(13,5) 

, UOM                                        STRING 

, SAMS                                       DECIMAL(9,4) 

, CHRG_AMT                                   DECIMAL(9,2) 

, COMMENT                                  STRING 

, WM_USER_ID                                 STRING 

, WM_CREATED_BY_USER_ID                      STRING 

, WM_VERSION_ID                              INT 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_vend_perf_tran' 
;

--DISTRIBUTE ON (WM_VEND_PERF_TRAN_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_WAVE_PARM" , ***** Creating table: "WM_WAVE_PARM"


CREATE TABLE  WM_WAVE_PARM
( LOCATION_ID INT not null

, WM_WAVE_PARM_ID                            INT                 not null

, WM_WHSE                                    STRING 

, WM_TC_COMPANY_ID                           INT 

, WM_WAVE_NBR                                STRING 

, WM_WAVE_DESC                               STRING 

, WM_WAVE_PROC_TYPE                          SMALLINT 

, WM_WAVE_PARM_TEMPLATE_ID                   INT 

, WM_PACK_WAVE_PARM_ID                       INT 

, WM_CAT_PARM_ID                             INT 

, WM_CARTON_TYPE_EVENT_ID                    INT 

, WM_CHUTE_ASSIGN_TYPE_EVENT_ID              INT 

, WM_LPN_BRK_ATTRIB_EVENT_ID                 INT 

, WM_RETAIN_PALLET_ID                        TINYINT 

, WM_PKT_CONSOL_PROF                         STRING 

, WM_PKT_CONSOL_DETRM_TYPE                   STRING 

, WM_CARTON_TYPE                             STRING 

, WM_PICK_LOCN_ASSIGN_TYPE                   STRING 

, WM_ALLOC_TYPE                              STRING 

, WM_REC_TYPE                                STRING 

, WM_FORCE_ALLOC_TYPE                        STRING 

, WM_FORCE_PICK_LOCN_ASSIGN_TYPE             STRING 

, WM_BULK_PICK_ASSIGN_TYPE                   STRING 

, WM_BULK_PICK_ZONE                          STRING 

, WM_DLVRY_WIN_RANGE_CD                      STRING 

, WM_DFLT_PLT_TYPE                           STRING 

, WM_SKU_CNSTR                               STRING 

, WM_SKU_SUB                                 STRING 

, WM_SHIP_VIA                                STRING 

, WM_SWC_NBR                                 STRING 

, WM_MANIF_NBR                               STRING 

, WM_BOL                                     STRING 

, WM_SHPMT_NBR                               STRING 

, DAY_OF_WEEK                                SMALLINT 

, SHIFT_NBR                                  STRING 

, SHIP_TSTMP                                 TIMESTAMP 

, SCHED_DELIVERY_TSTMP                       TIMESTAMP 

, SCHED_WAVE                                 STRING 

, WAVE_STAT_TSTMP                            TIMESTAMP 

, WM_WAVE_STAT_CD                            SMALLINT 

, WM_WAVE_CMPL_STAT_CD                       SMALLINT 

, WM_PICK_STAT_CD                            SMALLINT 

, WM_PACK_STAT_CD                            SMALLINT 

, WM_REPL_STAT_CD                            SMALLINT 

, WM_INDUC_STAT_CD                           SMALLINT 

, FORCE_WPT_FLAG                             TINYINT 

, REPL_TRIG                                  STRING 

, RTE_W_WAVE                                 STRING 

, CHG_PKT_QTY                                STRING 

, CARTON_BREAK_ON_AREA_CHG_FLAG              TINYINT 

, CARTON_BREAK_ON_ZONE_CHG_FLAG              TINYINT 

, CARTON_BREAK_ON_AISLE_CHG_FLAG             TINYINT 

, PKT_CONSOL_REJECT_RULE                     STRING 

, ASSIGN_PRO_NBR                             STRING 

, SWC_NBR_FLAG                               TINYINT 

, MANIF_FLAG                                 TINYINT 

, BOL_FLAG                                   TINYINT 

, SHPMT_FLAG                                 TINYINT 

, CLEAR_SPUR_LOCN                            STRING 

, SPUR                                       STRING 

, XCEED_CAPCTY_FLAG                          TINYINT 

, CONSL_LOCN_CLEAR_METHD                     STRING 

, PERP_PLT_POSN                              STRING 

, USE_INBD_LPN_AS_OUTBD_LPN_FLAG             TINYINT 

, GRP_PKT_BY_SWC_NBR_FLAG                    TINYINT 

, PULL_ALL_SWC                               TINYINT 

, ZONE_PICK_METHOD                           STRING 

, WAVE_TYPE_IND                              STRING 

, REJECT_DISTRO_RULE_FLAG                    TINYINT 

, BULK_WAVE_BAL_FROM_ACTV_FLAG               TINYINT 

, PRT_CS_LABELS_W_WAVE                       TINYINT 

, CREATE_SHPMT                               STRING 

, CREATE_LOAD                                STRING 

, CHG_WO_QTY                                 STRING 

, PACK_CMPL_W_WAVE_FLAG                      TINYINT 

, DESEL_SNGL_FLAG                            TINYINT 

, CONSOL_PLTZ_CARTONS_FLAG                   TINYINT 

, ACTV_REPL_ORGN                             STRING 

, LOAD_PLAN                                  STRING 

, ASSIGN_NEW_LOAD                            STRING 

, RESV_BAL_FLAG                              TINYINT 

, RESV_BALANCE_INT                           SMALLINT 

, ORDER_ROUND_FLAG                           TINYINT 

, ALLOC_REMAIN                               STRING 

, MHE_FLAG                                   TINYINT 

, MHE_ONE_SEND_FLAG                          TINYINT 

, MHE_PICK_DEST_LEVEL_FLAG                   TINYINT 

, FORCE_SHIP_DATE_FLAG                       TINYINT 

, REPL_WAVE_FLAG                             TINYINT 

, RESEQ_ACTIV_LOC                            TINYINT 

, CHASE_WAVE_FLAG                            TINYINT 

, DEFAULT_QUEUE_PRTY                         SMALLINT 

, ALLOC_LOGGING                              TINYINT 

, CARTON_BREAK_ON_LOCN_CLASS_FLAG            TINYINT 

, DESELECT_UNALLOCATED_SINGLES_FLAG          TINYINT 

, CUBING_ACTION_FOR_FAILED_ORDER_FLAG        TINYINT 

, IGNORE_SINGLES_PROCESSING_FLAG             TINYINT 

, RELEASE_ALL_TASK_FLAG                      TINYINT 

, HOLD_RELEASE_FLAG                          TINYINT 

, REPLENISHMENT_BUMP_UP_FLAG                 TINYINT 

, ORDER_SELECTION_LOGGING_LEVEL              TINYINT 

, AGG_LOGGING_LEVEL                          TINYINT 

, CUBING_LOGGING_LEVEL                       TINYINT 

, PACK_WAVE_LOGGING_LEVEL                    TINYINT 

, TASK_GEN_LOGGING_LEVEL                     TINYINT 

, LABEL_PRINT_LOGGING_LEVEL                  TINYINT 

, ORDER_SEQ_LOGGING                          TINYINT 

, ORDER_CONSOLIDATION_LOGGING_LEVEL          TINYINT 

, FORCE_ORDER_STREAMING_FLAG                 TINYINT 

, MAX_UNITS                                  INT 

, MAX_WT                                     DECIMAL(13,4) 

, MAX_VOL                                    DECIMAL(13,4) 

, MAX_ORDERS                                 INT 

, MAX_ORDER_LINES                            INT 

, MAX_CARTONS                                INT 

, MAX_PICK_WAVES                             INT 

, MAX_NBR_PACK_WAVES_TO_RLS                  SMALLINT 

, MAX_DYNAMIC_LOCN_OPTN                      SMALLINT 

, NBR_OF_PKTS                                INT 

, NBR_OF_CARTONS                             INT 

, NBR_OF_LOCN                                INT 

, NBR_OF_CHUTE                               INT 

, NBR_OF_PLT_POSN                            INT 

, ASSIGN_PLT_POSN                            SMALLINT 

, RETAIL_MAX_UNITS                           INT 

, RETAIL_UNIT_PCNT_OVER                      DECIMAL(5,2) 

, RETAIL_MAX_SKUS                            INT 

, RETAIL_MAX_STORES                          INT 

, RETAIL_MAX_ROUTES                          INT 

, NBR_OF_PIKRS_PAKRS                         SMALLINT 

, AVG_PIKS_PER_PIKR                          DECIMAL(5,1) 

, AVG_TMU_PER_PIKR                           DECIMAL(6,2) 

, LABEL_PRTR_REQSTR                          STRING 

, RPT_PRTR_REQSTR                            STRING 

, COLLATE_PRTR_REQSTR                        STRING 

, FILL_CAPCTY_STRK                           SMALLINT 

, VOCO_GRP_CARTONS_TO_TASK                   SMALLINT 

, VOCO_MAX_CARTON_ON_TASK                    SMALLINT 

, HOLD_PRINT                                 SMALLINT 

, SUPPR_PR40_REPL                            SMALLINT 

, COMB_4050_REPL                             SMALLINT 

, SUPPR_PR60_REPL                            SMALLINT 

, NBR_OF_LABELS_PER_SPOOL                    INT 

, NBR_OF_LABELS_PER_ROW                      SMALLINT 

, NBR_ALLOCATION_THREAD                      SMALLINT 

, DIFF_PIKR_ON_AREA_CHG                      STRING 

, DIFF_PIKR_ON_ZONE_CHG                      STRING 

, DIFF_PIKR_ON_AISLE_CHG                     STRING 

, DIFF_PIKR_ON_BAY_CHG                       STRING 

, DIFF_PIKR_ON_LVL_CHG                       STRING 

, DIFF_PIKR_ON_POSN_CHG                      STRING 

, SAME_PIKR_FOR_AREA                         STRING 

, SAME_PIKR_FOR_ZONE                         STRING 

, SAME_PIKR_FOR_AISLE                        STRING 

, SAME_PIKR_FOR_BAY                          STRING 

, SAME_PIKR_FOR_LVL                          STRING 

, SAME_PIKR_FOR_POSN                         STRING 

, OK_TO_PICK                                 STRING 

, OK_TO_PACK                                 STRING 

, OK_TO_REPL                                 STRING 

, OK_TO_INDUC                                STRING 

, OK_TO_MISC_1                               STRING 

, OK_TO_MISC_2                               STRING 

, START_PICK_TSTMP                           TIMESTAMP 

, END_PICK_TSTMP                             TIMESTAMP 

, START_PACK_TSTMP                           TIMESTAMP 

, END_PACK_TSTMP                             TIMESTAMP 

, START_REPL_TSTMP                           TIMESTAMP 

, END_REPL_TSTMP                             TIMESTAMP 

, START_INDUC_TSTMP                          TIMESTAMP 

, END_INDUC_TSTMP                            TIMESTAMP 

, START_MISC_1_TSTMP                         TIMESTAMP 

, END_MISC_1_TSTMP                           TIMESTAMP 

, START_MISC_2_TSTMP                         TIMESTAMP 

, END_MISC_2_TSTMP                           TIMESTAMP 

, CMPLTD_TSTMP                               TIMESTAMP 

, MISC_STAT_CD_1                             SMALLINT 

, MISC_STAT_CD_2                             SMALLINT 

, PROC_ATTR_1                                STRING 

, PROC_ATTR_2                                STRING 

, PROC_ATTR_3                                STRING 

, PROC_ATTR_4                                STRING 

, PROC_ATTR_5                                STRING 

, SPL_INSTR_CD_1                             STRING 

, SPL_INSTR_CD_2                             STRING 

, SPL_INSTR_CD_3                             STRING 

, SPL_INSTR_CD_4                             STRING 

, SPL_INSTR_CD_5                             STRING 

, WM_USER_ID                                 STRING 

, WM_VERSION_ID                              INT 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, WM_CREATE_TSTMP                            TIMESTAMP 

, WM_MOD_TSTMP                               TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_wave_parm' 
;

--DISTRIBUTE ON (WM_WAVE_PARM_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_YARD" , ***** Creating table: "WM_YARD"


CREATE TABLE  WM_YARD
( LOCATION_ID INT not null

, WM_YARD_ID                                 BIGINT                not null

, WM_TC_COMPANY_ID                           INT 

, WM_YARD_NAME                               STRING 

, WM_LOCATION_ID                             BIGINT 

, WM_TIME_ZONE_ID                            SMALLINT 

, GENERATE_MOVE_TASK_FLAG                    TINYINT 

, GENERATE_NEXT_EQUIP_FLAG                   TINYINT 

, RANGE_TASKS_FLAG                           TINYINT 

, SEAL_TASK_TRGD_FLAG                        TINYINT 

, OVERRIDE_SYSTEM_TASKS_FLAG                 TINYINT 

, TASKING_ALLOWED_FLAG                       TINYINT 

, LOCK_TRAILER_ON_MOVE_TO_DOOR_FLAG          TINYINT 

, YARD_SVG_FILE                              STRING 

, ADDRESS                                    STRING 

, CITY                                       STRING 

, STATE_PROV                                 STRING 

, POSTAL_CD                                  STRING 

, COUNTY                                     STRING 

, COUNTRY_CD                                 STRING 

, MAX_EQUIPMENT_ALLOWED                      INT 

, UPPER_CHECK_IN_TIME_MINS                   SMALLINT 

, LOWER_CHECK_IN_TIME_MINS                   SMALLINT 

, FIXED_TIME_MINS                            SMALLINT 

, THRESHOLD_PERCENT                          INT 

, MARK_FOR_DELETION                          TINYINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_yard' 
;

--DISTRIBUTE ON (WM_YARD_ID)

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_YARD_ZONE" , ***** Creating table: "WM_YARD_ZONE"


CREATE TABLE  WM_YARD_ZONE
( LOCATION_ID INT not null

, WM_YARD_ID                                 BIGINT                not null

, WM_YARD_ZONE_ID                            BIGINT                not null

, WM_YARD_ZONE_NAME                          STRING 

, WM_LOCATION_ID                             BIGINT 

, PUTAWAY_ELIGIBLE_FLAG                      TINYINT 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_yard_zone' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)







--*****  Creating table:  "WM_YARD_ZONE_SLOT" , ***** Creating table: "WM_YARD_ZONE_SLOT"


CREATE TABLE  WM_YARD_ZONE_SLOT
( LOCATION_ID INT not null

, WM_YARD_ID                                 BIGINT                not null

, WM_YARD_ZONE_ID                            BIGINT                not null

, WM_YARD_ZONE_SLOT_ID                       BIGINT                not null

, WM_YARD_ZONE_SLOT_NAME                     STRING 

, WM_YARD_ZONE_SLOT_STATUS                   SMALLINT 

, WM_LOCN_ID                                 STRING 

, X_COORDINATE                               DECIMAL(14,3) 

, Y_COORDINATE                               DECIMAL(14,3) 

, Z_COORDINATE                               DECIMAL(14,3) 

, MAX_CAPACITY                               INT 

, USED_CAPACITY                              INT 

, GUARD_HOUSE_FLAG                           TINYINT 

, THRESHOLD_GUARD_HOUSE_FLAG                 TINYINT 

, MARK_FOR_DELETION_FLAG                     TINYINT 

, WM_CREATED_SOURCE_TYPE                     TINYINT 

, WM_CREATED_SOURCE                          STRING 

, WM_CREATED_TSTMP                           TIMESTAMP 

, WM_LAST_UPDATED_SOURCE_TYPE                TINYINT 

, WM_LAST_UPDATED_SOURCE                     STRING 

, WM_LAST_UPDATED_TSTMP                      TIMESTAMP 

, DELETE_FLAG                                TINYINT 

, UPDATE_TSTMP                               TIMESTAMP                    not null

, LOAD_TSTMP                                 TIMESTAMP                    not null  

)
USING delta 
LOCATION 'gs://petm-bdpl-prod-refine-p1-gcs-gbl/supplychain/wms/wm_yard_zone_slot' 
;

--DISTRIBUTE ON RANDOM

--ORGANIZE   ON (LOCATION_ID)


