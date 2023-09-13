--*****  Creating table:  "cust_sensitive.legacy_sds_order_item_rpt" , ***** Creating table: "SITE_HOURS_DAY_PRE"

use cust_sensitive;

CREATE TABLE legacy_sds_order_item_rpt (
  SDS_WORK_ORDER_ID STRING,
  SDS_WORK_ORDER_NBR BIGINT,
  SDS_ORDER_ITEM_ID STRING,
  SDS_ORDER_ITEM_NBR BIGINT,
  SDS_ORDER_ID STRING,
  SDS_ORDER_NBR BIGINT,
  SDS_SERVICE_APPOINTMENT_ID STRING,
  SDS_REDEMPTION_ID STRING,
  PETM_POS_INVOICE_ID INT,
  INVOICE_CREATE_TSTMP TIMESTAMP,
  SDS_SERVICE_SUB_CATEGORY_ID INT,
  SDS_SERVICE_SUB_CATEGORY_DESC STRING,
  SVCS_SERVICE_SUB_CATEGORY_GID SMALLINT,
  SVCS_SERVICE_SUB_CATEGORY_DESC STRING,
  LOCATION_ID INT,
  SDS_LOCATION_TYPE STRING,
  PRODUCT_ID INT,
  UPC_ID BIGINT,
  PRIMARY_SERVICE_FLAG TINYINT,
  INTERNAL_TAX_CLASS STRING,
  EXTERNAL_TAX_CD STRING,
  APPT_TSTMP TIMESTAMP,
  SDS_WORK_ORDER_STATUS_ID INT,
  SDS_WORK_ORDER_STATUS_DESC STRING,
  SDS_APPT_STATUS_ID INT,
  SDS_APPT_STATUS_DESC STRING,
  SVCS_APPT_STATUS_GID SMALLINT,
  SVCS_APPT_STATUS_DESC STRING,
  SDS_APPT_CREATION_CHANNEL STRING,
  SDS_ORDER_STATUS_ID INT,
  SDS_ORDER_STATUS_DESC STRING,
  SVCS_ORDER_STATUS_GID SMALLINT,
  SVCS_ORDER_STATUS_DESC STRING,
  ORDER_VOID_TSTMP TIMESTAMP,
  ORDER_VOID_REASON STRING,
  SDS_SERVICE_CATEGORY_ID INT,
  SDS_SERVICE_CATEGORY_DESC STRING,
  SVCS_SERVICE_CATEGORY_GID SMALLINT,
  SVCS_SERVICE_CATEGORY_DESC STRING,
  SDS_ACCOUNT_ID STRING,
  PODS_CUSTOMER_ID BIGINT,
  SVCS_CUSTOMER_GID BIGINT,
  CUSTOMER_FIRST_NAME STRING,
  CUSTOMER_LAST_NAME STRING,
  CUSTOMER_PHONE_NBR STRING,
  CUSTOMER_EMAIL_ADDR STRING,
  CUSTOMER_ADDR1 STRING,
  CUSTOMER_ADDR2 STRING,
  CUSTOMER_CITY STRING,
  CUSTOMER_STATE STRING,
  CUSTOMER_POSTAL_CD STRING,
  CUSTOMER_POSTAL_CD_EXT STRING,
  CUSTOMER_COUNTRY_CD STRING,
  CUSTOMER_DO_NOT_BOOKED_FLAG TINYINT,
  CUSTOMER_DO_NOT_BOOKED_REASON STRING,
  CUSTOMER_LOYALTY_FLAG TINYINT,
  DIGITAL_LOYALTY_CUSTOMER_FLAG TINYINT,
  RECEIVE_EMAIL_NOTIFICATIONS_FLAG TINYINT,
  RECEIVE_TEXT_NOTIFICATION_FLAG TINYINT,
  RECEIVE_PUSH_NOTIFICATION_FLAG TINYINT,
  REMINDER_CALL_FLAG TINYINT,
  CUSTOMER_GROOMING_NEW_FLAG TINYINT,
  CUSTOMER_HOTEL_NEW_FLAG TINYINT,
  CUSTOMER_DDC_NEW_FLAG TINYINT,
  CUSTOMER_TRAINING_NEW_FLAG TINYINT,
  PODS_PET_ID BIGINT,
  SVCS_PET_GID BIGINT,
  PET_NAME STRING,
  PETM_PET_SPECIES_ID INT,
  PETM_PET_SPECIES_DESC STRING,
  PETM_PET_GENDER_ID INT,
  PETM_PET_GENDER_DESC STRING,
  PETM_PET_BREED_ID INT,
  PETM_PET_BREED_DESC STRING,
  MIXED_BREED_FLAG TINYINT,
  PETM_PET_COLOR_ID INT,
  PETM_PET_COLOR_DESC STRING,
  PET_STATUS STRING,
  GROOMING_PET_RATING_DESC STRING,
  HOTEL_PET_RATING_DESC STRING,
  DDC_PET_RATING_DESC STRING,
  TRAINING_PET_RATING_DESC STRING,
  GROOMING_NOTES STRING,
  GROOMING_SERVICE_CARD_ALERT_TEXT STRING,
  PET_BIRTH_DT DATE,
  BORDETELLA_EXP_DT DATE,
  RABIES_EXP_DT DATE,
  DISTEMPER_EXP_DT DATE,
  PARVOVIRUS_EXP_DT DATE,
  PARAINFLUENZA_EXP_DT DATE,
  FVRCCP_EXP_DT DATE,
  SDS_PET_CREATE_TSTMP TIMESTAMP,
  BOARDED_GUEST_FLAG TINYINT,
  CREATED_SDS_USER_ID STRING,
  CREATED_SDS_USER_NAME STRING,
  CREATED_SDS_USER_DISPLAY_NAME STRING,
  CREATED_SDS_PROFILE_DESC STRING,
  REQUESTED_SDS_EMPLOYEE_GROUP_ID INT,
  REQUESTED_SDS_EMPLOYEE_GROUP_DESC STRING,
  REQUESTED_SVCS_EMPLOYEE_GROUP_GID SMALLINT,
  REQUESTED_SVCS_EMPLOYEE_GROUP_DESC STRING,
  REQUESTED_EMPLOYEE_ID INT,
  REQUESTED_EMPLOYEE_FIRST_NAME STRING,
  REQUESTED_EMPLOYEE_LAST_NAME STRING,
  REQUESTED_EMPLOYEE_DISPLAY_NAME STRING,
  ASSIGNED_SDS_EMPLOYEE_GROUP_ID INT,
  ASSIGNED_SDS_EMPLOYEE_GROUP_DESC STRING,
  ASSIGNED_SVCS_EMPLOYEE_GROUP_GID SMALLINT,
  ASSIGNED_SVCS_EMPLOYEE_GROUP_DESC STRING,
  ASSIGNED_EMPLOYEE_ID INT,
  ASSIGNED_EMPLOYEE_FIRST_NAME STRING,
  ASSIGNED_EMPLOYEE_LAST_NAME STRING,
  ASSIGNED_EMPLOYEE_DISPLAY_NAME STRING,
  ORDER_CREATE_TSTMP TIMESTAMP,
  APPT_CREATE_TSTMP TIMESTAMP,
  APPT_LAST_MODIFY_TSTMP TIMESTAMP,
  APPT_SERVICE_CREATE_TSTMP TIMESTAMP,
  APPT_SERVICE_LAST_MODIFY_TSTMP TIMESTAMP,
  APPT_UPDATE_FLAG TINYINT,
  APPT_SERVICE_UPDATE_FLAG TINYINT,
  APPT_CONFIRMED_TSTMP TIMESTAMP,
  APPT_CONFIRMATION_CHANNEL STRING,
  APPT_CONFIRMED_BY STRING,
  PREPAID_FLAG TINYINT,
  MANUAL_APPOINTMENT_FLAG TINYINT,
  WALK_IN_FLAG TINYINT,
  PRE_CHECK_IN_FLAG TINYINT,
  REBOOKED_FLAG TINYINT,
  NO_SHOW_FLAG TINYINT,
  DUMMY_APPOINTMENT_FLAG TINYINT,
  SCHED_APPT_START_TSTMP TIMESTAMP,
  SCHED_APPT_END_TSTMP TIMESTAMP,
  SCHED_APPT_SERVICE_START_TSTMP TIMESTAMP,
  SCHED_APPT_SERVICE_END_TSTMP TIMESTAMP,
  APPT_CHECK_IN_TSTMP TIMESTAMP,
  APPT_CHECK_OUT_TSTMP TIMESTAMP,
  APPT_SERVICE_START_TSTMP TIMESTAMP,
  APPT_SERVICE_END_TSTMP TIMESTAMP,
  APPT_COMPLETE_FLAG TINYINT,
  APPT_SERVICE_COMPLETE_FLAG TINYINT,
  APPT_CANCEL_TSTMP TIMESTAMP,
  APPT_SERVICE_CANCEL_TSTMP TIMESTAMP,
  APPT_CANCEL_FLAG TINYINT,
  APPT_SERVICE_CANCEL_FLAG TINYINT,
  APPT_CANCELLATION_CHANNEL STRING,
  APPT_CANCELLED_BY STRING,
  APPT_SDS_CANCEL_REASON_ID SMALLINT,
  APPT_SDS_CANCEL_REASON_DESC STRING,
  APPT_SVCS_CANCEL_REASON_GID SMALLINT,
  APPT_SVCS_CANCEL_REASON_DESC STRING,
  APPT_CANCELLED_EMPLOYEE_ID INT,
  APPT_CANCELLED_EMPLOYEE_FIRST_NAME STRING,
  APPT_CANCELLED_EMPLOYEE_LAST_NAME STRING,
  APPT_CANCELLED_EMPLOYEE_DISPLAY_NAME STRING,
  APPT_TOTAL_PRICE_AMT DECIMAL(18,2),
  APPT_TOTAL_ITEM_QTY SMALLINT,
  APPT_SERVICE_ITEM_QTY INT,
  ORIG_UNIT_PRICE_AMT DECIMAL(18,2),
  LIST_PRICE_AMT DECIMAL(18,2),
  FINAL_UNIT_PRICE_AMT DECIMAL(18,2),
  TOTAL_GROOM_PAY_PRICE_AMT DECIMAL(18,2),
  SDS_PRICE_OVERRIDE_REASON_ID INT,
  SDS_PRICE_OVERRIDE_REASON_DESC STRING,
  SVCS_PRICE_OVERRIDE_REASON_GID INT,
  SVCS_PRICE_OVERRIDE_REASON_DESC STRING,
  APPT_SERVICE_PRICE_OVERRIDE_TSTMP TIMESTAMP,
  EMPLOYEE_COMMISSION_RATE_PCT DECIMAL(6,4),
  EMPLOYEE_COMMISSION_AMT DECIMAL(10,2),
  APPT_PAYMENT_TSTMP TIMESTAMP,
  APPT_PAYMENT_AMT DECIMAL(18,2),
  SDS_PAYMENT_METHOD_ID SMALLINT,
  SDS_PAYMENT_METHOD_DESC STRING,
  APPT_SVCS_PAYMENT_METHOD_GID INT,
  APPT_SVCS_PAYMENT_METHOD_DESC STRING,
  PAYMENT_ORIGIN STRING,
  EXCH_RATE_PCT DECIMAL(9,6),
  WEIGHT_IN_LBS DECIMAL(18,2),
  LOAD_TSTMP TIMESTAMP,
  ADDITIONAL_BOOKING_TYPE STRING,
  GROOMING_RATING_REASON STRING)
USING delta
LOCATION 'gs://petm-bdpl-dev-cust-sensitive-refine-p1-gcs-gbl/services/sds_order_item_rpt';