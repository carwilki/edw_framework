#Code converted on 2023-06-24 13:43:58
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from logging import getLogger, INFO



def m_WM_Asn_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Asn_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ASN_PRE"

    schemaName = raw
    source_schema = "WMSMIS"


    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]


    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now() #start timestamp of the script

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)

    # COMMAND ----------
    # Variable_declaration_comment
    dcnbr = dcnbr.strip()[2:]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)

    # COMMAND ----------
    # Processing node SQ_Shortcut_to_ASN, type SOURCE 
    # COLUMN COUNT: 141

    SQ_Shortcut_to_ASN = jdbcOracleConnection(
        f"""SELECT
                ASN.ASN_ID,
                ASN.TC_COMPANY_ID,
                ASN.TC_ASN_ID,
                ASN.TC_ASN_ID_U,
                ASN.ASN_TYPE,
                ASN.ASN_STATUS,
                ASN.TRACTOR_NUMBER,
                ASN.DELIVERY_STOP_SEQ,
                ASN.ORIGIN_FACILITY_ALIAS_ID,
                ASN.ORIGIN_FACILITY_ID,
                ASN.PICKUP_END_DTTM,
                ASN.ACTUAL_DEPARTURE_DTTM,
                ASN.DESTINATION_FACILITY_ALIAS_ID,
                ASN.DESTINATION_FACILITY_ID,
                ASN.DELIVERY_START_DTTM,
                ASN.DELIVERY_END_DTTM,
                ASN.ACTUAL_ARRIVAL_DTTM,
                ASN.RECEIPT_DTTM,
                ASN.INBOUND_REGION_NAME,
                ASN.OUTBOUND_REGION_NAME,
                ASN.REF_FIELD_1,
                ASN.REF_FIELD_2,
                ASN.REF_FIELD_3,
                ASN.TOTAL_WEIGHT,
                ASN.APPOINTMENT_ID,
                ASN.APPOINTMENT_DTTM,
                ASN.APPOINTMENT_DURATION,
                ASN.PALLET_FOOTPRINT,
                ASN.LAST_TRANSMITTED_DTTM,
                ASN.TOTAL_SHIPPED_QTY,
                ASN.TOTAL_RECEIVED_QTY,
                ASN.ASSIGNED_CARRIER_CODE,
                ASN.BILL_OF_LADING_NUMBER,
                ASN.PRO_NUMBER,
                ASN.EQUIPMENT_CODE,
                ASN.EQUIPMENT_CODE_ID,
                ASN.ASSIGNED_CARRIER_CODE_ID,
                ASN.TOTAL_VOLUME,
                ASN.VOLUME_UOM_ID_BASE,
                ASN.FIRM_APPT_IND,
                ASN.TRACTOR_CARRIER_CODE_ID,
                ASN.TRACTOR_CARRIER_CODE,
                ASN.BUYER_CODE,
                ASN.REP_NAME,
                ASN.CONTACT_ADDRESS_1,
                ASN.CONTACT_ADDRESS_2,
                ASN.CONTACT_ADDRESS_3,
                ASN.CONTACT_CITY,
                ASN.CONTACT_STATE_PROV,
                ASN.CONTACT_ZIP,
                ASN.CONTACT_NUMBER,
                ASN.EXT_CREATED_DTTM,
                ASN.INBOUND_REGION_ID,
                ASN.OUTBOUND_REGION_ID,
                ASN.REGION_ID,
                ASN.ASN_LEVEL,
                ASN.RECEIPT_VARIANCE,
                ASN.HAS_IMPORT_ERROR,
                ASN.HAS_SOFT_CHECK_ERROR,
                ASN.HAS_ALERTS,
                ASN.SYSTEM_ALLOCATED,
                ASN.IS_COGI_GENERATED,
                ASN.ASN_PRIORITY,
                ASN.SCHEDULE_APPT,
                ASN.IS_ASSOCIATED_TO_OUTBOUND,
                ASN.IS_CANCELLED,
                ASN.IS_CLOSED,
                ASN.BUSINESS_PARTNER_ID,
                ASN.MANIF_NBR,
                ASN.MANIF_TYPE,
                ASN.WORK_ORD_NBR,
                ASN.CUT_NBR,
                ASN.MFG_PLNT,
                ASN.IS_WHSE_TRANSFER,
                ASN.QUALITY_CHECK_HOLD_UPON_RCPT,
                ASN.QUALITY_AUDIT_PERCENT,
                ASN.SHIPPED_LPN_COUNT,
                ASN.RECEIVED_LPN_COUNT,
                ASN.ACTUAL_SHIPPED_DTTM,
                ASN.LAST_RECEIPT_DTTM,
                ASN.VERIFIED_DTTM,
                ASN.LABEL_PRINT_REQD,
                ASN.INITIATE_FLAG,
                ASN.CREATED_SOURCE_TYPE,
                ASN.CREATED_SOURCE,
                ASN.CREATED_DTTM,
                ASN.LAST_UPDATED_SOURCE_TYPE,
                ASN.LAST_UPDATED_SOURCE,
                ASN.LAST_UPDATED_DTTM,
                ASN.SHIPMENT_ID,
                ASN.TC_SHIPMENT_ID,
                ASN.VERIFICATION_ATTEMPTED,
                ASN.FLOW_THRU_ALLOC_IN_PROG,
                ASN.ALLOCATION_COMPLETED,
                ASN.EQUIPMENT_TYPE,
                ASN.ASN_ORGN_TYPE,
                ASN.DC_ORD_NBR,
                ASN.CONTRAC_LOCN,
                ASN.MHE_SENT,
                ASN.FLOW_THROUGH_ALLOCATION_METHOD,
                ASN.FIRST_RECEIPT_DTTM,
                ASN.MISC_INSTR_CODE_1,
                ASN.MISC_INSTR_CODE_2,
                ASN.DOCK_DOOR_ID,
                ASN.MODE_ID,
                ASN.SHIPPING_COST,
                ASN.SHIPPING_COST_CURRENCY_CODE,
                ASN.FLOWTHROUGH_ALLOCATION_STATUS,
                ASN.HIBERNATE_VERSION,
                ASN.QTY_UOM_ID_BASE,
                ASN.WEIGHT_UOM_ID_BASE,
                ASN.DRIVER_NAME,
                ASN.TRAILER_NUMBER,
                ASN.DESTINATION_TYPE,
                ASN.CONTACT_COUNTY,
                ASN.CONTACT_COUNTRY_CODE,
                ASN.RECEIPT_TYPE,
                ASN.VARIANCE_TYPE,
                ASN.HAS_NOTES,
                ASN.ORIGINAL_ASN_ID,
                ASN.RETURN_REFERENCE_NUMBER,
                ASN.REF_FIELD_4,
                ASN.REF_FIELD_5,
                ASN.REF_FIELD_6,
                ASN.REF_FIELD_7,
                ASN.REF_FIELD_8,
                ASN.REF_FIELD_9,
                ASN.REF_FIELD_10,
                ASN.REF_NUM1,
                ASN.REF_NUM2,
                ASN.REF_NUM3,
                ASN.REF_NUM4,
                ASN.REF_NUM5,
                ASN.INVOICE_DATE,
                ASN.INVOICE_NUMBER,
                ASN.PRE_RECEIPT_STATUS,
                ASN.PRE_ALLOCATION_FIT_PERCENTAGE,
                ASN.TRAILER_CLOSED,
                ASN.IS_GIFT,
                ASN.ORIGINAL_ORDER_NUMBER,
                ASN.ASN_SHPMT_TYPE
            FROM {source_schema}.ASN
            WHERE (TRUNC( CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC( LAST_UPDATED_DTTM) >=  TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 143

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ASN_temp = SQ_Shortcut_to_ASN.toDF(*["SQ_Shortcut_to_ASN___" + col for col in SQ_Shortcut_to_ASN.columns])

    EXPTRANS = SQ_Shortcut_to_ASN_temp.selectExpr( 
        "SQ_Shortcut_to_ASN___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_ASN___ASN_ID as ASN_ID", 
        "SQ_Shortcut_to_ASN___TC_COMPANY_ID as TC_COMPANY_ID", 
        "SQ_Shortcut_to_ASN___TC_ASN_ID as TC_ASN_ID", 
        "SQ_Shortcut_to_ASN___TC_ASN_ID_U as TC_ASN_ID_U", 
        "SQ_Shortcut_to_ASN___ASN_TYPE as ASN_TYPE", 
        "SQ_Shortcut_to_ASN___ASN_STATUS as ASN_STATUS", 
        "SQ_Shortcut_to_ASN___TRACTOR_NUMBER as TRACTOR_NUMBER", 
        "SQ_Shortcut_to_ASN___DELIVERY_STOP_SEQ as DELIVERY_STOP_SEQ", 
        "SQ_Shortcut_to_ASN___ORIGIN_FACILITY_ALIAS_ID as ORIGIN_FACILITY_ALIAS_ID", 
        "SQ_Shortcut_to_ASN___ORIGIN_FACILITY_ID as ORIGIN_FACILITY_ID", 
        "SQ_Shortcut_to_ASN___PICKUP_END_DTTM as PICKUP_END_DTTM", 
        "SQ_Shortcut_to_ASN___ACTUAL_DEPARTURE_DTTM as ACTUAL_DEPARTURE_DTTM", 
        "SQ_Shortcut_to_ASN___DESTINATION_FACILITY_ALIAS_ID as DESTINATION_FACILITY_ALIAS_ID", 
        "SQ_Shortcut_to_ASN___DESTINATION_FACILITY_ID as DESTINATION_FACILITY_ID", 
        "SQ_Shortcut_to_ASN___DELIVERY_START_DTTM as DELIVERY_START_DTTM", 
        "SQ_Shortcut_to_ASN___DELIVERY_END_DTTM as DELIVERY_END_DTTM", 
        "SQ_Shortcut_to_ASN___ACTUAL_ARRIVAL_DTTM as ACTUAL_ARRIVAL_DTTM", 
        "SQ_Shortcut_to_ASN___RECEIPT_DTTM as RECEIPT_DTTM", 
        "SQ_Shortcut_to_ASN___INBOUND_REGION_NAME as INBOUND_REGION_NAME", 
        "SQ_Shortcut_to_ASN___OUTBOUND_REGION_NAME as OUTBOUND_REGION_NAME", 
        "SQ_Shortcut_to_ASN___REF_FIELD_1 as REF_FIELD_1", 
        "SQ_Shortcut_to_ASN___REF_FIELD_2 as REF_FIELD_2", 
        "SQ_Shortcut_to_ASN___REF_FIELD_3 as REF_FIELD_3", 
        "SQ_Shortcut_to_ASN___TOTAL_WEIGHT as TOTAL_WEIGHT", 
        "SQ_Shortcut_to_ASN___APPOINTMENT_ID as APPOINTMENT_ID", 
        "SQ_Shortcut_to_ASN___APPOINTMENT_DTTM as APPOINTMENT_DTTM", 
        "SQ_Shortcut_to_ASN___APPOINTMENT_DURATION as APPOINTMENT_DURATION", 
        "SQ_Shortcut_to_ASN___PALLET_FOOTPRINT as PALLET_FOOTPRINT", 
        "SQ_Shortcut_to_ASN___LAST_TRANSMITTED_DTTM as LAST_TRANSMITTED_DTTM", 
        "SQ_Shortcut_to_ASN___TOTAL_SHIPPED_QTY as TOTAL_SHIPPED_QTY", 
        "SQ_Shortcut_to_ASN___TOTAL_RECEIVED_QTY as TOTAL_RECEIVED_QTY", 
        "SQ_Shortcut_to_ASN___ASSIGNED_CARRIER_CODE as ASSIGNED_CARRIER_CODE", 
        "SQ_Shortcut_to_ASN___BILL_OF_LADING_NUMBER as BILL_OF_LADING_NUMBER", 
        "SQ_Shortcut_to_ASN___PRO_NUMBER as PRO_NUMBER", 
        "SQ_Shortcut_to_ASN___EQUIPMENT_CODE as EQUIPMENT_CODE", 
        "SQ_Shortcut_to_ASN___EQUIPMENT_CODE_ID as EQUIPMENT_CODE_ID", 
        "SQ_Shortcut_to_ASN___ASSIGNED_CARRIER_CODE_ID as ASSIGNED_CARRIER_CODE_ID", 
        "SQ_Shortcut_to_ASN___TOTAL_VOLUME as TOTAL_VOLUME", 
        "SQ_Shortcut_to_ASN___VOLUME_UOM_ID_BASE as VOLUME_UOM_ID_BASE", 
        "SQ_Shortcut_to_ASN___FIRM_APPT_IND as FIRM_APPT_IND", 
        "SQ_Shortcut_to_ASN___TRACTOR_CARRIER_CODE_ID as TRACTOR_CARRIER_CODE_ID", 
        "SQ_Shortcut_to_ASN___TRACTOR_CARRIER_CODE as TRACTOR_CARRIER_CODE", 
        "SQ_Shortcut_to_ASN___BUYER_CODE as BUYER_CODE", 
        "SQ_Shortcut_to_ASN___REP_NAME as REP_NAME", 
        "SQ_Shortcut_to_ASN___CONTACT_ADDRESS_1 as CONTACT_ADDRESS_1", 
        "SQ_Shortcut_to_ASN___CONTACT_ADDRESS_2 as CONTACT_ADDRESS_2", 
        "SQ_Shortcut_to_ASN___CONTACT_ADDRESS_3 as CONTACT_ADDRESS_3", 
        "SQ_Shortcut_to_ASN___CONTACT_CITY as CONTACT_CITY", 
        "SQ_Shortcut_to_ASN___CONTACT_STATE_PROV as CONTACT_STATE_PROV", 
        "SQ_Shortcut_to_ASN___CONTACT_ZIP as CONTACT_ZIP", 
        "SQ_Shortcut_to_ASN___CONTACT_NUMBER as CONTACT_NUMBER", 
        "SQ_Shortcut_to_ASN___EXT_CREATED_DTTM as EXT_CREATED_DTTM", 
        "SQ_Shortcut_to_ASN___INBOUND_REGION_ID as INBOUND_REGION_ID", 
        "SQ_Shortcut_to_ASN___OUTBOUND_REGION_ID as OUTBOUND_REGION_ID", 
        "SQ_Shortcut_to_ASN___REGION_ID as REGION_ID", 
        "SQ_Shortcut_to_ASN___ASN_LEVEL as ASN_LEVEL", 
        "SQ_Shortcut_to_ASN___RECEIPT_VARIANCE as RECEIPT_VARIANCE", 
        "SQ_Shortcut_to_ASN___HAS_IMPORT_ERROR as HAS_IMPORT_ERROR", 
        "SQ_Shortcut_to_ASN___HAS_SOFT_CHECK_ERROR as HAS_SOFT_CHECK_ERROR", 
        "SQ_Shortcut_to_ASN___HAS_ALERTS as HAS_ALERTS", 
        "SQ_Shortcut_to_ASN___SYSTEM_ALLOCATED as SYSTEM_ALLOCATED", 
        "SQ_Shortcut_to_ASN___IS_COGI_GENERATED as IS_COGI_GENERATED", 
        "SQ_Shortcut_to_ASN___ASN_PRIORITY as ASN_PRIORITY", 
        "SQ_Shortcut_to_ASN___SCHEDULE_APPT as SCHEDULE_APPT", 
        "SQ_Shortcut_to_ASN___IS_ASSOCIATED_TO_OUTBOUND as IS_ASSOCIATED_TO_OUTBOUND", 
        "SQ_Shortcut_to_ASN___IS_CANCELLED as IS_CANCELLED", 
        "SQ_Shortcut_to_ASN___IS_CLOSED as IS_CLOSED", 
        "SQ_Shortcut_to_ASN___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", 
        "SQ_Shortcut_to_ASN___MANIF_NBR as MANIF_NBR", 
        "SQ_Shortcut_to_ASN___MANIF_TYPE as MANIF_TYPE", 
        "SQ_Shortcut_to_ASN___WORK_ORD_NBR as WORK_ORD_NBR", 
        "SQ_Shortcut_to_ASN___CUT_NBR as CUT_NBR", 
        "SQ_Shortcut_to_ASN___MFG_PLNT as MFG_PLNT", 
        "SQ_Shortcut_to_ASN___IS_WHSE_TRANSFER as IS_WHSE_TRANSFER", 
        "SQ_Shortcut_to_ASN___QUALITY_CHECK_HOLD_UPON_RCPT as QUALITY_CHECK_HOLD_UPON_RCPT", 
        "SQ_Shortcut_to_ASN___QUALITY_AUDIT_PERCENT as QUALITY_AUDIT_PERCENT", 
        "SQ_Shortcut_to_ASN___SHIPPED_LPN_COUNT as SHIPPED_LPN_COUNT", 
        "SQ_Shortcut_to_ASN___RECEIVED_LPN_COUNT as RECEIVED_LPN_COUNT", 
        "SQ_Shortcut_to_ASN___ACTUAL_SHIPPED_DTTM as ACTUAL_SHIPPED_DTTM", 
        "SQ_Shortcut_to_ASN___LAST_RECEIPT_DTTM as LAST_RECEIPT_DTTM", 
        "SQ_Shortcut_to_ASN___VERIFIED_DTTM as VERIFIED_DTTM", 
        "SQ_Shortcut_to_ASN___LABEL_PRINT_REQD as LABEL_PRINT_REQD", 
        "SQ_Shortcut_to_ASN___INITIATE_FLAG as INITIATE_FLAG", 
        "SQ_Shortcut_to_ASN___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_ASN___CREATED_SOURCE as CREATED_SOURCE", 
        "SQ_Shortcut_to_ASN___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_ASN___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_ASN___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
        "SQ_Shortcut_to_ASN___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "SQ_Shortcut_to_ASN___SHIPMENT_ID as SHIPMENT_ID", 
        "SQ_Shortcut_to_ASN___TC_SHIPMENT_ID as TC_SHIPMENT_ID", 
        "SQ_Shortcut_to_ASN___VERIFICATION_ATTEMPTED as VERIFICATION_ATTEMPTED", 
        "SQ_Shortcut_to_ASN___FLOW_THRU_ALLOC_IN_PROG as FLOW_THRU_ALLOC_IN_PROG", 
        "SQ_Shortcut_to_ASN___ALLOCATION_COMPLETED as ALLOCATION_COMPLETED", 
        "SQ_Shortcut_to_ASN___EQUIPMENT_TYPE as EQUIPMENT_TYPE", 
        "SQ_Shortcut_to_ASN___ASN_ORGN_TYPE as ASN_ORGN_TYPE", 
        "SQ_Shortcut_to_ASN___DC_ORD_NBR as DC_ORD_NBR", 
        "SQ_Shortcut_to_ASN___CONTRAC_LOCN as CONTRAC_LOCN", 
        "SQ_Shortcut_to_ASN___MHE_SENT as MHE_SENT", 
        "SQ_Shortcut_to_ASN___FLOW_THROUGH_ALLOCATION_METHOD as FLOW_THROUGH_ALLOCATION_METHOD", 
        "SQ_Shortcut_to_ASN___FIRST_RECEIPT_DTTM as FIRST_RECEIPT_DTTM", 
        "SQ_Shortcut_to_ASN___MISC_INSTR_CODE_1 as MISC_INSTR_CODE_1", 
        "SQ_Shortcut_to_ASN___MISC_INSTR_CODE_2 as MISC_INSTR_CODE_2", 
        "SQ_Shortcut_to_ASN___DOCK_DOOR_ID as DOCK_DOOR_ID", 
        "SQ_Shortcut_to_ASN___MODE_ID as MODE_ID", 
        "SQ_Shortcut_to_ASN___SHIPPING_COST as SHIPPING_COST", 
        "SQ_Shortcut_to_ASN___SHIPPING_COST_CURRENCY_CODE as SHIPPING_COST_CURRENCY_CODE", 
        "SQ_Shortcut_to_ASN___FLOWTHROUGH_ALLOCATION_STATUS as FLOWTHROUGH_ALLOCATION_STATUS", 
        "SQ_Shortcut_to_ASN___HIBERNATE_VERSION as HIBERNATE_VERSION", 
        "SQ_Shortcut_to_ASN___QTY_UOM_ID_BASE as QTY_UOM_ID_BASE", 
        "SQ_Shortcut_to_ASN___WEIGHT_UOM_ID_BASE as WEIGHT_UOM_ID_BASE", 
        "SQ_Shortcut_to_ASN___DRIVER_NAME as DRIVER_NAME", 
        "SQ_Shortcut_to_ASN___TRAILER_NUMBER as TRAILER_NUMBER", 
        "SQ_Shortcut_to_ASN___DESTINATION_TYPE as DESTINATION_TYPE", 
        "SQ_Shortcut_to_ASN___CONTACT_COUNTY as CONTACT_COUNTY", 
        "SQ_Shortcut_to_ASN___CONTACT_COUNTRY_CODE as CONTACT_COUNTRY_CODE", 
        "SQ_Shortcut_to_ASN___RECEIPT_TYPE as RECEIPT_TYPE", 
        "SQ_Shortcut_to_ASN___VARIANCE_TYPE as VARIANCE_TYPE", 
        "SQ_Shortcut_to_ASN___HAS_NOTES as HAS_NOTES", 
        "SQ_Shortcut_to_ASN___ORIGINAL_ASN_ID as ORIGINAL_ASN_ID", 
        "SQ_Shortcut_to_ASN___RETURN_REFERENCE_NUMBER as RETURN_REFERENCE_NUMBER", 
        "SQ_Shortcut_to_ASN___REF_FIELD_4 as REF_FIELD_4", 
        "SQ_Shortcut_to_ASN___REF_FIELD_5 as REF_FIELD_5", 
        "SQ_Shortcut_to_ASN___REF_FIELD_6 as REF_FIELD_6", 
        "SQ_Shortcut_to_ASN___REF_FIELD_7 as REF_FIELD_7", 
        "SQ_Shortcut_to_ASN___REF_FIELD_8 as REF_FIELD_8", 
        "SQ_Shortcut_to_ASN___REF_FIELD_9 as REF_FIELD_9", 
        "SQ_Shortcut_to_ASN___REF_FIELD_10 as REF_FIELD_10", 
        "SQ_Shortcut_to_ASN___REF_NUM1 as REF_NUM1", 
        "SQ_Shortcut_to_ASN___REF_NUM2 as REF_NUM2", 
        "SQ_Shortcut_to_ASN___REF_NUM3 as REF_NUM3", 
        "SQ_Shortcut_to_ASN___REF_NUM4 as REF_NUM4", 
        "SQ_Shortcut_to_ASN___REF_NUM5 as REF_NUM5", 
        "SQ_Shortcut_to_ASN___INVOICE_DATE as INVOICE_DATE", 
        "SQ_Shortcut_to_ASN___INVOICE_NUMBER as INVOICE_NUMBER", 
        "SQ_Shortcut_to_ASN___PRE_RECEIPT_STATUS as PRE_RECEIPT_STATUS", 
        "SQ_Shortcut_to_ASN___PRE_ALLOCATION_FIT_PERCENTAGE as PRE_ALLOCATION_FIT_PERCENTAGE", 
        "SQ_Shortcut_to_ASN___TRAILER_CLOSED as TRAILER_CLOSED", 
        "SQ_Shortcut_to_ASN___IS_GIFT as IS_GIFT", 
        "SQ_Shortcut_to_ASN___ORIGINAL_ORDER_NUMBER as ORIGINAL_ORDER_NUMBER", 
        "SQ_Shortcut_to_ASN___ASN_SHPMT_TYPE as ASN_SHPMT_TYPE", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ASN_PRE, type TARGET 
    # COLUMN COUNT: 143


    Shortcut_to_WM_ASN_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(ASN_ID AS BIGINT) as ASN_ID",
        "CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID",
        "CAST(TC_ASN_ID AS STRING) as TC_ASN_ID",
        "CAST(TC_ASN_ID_U AS STRING) as TC_ASN_ID_U",
        "CAST(ASN_TYPE AS TINYINT) as ASN_TYPE",
        "CAST(ASN_STATUS AS SMALLINT) as ASN_STATUS",
        "CAST(TRACTOR_NUMBER AS STRING) as TRACTOR_NUMBER",
        "CAST(DELIVERY_STOP_SEQ AS SMALLINT) as DELIVERY_STOP_SEQ",
        "CAST(ORIGIN_FACILITY_ALIAS_ID AS STRING) as ORIGIN_FACILITY_ALIAS_ID",
        "CAST(ORIGIN_FACILITY_ID AS INT) as ORIGIN_FACILITY_ID",
        "CAST(PICKUP_END_DTTM AS TIMESTAMP) as PICKUP_END_DTTM",
        "CAST(ACTUAL_DEPARTURE_DTTM AS TIMESTAMP) as ACTUAL_DEPARTURE_DTTM",
        "CAST(DESTINATION_FACILITY_ALIAS_ID AS STRING) as DESTINATION_FACILITY_ALIAS_ID",
        "CAST(DESTINATION_FACILITY_ID AS INT) as DESTINATION_FACILITY_ID",
        "CAST(DELIVERY_START_DTTM AS TIMESTAMP) as DELIVERY_START_DTTM",
        "CAST(DELIVERY_END_DTTM AS TIMESTAMP) as DELIVERY_END_DTTM",
        "CAST(ACTUAL_ARRIVAL_DTTM AS TIMESTAMP) as ACTUAL_ARRIVAL_DTTM",
        "CAST(RECEIPT_DTTM AS TIMESTAMP) as RECEIPT_DTTM",
        "CAST(INBOUND_REGION_NAME AS STRING) as INBOUND_REGION_NAME",
        "CAST(OUTBOUND_REGION_NAME AS STRING) as OUTBOUND_REGION_NAME",
        "CAST(REF_FIELD_1 AS STRING) as REF_FIELD_1",
        "CAST(REF_FIELD_2 AS STRING) as REF_FIELD_2",
        "CAST(REF_FIELD_3 AS STRING) as REF_FIELD_3",
        "CAST(TOTAL_WEIGHT AS DECIMAL(13,4)) as TOTAL_WEIGHT",
        "CAST(APPOINTMENT_ID AS STRING) as APPOINTMENT_ID",
        "CAST(APPOINTMENT_DTTM AS TIMESTAMP) as APPOINTMENT_DTTM",
        "CAST(APPOINTMENT_DURATION AS INT) as APPOINTMENT_DURATION",
        "CAST(PALLET_FOOTPRINT AS STRING) as PALLET_FOOTPRINT",
        "CAST(LAST_TRANSMITTED_DTTM AS TIMESTAMP) as LAST_TRANSMITTED_DTTM",
        "CAST(TOTAL_SHIPPED_QTY AS DECIMAL(16,4)) as TOTAL_SHIPPED_QTY",
        "CAST(TOTAL_RECEIVED_QTY AS DECIMAL(16,4)) as TOTAL_RECEIVED_QTY",
        "CAST(ASSIGNED_CARRIER_CODE AS STRING) as ASSIGNED_CARRIER_CODE",
        "CAST(BILL_OF_LADING_NUMBER AS STRING) as BILL_OF_LADING_NUMBER",
        "CAST(PRO_NUMBER AS STRING) as PRO_NUMBER",
        "CAST(EQUIPMENT_CODE AS STRING) as EQUIPMENT_CODE",
        "CAST(EQUIPMENT_CODE_ID AS INT) as EQUIPMENT_CODE_ID",
        "CAST(ASSIGNED_CARRIER_CODE_ID AS INT) as ASSIGNED_CARRIER_CODE_ID",
        "CAST(TOTAL_VOLUME AS DECIMAL(13,4)) as TOTAL_VOLUME",
        "CAST(VOLUME_UOM_ID_BASE AS INT) as VOLUME_UOM_ID_BASE",
        "CAST(FIRM_APPT_IND AS TINYINT) as FIRM_APPT_IND",
        "CAST(TRACTOR_CARRIER_CODE_ID AS INT) as TRACTOR_CARRIER_CODE_ID",
        "CAST(TRACTOR_CARRIER_CODE AS STRING) as TRACTOR_CARRIER_CODE",
        "CAST(BUYER_CODE AS STRING) as BUYER_CODE",
        "CAST(REP_NAME AS STRING) as REP_NAME",
        "CAST(CONTACT_ADDRESS_1 AS STRING) as CONTACT_ADDRESS_1",
        "CAST(CONTACT_ADDRESS_2 AS STRING) as CONTACT_ADDRESS_2",
        "CAST(CONTACT_ADDRESS_3 AS STRING) as CONTACT_ADDRESS_3",
        "CAST(CONTACT_CITY AS STRING) as CONTACT_CITY",
        "CAST(CONTACT_STATE_PROV AS STRING) as CONTACT_STATE_PROV",
        "CAST(CONTACT_ZIP AS STRING) as CONTACT_ZIP",
        "CAST(CONTACT_NUMBER AS STRING) as CONTACT_NUMBER",
        "CAST(EXT_CREATED_DTTM AS TIMESTAMP) as EXT_CREATED_DTTM",
        "CAST(INBOUND_REGION_ID AS INT) as INBOUND_REGION_ID",
        "CAST(OUTBOUND_REGION_ID AS INT) as OUTBOUND_REGION_ID",
        "CAST(REGION_ID AS INT) as REGION_ID",
        "CAST(ASN_LEVEL AS SMALLINT) as ASN_LEVEL",
        "CAST(RECEIPT_VARIANCE AS TINYINT) as RECEIPT_VARIANCE",
        "CAST(HAS_IMPORT_ERROR AS TINYINT) as HAS_IMPORT_ERROR",
        "CAST(HAS_SOFT_CHECK_ERROR AS TINYINT) as HAS_SOFT_CHECK_ERROR",
        "CAST(HAS_ALERTS AS TINYINT) as HAS_ALERTS",
        "CAST(SYSTEM_ALLOCATED AS TINYINT) as SYSTEM_ALLOCATED",
        "CAST(IS_COGI_GENERATED AS TINYINT) as IS_COGI_GENERATED",
        "CAST(ASN_PRIORITY AS TINYINT) as ASN_PRIORITY",
        "CAST(SCHEDULE_APPT AS TINYINT) as SCHEDULE_APPT",
        "CAST(IS_ASSOCIATED_TO_OUTBOUND AS TINYINT) as IS_ASSOCIATED_TO_OUTBOUND",
        "CAST(IS_CANCELLED AS TINYINT) as IS_CANCELLED",
        "CAST(IS_CLOSED AS TINYINT) as IS_CLOSED",
        "CAST(BUSINESS_PARTNER_ID AS STRING) as BUSINESS_PARTNER_ID",
        "CAST(MANIF_NBR AS STRING) as MANIF_NBR",
        "CAST(MANIF_TYPE AS STRING) as MANIF_TYPE",
        "CAST(WORK_ORD_NBR AS STRING) as WORK_ORD_NBR",
        "CAST(CUT_NBR AS STRING) as CUT_NBR",
        "CAST(MFG_PLNT AS STRING) as MFG_PLNT",
        "CAST(IS_WHSE_TRANSFER AS STRING) as IS_WHSE_TRANSFER",
        "CAST(QUALITY_CHECK_HOLD_UPON_RCPT AS STRING) as QUALITY_CHECK_HOLD_UPON_RCPT",
        "CAST(QUALITY_AUDIT_PERCENT AS DECIMAL(5,2)) as QUALITY_AUDIT_PERCENT",
        "CAST(SHIPPED_LPN_COUNT AS INT) as SHIPPED_LPN_COUNT",
        "CAST(RECEIVED_LPN_COUNT AS INT) as RECEIVED_LPN_COUNT",
        "CAST(ACTUAL_SHIPPED_DTTM AS TIMESTAMP) as ACTUAL_SHIPPED_DTTM",
        "CAST(LAST_RECEIPT_DTTM AS TIMESTAMP) as LAST_RECEIPT_DTTM",
        "CAST(VERIFIED_DTTM AS TIMESTAMP) as VERIFIED_DTTM",
        "CAST(LABEL_PRINT_REQD AS STRING) as LABEL_PRINT_REQD",
        "CAST(INITIATE_FLAG AS STRING) as INITIATE_FLAG",
        "CAST(CREATED_SOURCE_TYPE AS TINYINT) as CREATED_SOURCE_TYPE",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_SOURCE_TYPE AS TINYINT) as LAST_UPDATED_SOURCE_TYPE",
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(SHIPMENT_ID AS BIGINT) as SHIPMENT_ID",
        "CAST(TC_SHIPMENT_ID AS STRING) as TC_SHIPMENT_ID",
        "CAST(VERIFICATION_ATTEMPTED AS STRING) as VERIFICATION_ATTEMPTED",
        "CAST(FLOW_THRU_ALLOC_IN_PROG AS STRING) as FLOW_THRU_ALLOC_IN_PROG",
        "CAST(ALLOCATION_COMPLETED AS STRING) as ALLOCATION_COMPLETED",
        "CAST(EQUIPMENT_TYPE AS STRING) as EQUIPMENT_TYPE",
        "CAST(ASN_ORGN_TYPE AS STRING) as ASN_ORGN_TYPE",
        "CAST(DC_ORD_NBR AS STRING) as DC_ORD_NBR",
        "CAST(CONTRAC_LOCN AS STRING) as CONTRAC_LOCN",
        "CAST(MHE_SENT AS STRING) as MHE_SENT",
        "CAST(FLOW_THROUGH_ALLOCATION_METHOD AS STRING) as FLOW_THROUGH_ALLOCATION_METHOD",
        "CAST(FIRST_RECEIPT_DTTM AS TIMESTAMP) as FIRST_RECEIPT_DTTM",
        "CAST(MISC_INSTR_CODE_1 AS STRING) as MISC_INSTR_CODE_1",
        "CAST(MISC_INSTR_CODE_2 AS STRING) as MISC_INSTR_CODE_2",
        "CAST(DOCK_DOOR_ID AS BIGINT) as DOCK_DOOR_ID",
        "CAST(MODE_ID AS INT) as MODE_ID",
        "CAST(SHIPPING_COST AS DECIMAL(13,4)) as SHIPPING_COST",
        "CAST(SHIPPING_COST_CURRENCY_CODE AS STRING) as SHIPPING_COST_CURRENCY_CODE",
        "CAST(FLOWTHROUGH_ALLOCATION_STATUS AS TINYINT) as FLOWTHROUGH_ALLOCATION_STATUS",
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION",
        "CAST(QTY_UOM_ID_BASE AS INT) as QTY_UOM_ID_BASE",
        "CAST(WEIGHT_UOM_ID_BASE AS INT) as WEIGHT_UOM_ID_BASE",
        "CAST(DRIVER_NAME AS STRING) as DRIVER_NAME",
        "CAST(TRAILER_NUMBER AS STRING) as TRAILER_NUMBER",
        "CAST(DESTINATION_TYPE AS STRING) as DESTINATION_TYPE",
        "CAST(CONTACT_COUNTY AS STRING) as CONTACT_COUNTY",
        "CAST(CONTACT_COUNTRY_CODE AS STRING) as CONTACT_COUNTRY_CODE",
        "CAST(RECEIPT_TYPE AS TINYINT) as RECEIPT_TYPE",
        "CAST(VARIANCE_TYPE AS SMALLINT) as VARIANCE_TYPE",
        "CAST(HAS_NOTES AS TINYINT) as HAS_NOTES",
        "CAST(ORIGINAL_ASN_ID AS BIGINT) as ORIGINAL_ASN_ID",
        "CAST(RETURN_REFERENCE_NUMBER AS STRING) as RETURN_REFERENCE_NUMBER",
        "CAST(REF_FIELD_4 AS STRING) as REF_FIELD_4",
        "CAST(REF_FIELD_5 AS STRING) as REF_FIELD_5",
        "CAST(REF_FIELD_6 AS STRING) as REF_FIELD_6",
        "CAST(REF_FIELD_7 AS STRING) as REF_FIELD_7",
        "CAST(REF_FIELD_8 AS STRING) as REF_FIELD_8",
        "CAST(REF_FIELD_9 AS STRING) as REF_FIELD_9",
        "CAST(REF_FIELD_10 AS STRING) as REF_FIELD_10",
        "CAST(REF_NUM1 AS DECIMAL(13,5)) as REF_NUM1",
        "CAST(REF_NUM2 AS DECIMAL(13,5)) as REF_NUM2",
        "CAST(REF_NUM3 AS DECIMAL(13,5)) as REF_NUM3",
        "CAST(REF_NUM4 AS DECIMAL(13,5)) as REF_NUM4",
        "CAST(REF_NUM5 AS DECIMAL(13,5)) as REF_NUM5",
        "CAST(INVOICE_DATE AS TIMESTAMP) as INVOICE_DATE",
        "CAST(INVOICE_NUMBER AS STRING) as INVOICE_NUMBER",
        "CAST(PRE_RECEIPT_STATUS AS STRING) as PRE_RECEIPT_STATUS",
        "CAST(PRE_ALLOCATION_FIT_PERCENTAGE AS SMALLINT) as PRE_ALLOCATION_FIT_PERCENTAGE",
        "CAST(TRAILER_CLOSED AS TINYINT) as TRAILER_CLOSED",
        "CAST(IS_GIFT AS TINYINT) as IS_GIFT",
        "CAST(ORIGINAL_ORDER_NUMBER AS STRING) as ORIGINAL_ORDER_NUMBER",
        "CAST(ASN_SHPMT_TYPE AS STRING) as ASN_SHPMT_TYPE",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_ASN_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE is written to the target table - "
        + target_table_name
    )