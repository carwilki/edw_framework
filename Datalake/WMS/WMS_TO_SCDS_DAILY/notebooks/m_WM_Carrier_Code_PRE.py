#Code converted on 2023-06-24 13:43:01
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



def m_WM_Carrier_Code_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Carrier_Code_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_CARRIER_CODE_PRE"

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
    # Processing node SQ_Shortcut_to_CARRIER_CODE, type SOURCE 
    # COLUMN COUNT: 101

    SQ_Shortcut_to_CARRIER_CODE = jdbcOracleConnection(
        f"""SELECT
                CARRIER_CODE.CARRIER_ID,
                CARRIER_CODE.TC_COMPANY_ID,
                CARRIER_CODE.CARRIER_CODE,
                CARRIER_CODE.TP_COMPANY_ID,
                CARRIER_CODE.TP_COMPANY_NAME,
                CARRIER_CODE.CARRIER_CODE_NAME,
                CARRIER_CODE.ADDRESS,
                CARRIER_CODE.CITY,
                CARRIER_CODE.STATE_PROV,
                CARRIER_CODE.POSTAL_CODE,
                CARRIER_CODE.COUNTRY_CODE,
                CARRIER_CODE.CARRIER_CODE_STATUS,
                CARRIER_CODE.MARK_FOR_DELETION,
                CARRIER_CODE.COMM_METHOD,
                CARRIER_CODE.IS_UNION,
                CARRIER_CODE.IS_AUTO_DELIVER,
                CARRIER_CODE.AUTO_ACCEPT_TYPE,
                CARRIER_CODE.ACCEPT_MSG_REQD,
                CARRIER_CODE.APPT_MSG_REQD,
                CARRIER_CODE.DEPART_MSG_REQD,
                CARRIER_CODE.ARRIVE_MSG_REQD,
                CARRIER_CODE.CONFIRM_RECALL_REQD,
                CARRIER_CODE.CONFIRM_UPDATE_REQD,
                CARRIER_CODE.CREATED_SOURCE_TYPE,
                CARRIER_CODE.CREATED_SOURCE,
                CARRIER_CODE.CREATED_DTTM,
                CARRIER_CODE.LAST_UPDATED_SOURCE_TYPE,
                CARRIER_CODE.LAST_UPDATED_SOURCE,
                CARRIER_CODE.LAST_UPDATED_DTTM,
                CARRIER_CODE.IS_BROKER,
                CARRIER_CODE.BROKER_RECEIVE_UPDATES,
                CARRIER_CODE.IS_PREPAY,
                CARRIER_CODE.ALLOW_BOL_ENTRY,
                CARRIER_CODE.AUTO_DELIVER_DELAY_HOURS,
                CARRIER_CODE.DOCK_SCHEDULE_PERMISSION,
                CARRIER_CODE.ALLOW_LOAD_COMPLETION,
                CARRIER_CODE.RECEIVES_DOCK_INFO,
                CARRIER_CODE.ALLOW_TRAILER_ENTRY,
                CARRIER_CODE.ALLOW_TRACTOR_ENTRY,
                CARRIER_CODE.PAYMENT_REFERENCE_NUMBER,
                CARRIER_CODE.IS_PAYEE,
                CARRIER_CODE.ALLOW_CARRIER_CHARGES_VIEW,
                CARRIER_CODE.SUPPORTS_PARCEL,
                CARRIER_CODE.TARIFF_ID,
                CARRIER_CODE.IS_AUTO_CREATE_INVOICE,
                CARRIER_CODE.DOES_RECEIVE_FACILITY_INFO,
                CARRIER_CODE.IS_PRIVATE_FLEET,
                CARRIER_CODE.INVOICE_PAYMENT_TERMS,
                CARRIER_CODE.MATCH_PAY_EMAIL,
                CARRIER_CODE.DAYS_TO_DISPUTE_CLAIM,
                CARRIER_CODE.PAYMENT_METHOD,
                CARRIER_CODE.IS_PREFERRED,
                CARRIER_CODE.CUT_PARCEL_INVOICE,
                CARRIER_CODE.USE_CARRIER_ADDR,
                CARRIER_CODE.CARRIER_ADDR_FOR_CL,
                CARRIER_CODE.CLAIM_ON_DETENTION_APPROVAL,
                CARRIER_CODE.IS_RES_BASED_COSTING_ALLOWED,
                CARRIER_CODE.IS_ACTIVITY_BASED_COSTING,
                CARRIER_CODE.PCT_OVER_CAPACITY,
                CARRIER_CODE.ALLOW_CARRIER_BOOKING_REF,
                CARRIER_CODE.ALLOW_FORWARDER_AWB,
                CARRIER_CODE.ALLOW_MASTER_AWB,
                CARRIER_CODE.VALIDATE_INVOICE,
                CARRIER_CODE.MAX_FLEET_APPROVED_AMOUNT,
                CARRIER_CODE.HAS_HAZMAT,
                CARRIER_CODE.CARRIER_TYPE_ID,
                CARRIER_CODE.CHECK_NON_MACHINABLE,
                CARRIER_CODE.PRO_NUMBER_LEVEL,
                CARRIER_CODE.DEFAULT_PALLET_TYPE,
                CARRIER_CODE.LANG_ID,
                CARRIER_CODE.AWB_UOM,
                CARRIER_CODE.TRANS_EXCEL,
                CARRIER_CODE.INV_ON_CARR_SIZES,
                CARRIER_CODE.PRO_NUMBER_REQD,
                CARRIER_CODE.ALLOW_COUNTER_OFFER,
                CARRIER_CODE.DEPART_MSG_REQD_AT_FIRST_STOP,
                CARRIER_CODE.DOT_NUMBER,
                CARRIER_CODE.IS_BENCHMARK,
                CARRIER_CODE.EPI_CARRIER_CODE,
                CARRIER_CODE.SCAC_CODE,
                CARRIER_CODE.EPI_SUPPORTS_HOLD_PROCESS,
                CARRIER_CODE.MOTOR_CARRIER_NBR,
                CARRIER_CODE.FREIGHT_FRWRDR_NBR,
                CARRIER_CODE.TAX_ID,
                CARRIER_CODE.HAZMAT_REG_NBR,
                CARRIER_CODE.EXPIRY_DATE,
                CARRIER_CODE.EPI_SUPPORTS_EOD,
                CARRIER_CODE.LOAD_PRO_NEXT_UP_COUNTER,
                CARRIER_CODE.STOP_PRO_NEXT_UP_COUNTER,
                CARRIER_CODE.ALLOW_TENDER_REJECT,
                CARRIER_CODE.REF_FIELD1,
                CARRIER_CODE.REF_FIELD2,
                CARRIER_CODE.REF_FIELD3,
                CARRIER_CODE.REF_FIELD4,
                CARRIER_CODE.REF_FIELD5,
                CARRIER_CODE.REF_NUM1,
                CARRIER_CODE.REF_NUM2,
                CARRIER_CODE.REF_NUM3,
                CARRIER_CODE.REF_NUM4,
                CARRIER_CODE.REF_NUM5,
                CARRIER_CODE.MOBILE_SHIP_TRACK
            FROM {source_schema}.CARRIER_CODE
            WHERE (TRUNC( CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC( LAST_UPDATED_DTTM) >=  TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 103

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_CARRIER_CODE_temp = SQ_Shortcut_to_CARRIER_CODE.toDF(*["SQ_Shortcut_to_CARRIER_CODE___" + col for col in SQ_Shortcut_to_CARRIER_CODE.columns])

    EXPTRANS = SQ_Shortcut_to_CARRIER_CODE_temp.selectExpr( 
        "SQ_Shortcut_to_CARRIER_CODE___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_exp", 
        "SQ_Shortcut_to_CARRIER_CODE___CARRIER_ID as CARRIER_ID", 
        "SQ_Shortcut_to_CARRIER_CODE___TC_COMPANY_ID as TC_COMPANY_ID", 
        "SQ_Shortcut_to_CARRIER_CODE___CARRIER_CODE as CARRIER_CODE", 
        "SQ_Shortcut_to_CARRIER_CODE___TP_COMPANY_ID as TP_COMPANY_ID", 
        "SQ_Shortcut_to_CARRIER_CODE___TP_COMPANY_NAME as TP_COMPANY_NAME", 
        "SQ_Shortcut_to_CARRIER_CODE___CARRIER_CODE_NAME as CARRIER_CODE_NAME", 
        "SQ_Shortcut_to_CARRIER_CODE___ADDRESS as ADDRESS", 
        "SQ_Shortcut_to_CARRIER_CODE___CITY as CITY", 
        "SQ_Shortcut_to_CARRIER_CODE___STATE_PROV as STATE_PROV", 
        "SQ_Shortcut_to_CARRIER_CODE___POSTAL_CODE as POSTAL_CODE", 
        "SQ_Shortcut_to_CARRIER_CODE___COUNTRY_CODE as COUNTRY_CODE", 
        "SQ_Shortcut_to_CARRIER_CODE___CARRIER_CODE_STATUS as CARRIER_CODE_STATUS", 
        "SQ_Shortcut_to_CARRIER_CODE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
        "SQ_Shortcut_to_CARRIER_CODE___COMM_METHOD as COMM_METHOD", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_UNION as IS_UNION", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_AUTO_DELIVER as IS_AUTO_DELIVER", 
        "SQ_Shortcut_to_CARRIER_CODE___AUTO_ACCEPT_TYPE as AUTO_ACCEPT_TYPE", 
        "SQ_Shortcut_to_CARRIER_CODE___ACCEPT_MSG_REQD as ACCEPT_MSG_REQD", 
        "SQ_Shortcut_to_CARRIER_CODE___APPT_MSG_REQD as APPT_MSG_REQD", 
        "SQ_Shortcut_to_CARRIER_CODE___DEPART_MSG_REQD as DEPART_MSG_REQD", 
        "SQ_Shortcut_to_CARRIER_CODE___ARRIVE_MSG_REQD as ARRIVE_MSG_REQD", 
        "SQ_Shortcut_to_CARRIER_CODE___CONFIRM_RECALL_REQD as CONFIRM_RECALL_REQD", 
        "SQ_Shortcut_to_CARRIER_CODE___CONFIRM_UPDATE_REQD as CONFIRM_UPDATE_REQD", 
        "SQ_Shortcut_to_CARRIER_CODE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_CARRIER_CODE___CREATED_SOURCE as CREATED_SOURCE", 
        "SQ_Shortcut_to_CARRIER_CODE___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_CARRIER_CODE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_CARRIER_CODE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
        "SQ_Shortcut_to_CARRIER_CODE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_BROKER as IS_BROKER", 
        "SQ_Shortcut_to_CARRIER_CODE___BROKER_RECEIVE_UPDATES as BROKER_RECEIVE_UPDATES", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_PREPAY as IS_PREPAY", 
        "SQ_Shortcut_to_CARRIER_CODE___ALLOW_BOL_ENTRY as ALLOW_BOL_ENTRY", 
        "SQ_Shortcut_to_CARRIER_CODE___AUTO_DELIVER_DELAY_HOURS as AUTO_DELIVER_DELAY_HOURS", 
        "SQ_Shortcut_to_CARRIER_CODE___DOCK_SCHEDULE_PERMISSION as DOCK_SCHEDULE_PERMISSION", 
        "SQ_Shortcut_to_CARRIER_CODE___ALLOW_LOAD_COMPLETION as ALLOW_LOAD_COMPLETION", 
        "SQ_Shortcut_to_CARRIER_CODE___RECEIVES_DOCK_INFO as RECEIVES_DOCK_INFO", 
        "SQ_Shortcut_to_CARRIER_CODE___ALLOW_TRAILER_ENTRY as ALLOW_TRAILER_ENTRY", 
        "SQ_Shortcut_to_CARRIER_CODE___ALLOW_TRACTOR_ENTRY as ALLOW_TRACTOR_ENTRY", 
        "SQ_Shortcut_to_CARRIER_CODE___PAYMENT_REFERENCE_NUMBER as PAYMENT_REFERENCE_NUMBER", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_PAYEE as IS_PAYEE", 
        "SQ_Shortcut_to_CARRIER_CODE___ALLOW_CARRIER_CHARGES_VIEW as ALLOW_CARRIER_CHARGES_VIEW", 
        "SQ_Shortcut_to_CARRIER_CODE___SUPPORTS_PARCEL as SUPPORTS_PARCEL", 
        "SQ_Shortcut_to_CARRIER_CODE___TARIFF_ID as TARIFF_ID", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_AUTO_CREATE_INVOICE as IS_AUTO_CREATE_INVOICE", 
        "SQ_Shortcut_to_CARRIER_CODE___DOES_RECEIVE_FACILITY_INFO as DOES_RECEIVE_FACILITY_INFO", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_PRIVATE_FLEET as IS_PRIVATE_FLEET", 
        "SQ_Shortcut_to_CARRIER_CODE___INVOICE_PAYMENT_TERMS as INVOICE_PAYMENT_TERMS", 
        "SQ_Shortcut_to_CARRIER_CODE___MATCH_PAY_EMAIL as MATCH_PAY_EMAIL", 
        "SQ_Shortcut_to_CARRIER_CODE___DAYS_TO_DISPUTE_CLAIM as DAYS_TO_DISPUTE_CLAIM", 
        "SQ_Shortcut_to_CARRIER_CODE___PAYMENT_METHOD as PAYMENT_METHOD", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_PREFERRED as IS_PREFERRED", 
        "SQ_Shortcut_to_CARRIER_CODE___CUT_PARCEL_INVOICE as CUT_PARCEL_INVOICE", 
        "SQ_Shortcut_to_CARRIER_CODE___USE_CARRIER_ADDR as USE_CARRIER_ADDR", 
        "SQ_Shortcut_to_CARRIER_CODE___CARRIER_ADDR_FOR_CL as CARRIER_ADDR_FOR_CL", 
        "SQ_Shortcut_to_CARRIER_CODE___CLAIM_ON_DETENTION_APPROVAL as CLAIM_ON_DETENTION_APPROVAL", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_RES_BASED_COSTING_ALLOWED as IS_RES_BASED_COSTING_ALLOWED", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_ACTIVITY_BASED_COSTING as IS_ACTIVITY_BASED_COSTING", 
        "SQ_Shortcut_to_CARRIER_CODE___PCT_OVER_CAPACITY as PCT_OVER_CAPACITY", 
        "SQ_Shortcut_to_CARRIER_CODE___ALLOW_CARRIER_BOOKING_REF as ALLOW_CARRIER_BOOKING_REF", 
        "SQ_Shortcut_to_CARRIER_CODE___ALLOW_FORWARDER_AWB as ALLOW_FORWARDER_AWB", 
        "SQ_Shortcut_to_CARRIER_CODE___ALLOW_MASTER_AWB as ALLOW_MASTER_AWB", 
        "SQ_Shortcut_to_CARRIER_CODE___VALIDATE_INVOICE as VALIDATE_INVOICE", 
        "SQ_Shortcut_to_CARRIER_CODE___MAX_FLEET_APPROVED_AMOUNT as MAX_FLEET_APPROVED_AMOUNT", 
        "SQ_Shortcut_to_CARRIER_CODE___HAS_HAZMAT as HAS_HAZMAT", 
        "SQ_Shortcut_to_CARRIER_CODE___CARRIER_TYPE_ID as CARRIER_TYPE_ID", 
        "SQ_Shortcut_to_CARRIER_CODE___CHECK_NON_MACHINABLE as CHECK_NON_MACHINABLE", 
        "SQ_Shortcut_to_CARRIER_CODE___PRO_NUMBER_LEVEL as PRO_NUMBER_LEVEL", 
        "SQ_Shortcut_to_CARRIER_CODE___DEFAULT_PALLET_TYPE as DEFAULT_PALLET_TYPE", 
        "SQ_Shortcut_to_CARRIER_CODE___LANG_ID as LANG_ID", 
        "SQ_Shortcut_to_CARRIER_CODE___AWB_UOM as AWB_UOM", 
        "SQ_Shortcut_to_CARRIER_CODE___TRANS_EXCEL as TRANS_EXCEL", 
        "SQ_Shortcut_to_CARRIER_CODE___INV_ON_CARR_SIZES as INV_ON_CARR_SIZES", 
        "SQ_Shortcut_to_CARRIER_CODE___PRO_NUMBER_REQD as PRO_NUMBER_REQD", 
        "SQ_Shortcut_to_CARRIER_CODE___ALLOW_COUNTER_OFFER as ALLOW_COUNTER_OFFER", 
        "SQ_Shortcut_to_CARRIER_CODE___DEPART_MSG_REQD_AT_FIRST_STOP as DEPART_MSG_REQD_AT_FIRST_STOP", 
        "SQ_Shortcut_to_CARRIER_CODE___DOT_NUMBER as DOT_NUMBER", 
        "SQ_Shortcut_to_CARRIER_CODE___IS_BENCHMARK as IS_BENCHMARK", 
        "SQ_Shortcut_to_CARRIER_CODE___EPI_CARRIER_CODE as EPI_CARRIER_CODE", 
        "SQ_Shortcut_to_CARRIER_CODE___SCAC_CODE as SCAC_CODE", 
        "SQ_Shortcut_to_CARRIER_CODE___EPI_SUPPORTS_HOLD_PROCESS as EPI_SUPPORTS_HOLD_PROCESS", 
        "SQ_Shortcut_to_CARRIER_CODE___MOTOR_CARRIER_NBR as MOTOR_CARRIER_NBR", 
        "SQ_Shortcut_to_CARRIER_CODE___FREIGHT_FRWRDR_NBR as FREIGHT_FRWRDR_NBR", 
        "SQ_Shortcut_to_CARRIER_CODE___TAX_ID as TAX_ID", 
        "SQ_Shortcut_to_CARRIER_CODE___HAZMAT_REG_NBR as HAZMAT_REG_NBR", 
        "SQ_Shortcut_to_CARRIER_CODE___EXPIRY_DATE as EXPIRY_DATE", 
        "SQ_Shortcut_to_CARRIER_CODE___EPI_SUPPORTS_EOD as EPI_SUPPORTS_EOD", 
        "SQ_Shortcut_to_CARRIER_CODE___LOAD_PRO_NEXT_UP_COUNTER as LOAD_PRO_NEXT_UP_COUNTER", 
        "SQ_Shortcut_to_CARRIER_CODE___STOP_PRO_NEXT_UP_COUNTER as STOP_PRO_NEXT_UP_COUNTER", 
        "SQ_Shortcut_to_CARRIER_CODE___ALLOW_TENDER_REJECT as ALLOW_TENDER_REJECT", 
        "SQ_Shortcut_to_CARRIER_CODE___REF_FIELD1 as REF_FIELD1", 
        "SQ_Shortcut_to_CARRIER_CODE___REF_FIELD2 as REF_FIELD2", 
        "SQ_Shortcut_to_CARRIER_CODE___REF_FIELD3 as REF_FIELD3", 
        "SQ_Shortcut_to_CARRIER_CODE___REF_FIELD4 as REF_FIELD4", 
        "SQ_Shortcut_to_CARRIER_CODE___REF_FIELD5 as REF_FIELD5", 
        "SQ_Shortcut_to_CARRIER_CODE___REF_NUM1 as REF_NUM1", 
        "SQ_Shortcut_to_CARRIER_CODE___REF_NUM2 as REF_NUM2", 
        "SQ_Shortcut_to_CARRIER_CODE___REF_NUM3 as REF_NUM3", 
        "SQ_Shortcut_to_CARRIER_CODE___REF_NUM4 as REF_NUM4", 
        "SQ_Shortcut_to_CARRIER_CODE___REF_NUM5 as REF_NUM5", 
        "SQ_Shortcut_to_CARRIER_CODE___MOBILE_SHIP_TRACK as MOBILE_SHIP_TRACK", 
        "CURRENT_TIMESTAMP() as LOADTSTMP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_CARRIER_CODE_PRE, type TARGET 
    # COLUMN COUNT: 103


    Shortcut_to_WM_CARRIER_CODE_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_exp AS SMALLINT) as DC_NBR",
        "CAST(CARRIER_ID AS BIGINT) as CARRIER_ID",
        "CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID",
        "CAST(CARRIER_CODE AS STRING) as CARRIER_CODE",
        "CAST(TP_COMPANY_ID AS INT) as TP_COMPANY_ID",
        "CAST(TP_COMPANY_NAME AS STRING) as TP_COMPANY_NAME",
        "CAST(CARRIER_CODE_NAME AS STRING) as CARRIER_CODE_NAME",
        "CAST(ADDRESS AS STRING) as ADDRESS",
        "CAST(CITY AS STRING) as CITY",
        "CAST(STATE_PROV AS STRING) as STATE_PROV",
        "CAST(POSTAL_CODE AS STRING) as POSTAL_CODE",
        "CAST(COUNTRY_CODE AS STRING) as COUNTRY_CODE",
        "CAST(CARRIER_CODE_STATUS AS TINYINT) as CARRIER_CODE_STATUS",
        "CAST(MARK_FOR_DELETION AS TINYINT) as MARK_FOR_DELETION",
        "CAST(COMM_METHOD AS STRING) as COMM_METHOD",
        "CAST(IS_UNION AS TINYINT) as IS_UNION",
        "CAST(IS_AUTO_DELIVER AS TINYINT) as IS_AUTO_DELIVER",
        "CAST(AUTO_ACCEPT_TYPE AS TINYINT) as AUTO_ACCEPT_TYPE",
        "CAST(ACCEPT_MSG_REQD AS TINYINT) as ACCEPT_MSG_REQD",
        "CAST(APPT_MSG_REQD AS TINYINT) as APPT_MSG_REQD",
        "CAST(DEPART_MSG_REQD AS TINYINT) as DEPART_MSG_REQD",
        "CAST(ARRIVE_MSG_REQD AS TINYINT) as ARRIVE_MSG_REQD",
        "CAST(CONFIRM_RECALL_REQD AS TINYINT) as CONFIRM_RECALL_REQD",
        "CAST(CONFIRM_UPDATE_REQD AS TINYINT) as CONFIRM_UPDATE_REQD",
        "CAST(CREATED_SOURCE_TYPE AS TINYINT) as CREATED_SOURCE_TYPE",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_SOURCE_TYPE AS TINYINT) as LAST_UPDATED_SOURCE_TYPE",
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(IS_BROKER AS TINYINT) as IS_BROKER",
        "CAST(BROKER_RECEIVE_UPDATES AS TINYINT) as BROKER_RECEIVE_UPDATES",
        "CAST(IS_PREPAY AS TINYINT) as IS_PREPAY",
        "CAST(ALLOW_BOL_ENTRY AS TINYINT) as ALLOW_BOL_ENTRY",
        "CAST(AUTO_DELIVER_DELAY_HOURS AS INT) as AUTO_DELIVER_DELAY_HOURS",
        "CAST(DOCK_SCHEDULE_PERMISSION AS TINYINT) as DOCK_SCHEDULE_PERMISSION",
        "CAST(ALLOW_LOAD_COMPLETION AS TINYINT) as ALLOW_LOAD_COMPLETION",
        "CAST(RECEIVES_DOCK_INFO AS TINYINT) as RECEIVES_DOCK_INFO",
        "CAST(ALLOW_TRAILER_ENTRY AS TINYINT) as ALLOW_TRAILER_ENTRY",
        "CAST(ALLOW_TRACTOR_ENTRY AS TINYINT) as ALLOW_TRACTOR_ENTRY",
        "CAST(PAYMENT_REFERENCE_NUMBER AS STRING) as PAYMENT_REFERENCE_NUMBER",
        "CAST(IS_PAYEE AS TINYINT) as IS_PAYEE",
        "CAST(ALLOW_CARRIER_CHARGES_VIEW AS TINYINT) as ALLOW_CARRIER_CHARGES_VIEW",
        "CAST(SUPPORTS_PARCEL AS TINYINT) as SUPPORTS_PARCEL",
        "CAST(TARIFF_ID AS STRING) as TARIFF_ID",
        "CAST(IS_AUTO_CREATE_INVOICE AS TINYINT) as IS_AUTO_CREATE_INVOICE",
        "CAST(DOES_RECEIVE_FACILITY_INFO AS TINYINT) as DOES_RECEIVE_FACILITY_INFO",
        "CAST(IS_PRIVATE_FLEET AS TINYINT) as IS_PRIVATE_FLEET",
        "CAST(INVOICE_PAYMENT_TERMS AS STRING) as INVOICE_PAYMENT_TERMS",
        "CAST(MATCH_PAY_EMAIL AS STRING) as MATCH_PAY_EMAIL",
        "CAST(DAYS_TO_DISPUTE_CLAIM AS INT) as DAYS_TO_DISPUTE_CLAIM",
        "CAST(PAYMENT_METHOD AS STRING) as PAYMENT_METHOD",
        "CAST(IS_PREFERRED AS TINYINT) as IS_PREFERRED",
        "CAST(CUT_PARCEL_INVOICE AS TINYINT) as CUT_PARCEL_INVOICE",
        "CAST(USE_CARRIER_ADDR AS TINYINT) as USE_CARRIER_ADDR",
        "CAST(CARRIER_ADDR_FOR_CL AS STRING) as CARRIER_ADDR_FOR_CL",
        "CAST(CLAIM_ON_DETENTION_APPROVAL AS TINYINT) as CLAIM_ON_DETENTION_APPROVAL",
        "CAST(IS_RES_BASED_COSTING_ALLOWED AS TINYINT) as IS_RES_BASED_COSTING_ALLOWED",
        "CAST(IS_ACTIVITY_BASED_COSTING AS TINYINT) as IS_ACTIVITY_BASED_COSTING",
        "CAST(PCT_OVER_CAPACITY AS DECIMAL(13,4)) as PCT_OVER_CAPACITY",
        "CAST(ALLOW_CARRIER_BOOKING_REF AS TINYINT) as ALLOW_CARRIER_BOOKING_REF",
        "CAST(ALLOW_FORWARDER_AWB AS TINYINT) as ALLOW_FORWARDER_AWB",
        "CAST(ALLOW_MASTER_AWB AS TINYINT) as ALLOW_MASTER_AWB",
        "CAST(VALIDATE_INVOICE AS INT) as VALIDATE_INVOICE",
        "CAST(MAX_FLEET_APPROVED_AMOUNT AS DECIMAL(13,2)) as MAX_FLEET_APPROVED_AMOUNT",
        "CAST(HAS_HAZMAT AS TINYINT) as HAS_HAZMAT",
        "CAST(CARRIER_TYPE_ID AS SMALLINT) as CARRIER_TYPE_ID",
        "CAST(CHECK_NON_MACHINABLE AS TINYINT) as CHECK_NON_MACHINABLE",
        "CAST(PRO_NUMBER_LEVEL AS STRING) as PRO_NUMBER_LEVEL",
        "CAST(DEFAULT_PALLET_TYPE AS STRING) as DEFAULT_PALLET_TYPE",
        "CAST(LANG_ID AS STRING) as LANG_ID",
        "CAST(AWB_UOM AS STRING) as AWB_UOM",
        "CAST(TRANS_EXCEL AS SMALLINT) as TRANS_EXCEL",
        "CAST(INV_ON_CARR_SIZES AS TINYINT) as INV_ON_CARR_SIZES",
        "CAST(PRO_NUMBER_REQD AS TINYINT) as PRO_NUMBER_REQD",
        "CAST(ALLOW_COUNTER_OFFER AS TINYINT) as ALLOW_COUNTER_OFFER",
        "CAST(DEPART_MSG_REQD_AT_FIRST_STOP AS TINYINT) as DEPART_MSG_REQD_AT_FIRST_STOP",
        "CAST(DOT_NUMBER AS STRING) as DOT_NUMBER",
        "CAST(IS_BENCHMARK AS TINYINT) as IS_BENCHMARK",
        "CAST(EPI_CARRIER_CODE AS STRING) as EPI_CARRIER_CODE",
        "CAST(SCAC_CODE AS STRING) as SCAC_CODE",
        "CAST(EPI_SUPPORTS_HOLD_PROCESS AS TINYINT) as EPI_SUPPORTS_HOLD_PROCESS",
        "CAST(MOTOR_CARRIER_NBR AS INT) as MOTOR_CARRIER_NBR",
        "CAST(FREIGHT_FRWRDR_NBR AS INT) as FREIGHT_FRWRDR_NBR",
        "CAST(TAX_ID AS STRING) as TAX_ID",
        "CAST(HAZMAT_REG_NBR AS STRING) as HAZMAT_REG_NBR",
        "CAST(EXPIRY_DATE AS TIMESTAMP) as EXPIRY_DATE",
        "CAST(EPI_SUPPORTS_EOD AS TINYINT) as EPI_SUPPORTS_EOD",
        "CAST(LOAD_PRO_NEXT_UP_COUNTER AS STRING) as LOAD_PRO_NEXT_UP_COUNTER",
        "CAST(STOP_PRO_NEXT_UP_COUNTER AS STRING) as STOP_PRO_NEXT_UP_COUNTER",
        "CAST(ALLOW_TENDER_REJECT AS TINYINT) as ALLOW_TENDER_REJECT",
        "CAST(REF_FIELD1 AS STRING) as REF_FIELD1",
        "CAST(REF_FIELD2 AS STRING) as REF_FIELD2",
        "CAST(REF_FIELD3 AS STRING) as REF_FIELD3",
        "CAST(REF_FIELD4 AS STRING) as REF_FIELD4",
        "CAST(REF_FIELD5 AS STRING) as REF_FIELD5",
        "CAST(REF_NUM1 AS DECIMAL(13,5)) as REF_NUM1",
        "CAST(REF_NUM2 AS DECIMAL(13,5)) as REF_NUM2",
        "CAST(REF_NUM3 AS DECIMAL(13,5)) as REF_NUM3",
        "CAST(REF_NUM4 AS DECIMAL(13,5)) as REF_NUM4",
        "CAST(REF_NUM5 AS DECIMAL(13,5)) as REF_NUM5",
        "CAST(MOBILE_SHIP_TRACK AS TINYINT) as MOBILE_SHIP_TRACK",
        "CAST(LOADTSTMP AS TIMESTAMP) as LOAD_TSTMP"
    )
    overwriteDeltaPartition(Shortcut_to_WM_CARRIER_CODE_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_CARRIER_CODE_PRE is written to the target table - "
        + target_table_name
    )    