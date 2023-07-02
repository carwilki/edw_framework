#Code converted on 2023-06-24 13:44:23
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from utils.genericUtilities import *
from utils.configs import *
from utils.mergeUtils import *
from logging import getLogger, INFO



def m_WM_Asn_Detail_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Asn_Detail_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ASN_DETAIL_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "ASN_DETAIL"


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
    # Processing node SQ_Shortcut_to_ASN_DETAIL, type SOURCE 
    # COLUMN COUNT: 99

    SQ_Shortcut_to_ASN_DETAIL = jdbcOracleConnection(
        f"""SELECT
                ASN_DETAIL.ASN_DETAIL_ID,
                ASN_DETAIL.ASN_ID,
                ASN_DETAIL.TC_PURCHASE_ORDERS_ID,
                ASN_DETAIL.PURCHASE_ORDERS_ID,
                ASN_DETAIL.SKU_ID,
                ASN_DETAIL.SKU_NAME,
                ASN_DETAIL.SKU_ATTR_1,
                ASN_DETAIL.SKU_ATTR_2,
                ASN_DETAIL.SKU_ATTR_3,
                ASN_DETAIL.SKU_ATTR_4,
                ASN_DETAIL.SKU_ATTR_5,
                ASN_DETAIL.BUSINESS_PARTNER_ID,
                ASN_DETAIL.PACKAGE_TYPE_ID,
                ASN_DETAIL.PACKAGE_TYPE_DESC,
                ASN_DETAIL.PACKAGE_TYPE_INSTANCE,
                ASN_DETAIL.EPC_TRACKING_RFID_VALUE,
                ASN_DETAIL.ORDER_TYPE_DESC,
                ASN_DETAIL.GTIN,
                ASN_DETAIL.SHIPPED_QTY,
                ASN_DETAIL.STD_PACK_QTY,
                ASN_DETAIL.STD_CASE_QTY,
                ASN_DETAIL.ASN_DETAIL_STATUS,
                ASN_DETAIL.RECEIVED_QTY,
                ASN_DETAIL.STD_SUB_PACK_QTY,
                ASN_DETAIL.LPN_PER_TIER,
                ASN_DETAIL.TIER_PER_PALLET,
                ASN_DETAIL.MFG_PLNT,
                ASN_DETAIL.MFG_DATE,
                ASN_DETAIL.SHIP_BY_DATE,
                ASN_DETAIL.EXPIRE_DATE,
                ASN_DETAIL.INCUBATION_DATE,
                ASN_DETAIL.EPC_REQ_ON_ALL_CASES,
                ASN_DETAIL.WEIGHT_UOM_ID_BASE,
                ASN_DETAIL.REGION_ID,
                ASN_DETAIL.IS_ASSOCIATED_TO_OUTBOUND,
                ASN_DETAIL.IS_CANCELLED,
                ASN_DETAIL.IS_CLOSED,
                ASN_DETAIL.INVN_TYPE,
                ASN_DETAIL.PROD_STAT,
                ASN_DETAIL.BATCH_NBR,
                ASN_DETAIL.CNTRY_OF_ORGN,
                ASN_DETAIL.SHIPPED_LPN_COUNT,
                ASN_DETAIL.RECEIVED_LPN_COUNT,
                ASN_DETAIL.UNITS_ASSIGNED_TO_LPN,
                ASN_DETAIL.PROC_IMMD_NEEDS,
                ASN_DETAIL.QUALITY_CHECK_HOLD_UPON_RCPT,
                ASN_DETAIL.REFERENCE_ORDER_NBR,
                ASN_DETAIL.ACTUAL_WEIGHT,
                ASN_DETAIL.ACTUAL_WEIGHT_PACK_COUNT,
                ASN_DETAIL.NBR_OF_PACK_FOR_CATCH_WT,
                ASN_DETAIL.PUTWY_TYPE,
                ASN_DETAIL.RETAIL_PRICE,
                ASN_DETAIL.PRICE_TIX_AVAIL,
                ASN_DETAIL.CREATED_SOURCE_TYPE,
                ASN_DETAIL.CREATED_SOURCE,
                ASN_DETAIL.CREATED_DTTM,
                ASN_DETAIL.LAST_UPDATED_SOURCE_TYPE,
                ASN_DETAIL.LAST_UPDATED_SOURCE,
                ASN_DETAIL.LAST_UPDATED_DTTM,
                ASN_DETAIL.HIBERNATE_VERSION,
                ASN_DETAIL.TC_COMPANY_ID,
                ASN_DETAIL.TC_PO_LINE_ID,
                ASN_DETAIL.INVENTORY_SEGMENT_ID,
                ASN_DETAIL.PPACK_GRP_CODE,
                ASN_DETAIL.CUT_NBR,
                ASN_DETAIL.QTY_CONV_FACTOR,
                ASN_DETAIL.QTY_UOM_ID,
                ASN_DETAIL.WEIGHT_UOM_ID,
                ASN_DETAIL.QTY_UOM_ID_BASE,
                ASN_DETAIL.TC_ORDER_ID,
                ASN_DETAIL.ORDER_ID,
                ASN_DETAIL.TC_ORDER_LINE_ID,
                ASN_DETAIL.ORDER_LINE_ITEM_ID,
                ASN_DETAIL.SEQ_NBR,
                ASN_DETAIL.EXP_RECEIVE_CONDITION_CODE,
                ASN_DETAIL.ASN_RECV_RULES,
                ASN_DETAIL.CHECKSUM,
                ASN_DETAIL.ACTUAL_WEIGHT_RECEIVED,
                ASN_DETAIL.REF_FIELD_1,
                ASN_DETAIL.REF_FIELD_2,
                ASN_DETAIL.REF_FIELD_3,
                ASN_DETAIL.REF_FIELD_4,
                ASN_DETAIL.REF_FIELD_5,
                ASN_DETAIL.REF_FIELD_6,
                ASN_DETAIL.REF_FIELD_7,
                ASN_DETAIL.REF_FIELD_8,
                ASN_DETAIL.REF_FIELD_9,
                ASN_DETAIL.REF_FIELD_10,
                ASN_DETAIL.REF_NUM1,
                ASN_DETAIL.REF_NUM2,
                ASN_DETAIL.REF_NUM3,
                ASN_DETAIL.REF_NUM4,
                ASN_DETAIL.REF_NUM5,
                ASN_DETAIL.DISPOSITION_TYPE,
                ASN_DETAIL.PRE_RECEIPT_STATUS,
                ASN_DETAIL.INV_DISPOSITION,
                ASN_DETAIL.EXT_PLAN_ID,
                ASN_DETAIL.PURCHASE_ORDERS_LINE_ITEM_ID,
                ASN_DETAIL.PROCESSED_FOR_TRLR_MOVES
            FROM ASN_DETAIL
            WHERE (TRUNC( CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (TRUNC( LAST_UPDATED_DTTM) >=  TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 101

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ASN_DETAIL_temp = SQ_Shortcut_to_ASN_DETAIL.toDF(*["SQ_Shortcut_to_ASN_DETAIL___" + col for col in SQ_Shortcut_to_ASN_DETAIL.columns])

    EXPTRANS = SQ_Shortcut_to_ASN_DETAIL_temp.selectExpr( 
        "SQ_Shortcut_to_ASN_DETAIL___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_ASN_DETAIL___ASN_DETAIL_ID as ASN_DETAIL_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___ASN_ID as ASN_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___TC_PURCHASE_ORDERS_ID as TC_PURCHASE_ORDERS_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___PURCHASE_ORDERS_ID as PURCHASE_ORDERS_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___SKU_ID as SKU_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___SKU_NAME as SKU_NAME", 
        "SQ_Shortcut_to_ASN_DETAIL___SKU_ATTR_1 as SKU_ATTR_1", 
        "SQ_Shortcut_to_ASN_DETAIL___SKU_ATTR_2 as SKU_ATTR_2", 
        "SQ_Shortcut_to_ASN_DETAIL___SKU_ATTR_3 as SKU_ATTR_3", 
        "SQ_Shortcut_to_ASN_DETAIL___SKU_ATTR_4 as SKU_ATTR_4", 
        "SQ_Shortcut_to_ASN_DETAIL___SKU_ATTR_5 as SKU_ATTR_5", 
        "SQ_Shortcut_to_ASN_DETAIL___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___PACKAGE_TYPE_ID as PACKAGE_TYPE_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___PACKAGE_TYPE_DESC as PACKAGE_TYPE_DESC", 
        "SQ_Shortcut_to_ASN_DETAIL___PACKAGE_TYPE_INSTANCE as PACKAGE_TYPE_INSTANCE", 
        "SQ_Shortcut_to_ASN_DETAIL___EPC_TRACKING_RFID_VALUE as EPC_TRACKING_RFID_VALUE", 
        "SQ_Shortcut_to_ASN_DETAIL___ORDER_TYPE_DESC as ORDER_TYPE_DESC", 
        "SQ_Shortcut_to_ASN_DETAIL___GTIN as GTIN", 
        "SQ_Shortcut_to_ASN_DETAIL___SHIPPED_QTY as SHIPPED_QTY", 
        "SQ_Shortcut_to_ASN_DETAIL___STD_PACK_QTY as STD_PACK_QTY", 
        "SQ_Shortcut_to_ASN_DETAIL___STD_CASE_QTY as STD_CASE_QTY", 
        "SQ_Shortcut_to_ASN_DETAIL___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", 
        "SQ_Shortcut_to_ASN_DETAIL___RECEIVED_QTY as RECEIVED_QTY", 
        "SQ_Shortcut_to_ASN_DETAIL___STD_SUB_PACK_QTY as STD_SUB_PACK_QTY", 
        "SQ_Shortcut_to_ASN_DETAIL___LPN_PER_TIER as LPN_PER_TIER", 
        "SQ_Shortcut_to_ASN_DETAIL___TIER_PER_PALLET as TIER_PER_PALLET", 
        "SQ_Shortcut_to_ASN_DETAIL___MFG_PLNT as MFG_PLNT", 
        "SQ_Shortcut_to_ASN_DETAIL___MFG_DATE as MFG_DATE", 
        "SQ_Shortcut_to_ASN_DETAIL___SHIP_BY_DATE as SHIP_BY_DATE", 
        "SQ_Shortcut_to_ASN_DETAIL___EXPIRE_DATE as EXPIRE_DATE", 
        "SQ_Shortcut_to_ASN_DETAIL___INCUBATION_DATE as INCUBATION_DATE", 
        "SQ_Shortcut_to_ASN_DETAIL___EPC_REQ_ON_ALL_CASES as EPC_REQ_ON_ALL_CASES", 
        "SQ_Shortcut_to_ASN_DETAIL___WEIGHT_UOM_ID_BASE as WEIGHT_UOM_ID_BASE", 
        "SQ_Shortcut_to_ASN_DETAIL___REGION_ID as REGION_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___IS_ASSOCIATED_TO_OUTBOUND as IS_ASSOCIATED_TO_OUTBOUND", 
        "SQ_Shortcut_to_ASN_DETAIL___IS_CANCELLED as IS_CANCELLED", 
        "SQ_Shortcut_to_ASN_DETAIL___IS_CLOSED as IS_CLOSED", 
        "SQ_Shortcut_to_ASN_DETAIL___INVN_TYPE as INVN_TYPE", 
        "SQ_Shortcut_to_ASN_DETAIL___PROD_STAT as PROD_STAT", 
        "SQ_Shortcut_to_ASN_DETAIL___BATCH_NBR as BATCH_NBR", 
        "SQ_Shortcut_to_ASN_DETAIL___CNTRY_OF_ORGN as CNTRY_OF_ORGN", 
        "SQ_Shortcut_to_ASN_DETAIL___SHIPPED_LPN_COUNT as SHIPPED_LPN_COUNT", 
        "SQ_Shortcut_to_ASN_DETAIL___RECEIVED_LPN_COUNT as RECEIVED_LPN_COUNT", 
        "SQ_Shortcut_to_ASN_DETAIL___UNITS_ASSIGNED_TO_LPN as UNITS_ASSIGNED_TO_LPN", 
        "SQ_Shortcut_to_ASN_DETAIL___PROC_IMMD_NEEDS as PROC_IMMD_NEEDS", 
        "SQ_Shortcut_to_ASN_DETAIL___QUALITY_CHECK_HOLD_UPON_RCPT as QUALITY_CHECK_HOLD_UPON_RCPT", 
        "SQ_Shortcut_to_ASN_DETAIL___REFERENCE_ORDER_NBR as REFERENCE_ORDER_NBR", 
        "SQ_Shortcut_to_ASN_DETAIL___ACTUAL_WEIGHT as ACTUAL_WEIGHT", 
        "SQ_Shortcut_to_ASN_DETAIL___ACTUAL_WEIGHT_PACK_COUNT as ACTUAL_WEIGHT_PACK_COUNT", 
        "SQ_Shortcut_to_ASN_DETAIL___NBR_OF_PACK_FOR_CATCH_WT as NBR_OF_PACK_FOR_CATCH_WT", 
        "SQ_Shortcut_to_ASN_DETAIL___PUTWY_TYPE as PUTWY_TYPE", 
        "SQ_Shortcut_to_ASN_DETAIL___RETAIL_PRICE as RETAIL_PRICE", 
        "SQ_Shortcut_to_ASN_DETAIL___PRICE_TIX_AVAIL as PRICE_TIX_AVAIL", 
        "SQ_Shortcut_to_ASN_DETAIL___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_ASN_DETAIL___CREATED_SOURCE as CREATED_SOURCE", 
        "SQ_Shortcut_to_ASN_DETAIL___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_ASN_DETAIL___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_ASN_DETAIL___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
        "SQ_Shortcut_to_ASN_DETAIL___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "SQ_Shortcut_to_ASN_DETAIL___HIBERNATE_VERSION as HIBERNATE_VERSION", 
        "SQ_Shortcut_to_ASN_DETAIL___TC_COMPANY_ID as TC_COMPANY_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___TC_PO_LINE_ID as TC_PO_LINE_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___INVENTORY_SEGMENT_ID as INVENTORY_SEGMENT_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___PPACK_GRP_CODE as PPACK_GRP_CODE", 
        "SQ_Shortcut_to_ASN_DETAIL___CUT_NBR as CUT_NBR", 
        "SQ_Shortcut_to_ASN_DETAIL___QTY_CONV_FACTOR as QTY_CONV_FACTOR", 
        "SQ_Shortcut_to_ASN_DETAIL___QTY_UOM_ID as QTY_UOM_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___WEIGHT_UOM_ID as WEIGHT_UOM_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___QTY_UOM_ID_BASE as QTY_UOM_ID_BASE", 
        "SQ_Shortcut_to_ASN_DETAIL___TC_ORDER_ID as TC_ORDER_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___ORDER_ID as ORDER_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___TC_ORDER_LINE_ID as TC_ORDER_LINE_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___ORDER_LINE_ITEM_ID as ORDER_LINE_ITEM_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___SEQ_NBR as SEQ_NBR", 
        "SQ_Shortcut_to_ASN_DETAIL___EXP_RECEIVE_CONDITION_CODE as EXP_RECEIVE_CONDITION_CODE", 
        "SQ_Shortcut_to_ASN_DETAIL___ASN_RECV_RULES as ASN_RECV_RULES", 
        "SQ_Shortcut_to_ASN_DETAIL___CHECKSUM as CHECKSUM", 
        "SQ_Shortcut_to_ASN_DETAIL___ACTUAL_WEIGHT_RECEIVED as ACTUAL_WEIGHT_RECEIVED", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_FIELD_1 as REF_FIELD_1", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_FIELD_2 as REF_FIELD_2", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_FIELD_3 as REF_FIELD_3", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_FIELD_4 as REF_FIELD_4", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_FIELD_5 as REF_FIELD_5", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_FIELD_6 as REF_FIELD_6", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_FIELD_7 as REF_FIELD_7", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_FIELD_8 as REF_FIELD_8", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_FIELD_9 as REF_FIELD_9", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_FIELD_10 as REF_FIELD_10", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_NUM1 as REF_NUM1", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_NUM2 as REF_NUM2", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_NUM3 as REF_NUM3", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_NUM4 as REF_NUM4", 
        "SQ_Shortcut_to_ASN_DETAIL___REF_NUM5 as REF_NUM5", 
        "SQ_Shortcut_to_ASN_DETAIL___DISPOSITION_TYPE as DISPOSITION_TYPE", 
        "SQ_Shortcut_to_ASN_DETAIL___PRE_RECEIPT_STATUS as PRE_RECEIPT_STATUS", 
        "SQ_Shortcut_to_ASN_DETAIL___INV_DISPOSITION as INV_DISPOSITION", 
        "SQ_Shortcut_to_ASN_DETAIL___EXT_PLAN_ID as EXT_PLAN_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___PURCHASE_ORDERS_LINE_ITEM_ID as PURCHASE_ORDERS_LINE_ITEM_ID", 
        "SQ_Shortcut_to_ASN_DETAIL___PROCESSED_FOR_TRLR_MOVES as PROCESSED_FOR_TRLR_MOVES", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ASN_DETAIL_PRE, type TARGET 
    # COLUMN COUNT: 101


    Shortcut_to_WM_ASN_DETAIL_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(ASN_DETAIL_ID AS BIGINT) as ASN_DETAIL_ID", 
        "CAST(ASN_ID AS BIGINT) as ASN_ID", 
        "CAST(TC_PURCHASE_ORDERS_ID AS STRING) as TC_PURCHASE_ORDERS_ID", 
        "CAST(PURCHASE_ORDERS_ID AS BIGINT) as PURCHASE_ORDERS_ID", 
        "CAST(SKU_ID AS BIGINT) as SKU_ID", 
        "CAST(SKU_NAME AS STRING) as SKU_NAME", 
        "CAST(SKU_ATTR_1 AS STRING) as SKU_ATTR_1", 
        "CAST(SKU_ATTR_2 AS STRING) as SKU_ATTR_2", 
        "CAST(SKU_ATTR_3 AS STRING) as SKU_ATTR_3", 
        "CAST(SKU_ATTR_4 AS STRING) as SKU_ATTR_4", 
        "CAST(SKU_ATTR_5 AS STRING) as SKU_ATTR_5", 
        "CAST(BUSINESS_PARTNER_ID AS STRING) as BUSINESS_PARTNER_ID", 
        "CAST(PACKAGE_TYPE_ID AS BIGINT) as PACKAGE_TYPE_ID", 
        "CAST(PACKAGE_TYPE_DESC AS STRING) as PACKAGE_TYPE_DESC", 
        "CAST(PACKAGE_TYPE_INSTANCE AS STRING) as PACKAGE_TYPE_INSTANCE", 
        "CAST(EPC_TRACKING_RFID_VALUE AS STRING) as EPC_TRACKING_RFID_VALUE", 
        "CAST(ORDER_TYPE_DESC AS STRING) as ORDER_TYPE_DESC", 
        "CAST(GTIN AS STRING) as GTIN", 
        "CAST(SHIPPED_QTY AS BIGINT) as SHIPPED_QTY", 
        "CAST(STD_PACK_QTY AS BIGINT) as STD_PACK_QTY", 
        "CAST(STD_CASE_QTY AS BIGINT) as STD_CASE_QTY", 
        "CAST(ASN_DETAIL_STATUS AS BIGINT) as ASN_DETAIL_STATUS", 
        "CAST(RECEIVED_QTY AS BIGINT) as RECEIVED_QTY", 
        "CAST(STD_SUB_PACK_QTY AS BIGINT) as STD_SUB_PACK_QTY", 
        "CAST(LPN_PER_TIER AS BIGINT) as LPN_PER_TIER", 
        "CAST(TIER_PER_PALLET AS BIGINT) as TIER_PER_PALLET", 
        "CAST(MFG_DATE AS TIMESTAMP) as MFG_DATE", 
        "CAST(SHIP_BY_DATE AS TIMESTAMP) as SHIP_BY_DATE", 
        "CAST(MFG_PLNT AS STRING) as MFG_PLNT", 
        "CAST(EXPIRE_DATE AS TIMESTAMP) as EXPIRE_DATE", 
        "CAST(INCUBATION_DATE AS TIMESTAMP) as INCUBATION_DATE", 
        "CAST(EPC_REQ_ON_ALL_CASES AS STRING) as EPC_REQ_ON_ALL_CASES", 
        "CAST(WEIGHT_UOM_ID_BASE AS BIGINT) as WEIGHT_UOM_ID_BASE", 
        "CAST(REGION_ID AS BIGINT) as REGION_ID", 
        "CAST(IS_ASSOCIATED_TO_OUTBOUND AS BIGINT) as IS_ASSOCIATED_TO_OUTBOUND", 
        "CAST(IS_CANCELLED AS BIGINT) as IS_CANCELLED", 
        "CAST(IS_CLOSED AS BIGINT) as IS_CLOSED", 
        "CAST(INVN_TYPE AS STRING) as INVN_TYPE", 
        "CAST(PROD_STAT AS STRING) as PROD_STAT", 
        "CAST(BATCH_NBR AS STRING) as BATCH_NBR", 
        "CAST(CNTRY_OF_ORGN AS STRING) as CNTRY_OF_ORGN", 
        "CAST(SHIPPED_LPN_COUNT AS BIGINT) as SHIPPED_LPN_COUNT", 
        "CAST(RECEIVED_LPN_COUNT AS BIGINT) as RECEIVED_LPN_COUNT", 
        "CAST(UNITS_ASSIGNED_TO_LPN AS BIGINT) as UNITS_ASSIGNED_TO_LPN", 
        "CAST(PROC_IMMD_NEEDS AS STRING) as PROC_IMMD_NEEDS", 
        "CAST(QUALITY_CHECK_HOLD_UPON_RCPT AS STRING) as QUALITY_CHECK_HOLD_UPON_RCPT", 
        "CAST(REFERENCE_ORDER_NBR AS STRING) as REFERENCE_ORDER_NBR", 
        "CAST(ACTUAL_WEIGHT AS BIGINT) as ACTUAL_WEIGHT", 
        "CAST(ACTUAL_WEIGHT_PACK_COUNT AS BIGINT) as ACTUAL_WEIGHT_PACK_COUNT", 
        "CAST(NBR_OF_PACK_FOR_CATCH_WT AS BIGINT) as NBR_OF_PACK_FOR_CATCH_WT", 
        "CAST(PUTWY_TYPE AS STRING) as PUTWY_TYPE", 
        "CAST(RETAIL_PRICE AS BIGINT) as RETAIL_PRICE", 
        "CAST(PRICE_TIX_AVAIL AS STRING) as PRICE_TIX_AVAIL", 
        "CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", 
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", 
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
        "CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", 
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", 
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION", 
        "CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", 
        "CAST(TC_PO_LINE_ID AS STRING) as TC_PO_LINE_ID", 
        "CAST(INVENTORY_SEGMENT_ID AS BIGINT) as INVENTORY_SEGMENT_ID", 
        "CAST(PPACK_GRP_CODE AS STRING) as PPACK_GRP_CODE", 
        "CAST(CUT_NBR AS STRING) as CUT_NBR", 
        "CAST(QTY_CONV_FACTOR AS BIGINT) as QTY_CONV_FACTOR", 
        "CAST(QTY_UOM_ID AS BIGINT) as QTY_UOM_ID", 
        "CAST(WEIGHT_UOM_ID AS BIGINT) as WEIGHT_UOM_ID", 
        "CAST(QTY_UOM_ID_BASE AS BIGINT) as QTY_UOM_ID_BASE", 
        "CAST(TC_ORDER_ID AS STRING) as TC_ORDER_ID", 
        "CAST(ORDER_ID AS BIGINT) as ORDER_ID", 
        "CAST(TC_ORDER_LINE_ID AS STRING) as TC_ORDER_LINE_ID", 
        "CAST(ORDER_LINE_ITEM_ID AS BIGINT) as ORDER_LINE_ITEM_ID", 
        "CAST(SEQ_NBR AS STRING) as SEQ_NBR", 
        "CAST(EXP_RECEIVE_CONDITION_CODE AS STRING) as EXP_RECEIVE_CONDITION_CODE", 
        "CAST(ASN_RECV_RULES AS STRING) as ASN_RECV_RULES", 
        "CAST(CHECKSUM AS STRING) as CHECKSUM", 
        "CAST(ACTUAL_WEIGHT_RECEIVED AS BIGINT) as ACTUAL_WEIGHT_RECEIVED", 
        "CAST(REF_FIELD_1 AS STRING) as REF_FIELD_1", 
        "CAST(REF_FIELD_2 AS STRING) as REF_FIELD_3", 
        "CAST(REF_FIELD_3 AS STRING) as REF_FIELD_2", 
        "CAST(REF_FIELD_4 AS STRING) as REF_FIELD_4", 
        "CAST(REF_FIELD_5 AS STRING) as REF_FIELD_5", 
        "CAST(REF_FIELD_6 AS STRING) as REF_FIELD_6", 
        "CAST(REF_FIELD_7 AS STRING) as REF_FIELD_7", 
        "CAST(REF_FIELD_8 AS STRING) as REF_FIELD_8", 
        "CAST(REF_FIELD_9 AS STRING) as REF_FIELD_9", 
        "CAST(REF_FIELD_10 AS STRING) as REF_FIELD_10", 
        "CAST(REF_NUM1 AS BIGINT) as REF_NUM1", 
        "CAST(REF_NUM2 AS BIGINT) as REF_NUM2", 
        "CAST(REF_NUM3 AS BIGINT) as REF_NUM3", 
        "CAST(REF_NUM4 AS BIGINT) as REF_NUM4", 
        "CAST(REF_NUM5 AS BIGINT) as REF_NUM5", 
        "CAST(DISPOSITION_TYPE AS STRING) as DISPOSITION_TYPE", 
        "CAST(PRE_RECEIPT_STATUS AS STRING) as PRE_RECEIPT_STATUS", 
        "CAST(INV_DISPOSITION AS STRING) as INV_DISPOSITION", 
        "CAST(EXT_PLAN_ID AS STRING) as EXT_PLAN_ID", 
        "CAST(PURCHASE_ORDERS_LINE_ITEM_ID AS BIGINT) as PURCHASE_ORDERS_LINE_ITEM_ID", 
        "CAST(PROCESSED_FOR_TRLR_MOVES AS BIGINT) as PROCESSED_FOR_TRLR_MOVES", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    overwriteDeltaPartition(Shortcut_to_WM_ASN_DETAIL_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_ASN_DETAIL_PRE is written to the target table - "
        + target_table_name
    )