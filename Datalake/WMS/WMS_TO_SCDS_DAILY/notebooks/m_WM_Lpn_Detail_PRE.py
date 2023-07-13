#Code converted on 2023-06-27 09:40:34
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



def m_WM_Lpn_Detail_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Lpn_Detail_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_LPN_DETAIL_PRE"

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
    # Processing node SQ_Shortcut_to_LPN_DETAIL, type SOURCE 
    # COLUMN COUNT: 87

    SQ_Shortcut_to_LPN_DETAIL = jdbcOracleConnection(  f"""SELECT
    LPN_DETAIL.TC_COMPANY_ID,
    LPN_DETAIL.LPN_ID,
    LPN_DETAIL.LPN_DETAIL_ID,
    LPN_DETAIL.LPN_DETAIL_STATUS,
    LPN_DETAIL.INTERNAL_ORDER_DTL_ID,
    LPN_DETAIL.DISTRIBUTION_ORDER_DTL_ID,
    LPN_DETAIL.RECEIVED_QTY,
    LPN_DETAIL.BUSINESS_PARTNER_ID,
    LPN_DETAIL.ITEM_ID,
    LPN_DETAIL.GTIN,
    LPN_DETAIL.STD_PACK_QTY,
    LPN_DETAIL.STD_SUB_PACK_QTY,
    LPN_DETAIL.STD_BUNDLE_QTY,
    LPN_DETAIL.INCUBATION_DATE,
    LPN_DETAIL.EXPIRATION_DATE,
    LPN_DETAIL.SHIP_BY_DATE,
    LPN_DETAIL.SELL_BY_DTTM,
    LPN_DETAIL.CONSUMPTION_PRIORITY_DTTM,
    LPN_DETAIL.MANUFACTURED_DTTM,
    LPN_DETAIL.CNTRY_OF_ORGN,
    LPN_DETAIL.INVENTORY_TYPE,
    LPN_DETAIL.PRODUCT_STATUS,
    LPN_DETAIL.ITEM_ATTR_1,
    LPN_DETAIL.ITEM_ATTR_2,
    LPN_DETAIL.ITEM_ATTR_3,
    LPN_DETAIL.ITEM_ATTR_4,
    LPN_DETAIL.ITEM_ATTR_5,
    LPN_DETAIL.ASN_DTL_ID,
    LPN_DETAIL.PACK_WEIGHT,
    LPN_DETAIL.ESTIMATED_WEIGHT,
    LPN_DETAIL.ESTIMATED_VOLUME,
    LPN_DETAIL.SIZE_VALUE,
    LPN_DETAIL.WEIGHT,
    LPN_DETAIL.QTY_UOM_ID,
    LPN_DETAIL.WEIGHT_UOM_ID,
    LPN_DETAIL.VOLUME_UOM_ID,
    LPN_DETAIL.ASSORT_NBR,
    LPN_DETAIL.CUT_NBR,
    LPN_DETAIL.PURCHASE_ORDERS_ID,
    LPN_DETAIL.TC_PURCHASE_ORDERS_ID,
    LPN_DETAIL.PURCHASE_ORDERS_LINE_ID,
    LPN_DETAIL.TC_PURCHASE_ORDERS_LINE_ID,
    LPN_DETAIL.HIBERNATE_VERSION,
    LPN_DETAIL.INTERNAL_ORDER_ID,
    LPN_DETAIL.INSTRTN_CODE_1,
    LPN_DETAIL.INSTRTN_CODE_2,
    LPN_DETAIL.INSTRTN_CODE_3,
    LPN_DETAIL.INSTRTN_CODE_4,
    LPN_DETAIL.INSTRTN_CODE_5,
    LPN_DETAIL.CREATED_SOURCE_TYPE,
    LPN_DETAIL.CREATED_SOURCE,
    LPN_DETAIL.CREATED_DTTM,
    LPN_DETAIL.LAST_UPDATED_SOURCE_TYPE,
    LPN_DETAIL.LAST_UPDATED_SOURCE,
    LPN_DETAIL.LAST_UPDATED_DTTM,
    LPN_DETAIL.VENDOR_ITEM_NBR,
    LPN_DETAIL.MANUFACTURED_PLANT,
    LPN_DETAIL.BATCH_NBR,
    LPN_DETAIL.ASSIGNED_QTY,
    LPN_DETAIL.PREPACK_GROUP_CODE,
    LPN_DETAIL.PACK_CODE,
    LPN_DETAIL.SHIPPED_QTY,
    LPN_DETAIL.INITIAL_QTY,
    LPN_DETAIL.QTY_CONV_FACTOR,
    LPN_DETAIL.QTY_UOM_ID_BASE,
    LPN_DETAIL.WEIGHT_UOM_ID_BASE,
    LPN_DETAIL.VOLUME_UOM_ID_BASE,
    LPN_DETAIL.ITEM_NAME,
    LPN_DETAIL.TC_ORDER_LINE_ID,
    LPN_DETAIL.HAZMAT_UOM,
    LPN_DETAIL.HAZMAT_QTY,
    LPN_DETAIL.REF_FIELD_1,
    LPN_DETAIL.REF_FIELD_2,
    LPN_DETAIL.REF_FIELD_3,
    LPN_DETAIL.REF_FIELD_4,
    LPN_DETAIL.REF_FIELD_5,
    LPN_DETAIL.REF_FIELD_6,
    LPN_DETAIL.REF_FIELD_7,
    LPN_DETAIL.REF_FIELD_8,
    LPN_DETAIL.REF_FIELD_9,
    LPN_DETAIL.REF_FIELD_10,
    LPN_DETAIL.REF_NUM1,
    LPN_DETAIL.REF_NUM2,
    LPN_DETAIL.REF_NUM3,
    LPN_DETAIL.REF_NUM4,
    LPN_DETAIL.REF_NUM5,
    LPN_DETAIL.CHASE_WAVE_NBR
    FROM {source_schema}.LPN_DETAIL
    WHERE (trunc(LPN_DETAIL.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(LPN_DETAIL.LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)  AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXP_TRN, type EXPRESSION 
    # COLUMN COUNT: 89

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LPN_DETAIL_temp = SQ_Shortcut_to_LPN_DETAIL.toDF(*["SQ_Shortcut_to_LPN_DETAIL___" + col for col in SQ_Shortcut_to_LPN_DETAIL.columns])

    EXP_TRN = SQ_Shortcut_to_LPN_DETAIL_temp.selectExpr( \
        "SQ_Shortcut_to_LPN_DETAIL___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_LPN_DETAIL___TC_COMPANY_ID as TC_COMPANY_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___LPN_ID as LPN_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___LPN_DETAIL_ID as LPN_DETAIL_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___LPN_DETAIL_STATUS as LPN_DETAIL_STATUS", \
        "SQ_Shortcut_to_LPN_DETAIL___INTERNAL_ORDER_DTL_ID as INTERNAL_ORDER_DTL_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___DISTRIBUTION_ORDER_DTL_ID as DISTRIBUTION_ORDER_DTL_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___RECEIVED_QTY as RECEIVED_QTY", \
        "SQ_Shortcut_to_LPN_DETAIL___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___ITEM_ID as ITEM_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___GTIN as GTIN", \
        "SQ_Shortcut_to_LPN_DETAIL___STD_PACK_QTY as STD_PACK_QTY", \
        "SQ_Shortcut_to_LPN_DETAIL___STD_SUB_PACK_QTY as STD_SUB_PACK_QTY", \
        "SQ_Shortcut_to_LPN_DETAIL___STD_BUNDLE_QTY as STD_BUNDLE_QTY", \
        "SQ_Shortcut_to_LPN_DETAIL___INCUBATION_DATE as INCUBATION_DATE", \
        "SQ_Shortcut_to_LPN_DETAIL___EXPIRATION_DATE as EXPIRATION_DATE", \
        "SQ_Shortcut_to_LPN_DETAIL___SHIP_BY_DATE as SHIP_BY_DATE", \
        "SQ_Shortcut_to_LPN_DETAIL___SELL_BY_DTTM as SELL_BY_DTTM", \
        "SQ_Shortcut_to_LPN_DETAIL___CONSUMPTION_PRIORITY_DTTM as CONSUMPTION_PRIORITY_DTTM", \
        "SQ_Shortcut_to_LPN_DETAIL___MANUFACTURED_DTTM as MANUFACTURED_DTTM", \
        "SQ_Shortcut_to_LPN_DETAIL___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
        "SQ_Shortcut_to_LPN_DETAIL___INVENTORY_TYPE as INVENTORY_TYPE", \
        "SQ_Shortcut_to_LPN_DETAIL___PRODUCT_STATUS as PRODUCT_STATUS", \
        "SQ_Shortcut_to_LPN_DETAIL___ITEM_ATTR_1 as ITEM_ATTR_1", \
        "SQ_Shortcut_to_LPN_DETAIL___ITEM_ATTR_2 as ITEM_ATTR_2", \
        "SQ_Shortcut_to_LPN_DETAIL___ITEM_ATTR_3 as ITEM_ATTR_3", \
        "SQ_Shortcut_to_LPN_DETAIL___ITEM_ATTR_4 as ITEM_ATTR_4", \
        "SQ_Shortcut_to_LPN_DETAIL___ITEM_ATTR_5 as ITEM_ATTR_5", \
        "SQ_Shortcut_to_LPN_DETAIL___ASN_DTL_ID as ASN_DTL_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___PACK_WEIGHT as PACK_WEIGHT", \
        "SQ_Shortcut_to_LPN_DETAIL___ESTIMATED_WEIGHT as ESTIMATED_WEIGHT", \
        "SQ_Shortcut_to_LPN_DETAIL___ESTIMATED_VOLUME as ESTIMATED_VOLUME", \
        "SQ_Shortcut_to_LPN_DETAIL___SIZE_VALUE as SIZE_VALUE", \
        "SQ_Shortcut_to_LPN_DETAIL___WEIGHT as WEIGHT", \
        "SQ_Shortcut_to_LPN_DETAIL___QTY_UOM_ID as QTY_UOM_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___WEIGHT_UOM_ID as WEIGHT_UOM_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___VOLUME_UOM_ID as VOLUME_UOM_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___ASSORT_NBR as ASSORT_NBR", \
        "SQ_Shortcut_to_LPN_DETAIL___CUT_NBR as CUT_NBR", \
        "SQ_Shortcut_to_LPN_DETAIL___PURCHASE_ORDERS_ID as PURCHASE_ORDERS_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___TC_PURCHASE_ORDERS_ID as TC_PURCHASE_ORDERS_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___PURCHASE_ORDERS_LINE_ID as PURCHASE_ORDERS_LINE_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___TC_PURCHASE_ORDERS_LINE_ID as TC_PURCHASE_ORDERS_LINE_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___HIBERNATE_VERSION as HIBERNATE_VERSION", \
        "SQ_Shortcut_to_LPN_DETAIL___INTERNAL_ORDER_ID as INTERNAL_ORDER_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___INSTRTN_CODE_1 as INSTRTN_CODE_1", \
        "SQ_Shortcut_to_LPN_DETAIL___INSTRTN_CODE_2 as INSTRTN_CODE_2", \
        "SQ_Shortcut_to_LPN_DETAIL___INSTRTN_CODE_3 as INSTRTN_CODE_3", \
        "SQ_Shortcut_to_LPN_DETAIL___INSTRTN_CODE_4 as INSTRTN_CODE_4", \
        "SQ_Shortcut_to_LPN_DETAIL___INSTRTN_CODE_5 as INSTRTN_CODE_5", \
        "SQ_Shortcut_to_LPN_DETAIL___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LPN_DETAIL___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_LPN_DETAIL___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_LPN_DETAIL___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LPN_DETAIL___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_LPN_DETAIL___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_LPN_DETAIL___VENDOR_ITEM_NBR as VENDOR_ITEM_NBR", \
        "SQ_Shortcut_to_LPN_DETAIL___MANUFACTURED_PLANT as MANUFACTURED_PLANT", \
        "SQ_Shortcut_to_LPN_DETAIL___BATCH_NBR as BATCH_NBR", \
        "SQ_Shortcut_to_LPN_DETAIL___ASSIGNED_QTY as ASSIGNED_QTY", \
        "SQ_Shortcut_to_LPN_DETAIL___PREPACK_GROUP_CODE as PREPACK_GROUP_CODE", \
        "SQ_Shortcut_to_LPN_DETAIL___PACK_CODE as PACK_CODE", \
        "SQ_Shortcut_to_LPN_DETAIL___SHIPPED_QTY as SHIPPED_QTY", \
        "SQ_Shortcut_to_LPN_DETAIL___INITIAL_QTY as INITIAL_QTY", \
        "SQ_Shortcut_to_LPN_DETAIL___QTY_CONV_FACTOR as QTY_CONV_FACTOR", \
        "SQ_Shortcut_to_LPN_DETAIL___QTY_UOM_ID_BASE as QTY_UOM_ID_BASE", \
        "SQ_Shortcut_to_LPN_DETAIL___WEIGHT_UOM_ID_BASE as WEIGHT_UOM_ID_BASE", \
        "SQ_Shortcut_to_LPN_DETAIL___VOLUME_UOM_ID_BASE as VOLUME_UOM_ID_BASE", \
        "SQ_Shortcut_to_LPN_DETAIL___ITEM_NAME as ITEM_NAME", \
        "SQ_Shortcut_to_LPN_DETAIL___TC_ORDER_LINE_ID as TC_ORDER_LINE_ID", \
        "SQ_Shortcut_to_LPN_DETAIL___HAZMAT_UOM as HAZMAT_UOM", \
        "SQ_Shortcut_to_LPN_DETAIL___HAZMAT_QTY as HAZMAT_QTY", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_FIELD_1 as REF_FIELD_1", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_FIELD_2 as REF_FIELD_2", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_FIELD_3 as REF_FIELD_3", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_FIELD_4 as REF_FIELD_4", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_FIELD_5 as REF_FIELD_5", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_FIELD_6 as REF_FIELD_6", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_FIELD_7 as REF_FIELD_7", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_FIELD_8 as REF_FIELD_8", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_FIELD_9 as REF_FIELD_9", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_FIELD_10 as REF_FIELD_10", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_NUM1 as REF_NUM1", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_NUM2 as REF_NUM2", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_NUM3 as REF_NUM3", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_NUM4 as REF_NUM4", \
        "SQ_Shortcut_to_LPN_DETAIL___REF_NUM5 as REF_NUM5", \
        "SQ_Shortcut_to_LPN_DETAIL___CHASE_WAVE_NBR as CHASE_WAVE_NBR", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_LPN_DETAIL_PRE, type TARGET 
    # COLUMN COUNT: 89


    Shortcut_to_WM_LPN_DETAIL_PRE = EXP_TRN.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(LPN_ID AS BIGINT) as LPN_ID",
        "CAST(LPN_DETAIL_ID AS BIGINT) as LPN_DETAIL_ID",
        "CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID",
        "CAST(LPN_DETAIL_STATUS AS SMALLINT) as LPN_DETAIL_STATUS",
        "CAST(INTERNAL_ORDER_DTL_ID AS BIGINT) as INTERNAL_ORDER_DTL_ID",
        "CAST(DISTRIBUTION_ORDER_DTL_ID AS BIGINT) as DISTRIBUTION_ORDER_DTL_ID",
        "CAST(RECEIVED_QTY AS DECIMAL(13,4)) as RECEIVED_QTY",
        "CAST(BUSINESS_PARTNER_ID AS STRING) as BUSINESS_PARTNER_ID",
        "CAST(ITEM_ID AS INT) as ITEM_ID",
        "CAST(GTIN AS STRING) as GTIN",
        "CAST(STD_PACK_QTY AS DECIMAL(13,4)) as STD_PACK_QTY",
        "CAST(STD_SUB_PACK_QTY AS DECIMAL(13,4)) as STD_SUB_PACK_QTY",
        "CAST(STD_BUNDLE_QTY AS DECIMAL(13,4)) as STD_BUNDLE_QTY",
        "CAST(INCUBATION_DATE AS TIMESTAMP) as INCUBATION_DATE",
        "CAST(EXPIRATION_DATE AS TIMESTAMP) as EXPIRATION_DATE",
        "CAST(SHIP_BY_DATE AS TIMESTAMP) as SHIP_BY_DATE",
        "CAST(SELL_BY_DTTM AS TIMESTAMP) as SELL_BY_DTTM",
        "CAST(CONSUMPTION_PRIORITY_DTTM AS TIMESTAMP) as CONSUMPTION_PRIORITY_DTTM",
        "CAST(MANUFACTURED_DTTM AS TIMESTAMP) as MANUFACTURED_DTTM",
        "CAST(CNTRY_OF_ORGN AS STRING) as CNTRY_OF_ORGN",
        "CAST(INVENTORY_TYPE AS STRING) as INVENTORY_TYPE",
        "CAST(PRODUCT_STATUS AS STRING) as PRODUCT_STATUS",
        "CAST(ITEM_ATTR_1 AS STRING) as ITEM_ATTR_1",
        "CAST(ITEM_ATTR_2 AS STRING) as ITEM_ATTR_2",
        "CAST(ITEM_ATTR_3 AS STRING) as ITEM_ATTR_3",
        "CAST(ITEM_ATTR_4 AS STRING) as ITEM_ATTR_4",
        "CAST(ITEM_ATTR_5 AS STRING) as ITEM_ATTR_5",
        "CAST(ASN_DTL_ID AS BIGINT) as ASN_DTL_ID",
        "CAST(PACK_WEIGHT AS DECIMAL(13,4)) as PACK_WEIGHT",
        "CAST(ESTIMATED_WEIGHT AS DECIMAL(13,4)) as ESTIMATED_WEIGHT",
        "CAST(ESTIMATED_VOLUME AS DECIMAL(13,4)) as ESTIMATED_VOLUME",
        "CAST(SIZE_VALUE AS DECIMAL(16,4)) as SIZE_VALUE",
        "CAST(WEIGHT AS DECIMAL(16,4)) as WEIGHT",
        "CAST(QTY_UOM_ID AS BIGINT) as QTY_UOM_ID",
        "CAST(WEIGHT_UOM_ID AS INT) as WEIGHT_UOM_ID",
        "CAST(VOLUME_UOM_ID AS INT) as VOLUME_UOM_ID",
        "CAST(ASSORT_NBR AS STRING) as ASSORT_NBR",
        "CAST(CUT_NBR AS STRING) as CUT_NBR",
        "CAST(PURCHASE_ORDERS_ID AS BIGINT) as PURCHASE_ORDERS_ID",
        "CAST(TC_PURCHASE_ORDERS_ID AS STRING) as TC_PURCHASE_ORDERS_ID",
        "CAST(PURCHASE_ORDERS_LINE_ID AS BIGINT) as PURCHASE_ORDERS_LINE_ID",
        "CAST(TC_PURCHASE_ORDERS_LINE_ID AS STRING) as TC_PURCHASE_ORDERS_LINE_ID",
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION",
        "CAST(INTERNAL_ORDER_ID AS STRING) as INTERNAL_ORDER_ID",
        "CAST(INSTRTN_CODE_1 AS STRING) as INSTRTN_CODE_1",
        "CAST(INSTRTN_CODE_2 AS STRING) as INSTRTN_CODE_2",
        "CAST(INSTRTN_CODE_3 AS STRING) as INSTRTN_CODE_3",
        "CAST(INSTRTN_CODE_4 AS STRING) as INSTRTN_CODE_4",
        "CAST(INSTRTN_CODE_5 AS STRING) as INSTRTN_CODE_5",
        "CAST(CREATED_SOURCE_TYPE AS SMALLINT) as CREATED_SOURCE_TYPE",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_SOURCE_TYPE AS SMALLINT) as LAST_UPDATED_SOURCE_TYPE",
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(VENDOR_ITEM_NBR AS STRING) as VENDOR_ITEM_NBR",
        "CAST(MANUFACTURED_PLANT AS STRING) as MANUFACTURED_PLANT",
        "CAST(BATCH_NBR AS STRING) as BATCH_NBR",
        "CAST(ASSIGNED_QTY AS DECIMAL(13,4)) as ASSIGNED_QTY",
        "CAST(PREPACK_GROUP_CODE AS STRING) as PREPACK_GROUP_CODE",
        "CAST(PACK_CODE AS TINYINT) as PACK_CODE",
        "CAST(SHIPPED_QTY AS DECIMAL(13,4)) as SHIPPED_QTY",
        "CAST(INITIAL_QTY AS DECIMAL(13,4)) as INITIAL_QTY",
        "CAST(QTY_CONV_FACTOR AS DECIMAL(17,8)) as QTY_CONV_FACTOR",
        "CAST(QTY_UOM_ID_BASE AS INT) as QTY_UOM_ID_BASE",
        "CAST(WEIGHT_UOM_ID_BASE AS INT) as WEIGHT_UOM_ID_BASE",
        "CAST(VOLUME_UOM_ID_BASE AS INT) as VOLUME_UOM_ID_BASE",
        "CAST(ITEM_NAME AS STRING) as ITEM_NAME",
        "CAST(TC_ORDER_LINE_ID AS STRING) as TC_ORDER_LINE_ID",
        "CAST(HAZMAT_UOM AS INT) as HAZMAT_UOM",
        "CAST(HAZMAT_QTY AS DECIMAL(13,4)) as HAZMAT_QTY",
        "CAST(REF_FIELD_1 AS STRING) as REF_FIELD_1",
        "CAST(REF_FIELD_2 AS STRING) as REF_FIELD_2",
        "CAST(REF_FIELD_3 AS STRING) as REF_FIELD_3",
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
        "CAST(CHASE_WAVE_NBR AS STRING) as CHASE_WAVE_NBR",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )

    
    overwriteDeltaPartition(Shortcut_to_WM_LPN_DETAIL_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_LPN_DETAIL_PRE is written to the target table - "
        + target_table_name
    )    