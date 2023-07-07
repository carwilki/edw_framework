#Code converted on 2023-06-26 17:05:29
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
from Datalake.utils.logger import *


def m_WM_Outpt_Lpn_Detail_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Outpt_Lpn_Detail_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_OUTPT_LPN_DETAIL_PRE"
    schemaName = raw
    source_schema = "WMSMIS"

    
    target_table_name = schemaName + "." + tableName
    refine_table_name = tableName[:-4]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + Prev_Run_Dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    query = f"""SELECT
                    OUTPT_LPN_DETAIL.ASSORT_NBR,
                    OUTPT_LPN_DETAIL.BUSINESS_PARTNER,
                    OUTPT_LPN_DETAIL.CONSUMPTION_PRIORITY_DTTM,
                    OUTPT_LPN_DETAIL.CREATED_DTTM,
                    OUTPT_LPN_DETAIL.CREATED_SOURCE,
                    OUTPT_LPN_DETAIL.CREATED_SOURCE_TYPE,
                    OUTPT_LPN_DETAIL.GTIN,
                    OUTPT_LPN_DETAIL.INVC_BATCH_NBR,
                    OUTPT_LPN_DETAIL.INVENTORY_TYPE,
                    OUTPT_LPN_DETAIL.ITEM_ATTR_1,
                    OUTPT_LPN_DETAIL.ITEM_ATTR_2,
                    OUTPT_LPN_DETAIL.ITEM_ATTR_3,
                    OUTPT_LPN_DETAIL.ITEM_ATTR_4,
                    OUTPT_LPN_DETAIL.ITEM_ATTR_5,
                    OUTPT_LPN_DETAIL.ITEM_ID,
                    OUTPT_LPN_DETAIL.LAST_UPDATED_DTTM,
                    OUTPT_LPN_DETAIL.LAST_UPDATED_SOURCE,
                    OUTPT_LPN_DETAIL.LAST_UPDATED_SOURCE_TYPE,
                    OUTPT_LPN_DETAIL.LPN_DETAIL_ID,
                    OUTPT_LPN_DETAIL.MANUFACTURED_DTTM,
                    OUTPT_LPN_DETAIL.BATCH_NBR,
                    OUTPT_LPN_DETAIL.MANUFACTURED_PLANT,
                    OUTPT_LPN_DETAIL.CNTRY_OF_ORGN,
                    OUTPT_LPN_DETAIL.OUTPT_LPN_DETAIL_ID,
                    OUTPT_LPN_DETAIL.PRODUCT_STATUS,
                    OUTPT_LPN_DETAIL.PROC_DTTM,
                    OUTPT_LPN_DETAIL.PROC_STAT_CODE,
                    OUTPT_LPN_DETAIL.QTY_UOM,
                    OUTPT_LPN_DETAIL.REC_PROC_INDIC,
                    OUTPT_LPN_DETAIL.SIZE_VALUE,
                    OUTPT_LPN_DETAIL.TC_COMPANY_ID,
                    OUTPT_LPN_DETAIL.TC_LPN_ID,
                    OUTPT_LPN_DETAIL.VERSION_NBR,
                    OUTPT_LPN_DETAIL.DISTRIBUTION_ORDER_DTL_ID,
                    OUTPT_LPN_DETAIL.ITEM_COLOR,
                    OUTPT_LPN_DETAIL.ITEM_COLOR_SFX,
                    OUTPT_LPN_DETAIL.ITEM_SEASON,
                    OUTPT_LPN_DETAIL.ITEM_SEASON_YEAR,
                    OUTPT_LPN_DETAIL.ITEM_SECOND_DIM,
                    OUTPT_LPN_DETAIL.ITEM_SIZE_DESC,
                    OUTPT_LPN_DETAIL.ITEM_QUALITY,
                    OUTPT_LPN_DETAIL.ITEM_STYLE,
                    OUTPT_LPN_DETAIL.ITEM_STYLE_SFX,
                    OUTPT_LPN_DETAIL.SIZE_RANGE_CODE,
                    OUTPT_LPN_DETAIL.SIZE_REL_POSN_IN_TABLE,
                    OUTPT_LPN_DETAIL.VENDOR_ITEM_NBR,
                    OUTPT_LPN_DETAIL.MINOR_ORDER_NBR,
                    OUTPT_LPN_DETAIL.MINOR_PO_NBR,
                    OUTPT_LPN_DETAIL.ITEM_NAME,
                    OUTPT_LPN_DETAIL.TC_ORDER_LINE_ID,
                    OUTPT_LPN_DETAIL.DISTRO_NUMBER
                FROM {source_schema}.OUTPT_LPN_DETAIL
                WHERE  (trunc(OUTPT_LPN_DETAIL.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(OUTPT_LPN_DETAIL.LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""

    SQ_Shortcut_to_OUTPT_LPN_DETAIL = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_OUTPT_LPN_DETAIL is executed and data is loaded using jdbc")

    
    # Processing node EXP_TRN, type EXPRESSION 
    # COLUMN COUNT: 53
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_OUTPT_LPN_DETAIL_temp = SQ_Shortcut_to_OUTPT_LPN_DETAIL.toDF(*["SQ_Shortcut_to_OUTPT_LPN_DETAIL___" + col for col in SQ_Shortcut_to_OUTPT_LPN_DETAIL.columns])
    
    EXP_TRN = SQ_Shortcut_to_OUTPT_LPN_DETAIL_temp.selectExpr( \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___sys_row_id as sys_row_id", \
    	f"{dcnbr} as DC_NBR_EXP", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ASSORT_NBR as ASSORT_NBR", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___BUSINESS_PARTNER as BUSINESS_PARTNER", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___CONSUMPTION_PRIORITY_DTTM as CONSUMPTION_PRIORITY_DTTM", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___CREATED_DTTM as CREATED_DTTM", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___CREATED_SOURCE as CREATED_SOURCE", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___GTIN as GTIN", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___INVC_BATCH_NBR as INVC_BATCH_NBR", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___INVENTORY_TYPE as INVENTORY_TYPE", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_ATTR_1 as ITEM_ATTR_1", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_ATTR_2 as ITEM_ATTR_2", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_ATTR_3 as ITEM_ATTR_3", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_ATTR_4 as ITEM_ATTR_4", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_ATTR_5 as ITEM_ATTR_5", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_ID as ITEM_ID", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___LPN_DETAIL_ID as LPN_DETAIL_ID", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___MANUFACTURED_DTTM as MANUFACTURED_DTTM", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___BATCH_NBR as BATCH_NBR", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___MANUFACTURED_PLANT as MANUFACTURED_PLANT", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___OUTPT_LPN_DETAIL_ID as OUTPT_LPN_DETAIL_ID", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___PRODUCT_STATUS as PRODUCT_STATUS", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___PROC_DTTM as PROC_DTTM", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___PROC_STAT_CODE as PROC_STAT_CODE", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___QTY_UOM as QTY_UOM", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___REC_PROC_INDIC as REC_PROC_INDIC", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___SIZE_VALUE as SIZE_VALUE", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___TC_COMPANY_ID as TC_COMPANY_ID", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___TC_LPN_ID as TC_LPN_ID", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___VERSION_NBR as VERSION_NBR", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___DISTRIBUTION_ORDER_DTL_ID as DISTRIBUTION_ORDER_DTL_ID", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_COLOR as ITEM_COLOR", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_COLOR_SFX as ITEM_COLOR_SFX", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_SEASON as ITEM_SEASON", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_SEASON_YEAR as ITEM_SEASON_YEAR", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_SECOND_DIM as ITEM_SECOND_DIM", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_SIZE_DESC as ITEM_SIZE_DESC", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_QUALITY as ITEM_QUALITY", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_STYLE as ITEM_STYLE", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_STYLE_SFX as ITEM_STYLE_SFX", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___VENDOR_ITEM_NBR as VENDOR_ITEM_NBR", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___MINOR_ORDER_NBR as MINOR_ORDER_NBR", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___MINOR_PO_NBR as MINOR_PO_NBR", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___ITEM_NAME as ITEM_NAME", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___TC_ORDER_LINE_ID as TC_ORDER_LINE_ID", \
    	"SQ_Shortcut_to_OUTPT_LPN_DETAIL___DISTRO_NUMBER as DISTRO_NUMBER", \
    	"CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE, type TARGET 
    # COLUMN COUNT: 53
    
    
    Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE = EXP_TRN.selectExpr( \
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
    	"CAST(OUTPT_LPN_DETAIL_ID AS BIGINT) as OUTPT_LPN_DETAIL_ID", \
    	"CAST(ASSORT_NBR AS STRING) as ASSORT_NBR", \
    	"CAST(BUSINESS_PARTNER AS STRING) as BUSINESS_PARTNER", \
    	"CAST(CONSUMPTION_PRIORITY_DTTM AS TIMESTAMP) as CONSUMPTION_PRIORITY_DTTM", \
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
    	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
    	"CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", \
    	"CAST(GTIN AS STRING) as GTIN", \
    	"CAST(INVC_BATCH_NBR AS BIGINT) as INVC_BATCH_NBR", \
    	"CAST(INVENTORY_TYPE AS STRING) as INVENTORY_TYPE", \
    	"CAST(ITEM_ATTR_1 AS STRING) as ITEM_ATTR_1", \
    	"CAST(ITEM_ATTR_2 AS STRING) as ITEM_ATTR_2", \
    	"CAST(ITEM_ATTR_3 AS STRING) as ITEM_ATTR_3", \
    	"CAST(ITEM_ATTR_4 AS STRING) as ITEM_ATTR_4", \
    	"CAST(ITEM_ATTR_5 AS STRING) as ITEM_ATTR_5", \
    	"CAST(ITEM_ID AS BIGINT) as ITEM_ID", \
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
    	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
    	"CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", \
    	"CAST(LPN_DETAIL_ID AS BIGINT) as LPN_DETAIL_ID", \
    	"CAST(MANUFACTURED_DTTM AS TIMESTAMP) as MANUFACTURED_DTTM", \
    	"CAST(BATCH_NBR AS STRING) as BATCH_NBR", \
    	"CAST(MANUFACTURED_PLANT AS STRING) as MANUFACTURED_PLANT", \
    	"CAST(CNTRY_OF_ORGN AS STRING) as CNTRY_OF_ORGN", \
    	"CAST(PRODUCT_STATUS AS STRING) as PRODUCT_STATUS", \
    	"CAST(PROC_DTTM AS TIMESTAMP) as PROC_DTTM", \
    	"CAST(PROC_STAT_CODE AS BIGINT) as PROC_STAT_CODE", \
    	"CAST(QTY_UOM AS STRING) as QTY_UOM", \
    	"CAST(REC_PROC_INDIC AS STRING) as REC_PROC_INDIC", \
    	"CAST(SIZE_VALUE AS BIGINT) as SIZE_VALUE", \
    	"CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", \
    	"CAST(TC_LPN_ID AS STRING) as TC_LPN_ID", \
    	"CAST(VERSION_NBR AS BIGINT) as VERSION_NBR", \
    	"CAST(DISTRIBUTION_ORDER_DTL_ID AS BIGINT) as DISTRIBUTION_ORDER_DTL_ID", \
    	"CAST(ITEM_COLOR AS STRING) as ITEM_COLOR", \
    	"CAST(ITEM_COLOR_SFX AS STRING) as ITEM_COLOR_SFX", \
    	"CAST(ITEM_SEASON AS STRING) as ITEM_SEASON", \
    	"CAST(ITEM_SEASON_YEAR AS STRING) as ITEM_SEASON_YEAR", \
    	"CAST(ITEM_SECOND_DIM AS STRING) as ITEM_SECOND_DIM", \
    	"CAST(ITEM_SIZE_DESC AS STRING) as ITEM_SIZE_DESC", \
    	"CAST(ITEM_QUALITY AS STRING) as ITEM_QUALITY", \
    	"CAST(ITEM_STYLE AS STRING) as ITEM_STYLE", \
    	"CAST(ITEM_STYLE_SFX AS STRING) as ITEM_STYLE_SFX", \
    	"CAST(SIZE_RANGE_CODE AS STRING) as SIZE_RANGE_CODE", \
    	"CAST(SIZE_REL_POSN_IN_TABLE AS STRING) as SIZE_REL_POSN_IN_TABLE", \
    	"CAST(VENDOR_ITEM_NBR AS STRING) as VENDOR_ITEM_NBR", \
    	"CAST(MINOR_ORDER_NBR AS STRING) as MINOR_ORDER_NBR", \
    	"CAST(MINOR_PO_NBR AS STRING) as MINOR_PO_NBR", \
    	"CAST(ITEM_NAME AS STRING) as ITEM_NAME", \
    	"CAST(TC_ORDER_LINE_ID AS STRING) as TC_ORDER_LINE_ID", \
    	"CAST(DISTRO_NUMBER AS STRING) as DISTRO_NUMBER", \
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE is written to the target table - " + target_table_name)
