#Code converted on 2023-06-26 17:05:08
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



def m_WM_Outpt_Order_Line_Item_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark = SparkSession.getActiveSession()
    logger.info("inside m_WM_Outpt_Order_Line_Item_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_OUTPT_ORDER_LINE_ITEM_PRE"
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
                    OUTPT_ORDER_LINE_ITEM.ACTUAL_COST_CURRENCY_CODE,
                    OUTPT_ORDER_LINE_ITEM.ACTUAL_SHIPPED_DTTM,
                    OUTPT_ORDER_LINE_ITEM.ASSORT_NBR,
                    OUTPT_ORDER_LINE_ITEM.BACK_ORD_QTY,
                    OUTPT_ORDER_LINE_ITEM.BATCH_CTRL_NBR,
                    OUTPT_ORDER_LINE_ITEM.BATCH_NBR,
                    OUTPT_ORDER_LINE_ITEM.CNTRY_OF_ORGN,
                    OUTPT_ORDER_LINE_ITEM.COMMODITY_CLASS,
                    OUTPT_ORDER_LINE_ITEM.CREATED_DTTM,
                    OUTPT_ORDER_LINE_ITEM.CREATED_SOURCE,
                    OUTPT_ORDER_LINE_ITEM.CREATED_SOURCE_TYPE,
                    OUTPT_ORDER_LINE_ITEM.CUSTOMER_ITEM,
                    OUTPT_ORDER_LINE_ITEM.EXP_INFO_CODE,
                    OUTPT_ORDER_LINE_ITEM.GTIN,
                    OUTPT_ORDER_LINE_ITEM.INVC_BATCH_NBR,
                    OUTPT_ORDER_LINE_ITEM.INVN_TYPE,
                    OUTPT_ORDER_LINE_ITEM.ITEM_ATTR_1,
                    OUTPT_ORDER_LINE_ITEM.ITEM_ATTR_2,
                    OUTPT_ORDER_LINE_ITEM.ITEM_ATTR_3,
                    OUTPT_ORDER_LINE_ITEM.ITEM_ATTR_4,
                    OUTPT_ORDER_LINE_ITEM.ITEM_ATTR_5,
                    OUTPT_ORDER_LINE_ITEM.ITEM_ID,
                    OUTPT_ORDER_LINE_ITEM.LAST_UPDATED_DTTM,
                    OUTPT_ORDER_LINE_ITEM.LAST_UPDATED_SOURCE,
                    OUTPT_ORDER_LINE_ITEM.LAST_UPDATED_SOURCE_TYPE,
                    OUTPT_ORDER_LINE_ITEM.LINE_ITEM_ID,
                    OUTPT_ORDER_LINE_ITEM.LINE_TYPE,
                    OUTPT_ORDER_LINE_ITEM.MANUFACTURING_DTTM,
                    OUTPT_ORDER_LINE_ITEM.MISC_INSTR_10_BYTE_1,
                    OUTPT_ORDER_LINE_ITEM.MISC_INSTR_10_BYTE_2,
                    OUTPT_ORDER_LINE_ITEM.ORDER_QTY,
                    OUTPT_ORDER_LINE_ITEM.ORDER_QTY_UOM,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ITEM_ID,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ORDER_LINE_ITEM_ID,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ORDER_QTY,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ORDER_QTY_UOM,
                    OUTPT_ORDER_LINE_ITEM.OUTPT_ORDER_LINE_ITEM_ID,
                    OUTPT_ORDER_LINE_ITEM.PARENT_LINE_ITEM_ID,
                    OUTPT_ORDER_LINE_ITEM.PICKUP_END_DTTM,
                    OUTPT_ORDER_LINE_ITEM.PICKUP_REFERENCE_NUMBER,
                    OUTPT_ORDER_LINE_ITEM.PICKUP_START_DTTM,
                    OUTPT_ORDER_LINE_ITEM.PLANNED_QUANTITY,
                    OUTPT_ORDER_LINE_ITEM.PLANNED_QUANTITY_UOM,
                    OUTPT_ORDER_LINE_ITEM.PLANNED_SHIP_DATE,
                    OUTPT_ORDER_LINE_ITEM.PPACK_GRP_CODE,
                    OUTPT_ORDER_LINE_ITEM.PPACK_QTY,
                    OUTPT_ORDER_LINE_ITEM.PRICE,
                    OUTPT_ORDER_LINE_ITEM.PRICE_TKT_TYPE,
                    OUTPT_ORDER_LINE_ITEM.PROC_DTTM,
                    OUTPT_ORDER_LINE_ITEM.PROC_STAT_CODE,
                    OUTPT_ORDER_LINE_ITEM.PRODUCT_STATUS,
                    OUTPT_ORDER_LINE_ITEM.PURCHASE_ORDER_LINE_NUMBER,
                    OUTPT_ORDER_LINE_ITEM.REASON_CODE,
                    OUTPT_ORDER_LINE_ITEM.REC_PROC_FLAG,
                    OUTPT_ORDER_LINE_ITEM.REC_XPANS_FIELD,
                    OUTPT_ORDER_LINE_ITEM.RETAIL_PRICE,
                    OUTPT_ORDER_LINE_ITEM.SHIPPED_QTY,
                    OUTPT_ORDER_LINE_ITEM.TC_COMPANY_ID,
                    OUTPT_ORDER_LINE_ITEM.UNIT_VOL,
                    OUTPT_ORDER_LINE_ITEM.UNIT_WT,
                    OUTPT_ORDER_LINE_ITEM.UOM,
                    OUTPT_ORDER_LINE_ITEM.USER_CANCELED_QTY,
                    OUTPT_ORDER_LINE_ITEM.VERSION_NBR,
                    OUTPT_ORDER_LINE_ITEM.AISLE,
                    OUTPT_ORDER_LINE_ITEM.AREA,
                    OUTPT_ORDER_LINE_ITEM.BAY,
                    OUTPT_ORDER_LINE_ITEM.LVL,
                    OUTPT_ORDER_LINE_ITEM.POSN,
                    OUTPT_ORDER_LINE_ITEM.ZONE,
                    OUTPT_ORDER_LINE_ITEM.ITEM_COLOR,
                    OUTPT_ORDER_LINE_ITEM.ITEM_COLOR_SFX,
                    OUTPT_ORDER_LINE_ITEM.ITEM_SEASON,
                    OUTPT_ORDER_LINE_ITEM.ITEM_SEASON_YEAR,
                    OUTPT_ORDER_LINE_ITEM.ITEM_SECOND_DIM,
                    OUTPT_ORDER_LINE_ITEM.ITEM_SIZE_DESC,
                    OUTPT_ORDER_LINE_ITEM.ITEM_QUALITY,
                    OUTPT_ORDER_LINE_ITEM.ITEM_STYLE,
                    OUTPT_ORDER_LINE_ITEM.ITEM_STYLE_SFX,
                    OUTPT_ORDER_LINE_ITEM.SIZE_RANGE_CODE,
                    OUTPT_ORDER_LINE_ITEM.SIZE_REL_POSN_IN_TABLE,
                    OUTPT_ORDER_LINE_ITEM.SHELF_DAYS,
                    OUTPT_ORDER_LINE_ITEM.TEMP_ZONE,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ITEM_COLOR,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ITEM_COLOR_SFX,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ITEM_SEASON,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ITEM_SEASON_YEAR,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ITEM_SECOND_DIM,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ITEM_SIZE_DESC,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ITEM_QUALITY,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ITEM_STYLE,
                    OUTPT_ORDER_LINE_ITEM.ORIG_ITEM_STYLE_SFX,
                    OUTPT_ORDER_LINE_ITEM.TC_ORDER_ID,
                    OUTPT_ORDER_LINE_ITEM.TC_PURCHASE_ORDERS_ID,
                    OUTPT_ORDER_LINE_ITEM.ITEM_NAME,
                    OUTPT_ORDER_LINE_ITEM.ACTUAL_COST,
                    OUTPT_ORDER_LINE_ITEM.TC_ORDER_LINE_ID,
                    OUTPT_ORDER_LINE_ITEM.PURCHASE_ORDER_NUMBER,
                    OUTPT_ORDER_LINE_ITEM.EXT_PURCHASE_ORDER
                FROM {source_schema}.OUTPT_ORDER_LINE_ITEM
                WHERE  (trunc(OUTPT_ORDER_LINE_ITEM.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(OUTPT_ORDER_LINE_ITEM.LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""

    SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM is executed and data is loaded using jdbc")

    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 100
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM_temp = SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM.toDF(*["SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___" + col for col in SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM.columns])
    
    EXPTRANS = SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM_temp.selectExpr( \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___sys_row_id as sys_row_id", \
    	f"{dcnbr} as DC_NBR_EXP", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ACTUAL_COST_CURRENCY_CODE as ACTUAL_COST_CURRENCY_CODE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ACTUAL_SHIPPED_DTTM as ACTUAL_SHIPPED_DTTM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ASSORT_NBR as ASSORT_NBR", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___BACK_ORD_QTY as BACK_ORD_QTY", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___BATCH_CTRL_NBR as BATCH_CTRL_NBR", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___BATCH_NBR as BATCH_NBR", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___COMMODITY_CLASS as COMMODITY_CLASS", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___CREATED_DTTM as CREATED_DTTM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___CREATED_SOURCE as CREATED_SOURCE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___CUSTOMER_ITEM as CUSTOMER_ITEM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___EXP_INFO_CODE as EXP_INFO_CODE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___GTIN as GTIN", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___INVC_BATCH_NBR as INVC_BATCH_NBR", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___INVN_TYPE as INVN_TYPE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_ATTR_1 as ITEM_ATTR_1", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_ATTR_2 as ITEM_ATTR_2", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_ATTR_3 as ITEM_ATTR_3", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_ATTR_4 as ITEM_ATTR_4", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_ATTR_5 as ITEM_ATTR_5", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_ID as ITEM_ID", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___LINE_ITEM_ID as LINE_ITEM_ID", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___LINE_TYPE as LINE_TYPE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___MANUFACTURING_DTTM as MANUFACTURING_DTTM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___MISC_INSTR_10_BYTE_1 as MISC_INSTR_10_BYTE_1", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___MISC_INSTR_10_BYTE_2 as MISC_INSTR_10_BYTE_2", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORDER_QTY as ORDER_QTY", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORDER_QTY_UOM as ORDER_QTY_UOM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ITEM_ID as ORIG_ITEM_ID", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ORDER_LINE_ITEM_ID as ORIG_ORDER_LINE_ITEM_ID", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ORDER_QTY as ORIG_ORDER_QTY", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ORDER_QTY_UOM as ORIG_ORDER_QTY_UOM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___OUTPT_ORDER_LINE_ITEM_ID as OUTPT_ORDER_LINE_ITEM_ID", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PARENT_LINE_ITEM_ID as PARENT_LINE_ITEM_ID", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PICKUP_END_DTTM as PICKUP_END_DTTM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PICKUP_REFERENCE_NUMBER as PICKUP_REFERENCE_NUMBER", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PICKUP_START_DTTM as PICKUP_START_DTTM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PLANNED_QUANTITY as PLANNED_QUANTITY", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PLANNED_QUANTITY_UOM as PLANNED_QUANTITY_UOM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PLANNED_SHIP_DATE as PLANNED_SHIP_DATE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PPACK_GRP_CODE as PPACK_GRP_CODE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PPACK_QTY as PPACK_QTY", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PRICE as PRICE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PRICE_TKT_TYPE as PRICE_TKT_TYPE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PROC_DTTM as PROC_DTTM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PROC_STAT_CODE as PROC_STAT_CODE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PRODUCT_STATUS as PRODUCT_STATUS", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PURCHASE_ORDER_LINE_NUMBER as PURCHASE_ORDER_LINE_NUMBER", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___REASON_CODE as REASON_CODE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___REC_PROC_FLAG as REC_PROC_FLAG", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___REC_XPANS_FIELD as REC_XPANS_FIELD", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___RETAIL_PRICE as RETAIL_PRICE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___SHIPPED_QTY as SHIPPED_QTY", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___TC_COMPANY_ID as TC_COMPANY_ID", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___UNIT_VOL as UNIT_VOL", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___UNIT_WT as UNIT_WT", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___UOM as UOM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___USER_CANCELED_QTY as USER_CANCELED_QTY", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___VERSION_NBR as VERSION_NBR", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___AISLE as AISLE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___AREA as AREA", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___BAY as BAY", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___LVL as LVL", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___POSN as POSN", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ZONE as ZONE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_COLOR as ITEM_COLOR", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_COLOR_SFX as ITEM_COLOR_SFX", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_SEASON as ITEM_SEASON", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_SEASON_YEAR as ITEM_SEASON_YEAR", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_SECOND_DIM as ITEM_SECOND_DIM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_SIZE_DESC as ITEM_SIZE_DESC", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_QUALITY as ITEM_QUALITY", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_STYLE as ITEM_STYLE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_STYLE_SFX as ITEM_STYLE_SFX", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___SHELF_DAYS as SHELF_DAYS", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___TEMP_ZONE as TEMP_ZONE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ITEM_COLOR as ORIG_ITEM_COLOR", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ITEM_COLOR_SFX as ORIG_ITEM_COLOR_SFX", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ITEM_SEASON as ORIG_ITEM_SEASON", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ITEM_SEASON_YEAR as ORIG_ITEM_SEASON_YEAR", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ITEM_SECOND_DIM as ORIG_ITEM_SECOND_DIM", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ITEM_SIZE_DESC as ORIG_ITEM_SIZE_DESC", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ITEM_QUALITY as ORIG_ITEM_QUALITY", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ITEM_STYLE as ORIG_ITEM_STYLE", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ORIG_ITEM_STYLE_SFX as ORIG_ITEM_STYLE_SFX", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___TC_ORDER_ID as TC_ORDER_ID", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___TC_PURCHASE_ORDERS_ID as TC_PURCHASE_ORDERS_ID", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ITEM_NAME as ITEM_NAME", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___ACTUAL_COST as ACTUAL_COST", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___TC_ORDER_LINE_ID as TC_ORDER_LINE_ID", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___PURCHASE_ORDER_NUMBER as PURCHASE_ORDER_NUMBER", \
    	"SQ_Shortcut_to_OUTPT_ORDER_LINE_ITEM___EXT_PURCHASE_ORDER as EXT_PURCHASE_ORDER", \
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_OUTPT_ORDER_LINE_ITEM_PRE, type TARGET 
    # COLUMN COUNT: 100
    
    
    Shortcut_to_WM_OUTPT_ORDER_LINE_ITEM_PRE = EXPTRANS.selectExpr( \
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
    	"CAST(OUTPT_ORDER_LINE_ITEM_ID AS BIGINT) as OUTPT_ORDER_LINE_ITEM_ID", \
    	"CAST(ACTUAL_COST_CURRENCY_CODE AS STRING) as ACTUAL_COST_CURRENCY_CODE", \
    	"CAST(ACTUAL_SHIPPED_DTTM AS TIMESTAMP) as ACTUAL_SHIPPED_DTTM", \
    	"CAST(ASSORT_NBR AS STRING) as ASSORT_NBR", \
    	"CAST(BACK_ORD_QTY AS BIGINT) as BACK_ORD_QTY", \
    	"CAST(BATCH_CTRL_NBR AS STRING) as BATCH_CTRL_NBR", \
    	"CAST(BATCH_NBR AS STRING) as BATCH_NBR", \
    	"CAST(CNTRY_OF_ORGN AS STRING) as CNTRY_OF_ORGN", \
    	"CAST(COMMODITY_CLASS AS STRING) as COMMODITY_CLASS", \
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
    	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
    	"CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", \
    	"CAST(CUSTOMER_ITEM AS STRING) as CUSTOMER_ITEM", \
    	"CAST(EXP_INFO_CODE AS STRING) as EXP_INFO_CODE", \
    	"CAST(GTIN AS STRING) as GTIN", \
    	"CAST(INVC_BATCH_NBR AS BIGINT) as INVC_BATCH_NBR", \
    	"CAST(INVN_TYPE AS STRING) as INVN_TYPE", \
    	"CAST(ITEM_ATTR_1 AS STRING) as ITEM_ATTR_1", \
    	"CAST(ITEM_ATTR_2 AS STRING) as ITEM_ATTR_2", \
    	"CAST(ITEM_ATTR_3 AS STRING) as ITEM_ATTR_3", \
    	"CAST(ITEM_ATTR_4 AS STRING) as ITEM_ATTR_4", \
    	"CAST(ITEM_ATTR_5 AS STRING) as ITEM_ATTR_5", \
    	"CAST(ITEM_ID AS BIGINT) as ITEM_ID", \
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
    	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
    	"CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", \
    	"CAST(LINE_ITEM_ID AS BIGINT) as LINE_ITEM_ID", \
    	"CAST(LINE_TYPE AS STRING) as LINE_TYPE", \
    	"CAST(MANUFACTURING_DTTM AS TIMESTAMP) as MANUFACTURING_DTTM", \
    	"CAST(MISC_INSTR_10_BYTE_1 AS STRING) as MISC_INSTR_10_BYTE_1", \
    	"CAST(MISC_INSTR_10_BYTE_2 AS STRING) as MISC_INSTR_10_BYTE_2", \
    	"CAST(ORDER_QTY AS BIGINT) as ORDER_QTY", \
    	"CAST(ORDER_QTY_UOM AS STRING) as ORDER_QTY_UOM", \
    	"CAST(ORIG_ITEM_ID AS BIGINT) as ORIG_ITEM_ID", \
    	"CAST(ORIG_ORDER_LINE_ITEM_ID AS BIGINT) as ORIG_ORDER_LINE_ITEM_ID", \
    	"CAST(ORIG_ORDER_QTY AS BIGINT) as ORIG_ORDER_QTY", \
    	"CAST(ORIG_ORDER_QTY_UOM AS STRING) as ORIG_ORDER_QTY_UOM", \
    	"CAST(PARENT_LINE_ITEM_ID AS BIGINT) as PARENT_LINE_ITEM_ID", \
    	"CAST(PICKUP_END_DTTM AS TIMESTAMP) as PICKUP_END_DTTM", \
    	"CAST(PICKUP_REFERENCE_NUMBER AS STRING) as PICKUP_REFERENCE_NUMBER", \
    	"CAST(PICKUP_START_DTTM AS TIMESTAMP) as PICKUP_START_DTTM", \
    	"CAST(PLANNED_QUANTITY AS BIGINT) as PLANNED_QUANTITY", \
    	"CAST(PLANNED_QUANTITY_UOM AS STRING) as PLANNED_QUANTITY_UOM", \
    	"CAST(PLANNED_SHIP_DATE AS TIMESTAMP) as PLANNED_SHIP_DATE", \
    	"CAST(PPACK_GRP_CODE AS STRING) as PPACK_GRP_CODE", \
    	"CAST(PPACK_QTY AS BIGINT) as PPACK_QTY", \
    	"CAST(PRICE AS BIGINT) as PRICE", \
    	"CAST(PRICE_TKT_TYPE AS STRING) as PRICE_TKT_TYPE", \
    	"CAST(PROC_DTTM AS TIMESTAMP) as PROC_DTTM", \
    	"CAST(PROC_STAT_CODE AS BIGINT) as PROC_STAT_CODE", \
    	"CAST(PRODUCT_STATUS AS STRING) as PRODUCT_STATUS", \
    	"CAST(PURCHASE_ORDER_LINE_NUMBER AS STRING) as PURCHASE_ORDER_LINE_NUMBER", \
    	"CAST(REASON_CODE AS STRING) as REASON_CODE", \
    	"CAST(REC_PROC_FLAG AS STRING) as REC_PROC_FLAG", \
    	"CAST(REC_XPANS_FIELD AS STRING) as REC_XPANS_FIELD", \
    	"CAST(RETAIL_PRICE AS BIGINT) as RETAIL_PRICE", \
    	"CAST(SHIPPED_QTY AS BIGINT) as SHIPPED_QTY", \
    	"CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", \
    	"CAST(UNIT_VOL AS BIGINT) as UNIT_VOL", \
    	"CAST(UNIT_WT AS BIGINT) as UNIT_WT", \
    	"CAST(UOM AS STRING) as UOM", \
    	"CAST(USER_CANCELED_QTY AS BIGINT) as USER_CANCELED_QTY", \
    	"CAST(VERSION_NBR AS BIGINT) as VERSION_NBR", \
    	"CAST(AISLE AS STRING) as AISLE", \
    	"CAST(AREA AS STRING) as AREA", \
    	"CAST(BAY AS STRING) as BAY", \
    	"CAST(LVL AS STRING) as LVL", \
    	"CAST(POSN AS STRING) as POSN", \
    	"CAST(ZONE AS STRING) as ZONE", \
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
    	"CAST(SHELF_DAYS AS BIGINT) as SHELF_DAYS", \
    	"CAST(TEMP_ZONE AS STRING) as TEMP_ZONE", \
    	"CAST(ORIG_ITEM_COLOR AS STRING) as ORIG_ITEM_COLOR", \
    	"CAST(ORIG_ITEM_COLOR_SFX AS STRING) as ORIG_ITEM_COLOR_SFX", \
    	"CAST(ORIG_ITEM_SEASON AS STRING) as ORIG_ITEM_SEASON", \
    	"CAST(ORIG_ITEM_SEASON_YEAR AS STRING) as ORIG_ITEM_SEASON_YEAR", \
    	"CAST(ORIG_ITEM_SECOND_DIM AS STRING) as ORIG_ITEM_SECOND_DIM", \
    	"CAST(ORIG_ITEM_SIZE_DESC AS STRING) as ORIG_ITEM_SIZE_DESC", \
    	"CAST(ORIG_ITEM_QUALITY AS STRING) as ORIG_ITEM_QUALITY", \
    	"CAST(ORIG_ITEM_STYLE AS STRING) as ORIG_ITEM_STYLE", \
    	"CAST(ORIG_ITEM_STYLE_SFX AS STRING) as ORIG_ITEM_STYLE_SFX", \
    	"CAST(TC_ORDER_ID AS STRING) as TC_ORDER_ID", \
    	"CAST(TC_PURCHASE_ORDERS_ID AS STRING) as TC_PURCHASE_ORDERS_ID", \
    	"CAST(ITEM_NAME AS STRING) as ITEM_NAME", \
    	"CAST(ACTUAL_COST AS BIGINT) as ACTUAL_COST", \
    	"CAST(TC_ORDER_LINE_ID AS STRING) as TC_ORDER_LINE_ID", \
    	"CAST(PURCHASE_ORDER_NUMBER AS STRING) as PURCHASE_ORDER_NUMBER", \
    	"CAST(EXT_PURCHASE_ORDER AS STRING) as EXT_PURCHASE_ORDER", \
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_OUTPT_ORDER_LINE_ITEM_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_OUTPT_ORDER_LINE_ITEM_PRE is written to the target table - " + target_table_name)
