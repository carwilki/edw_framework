#Code converted on 2023-06-26 17:03:50
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
from utils.logger import *



def m_WM_Picking_Short_Item_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Picking_Short_Item_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_PICKING_SHORT_ITEM_PRE"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_PICKING_SHORT_ITEM"
    prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + prev_run_dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    query = f"""SELECT
                    PICKING_SHORT_ITEM.PICKING_SHORT_ITEM_ID,
                    PICKING_SHORT_ITEM.ITEM_ID,
                    PICKING_SHORT_ITEM.LOCN_ID,
                    PICKING_SHORT_ITEM.LINE_ITEM_ID,
                    PICKING_SHORT_ITEM.TC_ORDER_ID,
                    PICKING_SHORT_ITEM.WAVE_NBR,
                    PICKING_SHORT_ITEM.SHORT_QTY,
                    PICKING_SHORT_ITEM.STAT_CODE,
                    PICKING_SHORT_ITEM.TC_COMPANY_ID,
                    PICKING_SHORT_ITEM.CREATED_DTTM,
                    PICKING_SHORT_ITEM.CREATED_SOURCE,
                    PICKING_SHORT_ITEM.LAST_UPDATED_DTTM,
                    PICKING_SHORT_ITEM.LAST_UPDATED_SOURCE,
                    PICKING_SHORT_ITEM.SHORT_TYPE,
                    PICKING_SHORT_ITEM.TC_LPN_ID,
                    PICKING_SHORT_ITEM.LPN_DETAIL_ID,
                    PICKING_SHORT_ITEM.REQD_INVN_TYPE,
                    PICKING_SHORT_ITEM.REQD_PROD_STAT,
                    PICKING_SHORT_ITEM.REQD_BATCH_NBR,
                    PICKING_SHORT_ITEM.REQD_SKU_ATTR_1,
                    PICKING_SHORT_ITEM.REQD_SKU_ATTR_2,
                    PICKING_SHORT_ITEM.REQD_SKU_ATTR_3,
                    PICKING_SHORT_ITEM.REQD_SKU_ATTR_4,
                    PICKING_SHORT_ITEM.REQD_SKU_ATTR_5,
                    PICKING_SHORT_ITEM.REQD_CNTRY_OF_ORGN,
                    PICKING_SHORT_ITEM.SHIPMENT_ID,
                    PICKING_SHORT_ITEM.TC_SHIPMENT_ID
                FROM PICKING_SHORT_ITEM
                WHERE {Initial_Load} (trunc(PICKING_SHORT_ITEM.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (trunc(PICKING_SHORT_ITEM.LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1)"""


    SQ_Shortcut_to_PICKING_SHORT_ITEM = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_PICKING_SHORT_ITEM is executed and data is loaded using jdbc")

    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 29
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_PICKING_SHORT_ITEM_temp = SQ_Shortcut_to_PICKING_SHORT_ITEM.toDF(*["SQ_Shortcut_to_PICKING_SHORT_ITEM___" + col for col in SQ_Shortcut_to_PICKING_SHORT_ITEM.columns])
    
    EXPTRANS = SQ_Shortcut_to_PICKING_SHORT_ITEM_temp.selectExpr( \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___sys_row_id as sys_row_id", \
    	f"{DC_NBR} as DC_NBR_EXP", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___PICKING_SHORT_ITEM_ID as PICKING_SHORT_ITEM_ID", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___ITEM_ID as ITEM_ID", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___LOCN_ID as LOCN_ID", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___LINE_ITEM_ID as LINE_ITEM_ID", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___TC_ORDER_ID as TC_ORDER_ID", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___WAVE_NBR as WAVE_NBR", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___SHORT_QTY as SHORT_QTY", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___STAT_CODE as STAT_CODE", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___TC_COMPANY_ID as TC_COMPANY_ID", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___CREATED_DTTM as CREATED_DTTM", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___CREATED_SOURCE as CREATED_SOURCE", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___SHORT_TYPE as SHORT_TYPE", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___TC_LPN_ID as TC_LPN_ID", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___LPN_DETAIL_ID as LPN_DETAIL_ID", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___REQD_INVN_TYPE as REQD_INVN_TYPE", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___REQD_PROD_STAT as REQD_PROD_STAT", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___REQD_BATCH_NBR as REQD_BATCH_NBR", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___REQD_SKU_ATTR_1 as REQD_SKU_ATTR_1", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___REQD_SKU_ATTR_2 as REQD_SKU_ATTR_2", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___REQD_SKU_ATTR_3 as REQD_SKU_ATTR_3", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___REQD_SKU_ATTR_4 as REQD_SKU_ATTR_4", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___REQD_SKU_ATTR_5 as REQD_SKU_ATTR_5", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___REQD_CNTRY_OF_ORGN as REQD_CNTRY_OF_ORGN", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___SHIPMENT_ID as SHIPMENT_ID", \
    	"SQ_Shortcut_to_PICKING_SHORT_ITEM___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_PICKING_SHORT_ITEM_PRE, type TARGET 
    # COLUMN COUNT: 29
    
    
    Shortcut_to_WM_PICKING_SHORT_ITEM_PRE = EXPTRANS.selectExpr( \
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
    	"CAST(PICKING_SHORT_ITEM_ID AS BIGINT) as PICKING_SHORT_ITEM_ID", \
    	"CAST(ITEM_ID AS BIGINT) as ITEM_ID", \
    	"CAST(LOCN_ID AS STRING) as LOCN_ID", \
    	"CAST(LINE_ITEM_ID AS BIGINT) as LINE_ITEM_ID", \
    	"CAST(TC_ORDER_ID AS STRING) as TC_ORDER_ID", \
    	"CAST(WAVE_NBR AS STRING) as WAVE_NBR", \
    	"CAST(SHORT_QTY AS BIGINT) as SHORT_QTY", \
    	"CAST(STAT_CODE AS BIGINT) as STAT_CODE", \
    	"CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", \
    	"CAST(CREATED_DTTM AS DATE) as CREATED_DTTM", \
    	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
    	"CAST(LAST_UPDATED_DTTM AS DATE) as LAST_UPDATED_DTTM", \
    	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
    	"CAST(SHORT_TYPE AS STRING) as SHORT_TYPE", \
    	"CAST(TC_LPN_ID AS STRING) as TC_LPN_ID", \
    	"CAST(LPN_DETAIL_ID AS BIGINT) as LPN_DETAIL_ID", \
    	"CAST(REQD_INVN_TYPE AS STRING) as REQD_INVN_TYPE", \
    	"CAST(REQD_PROD_STAT AS STRING) as REQD_PROD_STAT", \
    	"CAST(REQD_BATCH_NBR AS STRING) as REQD_BATCH_NBR", \
    	"CAST(REQD_SKU_ATTR_1 AS STRING) as REQD_SKU_ATTR_1", \
    	"CAST(REQD_SKU_ATTR_2 AS STRING) as REQD_SKU_ATTR_2", \
    	"CAST(REQD_SKU_ATTR_3 AS STRING) as REQD_SKU_ATTR_3", \
    	"CAST(REQD_SKU_ATTR_4 AS STRING) as REQD_SKU_ATTR_4", \
    	"CAST(REQD_SKU_ATTR_5 AS STRING) as REQD_SKU_ATTR_5", \
    	"CAST(REQD_CNTRY_OF_ORGN AS STRING) as REQD_CNTRY_OF_ORGN", \
    	"CAST(SHIPMENT_ID AS BIGINT) as SHIPMENT_ID", \
    	"CAST(TC_SHIPMENT_ID AS STRING) as TC_SHIPMENT_ID", \
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    gu.overwriteDeltaPartition(Shortcut_to_WM_PICKING_SHORT_ITEM_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_PICKING_SHORT_ITEM_PRE is written to the target table - " + target_table_name)
