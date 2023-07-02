#Code converted on 2023-06-22 20:24:57
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



def m_WM_Resv_Locn_Hdr_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Resv_Locn_Hdr_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_RESV_LOCN_HDR_PRE"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_RESV_LOCN_HDR"
    prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + prev_run_dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
                    RESV_LOCN_HDR.LOCN_ID,
                    RESV_LOCN_HDR.LOCN_SIZE_TYPE,
                    RESV_LOCN_HDR.LOCN_PUTAWAY_LOCK,
                    RESV_LOCN_HDR.INVN_LOCK_CODE,
                    RESV_LOCN_HDR.CURR_WT,
                    RESV_LOCN_HDR.DIRCT_WT,
                    RESV_LOCN_HDR.MAX_WT,
                    RESV_LOCN_HDR.CURR_VOL,
                    RESV_LOCN_HDR.DIRCT_VOL,
                    RESV_LOCN_HDR.MAX_VOL,
                    RESV_LOCN_HDR.CURR_UOM_QTY,
                    RESV_LOCN_HDR.DIRCT_UOM_QTY,
                    RESV_LOCN_HDR.MAX_UOM_QTY,
                    RESV_LOCN_HDR.CREATE_DATE_TIME,
                    RESV_LOCN_HDR.MOD_DATE_TIME,
                    RESV_LOCN_HDR.USER_ID,
                    RESV_LOCN_HDR.DEDCTN_BATCH_NBR,
                    RESV_LOCN_HDR.DEDCTN_PACK_QTY,
                    RESV_LOCN_HDR.PACK_ZONE,
                    RESV_LOCN_HDR.SORT_LOCN_FLAG,
                    RESV_LOCN_HDR.INBD_STAGING_FLAG,
                    RESV_LOCN_HDR.RESV_LOCN_HDR_ID,
                    RESV_LOCN_HDR.WM_VERSION_ID,
                    RESV_LOCN_HDR.DEDCTN_ITEM_ID,
                    RESV_LOCN_HDR.LOCN_HDR_ID
                FROM RESV_LOCN_HDR
                WHERE {Initial_Load} (TRUNC(CREATE_DATE_TIME) >= TRUNC(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-14) OR (TRUNC(MOD_DATE_TIME) >=  TRUNC(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-14)"""
    

    spark.read.format('jdbc').option('url', connection_string).option( = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for spark.read.format('jdbc').option('url', connection_string).option( is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 27
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_RESV_LOCN_HDR_temp = SQ_Shortcut_to_RESV_LOCN_HDR.toDF(*["SQ_Shortcut_to_RESV_LOCN_HDR___" + col for col in SQ_Shortcut_to_RESV_LOCN_HDR.columns])
    
    EXPTRANS = SQ_Shortcut_to_RESV_LOCN_HDR_temp.selectExpr( 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___sys_row_id as sys_row_id", 
    	f"{DC_NBR} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___LOCN_ID as LOCN_ID", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___LOCN_SIZE_TYPE as LOCN_SIZE_TYPE", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___LOCN_PUTAWAY_LOCK as LOCN_PUTAWAY_LOCK", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___INVN_LOCK_CODE as INVN_LOCK_CODE", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___CURR_WT as CURR_WT", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___DIRCT_WT as DIRCT_WT", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___MAX_WT as MAX_WT", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___CURR_VOL as CURR_VOL", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___DIRCT_VOL as DIRCT_VOL", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___MAX_VOL as MAX_VOL", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___CURR_UOM_QTY as CURR_UOM_QTY", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___DIRCT_UOM_QTY as DIRCT_UOM_QTY", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___MAX_UOM_QTY as MAX_UOM_QTY", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___CREATE_DATE_TIME as CREATE_DATE_TIME", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___MOD_DATE_TIME as MOD_DATE_TIME", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___USER_ID as USER_ID", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___DEDCTN_BATCH_NBR as DEDCTN_BATCH_NBR", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___DEDCTN_PACK_QTY as DEDCTN_PACK_QTY", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___PACK_ZONE as PACK_ZONE", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___SORT_LOCN_FLAG as SORT_LOCN_FLAG", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___INBD_STAGING_FLAG as INBD_STAGING_FLAG", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___RESV_LOCN_HDR_ID as RESV_LOCN_HDR_ID", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___WM_VERSION_ID as WM_VERSION_ID", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___DEDCTN_ITEM_ID as DEDCTN_ITEM_ID", 
    	"SQ_Shortcut_to_RESV_LOCN_HDR___LOCN_HDR_ID as LOCN_HDR_ID", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_RESV_LOCN_HDR_PRE, type TARGET 
    # COLUMN COUNT: 27
    
    
    Shortcut_to_WM_RESV_LOCN_HDR_PRE = EXPTRANS.selectExpr( 
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
    	"CAST(RESV_LOCN_HDR_ID AS BIGINT) as RESV_LOCN_HDR_ID", 
    	"CAST(LOCN_ID AS STRING) as LOCN_ID", 
    	"CAST(LOCN_SIZE_TYPE AS STRING) as LOCN_SIZE_TYPE", 
    	"CAST(LOCN_PUTAWAY_LOCK AS STRING) as LOCN_PUTAWAY_LOCK", 
    	"CAST(INVN_LOCK_CODE AS STRING) as INVN_LOCK_CODE", 
    	"CAST(CURR_WT AS BIGINT) as CURR_WT", 
    	"CAST(DIRCT_WT AS BIGINT) as DIRCT_WT", 
    	"CAST(MAX_WT AS BIGINT) as MAX_WT", 
    	"CAST(CURR_VOL AS BIGINT) as CURR_VOL", 
    	"CAST(DIRCT_VOL AS BIGINT) as DIRCT_VOL", 
    	"CAST(MAX_VOL AS BIGINT) as MAX_VOL", 
    	"CAST(CURR_UOM_QTY AS BIGINT) as CURR_UOM_QTY", 
    	"CAST(DIRCT_UOM_QTY AS BIGINT) as DIRCT_UOM_QTY", 
    	"CAST(MAX_UOM_QTY AS BIGINT) as MAX_UOM_QTY", 
    	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
    	"CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
    	"CAST(USER_ID AS STRING) as USER_ID", 
    	"CAST(DEDCTN_BATCH_NBR AS STRING) as DEDCTN_BATCH_NBR", 
    	"CAST(DEDCTN_PACK_QTY AS BIGINT) as DEDCTN_PACK_QTY", 
    	"CAST(PACK_ZONE AS STRING) as PACK_ZONE", 
    	"CAST(SORT_LOCN_FLAG AS BIGINT) as SORT_LOCN_FLAG", 
    	"CAST(INBD_STAGING_FLAG AS STRING) as INBD_STAGING_FLAG", 
    	"CAST(WM_VERSION_ID AS BIGINT) as WM_VERSION_ID", 
    	"CAST(DEDCTN_ITEM_ID AS BIGINT) as DEDCTN_ITEM_ID", 
    	"CAST(LOCN_HDR_ID AS BIGINT) as LOCN_HDR_ID", 
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    gu.overwriteDeltaPartition(Shortcut_to_WM_RESV_LOCN_HDR_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_RESV_LOCN_HDR_PRE is written to the target table - " + target_table_name)
