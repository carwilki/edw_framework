#Code converted on 2023-06-26 17:03:43
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



def m_WM_Pick_Locn_Dtl_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Pick_Locn_Dtl_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_PICK_LOCN_DTL_PRE"
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
                    PICK_LOCN_DTL.LOCN_ID,
                    PICK_LOCN_DTL.LOCN_SEQ_NBR,
                    PICK_LOCN_DTL.SKU_ATTR_1,
                    PICK_LOCN_DTL.SKU_ATTR_2,
                    PICK_LOCN_DTL.SKU_ATTR_3,
                    PICK_LOCN_DTL.SKU_ATTR_4,
                    PICK_LOCN_DTL.SKU_ATTR_5,
                    PICK_LOCN_DTL.INVN_TYPE,
                    PICK_LOCN_DTL.PROD_STAT,
                    PICK_LOCN_DTL.BATCH_NBR,
                    PICK_LOCN_DTL.CNTRY_OF_ORGN,
                    PICK_LOCN_DTL.MAX_INVN_QTY,
                    PICK_LOCN_DTL.MIN_INVN_QTY,
                    PICK_LOCN_DTL.ACTL_INVN_CASES,
                    PICK_LOCN_DTL.MIN_INVN_CASES,
                    PICK_LOCN_DTL.MAX_INVN_CASES,
                    PICK_LOCN_DTL.TRIG_REPL_FOR_SKU,
                    PICK_LOCN_DTL.PRIM_LOCN_FOR_SKU,
                    PICK_LOCN_DTL.FIRST_WAVE_NBR,
                    PICK_LOCN_DTL.LAST_WAVE_NBR,
                    PICK_LOCN_DTL.LTST_SKU_ASSIGN,
                    PICK_LOCN_DTL.CREATE_DATE_TIME,
                    PICK_LOCN_DTL.MOD_DATE_TIME,
                    PICK_LOCN_DTL.USER_ID,
                    PICK_LOCN_DTL.LTST_PICK_ASSIGN_DATE_TIME,
                    PICK_LOCN_DTL.PIKNG_LOCK_CODE,
                    PICK_LOCN_DTL.TO_BE_FILLD_CASES,
                    PICK_LOCN_DTL.LANES,
                    PICK_LOCN_DTL.STACKING,
                    PICK_LOCN_DTL.MIN_QTY_TO_RLS_HELD_RPLN,
                    PICK_LOCN_DTL.MIN_CASES_TO_RLS_HELD_RPLN,
                    PICK_LOCN_DTL.TASK_RELEASED,
                    PICK_LOCN_DTL.PACK_QTY,
                    PICK_LOCN_DTL.PICK_LOCN_DTL_ID,
                    PICK_LOCN_DTL.PICK_LOCN_HDR_ID,
                    PICK_LOCN_DTL.WM_VERSION_ID,
                    PICK_LOCN_DTL.ITEM_MASTER_ID,
                    PICK_LOCN_DTL.ITEM_ID,
                    PICK_LOCN_DTL.PRE_ALLOCATED_QTY,
                    PICK_LOCN_DTL.REPLEN_CONTROL,
                    PICK_LOCN_DTL.STOP_QTY,
                    PICK_LOCN_DTL.STOP_DTTM,
                    PICK_LOCN_DTL.PICK_TO_ZERO_ACTION,
                    PICK_LOCN_DTL.UTIL_PERCENT,
                    PICK_LOCN_DTL.CREATED_DTTM,
                    PICK_LOCN_DTL.LAST_UPDATED_DTTM
                FROM {source_schema}.PICK_LOCN_DTL
                WHERE  (trunc(PICK_LOCN_DTL.CREATE_DATE_TIME) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (trunc(PICK_LOCN_DTL.MOD_DATE_TIME) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (trunc(PICK_LOCN_DTL.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (trunc(PICK_LOCN_DTL.LAST_UPDATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14)"""

    SQ_Shortcut_to_PICK_LOCN_DTL = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_PICK_LOCN_DTL is executed and data is loaded using jdbc")

    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 48
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_PICK_LOCN_DTL_temp = SQ_Shortcut_to_PICK_LOCN_DTL.toDF(*["SQ_Shortcut_to_PICK_LOCN_DTL___" + col for col in SQ_Shortcut_to_PICK_LOCN_DTL.columns])
    
    EXPTRANS = SQ_Shortcut_to_PICK_LOCN_DTL_temp.selectExpr( \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___sys_row_id as sys_row_id", \
    	f"{dcnbr} as DC_NBR_EXP", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___LOCN_ID as LOCN_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___LOCN_SEQ_NBR as LOCN_SEQ_NBR", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___SKU_ATTR_1 as SKU_ATTR_1", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___SKU_ATTR_2 as SKU_ATTR_2", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___SKU_ATTR_3 as SKU_ATTR_3", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___SKU_ATTR_4 as SKU_ATTR_4", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___SKU_ATTR_5 as SKU_ATTR_5", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___INVN_TYPE as INVN_TYPE", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___PROD_STAT as PROD_STAT", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___BATCH_NBR as BATCH_NBR", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___MAX_INVN_QTY as MAX_INVN_QTY", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___MIN_INVN_QTY as MIN_INVN_QTY", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___ACTL_INVN_CASES as ACTL_INVN_CASES", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___MIN_INVN_CASES as MIN_INVN_CASES", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___MAX_INVN_CASES as MAX_INVN_CASES", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___TRIG_REPL_FOR_SKU as TRIG_REPL_FOR_SKU", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___PRIM_LOCN_FOR_SKU as PRIM_LOCN_FOR_SKU", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___FIRST_WAVE_NBR as FIRST_WAVE_NBR", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___LAST_WAVE_NBR as LAST_WAVE_NBR", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___LTST_SKU_ASSIGN as LTST_SKU_ASSIGN", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___CREATE_DATE_TIME as CREATE_DATE_TIME", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___MOD_DATE_TIME as MOD_DATE_TIME", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___USER_ID as USER_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___LTST_PICK_ASSIGN_DATE_TIME as LTST_PICK_ASSIGN_DATE_TIME", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___PIKNG_LOCK_CODE as PIKNG_LOCK_CODE", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___TO_BE_FILLD_CASES as TO_BE_FILLD_CASES", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___LANES as LANES", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___STACKING as STACKING", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___MIN_QTY_TO_RLS_HELD_RPLN as MIN_QTY_TO_RLS_HELD_RPLN", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___MIN_CASES_TO_RLS_HELD_RPLN as MIN_CASES_TO_RLS_HELD_RPLN", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___TASK_RELEASED as TASK_RELEASED", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___PACK_QTY as PACK_QTY", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___PICK_LOCN_DTL_ID as PICK_LOCN_DTL_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___PICK_LOCN_HDR_ID as PICK_LOCN_HDR_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___WM_VERSION_ID as WM_VERSION_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___ITEM_MASTER_ID as ITEM_MASTER_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___ITEM_ID as ITEM_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___PRE_ALLOCATED_QTY as PRE_ALLOCATED_QTY", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___REPLEN_CONTROL as REPLEN_CONTROL", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___STOP_QTY as STOP_QTY", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___STOP_DTTM as STOP_DTTM", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___PICK_TO_ZERO_ACTION as PICK_TO_ZERO_ACTION", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___UTIL_PERCENT as UTIL_PERCENT", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___CREATED_DTTM as CREATED_DTTM", \
    	"SQ_Shortcut_to_PICK_LOCN_DTL___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_PICK_LOCN_DTL_PRE, type TARGET 
    # COLUMN COUNT: 48
    
    
    Shortcut_to_WM_PICK_LOCN_DTL_PRE = EXPTRANS.selectExpr( \
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
    	"CAST(PICK_LOCN_DTL_ID AS BIGINT) as PICK_LOCN_DTL_ID", \
    	"CAST(LOCN_ID AS STRING) as LOCN_ID", \
    	"CAST(LOCN_SEQ_NBR AS BIGINT) as LOCN_SEQ_NBR", \
    	"CAST(SKU_ATTR_1 AS STRING) as SKU_ATTR_1", \
    	"CAST(SKU_ATTR_2 AS STRING) as SKU_ATTR_2", \
    	"CAST(SKU_ATTR_3 AS STRING) as SKU_ATTR_3", \
    	"CAST(SKU_ATTR_4 AS STRING) as SKU_ATTR_4", \
    	"CAST(SKU_ATTR_5 AS STRING) as SKU_ATTR_5", \
    	"CAST(INVN_TYPE AS STRING) as INVN_TYPE", \
    	"CAST(PROD_STAT AS STRING) as PROD_STAT", \
    	"CAST(BATCH_NBR AS STRING) as BATCH_NBR", \
    	"CAST(CNTRY_OF_ORGN AS STRING) as CNTRY_OF_ORGN", \
    	"CAST(MAX_INVN_QTY AS BIGINT) as MAX_INVN_QTY", \
    	"CAST(MIN_INVN_QTY AS BIGINT) as MIN_INVN_QTY", \
    	"CAST(ACTL_INVN_CASES AS BIGINT) as ACTL_INVN_CASES", \
    	"CAST(MIN_INVN_CASES AS BIGINT) as MIN_INVN_CASES", \
    	"CAST(MAX_INVN_CASES AS BIGINT) as MAX_INVN_CASES", \
    	"CAST(TRIG_REPL_FOR_SKU AS STRING) as TRIG_REPL_FOR_SKU", \
    	"CAST(PRIM_LOCN_FOR_SKU AS STRING) as PRIM_LOCN_FOR_SKU", \
    	"CAST(FIRST_WAVE_NBR AS STRING) as FIRST_WAVE_NBR", \
    	"CAST(LAST_WAVE_NBR AS STRING) as LAST_WAVE_NBR", \
    	"CAST(LTST_SKU_ASSIGN AS STRING) as LTST_SKU_ASSIGN", \
    	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", \
    	"CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", \
    	"CAST(USER_ID AS STRING) as USER_ID", \
    	"CAST(LTST_PICK_ASSIGN_DATE_TIME AS TIMESTAMP) as LTST_PICK_ASSIGN_DATE_TIME", \
    	"CAST(PIKNG_LOCK_CODE AS STRING) as PIKNG_LOCK_CODE", \
    	"CAST(TO_BE_FILLD_CASES AS BIGINT) as TO_BE_FILLD_CASES", \
    	"CAST(LANES AS STRING) as LANES", \
    	"CAST(STACKING AS STRING) as STACKING", \
    	"CAST(MIN_QTY_TO_RLS_HELD_RPLN AS BIGINT) as MIN_QTY_TO_RLS_HELD_RPLN", \
    	"CAST(MIN_CASES_TO_RLS_HELD_RPLN AS BIGINT) as MIN_CASES_TO_RLS_HELD_RPLN", \
    	"CAST(TASK_RELEASED AS STRING) as TASK_RELEASED", \
    	"CAST(PACK_QTY AS BIGINT) as PACK_QTY", \
    	"CAST(PICK_LOCN_HDR_ID AS BIGINT) as PICK_LOCN_HDR_ID", \
    	"CAST(WM_VERSION_ID AS BIGINT) as WM_VERSION_ID", \
    	"CAST(ITEM_MASTER_ID AS BIGINT) as ITEM_MASTER_ID", \
    	"CAST(ITEM_ID AS BIGINT) as ITEM_ID", \
    	"CAST(PRE_ALLOCATED_QTY AS BIGINT) as PRE_ALLOCATED_QTY", \
    	"CAST(REPLEN_CONTROL AS STRING) as REPLEN_CONTROL", \
    	"CAST(STOP_QTY AS BIGINT) as STOP_QTY", \
    	"CAST(STOP_DTTM AS TIMESTAMP) as STOP_DTTM", \
    	"CAST(PICK_TO_ZERO_ACTION AS STRING) as PICK_TO_ZERO_ACTION", \
    	"CAST(UTIL_PERCENT AS BIGINT) as UTIL_PERCENT", \
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_PICK_LOCN_DTL_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_PICK_LOCN_DTL_PRE is written to the target table - " + target_table_name)
