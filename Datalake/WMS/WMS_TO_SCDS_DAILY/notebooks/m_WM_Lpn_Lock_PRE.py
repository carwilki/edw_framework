#Code converted on 2023-06-20 18:04:29
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from utils.genericUtilities import *
from utils.configs import *
from utils.mergeUtils import *
from utils.logger import *



def m_WM_Lpn_Lock_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Lpn_Lock_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_LPN_LOCK_PRE', mode = 'append"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_LPN_LOCK', mode = 'append"
    prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + prev_run_dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
                    LPN_LOCK.LPN_LOCK_ID,
                    LPN_LOCK.LPN_ID,
                    LPN_LOCK.INVENTORY_LOCK_CODE,
                    LPN_LOCK.REASON_CODE,
                    LPN_LOCK.LOCK_COUNT,
                    LPN_LOCK.TC_LPN_ID,
                    LPN_LOCK.CREATED_SOURCE_TYPE,
                    LPN_LOCK.CREATED_SOURCE,
                    LPN_LOCK.CREATED_DTTM,
                    LPN_LOCK.LAST_UPDATED_SOURCE_TYPE,
                    LPN_LOCK.LAST_UPDATED_SOURCE,
                    LPN_LOCK.LAST_UPDATED_DTTM
                FROM LPN_LOCK
                WHERE {Initial_Load}  (trunc(CREATED_DTTM)>= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14) OR (trunc(LAST_UPDATED_DTTM)>= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14)"""

    SQ_Shortcut_to_LPN_LOCK = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_LPN_LOCK is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 14
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LPN_LOCK_temp = SQ_Shortcut_to_LPN_LOCK.toDF(*["SQ_Shortcut_to_LPN_LOCK___" + col for col in SQ_Shortcut_to_LPN_LOCK.columns])
    
    EXPTRANS = SQ_Shortcut_to_LPN_LOCK_temp.selectExpr( \
    	"SQ_Shortcut_to_LPN_LOCK___sys_row_id as sys_row_id", \
    	f"{DC_NBR} as DC_NBR_EXP", \
    	"SQ_Shortcut_to_LPN_LOCK___LPN_LOCK_ID as LPN_LOCK_ID", \
    	"SQ_Shortcut_to_LPN_LOCK___LPN_ID as LPN_ID", \
    	"SQ_Shortcut_to_LPN_LOCK___INVENTORY_LOCK_CODE as INVENTORY_LOCK_CODE", \
    	"SQ_Shortcut_to_LPN_LOCK___REASON_CODE as REASON_CODE", \
    	"SQ_Shortcut_to_LPN_LOCK___LOCK_COUNT as LOCK_COUNT", \
    	"SQ_Shortcut_to_LPN_LOCK___TC_LPN_ID as TC_LPN_ID", \
    	"SQ_Shortcut_to_LPN_LOCK___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
    	"SQ_Shortcut_to_LPN_LOCK___CREATED_SOURCE as CREATED_SOURCE", \
    	"SQ_Shortcut_to_LPN_LOCK___CREATED_DTTM as CREATED_DTTM", \
    	"SQ_Shortcut_to_LPN_LOCK___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
    	"SQ_Shortcut_to_LPN_LOCK___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
    	"SQ_Shortcut_to_LPN_LOCK___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
    	"CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_LPN_LOCK_PRE, type TARGET 
    # COLUMN COUNT: 14
    
    
    Shortcut_to_WM_LPN_LOCK_PRE = EXPTRANS.selectExpr( \
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
    	"CAST(LPN_LOCK_ID AS BIGINT) as LPN_LOCK_ID", \
    	"CAST(LPN_ID AS BIGINT) as LPN_ID", \
    	"CAST(INVENTORY_LOCK_CODE AS STRING) as INVENTORY_LOCK_CODE", \
    	"CAST(REASON_CODE AS STRING) as REASON_CODE", \
    	"CAST(LOCK_COUNT AS BIGINT) as LOCK_COUNT", \
    	"CAST(TC_LPN_ID AS STRING) as TC_LPN_ID", \
    	"CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", \
    	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
    	"CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", \
    	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    gu.overwriteDeltaPartition(Shortcut_to_WM_LPN_LOCK_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_LPN_LOCK_PRE is written to the target table - " + target_table_name)
