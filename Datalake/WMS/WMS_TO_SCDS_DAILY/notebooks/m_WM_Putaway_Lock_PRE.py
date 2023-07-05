#Code converted on 2023-06-22 17:50:12
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



def m_WM_Putaway_Lock_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Putaway_Lock_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_PUTAWAY_LOCK_PRE"
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
                    PUTAWAY_LOCK.LOCN_ID,
                    PUTAWAY_LOCK.LOCK_COUNTER,
                    PUTAWAY_LOCK.CREATED_DTTM,
                    PUTAWAY_LOCK.CREATED_SOURCE_TYPE,
                    PUTAWAY_LOCK.CREATED_SOURCE,
                    PUTAWAY_LOCK.LAST_UPDATED_DTTM,
                    PUTAWAY_LOCK.LAST_UPDATED_SOURCE_TYPE,
                    PUTAWAY_LOCK.LAST_UPDATED_SOURCE
                FROM {source_schema}.PUTAWAY_LOCK
                WHERE  (TRUNC(CREATED_DTTM) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (TRUNC(LAST_UPDATED_DTTM) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14)"""
                        

    SQ_Shortcut_to_PUTAWAY_LOCK = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_PUTAWAY_LOCK is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 10
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_PUTAWAY_LOCK_temp = SQ_Shortcut_to_PUTAWAY_LOCK.toDF(*["SQ_Shortcut_to_PUTAWAY_LOCK___" + col for col in SQ_Shortcut_to_PUTAWAY_LOCK.columns])
    
    EXPTRANS = SQ_Shortcut_to_PUTAWAY_LOCK_temp.selectExpr( \
    	"SQ_Shortcut_to_PUTAWAY_LOCK___sys_row_id as sys_row_id", 
    	"{DC_NBR} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_PUTAWAY_LOCK___LOCN_ID as LOCN_ID", 
    	"SQ_Shortcut_to_PUTAWAY_LOCK___LOCK_COUNTER as LOCK_COUNTER", 
    	"SQ_Shortcut_to_PUTAWAY_LOCK___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_PUTAWAY_LOCK___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_PUTAWAY_LOCK___CREATED_SOURCE as CREATED_SOURCE", 
    	"SQ_Shortcut_to_PUTAWAY_LOCK___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"SQ_Shortcut_to_PUTAWAY_LOCK___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_PUTAWAY_LOCK___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP" 
    )
    
    
    # Processing node Shortcut_to_WM_PUTAWAY_LOCK_PRE, type TARGET 
    # COLUMN COUNT: 10
    
    
    Shortcut_to_WM_PUTAWAY_LOCK_PRE = EXPTRANS.selectExpr( 
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
    	"CAST(LOCN_ID AS STRING) as LOCN_ID", 
    	"CAST(LOCK_COUNTER AS BIGINT) as LOCK_COUNTER", 
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
    	"CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", 
    	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", 
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
    	"CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", 
    	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", 
    	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_PUTAWAY_LOCK_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_PUTAWAY_LOCK_PRE is written to the target table - " + target_table_name)
