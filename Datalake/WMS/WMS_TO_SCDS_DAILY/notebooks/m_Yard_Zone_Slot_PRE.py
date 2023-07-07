#Code converted on 2023-06-22 21:04:37
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



def m_Yard_Zone_Slot_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_Yard_Zone_Slot_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_YARD_ZONE_SLOT_PRE"
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
                    YARD_ZONE_SLOT.YARD_ID,
                    YARD_ZONE_SLOT.YARD_ZONE_ID,
                    YARD_ZONE_SLOT.YARD_ZONE_SLOT_ID,
                    YARD_ZONE_SLOT.YARD_ZONE_SLOT_NAME,
                    YARD_ZONE_SLOT.YARD_ZONE_SLOT_STATUS,
                    YARD_ZONE_SLOT.X_COORDINATE,
                    YARD_ZONE_SLOT.Y_COORDINATE,
                    YARD_ZONE_SLOT.Z_COORDINATE,
                    YARD_ZONE_SLOT.MAX_CAPACITY,
                    YARD_ZONE_SLOT.USED_CAPACITY,
                    YARD_ZONE_SLOT.MARK_FOR_DELETION,
                    YARD_ZONE_SLOT.IS_GUARD_HOUSE,
                    YARD_ZONE_SLOT.IS_THRESHOLD_GUARD_HOUSE,
                    YARD_ZONE_SLOT.CREATED_DTTM,
                    YARD_ZONE_SLOT.LAST_UPDATED_DTTM,
                    YARD_ZONE_SLOT.LOCN_ID,
                    YARD_ZONE_SLOT.CREATED_SOURCE,
                    YARD_ZONE_SLOT.CREATED_SOURCE_TYPE,
                    YARD_ZONE_SLOT.LAST_UPDATED_SOURCE,
                    YARD_ZONE_SLOT.LAST_UPDATED_SOURCE_TYPE
                FROM {source_schema}.YARD_ZONE_SLOT
                WHERE  (TRUNC( CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (TRUNC( LAST_UPDATED_DTTM) >=  TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14)"""
    

    SQ_Shortcut_to_YARD_ZONE_SLOT = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_YARD_ZONE_SLOT is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 22
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_YARD_ZONE_SLOT_temp = SQ_Shortcut_to_YARD_ZONE_SLOT.toDF(*["SQ_Shortcut_to_YARD_ZONE_SLOT___" + col for col in SQ_Shortcut_to_YARD_ZONE_SLOT.columns])
    
    EXPTRANS = SQ_Shortcut_to_YARD_ZONE_SLOT_temp.selectExpr( 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___YARD_ID as YARD_ID", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___YARD_ZONE_ID as YARD_ZONE_ID", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___YARD_ZONE_SLOT_ID as YARD_ZONE_SLOT_ID", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___YARD_ZONE_SLOT_NAME as YARD_ZONE_SLOT_NAME", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___YARD_ZONE_SLOT_STATUS as YARD_ZONE_SLOT_STATUS", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___X_COORDINATE as X_COORDINATE", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___Y_COORDINATE as Y_COORDINATE", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___Z_COORDINATE as Z_COORDINATE", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___MAX_CAPACITY as MAX_CAPACITY", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___USED_CAPACITY as USED_CAPACITY", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___MARK_FOR_DELETION as MARK_FOR_DELETION", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___IS_GUARD_HOUSE as IS_GUARD_HOUSE", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___IS_THRESHOLD_GUARD_HOUSE as IS_THRESHOLD_GUARD_HOUSE", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___LOCN_ID as LOCN_ID", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___CREATED_SOURCE as CREATED_SOURCE", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
    	"SQ_Shortcut_to_YARD_ZONE_SLOT___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_YARD_ZONE_SLOT_PRE, type TARGET 
    # COLUMN COUNT: 22
    
    
    Shortcut_to_WM_YARD_ZONE_SLOT_PRE = EXPTRANS.selectExpr( 
    	"CAST(DC_NBR AS BIGINT) as DC_NBR", 
    	"CAST(YARD_ID AS BIGINT) as YARD_ID", 
    	"CAST(YARD_ZONE_ID AS BIGINT) as YARD_ZONE_ID", 
    	"CAST(YARD_ZONE_SLOT_ID AS BIGINT) as YARD_ZONE_SLOT_ID", 
    	"CAST(YARD_ZONE_SLOT_NAME AS STRING) as YARD_ZONE_SLOT_NAME", 
    	"CAST(YARD_ZONE_SLOT_STATUS AS BIGINT) as YARD_ZONE_SLOT_STATUS", 
    	"CAST(X_COORDINATE AS BIGINT) as X_COORDINATE", 
    	"CAST(Y_COORDINATE AS BIGINT) as Y_COORDINATE", 
    	"CAST(Z_COORDINATE AS BIGINT) as Z_COORDINATE", 
    	"CAST(MAX_CAPACITY AS BIGINT) as MAX_CAPACITY", 
    	"CAST(USED_CAPACITY AS BIGINT) as USED_CAPACITY", 
    	"CAST(MARK_FOR_DELETION AS BIGINT) as MARK_FOR_DELETION", 
    	"CAST(IS_GUARD_HOUSE AS BIGINT) as IS_GUARD_HOUSE", 
    	"CAST(IS_THRESHOLD_GUARD_HOUSE AS BIGINT) as IS_THRESHOLD_GUARD_HOUSE", 
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
    	"CAST(LOCN_ID AS STRING) as LOCN_ID", 
    	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", 
    	"CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", 
    	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", 
    	"CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", 
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_YARD_ZONE_SLOT_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_YARD_ZONE_SLOT_PRE is written to the target table - " + target_table_name)
