#Code converted on 2023-06-24 13:33:08
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



def m_WM_Stop_Status_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Stop_Status_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_STOP_STATUS_PRE"
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
                    STOP_STATUS.STOP_STATUS,
                    STOP_STATUS.DESCRIPTION,
                    STOP_STATUS.SHORT_DESC
                FROM {source_schema}.STOP_STATUS"""
    

    SQ_Shortcut_to_STOP_STATUS = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_STOP_STATUS is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 5
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_STOP_STATUS_temp = SQ_Shortcut_to_STOP_STATUS.toDF(*["SQ_Shortcut_to_STOP_STATUS___" + col for col in SQ_Shortcut_to_STOP_STATUS.columns])
    
    EXPTRANS = SQ_Shortcut_to_STOP_STATUS_temp.selectExpr( 
    	"SQ_Shortcut_to_STOP_STATUS___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR", 
    	"SQ_Shortcut_to_STOP_STATUS___STOP_STATUS as STOP_STATUS", 
    	"SQ_Shortcut_to_STOP_STATUS___DESCRIPTION as DESCRIPTION", 
    	"SQ_Shortcut_to_STOP_STATUS___SHORT_DESC as SHORT_DESC", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP" 
    )
    
    
    # Processing node Shortcut_to_WM_STOP_STATUS_PRE, type TARGET 
    # COLUMN COUNT: 5
    
    
    Shortcut_to_WM_STOP_STATUS_PRE = EXPTRANS.selectExpr(
    "CAST(DC_NBR AS SMALLINT) as DC_NBR",
    "CAST(STOP_STATUS AS SMALLINT) as STOP_STATUS",
    "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
    "CAST(SHORT_DESC AS STRING) as SHORT_DESC",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_STOP_STATUS_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_STOP_STATUS_PRE is written to the target table - " + target_table_name)
