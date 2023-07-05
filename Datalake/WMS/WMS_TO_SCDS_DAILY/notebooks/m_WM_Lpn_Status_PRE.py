#Code converted on 2023-06-20 18:04:11
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *


def m_WM_Lpn_Status_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Lpn_Status_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_LPN_STATUS_PRE', mode = 'append"
    schemaName = raw
    source_schema = "WMSMIS"

    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_LPN_STATUS', mode = 'append"
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + Prev_Run_Dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    
    query = f"""SELECT
                    LPN_STATUS.LPN_STATUS,
                    LPN_STATUS.DESCRIPTION
                FROM {source_schema}.LPN_STATUS"""

    SQ_Shortcut_to_LPN_STATUS = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_LPN_STATUS is executed and data is loaded using jdbc")
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 4
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LPN_STATUS_temp = SQ_Shortcut_to_LPN_STATUS.toDF(*["SQ_Shortcut_to_LPN_STATUS___" + col for col in SQ_Shortcut_to_LPN_STATUS.columns])
    
    EXPTRANS = SQ_Shortcut_to_LPN_STATUS_temp.selectExpr( \
    	"SQ_Shortcut_to_LPN_STATUS___sys_row_id as sys_row_id", \
    	f"{dcnbr}  as DC_NBR_EXP", \
    	"SQ_Shortcut_to_LPN_STATUS___LPN_STATUS as LPN_STATUS", \
    	"SQ_Shortcut_to_LPN_STATUS___DESCRIPTION as DESCRIPTION", \
    	"CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_LPN_STATUS_PRE, type TARGET 
    # COLUMN COUNT: 4
    
    
    Shortcut_to_WM_LPN_STATUS_PRE = EXPTRANS.selectExpr( \
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
    	"CAST(LPN_STATUS AS BIGINT) as LPN_STATUS", \
    	"CAST(DESCRIPTION AS STRING) as DESCRIPTION", \
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_LPN_STATUS_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_LPN_STATUS_PRE is written to the target table - " + target_table_name)
