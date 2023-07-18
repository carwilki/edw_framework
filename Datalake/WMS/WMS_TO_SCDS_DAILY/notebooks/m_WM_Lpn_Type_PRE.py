#Code converted on 2023-06-20 18:04:07
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


def m_WM_Lpn_Type_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Lpn_Type_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_LPN_TYPE_PRE"
    schemaName = raw
    source_schema = "WMSMIS"

    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_LPN_TYPE"
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + Prev_Run_Dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
                    LPN_TYPE.LPN_TYPE,
                    LPN_TYPE.DESCRIPTION,
                    LPN_TYPE.PHYSICAL_ENTITY_CODE
                FROM {source_schema}.LPN_TYPE"""  

    SQ_Shortcut_to_LPN_TYPE = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_LPN_TYPE is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 5
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LPN_TYPE_temp = SQ_Shortcut_to_LPN_TYPE.toDF(*["SQ_Shortcut_to_LPN_TYPE___" + col for col in SQ_Shortcut_to_LPN_TYPE.columns])
    
    EXPTRANS = SQ_Shortcut_to_LPN_TYPE_temp.selectExpr( \
    	"SQ_Shortcut_to_LPN_TYPE___sys_row_id as sys_row_id", \
    	f"{dcnbr} as DC_NBR_EXP", \
    	"SQ_Shortcut_to_LPN_TYPE___LPN_TYPE as LPN_TYPE", \
    	"SQ_Shortcut_to_LPN_TYPE___DESCRIPTION as DESCRIPTION", \
    	"SQ_Shortcut_to_LPN_TYPE___PHYSICAL_ENTITY_CODE as PHYSICAL_ENTITY_CODE", \
    	"CURRENT_TIMESTAMP () as LOAD_TSTMP" \
    )
    
    
    # Processing node Shortcut_to_WM_LPN_TYPE_PRE, type TARGET 
    # COLUMN COUNT: 5
    
    
    Shortcut_to_WM_LPN_TYPE_PRE = EXPTRANS.selectExpr(
    "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
    "CAST(LPN_TYPE AS SMALLINT) as LPN_TYPE",
    "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
    "CAST(PHYSICAL_ENTITY_CODE AS STRING) as PHYSICAL_ENTITY_CODE",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
    )

    
    overwriteDeltaPartition(Shortcut_to_WM_LPN_TYPE_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_LPN_TYPE_PRE is written to the target table - " + target_table_name)
