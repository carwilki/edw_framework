#Code converted on 2023-06-22 21:02:50
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



def m_WM_Trailer_Type_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Trailer_Type_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_TRAILER_TYPE_PRE"
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
                    TRAILER_TYPE.TRAILER_TYPE,
                    TRAILER_TYPE.DESCRIPTION,
                    TRAILER_TYPE.CREATED_DTTM,
                    TRAILER_TYPE.LAST_UPDATED_DTTM
                FROM {source_schema}.TRAILER_TYPE
                WHERE  (TRUNC(CREATED_DTTM) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC(LAST_UPDATED_DTTM) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""
                        

    SQ_Shortcut_to_TRAILER_TYPE = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_TRAILER_TYPE is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 6
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_TRAILER_TYPE_temp = SQ_Shortcut_to_TRAILER_TYPE.toDF(*["SQ_Shortcut_to_TRAILER_TYPE___" + col for col in SQ_Shortcut_to_TRAILER_TYPE.columns])
    
    EXPTRANS = SQ_Shortcut_to_TRAILER_TYPE_temp.selectExpr( 
    	"SQ_Shortcut_to_TRAILER_TYPE___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR_exp", 
    	"SQ_Shortcut_to_TRAILER_TYPE___TRAILER_TYPE as TRAILER_TYPE", 
    	"SQ_Shortcut_to_TRAILER_TYPE___DESCRIPTION as DESCRIPTION", 
    	"SQ_Shortcut_to_TRAILER_TYPE___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_TRAILER_TYPE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP" 
    )
    
    
    # Processing node Shortcut_to_WM_TRAILER_TYPE_PRE, type TARGET 
    # COLUMN COUNT: 6
    
    
    Shortcut_to_WM_TRAILER_TYPE_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_exp AS SMALLINT) as DC_NBR",
        "CAST(TRAILER_TYPE AS TINYINT) as TRAILER_TYPE",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_TRAILER_TYPE_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_TRAILER_TYPE_PRE is written to the target table - " + target_table_name)
