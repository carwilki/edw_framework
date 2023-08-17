#Code converted on 2023-06-26 17:58:23
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


def m_WM_Order_Status_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Order_Status_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_ORDER_STATUS_PRE"
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
                    ORDER_STATUS.ORDER_STATUS,
                    ORDER_STATUS.DESCRIPTION,
                    ORDER_STATUS.CREATED_DTTM,
                    ORDER_STATUS.LAST_UPDATED_DTTM
                FROM {source_schema}.ORDER_STATUS
                WHERE  (trunc(CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""
     
    SQ_Shortcut_to_ORDER_STATUS = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for Shortcut_to_WM_ORDER_STATUS_PRE is executed and data is loaded using jdbc")
     
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 6
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ORDER_STATUS_temp = SQ_Shortcut_to_ORDER_STATUS.toDF(*["SQ_Shortcut_to_ORDER_STATUS___" + col for col in SQ_Shortcut_to_ORDER_STATUS.columns])
    
    EXPTRANS = SQ_Shortcut_to_ORDER_STATUS_temp.selectExpr( \
    	"SQ_Shortcut_to_ORDER_STATUS___sys_row_id as sys_row_id", \
    	f"{dcnbr} as DC_NBR_exp", \
    	"SQ_Shortcut_to_ORDER_STATUS___ORDER_STATUS as ORDER_STATUS", \
    	"SQ_Shortcut_to_ORDER_STATUS___DESCRIPTION as DESCRIPTION", \
    	"SQ_Shortcut_to_ORDER_STATUS___CREATED_DTTM as CREATED_DTTM", \
    	"SQ_Shortcut_to_ORDER_STATUS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
    	"CURRENT_TIMESTAMP () as LOADTSTMP" \
    )
    
    
    # Processing node Shortcut_to_WM_ORDER_STATUS_PRE, type TARGET 
    # COLUMN COUNT: 6
    
    
    Shortcut_to_WM_ORDER_STATUS_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_exp AS SMALLINT) as DC_NBR",
        "CAST(ORDER_STATUS AS SMALLINT) as ORDER_STATUS",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOADTSTMP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_ORDER_STATUS_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_ORDER_STATUS_PRE is written to the target table - " + target_table_name)
