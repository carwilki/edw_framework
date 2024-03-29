#Code converted on 2023-06-22 17:50:19
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



def m_WM_Purchase_Orders_Line_Status_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Purchase_Orders_Line_Status_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_PURCHASE_ORDERS_LINE_STATUS_PRE"
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
                    PURCHASE_ORDERS_LINE_STATUS.PURCHASE_ORDERS_LINE_STATUS,
                    PURCHASE_ORDERS_LINE_STATUS.DESCRIPTION,
                    PURCHASE_ORDERS_LINE_STATUS.NOTE
                FROM {source_schema}.PURCHASE_ORDERS_LINE_STATUS"""
                        

    SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 5
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS_temp = SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS.toDF(*["SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS___" + col for col in SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS.columns])
    
    EXPTRANS = SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS_temp.selectExpr( 
    	"SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR", 
    	"SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS___PURCHASE_ORDERS_LINE_STATUS as PURCHASE_ORDERS_LINE_STATUS", 
    	"SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS___DESCRIPTION as DESCRIPTION", 
    	"SQ_Shortcut_to_PURCHASE_ORDERS_LINE_STATUS___NOTE as NOTE", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE, type TARGET 
    # COLUMN COUNT: 5
    
    
    Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR AS SMALLINT) as DC_NBR",
        "CAST(PURCHASE_ORDERS_LINE_STATUS AS SMALLINT) as PURCHASE_ORDERS_LINE_STATUS",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(NOTE AS STRING) as NOTE",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE is written to the target table - " + target_table_name)
