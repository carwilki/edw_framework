#Code converted on 2023-06-24 13:33:32
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from utils.genericUtilities import *
from utils.configs import *
from utils.mergeUtils import *
from utils.logger import *



def m_WM_Standard_Uom_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Standard_Uom_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_STANDARD_UOM_PRE"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_STANDARD_UOM"
    prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + prev_run_dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
                    STANDARD_UOM.STANDARD_UOM,
                    STANDARD_UOM.STANDARD_UOM_TYPE,
                    STANDARD_UOM.ABBREVIATION,
                    STANDARD_UOM.DESCRIPTION,
                    STANDARD_UOM.UOM_SYSTEM,
                    STANDARD_UOM.IS_TYPE_SYS_DFLT,
                    STANDARD_UOM.UNITS_IN_TYPE_SYS_DFLT,
                    STANDARD_UOM.IS_DB_UOM,
                    STANDARD_UOM.IS_SYSTEM_DEFINED,
                    STANDARD_UOM.CREATED_DTTM,
                    STANDARD_UOM.LAST_UPDATED_DTTM
                FROM STANDARD_UOM
                WHERE {Initial_Load} (TRUNC( CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (TRUNC( LAST_UPDATED_DTTM) >=  TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1)"""
                        

    SQ_Shortcut_to_STANDARD_UOM = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_STANDARD_UOM is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 13
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_STANDARD_UOM_temp = SQ_Shortcut_to_STANDARD_UOM.toDF(*["SQ_Shortcut_to_STANDARD_UOM___" + col for col in SQ_Shortcut_to_STANDARD_UOM.columns])
    
    EXPTRANS = SQ_Shortcut_to_STANDARD_UOM_temp.selectExpr( 
    	"SQ_Shortcut_to_STANDARD_UOM___sys_row_id as sys_row_id", 
    	f"{DC_NBR} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_STANDARD_UOM___STANDARD_UOM as STANDARD_UOM", 
    	"SQ_Shortcut_to_STANDARD_UOM___STANDARD_UOM_TYPE as STANDARD_UOM_TYPE", 
    	"SQ_Shortcut_to_STANDARD_UOM___ABBREVIATION as ABBREVIATION", 
    	"SQ_Shortcut_to_STANDARD_UOM___DESCRIPTION as DESCRIPTION", 
    	"SQ_Shortcut_to_STANDARD_UOM___UOM_SYSTEM as UOM_SYSTEM", 
    	"SQ_Shortcut_to_STANDARD_UOM___IS_TYPE_SYS_DFLT as IS_TYPE_SYS_DFLT", 
    	"SQ_Shortcut_to_STANDARD_UOM___UNITS_IN_TYPE_SYS_DFLT as UNITS_IN_TYPE_SYS_DFLT", 
    	"SQ_Shortcut_to_STANDARD_UOM___IS_DB_UOM as IS_DB_UOM", 
    	"SQ_Shortcut_to_STANDARD_UOM___IS_SYSTEM_DEFINED as IS_SYSTEM_DEFINED", 
    	"SQ_Shortcut_to_STANDARD_UOM___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_STANDARD_UOM___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP" 
    )
    
    
    # Processing node Shortcut_to_WM_STANDARD_UOM_PRE, type TARGET 
    # COLUMN COUNT: 13
    
    
    Shortcut_to_WM_STANDARD_UOM_PRE = EXPTRANS.selectExpr( 
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
    	"CAST(STANDARD_UOM AS BIGINT) as STANDARD_UOM", 
    	"CAST(STANDARD_UOM_TYPE AS BIGINT) as STANDARD_UOM_TYPE", 
    	"CAST(ABBREVIATION AS STRING) as ABBREVIATION", 
    	"CAST(DESCRIPTION AS STRING) as DESCRIPTION", 
    	"CAST(UOM_SYSTEM AS STRING) as UOM_SYSTEM", 
    	"CAST(IS_TYPE_SYS_DFLT AS BIGINT) as IS_TYPE_SYS_DFLT", 
    	"CAST(UNITS_IN_TYPE_SYS_DFLT AS BIGINT) as UNITS_IN_TYPE_SYS_DFLT", 
    	"CAST(IS_DB_UOM AS BIGINT) as IS_DB_UOM", 
    	"CAST(IS_SYSTEM_DEFINED AS BIGINT) as IS_SYSTEM_DEFINED", 
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
    	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    gu.overwriteDeltaPartition(Shortcut_to_WM_STANDARD_UOM_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_STANDARD_UOM_PRE is written to the target table - " + target_table_name)
