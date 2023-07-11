#Code converted on 2023-06-22 21:01:45
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



def m_WM_Sys_Code_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Sys_Code_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_SYS_CODE_PRE"
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
                    SYS_CODE.REC_TYPE,
                    SYS_CODE.CODE_TYPE,
                    SYS_CODE.CODE_ID,
                    SYS_CODE.CODE_DESC,
                    SYS_CODE.SHORT_DESC,
                    SYS_CODE.MISC_FLAGS,
                    SYS_CODE.CREATE_DATE_TIME,
                    SYS_CODE.MOD_DATE_TIME,
                    SYS_CODE.USER_ID,
                    SYS_CODE.WM_VERSION_ID,
                    SYS_CODE.SYS_CODE_ID,
                    SYS_CODE.SYS_CODE_TYPE_ID,
                    SYS_CODE.CREATED_DTTM,
                    SYS_CODE.LAST_UPDATED_DTTM
                FROM {source_schema}.SYS_CODE
                WHERE  (TRUNC(SYS_CODE.CREATE_DATE_TIME)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (TRUNC(SYS_CODE.MOD_DATE_TIME)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (TRUNC(SYS_CODE.CREATED_DTTM)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (TRUNC(SYS_CODE.LAST_UPDATED_DTTM)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14)"""
    

    SQ_Shortcut_to_SYS_CODE = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_SYS_CODE is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 20
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_SYS_CODE_temp = SQ_Shortcut_to_SYS_CODE.toDF(*["SQ_Shortcut_to_SYS_CODE___" + col for col in SQ_Shortcut_to_SYS_CODE.columns])
    
    EXPTRANS = SQ_Shortcut_to_SYS_CODE_temp.selectExpr( 
    	"SQ_Shortcut_to_SYS_CODE___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_SYS_CODE___REC_TYPE as REC_TYPE", 
    	"SQ_Shortcut_to_SYS_CODE___CODE_TYPE as CODE_TYPE", 
    	"SQ_Shortcut_to_SYS_CODE___CODE_ID as CODE_ID", 
    	"REPLACE(SQ_Shortcut_to_SYS_CODE___CODE_ID, '|', ' ') as CODE_ID_EXP", 
    	"SQ_Shortcut_to_SYS_CODE___CODE_DESC as CODE_DESC", 
    	"REGEXP_REPLACE(REPLACE(SQ_Shortcut_to_SYS_CODE___CODE_DESC, '|', ' '), '\\(|\\r|\\n', '') as CODE_DESC_EXP", 
    	"SQ_Shortcut_to_SYS_CODE___SHORT_DESC as SHORT_DESC", 
    	"REGEXP_REPLACE(REPLACE(SQ_Shortcut_to_SYS_CODE___SHORT_DESC, '|', ' '), '\\(|\\r|\\n', '') as SHORT_DESC_EXP", 
    	"SQ_Shortcut_to_SYS_CODE___MISC_FLAGS as MISC_FLAGS", 
    	"REGEXP_REPLACE(REPLACE(SQ_Shortcut_to_SYS_CODE___MISC_FLAGS, '|', ' '), '\\(|\\r|\\n', '') as MISC_FLAGS_EXP", 
    	"SQ_Shortcut_to_SYS_CODE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
    	"SQ_Shortcut_to_SYS_CODE___MOD_DATE_TIME as MOD_DATE_TIME", 
    	"SQ_Shortcut_to_SYS_CODE___USER_ID as USER_ID", 
    	"SQ_Shortcut_to_SYS_CODE___WM_VERSION_ID as WM_VERSION_ID", 
    	"SQ_Shortcut_to_SYS_CODE___SYS_CODE_ID as SYS_CODE_ID", 
    	"SQ_Shortcut_to_SYS_CODE___SYS_CODE_TYPE_ID as SYS_CODE_TYPE_ID", 
    	"SQ_Shortcut_to_SYS_CODE___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_SYS_CODE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_SYS_CODE_PRE, type TARGET 
    # COLUMN COUNT: 16
    
    
    Shortcut_to_WM_SYS_CODE_PRE = EXPTRANS.selectExpr(
    "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
    "CAST(REC_TYPE AS STRING) as REC_TYPE",
    "CAST(CODE_TYPE AS STRING) as CODE_TYPE",
    "CAST(CODE_ID_EXP AS STRING) as CODE_ID",
    "CAST(CODE_DESC_EXP AS STRING) as CODE_DESC",
    "CAST(SHORT_DESC_EXP AS STRING) as SHORT_DESC",
    "CAST(MISC_FLAGS_EXP AS STRING) as MISC_FLAGS",
    "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
    "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
    "CAST(USER_ID AS STRING) as USER_ID",
    "CAST(WM_VERSION_ID AS INT) as WM_VERSION_ID",
    "CAST(SYS_CODE_ID AS INT) as SYS_CODE_ID",
    "CAST(SYS_CODE_TYPE_ID AS INT) as SYS_CODE_TYPE_ID",
    "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
    "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
    "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_SYS_CODE_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_SYS_CODE_PRE is written to the target table - " + target_table_name)
