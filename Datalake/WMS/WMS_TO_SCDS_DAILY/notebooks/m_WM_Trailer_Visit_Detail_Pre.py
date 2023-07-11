#Code converted on 2023-06-22 21:02:48
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



def m_WM_Trailer_Visit_Detail_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Trailer_Visit_Detail_Pre")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_TRAILER_VISIT_DETAIL_PRE"
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
                    TRAILER_VISIT_DETAIL.VISIT_DETAIL_ID,
                    TRAILER_VISIT_DETAIL.VISIT_ID,
                    TRAILER_VISIT_DETAIL.TYPE,
                    TRAILER_VISIT_DETAIL.APPOINTMENT_ID,
                    TRAILER_VISIT_DETAIL.DRIVER_ID,
                    TRAILER_VISIT_DETAIL.TRACTOR_ID,
                    TRAILER_VISIT_DETAIL.SEAL_NUMBER,
                    TRAILER_VISIT_DETAIL.FOB_INDICATOR,
                    TRAILER_VISIT_DETAIL.START_DTTM,
                    TRAILER_VISIT_DETAIL.END_DTTM,
                    TRAILER_VISIT_DETAIL.CREATED_DTTM,
                    TRAILER_VISIT_DETAIL.CREATED_SOURCE_TYPE,
                    TRAILER_VISIT_DETAIL.CREATED_SOURCE,
                    TRAILER_VISIT_DETAIL.LAST_UPDATED_DTTM,
                    TRAILER_VISIT_DETAIL.LAST_UPDATED_SOURCE_TYPE,
                    TRAILER_VISIT_DETAIL.LAST_UPDATED_SOURCE,
                    TRAILER_VISIT_DETAIL.PROTECTION_LEVEL,
                    TRAILER_VISIT_DETAIL.PRODUCT_CLASS
                FROM {source_schema}.TRAILER_VISIT_DETAIL
                WHERE  (TRUNC(CREATED_DTTM) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC(LAST_UPDATED_DTTM) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""
    

    SQ_Shortcut_to_TRAILER_VISIT_DETAIL = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_TRAILER_VISIT_DETAIL is executed and data is loaded using jdbc")
    
    
    # Processing node ExP_TRN, type EXPRESSION 
    # COLUMN COUNT: 20
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_TRAILER_VISIT_DETAIL_temp = SQ_Shortcut_to_TRAILER_VISIT_DETAIL.toDF(*["SQ_Shortcut_to_TRAILER_VISIT_DETAIL___" + col for col in SQ_Shortcut_to_TRAILER_VISIT_DETAIL.columns])
    
    ExP_TRN = SQ_Shortcut_to_TRAILER_VISIT_DETAIL_temp.selectExpr( 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___VISIT_ID as VISIT_ID", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___TYPE as TYPE", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___APPOINTMENT_ID as APPOINTMENT_ID", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___DRIVER_ID as DRIVER_ID", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___TRACTOR_ID as TRACTOR_ID", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___SEAL_NUMBER as SEAL_NUMBER", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___FOB_INDICATOR as FOB_INDICATOR", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___START_DTTM as START_DTTM", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___END_DTTM as END_DTTM", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___CREATED_SOURCE as CREATED_SOURCE", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___PROTECTION_LEVEL as PROTECTION_LEVEL", 
    	"SQ_Shortcut_to_TRAILER_VISIT_DETAIL___PRODUCT_CLASS as PRODUCT_CLASS", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE, type TARGET 
    # COLUMN COUNT: 20
    
    
    Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE = ExP_TRN.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(VISIT_DETAIL_ID AS INT) as VISIT_DETAIL_ID",
        "CAST(VISIT_ID AS INT) as VISIT_ID",
        "CAST(TYPE AS STRING) as TYPE",
        "CAST(APPOINTMENT_ID AS INT) as APPOINTMENT_ID",
        "CAST(DRIVER_ID AS BIGINT) as DRIVER_ID",
        "CAST(TRACTOR_ID AS BIGINT) as TRACTOR_ID",
        "CAST(SEAL_NUMBER AS STRING) as SEAL_NUMBER",
        "CAST(FOB_INDICATOR AS SMALLINT) as FOB_INDICATOR",
        "CAST(START_DTTM AS TIMESTAMP) as START_DTTM",
        "CAST(END_DTTM AS TIMESTAMP) as END_DTTM",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(CREATED_SOURCE_TYPE AS SMALLINT) as CREATED_SOURCE_TYPE",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LAST_UPDATED_SOURCE_TYPE AS SMALLINT) as LAST_UPDATED_SOURCE_TYPE",
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
        "CAST(PROTECTION_LEVEL AS STRING) as PROTECTION_LEVEL",
        "CAST(PRODUCT_CLASS AS STRING) as PRODUCT_CLASS",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE is written to the target table - " + target_table_name)
