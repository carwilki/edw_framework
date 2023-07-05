#Code converted on 2023-06-22 21:02:47
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



def m_WM_Trailer_Visit_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Trailer_Visit_Pre")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_TRAILER_VISIT_PRE"
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
                    TRAILER_VISIT.VISIT_ID,
                    TRAILER_VISIT.FACILITY_ID,
                    TRAILER_VISIT.TRAILER_ID,
                    TRAILER_VISIT.CHECKIN_DTTM,
                    TRAILER_VISIT.CHECKOUT_DTTM,
                    TRAILER_VISIT.CREATED_DTTM,
                    TRAILER_VISIT.CREATED_SOURCE_TYPE,
                    TRAILER_VISIT.CREATED_SOURCE,
                    TRAILER_VISIT.LAST_UPDATED_DTTM,
                    TRAILER_VISIT.LAST_UPDATED_SOURCE_TYPE,
                    TRAILER_VISIT.LAST_UPDATED_SOURCE
                FROM {source_schema}.TRAILER_VISIT
                WHERE  (TRUNC(CREATED_DTTM) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC(LAST_UPDATED_DTTM) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""
    

    SQ_Shortcut_to_TRAILER_VISIT = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_TRAILER_VISIT is executed and data is loaded using jdbc")
    
    
    # Processing node ExP_TRN, type EXPRESSION 
    # COLUMN COUNT: 13
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_TRAILER_VISIT_temp = SQ_Shortcut_to_TRAILER_VISIT.toDF(*["SQ_Shortcut_to_TRAILER_VISIT___" + col for col in SQ_Shortcut_to_TRAILER_VISIT.columns])
    
    ExP_TRN = SQ_Shortcut_to_TRAILER_VISIT_temp.selectExpr( 
    	"SQ_Shortcut_to_TRAILER_VISIT___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_TRAILER_VISIT___VISIT_ID as VISIT_ID", 
    	"SQ_Shortcut_to_TRAILER_VISIT___FACILITY_ID as FACILITY_ID", 
    	"SQ_Shortcut_to_TRAILER_VISIT___TRAILER_ID as TRAILER_ID", 
    	"SQ_Shortcut_to_TRAILER_VISIT___CHECKIN_DTTM as CHECKIN_DTTM", 
    	"SQ_Shortcut_to_TRAILER_VISIT___CHECKOUT_DTTM as CHECKOUT_DTTM", 
    	"SQ_Shortcut_to_TRAILER_VISIT___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_TRAILER_VISIT___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_TRAILER_VISIT___CREATED_SOURCE as CREATED_SOURCE", 
    	"SQ_Shortcut_to_TRAILER_VISIT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"SQ_Shortcut_to_TRAILER_VISIT___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_TRAILER_VISIT___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_TRAILER_VISIT_PRE, type TARGET 
    # COLUMN COUNT: 13
    
    
    Shortcut_to_WM_TRAILER_VISIT_PRE = ExP_TRN.selectExpr( 
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
    	"CAST(VISIT_ID AS BIGINT) as VISIT_ID", 
    	"CAST(FACILITY_ID AS BIGINT) as FACILITY_ID", 
    	"CAST(TRAILER_ID AS BIGINT) as TRAILER_ID", 
    	"CAST(CHECKIN_DTTM AS TIMESTAMP) as CHECKIN_DTTM", 
    	"CAST(CHECKOUT_DTTM AS TIMESTAMP) as CHECKOUT_DTTM", 
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
    	"CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", 
    	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", 
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
    	"CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", 
    	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", 
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_TRAILER_VISIT_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_TRAILER_VISIT_PRE is written to the target table - " + target_table_name)
