#Code converted on 2023-06-22 21:02:51
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



def m_WM_Trailer_Ref_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Trailer_Ref_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_TRAILER_REF_PRE"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_TRAILER_REF"
    prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + prev_run_dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
                    TRAILER_REF.TRAILER_ID,
                    TRAILER_REF.TRAILER_STATUS,
                    TRAILER_REF.CURRENT_LOCATION_ID,
                    TRAILER_REF.ASSIGNED_LOCATION_ID,
                    TRAILER_REF.ACTIVE_VISIT_ID,
                    TRAILER_REF.ACTIVE_VISIT_DETAIL_ID,
                    TRAILER_REF.CREATED_DTTM,
                    TRAILER_REF.CREATED_SOURCE_TYPE,
                    TRAILER_REF.CREATED_SOURCE,
                    TRAILER_REF.LAST_UPDATED_DTTM,
                    TRAILER_REF.LAST_UPDATED_SOURCE_TYPE,
                    TRAILER_REF.LAST_UPDATED_SOURCE,
                    TRAILER_REF.TRAILER_LOCATION_STATUS,
                    TRAILER_REF.CONVEYABLE,
                    TRAILER_REF.PROTECTION_LEVEL,
                    TRAILER_REF.PRODUCT_CLASS
                FROM TRAILER_REF
                WHERE {Initial_Load} (TRUNC(TRAILER_REF.CREATED_DTTM)>= TRUNC(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC(TRAILER_REF.LAST_UPDATED_DTTM)>= TRUNC(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)"""
    

    SQ_Shortcut_to_TRAILER_REF = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_TRAILER_REF is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 18
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_TRAILER_REF_temp = SQ_Shortcut_to_TRAILER_REF.toDF(*["SQ_Shortcut_to_TRAILER_REF___" + col for col in SQ_Shortcut_to_TRAILER_REF.columns])
    
    EXPTRANS = SQ_Shortcut_to_TRAILER_REF_temp.selectExpr( 
    	"SQ_Shortcut_to_TRAILER_REF___sys_row_id as sys_row_id", 
    	f"{DC_NBR} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_TRAILER_REF___TRAILER_ID as TRAILER_ID", 
    	"SQ_Shortcut_to_TRAILER_REF___TRAILER_STATUS as TRAILER_STATUS", 
    	"SQ_Shortcut_to_TRAILER_REF___CURRENT_LOCATION_ID as CURRENT_LOCATION_ID", 
    	"SQ_Shortcut_to_TRAILER_REF___ASSIGNED_LOCATION_ID as ASSIGNED_LOCATION_ID", 
    	"SQ_Shortcut_to_TRAILER_REF___ACTIVE_VISIT_ID as ACTIVE_VISIT_ID", 
    	"SQ_Shortcut_to_TRAILER_REF___ACTIVE_VISIT_DETAIL_ID as ACTIVE_VISIT_DETAIL_ID", 
    	"SQ_Shortcut_to_TRAILER_REF___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_TRAILER_REF___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_TRAILER_REF___CREATED_SOURCE as CREATED_SOURCE", 
    	"SQ_Shortcut_to_TRAILER_REF___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"SQ_Shortcut_to_TRAILER_REF___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_TRAILER_REF___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
    	"SQ_Shortcut_to_TRAILER_REF___TRAILER_LOCATION_STATUS as TRAILER_LOCATION_STATUS", 
    	"SQ_Shortcut_to_TRAILER_REF___CONVEYABLE as CONVEYABLE", 
    	"SQ_Shortcut_to_TRAILER_REF___PROTECTION_LEVEL as PROTECTION_LEVEL", 
    	"SQ_Shortcut_to_TRAILER_REF___PRODUCT_CLASS as PRODUCT_CLASS", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_TRAILER_REF_PRE, type TARGET 
    # COLUMN COUNT: 18
    
    
    Shortcut_to_WM_TRAILER_REF_PRE = EXPTRANS.selectExpr( 
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
    	"CAST(TRAILER_ID AS BIGINT) as TRAILER_ID", 
    	"CAST(TRAILER_STATUS AS BIGINT) as TRAILER_STATUS", 
    	"CAST(CURRENT_LOCATION_ID AS STRING) as CURRENT_LOCATION_ID", 
    	"CAST(ASSIGNED_LOCATION_ID AS STRING) as ASSIGNED_LOCATION_ID", 
    	"CAST(ACTIVE_VISIT_ID AS BIGINT) as ACTIVE_VISIT_ID", 
    	"CAST(ACTIVE_VISIT_DETAIL_ID AS BIGINT) as ACTIVE_VISIT_DETAIL_ID", 
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
    	"CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", 
    	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", 
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
    	"CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", 
    	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", 
    	"CAST(TRAILER_LOCATION_STATUS AS BIGINT) as TRAILER_LOCATION_STATUS", 
    	"CAST(CONVEYABLE AS BIGINT) as CONVEYABLE", 
    	"CAST(PROTECTION_LEVEL AS BIGINT) as PROTECTION_LEVEL", 
    	"CAST(PRODUCT_CLASS AS BIGINT) as PRODUCT_CLASS", 
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    gu.overwriteDeltaPartition(Shortcut_to_WM_TRAILER_REF_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_TRAILER_REF_PRE is written to the target table - " + target_table_name)
