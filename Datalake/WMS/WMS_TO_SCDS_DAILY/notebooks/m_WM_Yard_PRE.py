#Code converted on 2023-06-22 21:04:44
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



def m_WM_Yard_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Yard_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_YARD_PRE"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_YARD"
    prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + prev_run_dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
                    YARD.YARD_ID,
                    YARD.TC_COMPANY_ID,
                    YARD.YARD_NAME,
                    YARD.CREATED_SOURCE_TYPE,
                    YARD.CREATED_SOURCE,
                    YARD.CREATED_DTTM,
                    YARD.LAST_UPDATED_SOURCE_TYPE,
                    YARD.LAST_UPDATED_SOURCE,
                    YARD.LAST_UPDATED_DTTM,
                    YARD.DO_GENERATE_MOVE_TASK,
                    YARD.DO_GENERATE_NEXT_EQUIP,
                    YARD.IS_RANGE_TASKS,
                    YARD.IS_SEAL_TASK_TRGD,
                    YARD.DO_OVERRIDE_SYSTEM_TASKS,
                    YARD.IS_TASKING_ALLOWED,
                    YARD.ADDRESS,
                    YARD.CITY,
                    YARD.STATE_PROV,
                    YARD.POSTAL_CODE,
                    YARD.COUNTY,
                    YARD.COUNTRY_CODE,
                    YARD.TIME_ZONE_ID,
                    YARD.MAX_EQUIPMENT_ALLOWED,
                    YARD.UPPER_CHECKIN_TIME_MINS,
                    YARD.LOWER_CHECKIN_TIME_MINS,
                    YARD.FIXED_TIME_MINS,
                    YARD.MARK_FOR_DELETION,
                    YARD.LOCK_TRAILER_ON_MOVE_TO_DOOR,
                    YARD.YARD_SVG_FILE,
                    YARD.LOCATION_ID,
                    YARD.THRESHOLD_PERCENT
                FROM YARD
                WHERE {Initial_Load} (TRUNC( YARD.CREATED_DTTM)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC( YARD.LAST_UPDATED_DTTM)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)"""
    

    SQ_Shortcut_to_YARD = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_YARD is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 33
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_YARD_temp = SQ_Shortcut_to_YARD.toDF(*["SQ_Shortcut_to_YARD___" + col for col in SQ_Shortcut_to_YARD.columns])
    
    EXPTRANS = SQ_Shortcut_to_YARD_temp.selectExpr( 
    	"SQ_Shortcut_to_YARD___sys_row_id as sys_row_id", 
    	f"{DC_NBR}as DC_NBR_EXP", 
    	"SQ_Shortcut_to_YARD___YARD_ID as YARD_ID", 
    	"SQ_Shortcut_to_YARD___TC_COMPANY_ID as TC_COMPANY_ID", 
    	"SQ_Shortcut_to_YARD___YARD_NAME as YARD_NAME", 
    	"SQ_Shortcut_to_YARD___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_YARD___CREATED_SOURCE as CREATED_SOURCE", 
    	"SQ_Shortcut_to_YARD___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_YARD___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_YARD___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
    	"SQ_Shortcut_to_YARD___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"SQ_Shortcut_to_YARD___DO_GENERATE_MOVE_TASK as DO_GENERATE_MOVE_TASK", 
    	"SQ_Shortcut_to_YARD___DO_GENERATE_NEXT_EQUIP as DO_GENERATE_NEXT_EQUIP", 
    	"SQ_Shortcut_to_YARD___IS_RANGE_TASKS as IS_RANGE_TASKS", 
    	"SQ_Shortcut_to_YARD___IS_SEAL_TASK_TRGD as IS_SEAL_TASK_TRGD", 
    	"SQ_Shortcut_to_YARD___DO_OVERRIDE_SYSTEM_TASKS as DO_OVERRIDE_SYSTEM_TASKS", 
    	"SQ_Shortcut_to_YARD___IS_TASKING_ALLOWED as IS_TASKING_ALLOWED", 
    	"SQ_Shortcut_to_YARD___ADDRESS as ADDRESS", 
    	"SQ_Shortcut_to_YARD___CITY as CITY", 
    	"SQ_Shortcut_to_YARD___STATE_PROV as STATE_PROV", 
    	"SQ_Shortcut_to_YARD___POSTAL_CODE as POSTAL_CODE", 
    	"SQ_Shortcut_to_YARD___COUNTY as COUNTY", 
    	"SQ_Shortcut_to_YARD___COUNTRY_CODE as COUNTRY_CODE", 
    	"SQ_Shortcut_to_YARD___TIME_ZONE_ID as TIME_ZONE_ID", 
    	"SQ_Shortcut_to_YARD___MAX_EQUIPMENT_ALLOWED as MAX_EQUIPMENT_ALLOWED", 
    	"SQ_Shortcut_to_YARD___UPPER_CHECKIN_TIME_MINS as UPPER_CHECKIN_TIME_MINS", 
    	"SQ_Shortcut_to_YARD___LOWER_CHECKIN_TIME_MINS as LOWER_CHECKIN_TIME_MINS", 
    	"SQ_Shortcut_to_YARD___FIXED_TIME_MINS as FIXED_TIME_MINS", 
    	"SQ_Shortcut_to_YARD___MARK_FOR_DELETION as MARK_FOR_DELETION", 
    	"SQ_Shortcut_to_YARD___LOCK_TRAILER_ON_MOVE_TO_DOOR as LOCK_TRAILER_ON_MOVE_TO_DOOR", 
    	"SQ_Shortcut_to_YARD___YARD_SVG_FILE as YARD_SVG_FILE", 
    	"SQ_Shortcut_to_YARD___LOCATION_ID as LOCATION_ID", 
    	"SQ_Shortcut_to_YARD___THRESHOLD_PERCENT as THRESHOLD_PERCENT", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_YARD_PRE, type TARGET 
    # COLUMN COUNT: 33
    
    
    Shortcut_to_WM_YARD_PRE = EXPTRANS.selectExpr( 
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
    	"CAST(YARD_ID AS BIGINT) as YARD_ID", 
    	"CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", 
    	"CAST(YARD_NAME AS STRING) as YARD_NAME", 
    	"CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", 
    	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", 
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
    	"CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", 
    	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", 
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
    	"CAST(DO_GENERATE_MOVE_TASK AS BIGINT) as DO_GENERATE_MOVE_TASK", 
    	"CAST(DO_GENERATE_NEXT_EQUIP AS BIGINT) as DO_GENERATE_NEXT_EQUIP", 
    	"CAST(IS_RANGE_TASKS AS BIGINT) as IS_RANGE_TASKS", 
    	"CAST(IS_SEAL_TASK_TRGD AS BIGINT) as IS_SEAL_TASK_TRGD", 
    	"CAST(DO_OVERRIDE_SYSTEM_TASKS AS BIGINT) as DO_OVERRIDE_SYSTEM_TASKS", 
    	"CAST(IS_TASKING_ALLOWED AS BIGINT) as IS_TASKING_ALLOWED", 
    	"CAST(ADDRESS AS STRING) as ADDRESS", 
    	"CAST(CITY AS STRING) as CITY", 
    	"CAST(STATE_PROV AS STRING) as STATE_PROV", 
    	"CAST(POSTAL_CODE AS STRING) as POSTAL_CODE", 
    	"CAST(COUNTY AS STRING) as COUNTY", 
    	"CAST(COUNTRY_CODE AS STRING) as COUNTRY_CODE", 
    	"CAST(TIME_ZONE_ID AS BIGINT) as TIME_ZONE_ID", 
    	"CAST(MAX_EQUIPMENT_ALLOWED AS BIGINT) as MAX_EQUIPMENT_ALLOWED", 
    	"CAST(UPPER_CHECKIN_TIME_MINS AS BIGINT) as UPPER_CHECKIN_TIME_MINS", 
    	"CAST(LOWER_CHECKIN_TIME_MINS AS BIGINT) as LOWER_CHECKIN_TIME_MINS", 
    	"CAST(FIXED_TIME_MINS AS BIGINT) as FIXED_TIME_MINS", 
    	"CAST(MARK_FOR_DELETION AS BIGINT) as MARK_FOR_DELETION", 
    	"CAST(LOCK_TRAILER_ON_MOVE_TO_DOOR AS BIGINT) as LOCK_TRAILER_ON_MOVE_TO_DOOR", 
    	"CAST(YARD_SVG_FILE AS STRING) as YARD_SVG_FILE", 
    	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", 
    	"CAST(THRESHOLD_PERCENT AS BIGINT) as THRESHOLD_PERCENT", 
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    gu.overwriteDeltaPartition(Shortcut_to_WM_YARD_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_YARD_PRE is written to the target table - " + target_table_name)
