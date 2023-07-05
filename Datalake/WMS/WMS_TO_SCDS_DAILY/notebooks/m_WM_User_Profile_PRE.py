#Code converted on 2023-06-22 21:03:26
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



def m_WM_User_Profile_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_User_Profile_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_USER_PROFILE_PRE"
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
                    USER_PROFILE.LOGIN_USER_ID,
                    USER_PROFILE.MENU_ID,
                    USER_PROFILE.EMPLYE_ID,
                    USER_PROFILE.RESTR_TASK_GRP_TO_DFLT,
                    USER_PROFILE.RESTR_MENU_MODE_TO_DFLT,
                    USER_PROFILE.DFLT_RF_MENU_MODE,
                    USER_PROFILE.LANG_ID,
                    USER_PROFILE.DATE_MASK,
                    USER_PROFILE.LAST_TASK,
                    USER_PROFILE.LAST_LOCN,
                    USER_PROFILE.LAST_WORK_GRP,
                    USER_PROFILE.LAST_WORK_AREA,
                    USER_PROFILE.ALLOW_TASK_INT_CHG,
                    USER_PROFILE.NBR_OF_TASK_TO_DSP,
                    USER_PROFILE.TASK_DSP_MODE,
                    USER_PROFILE.DB_USER_ID,
                    USER_PROFILE.DB_PSWD,
                    USER_PROFILE.DB_CONNECT_STRING,
                    USER_PROFILE.PRTR_REQSTR,
                    USER_PROFILE.IDLE_TIME_BEF_SHTDWN,
                    USER_PROFILE.RF_MENU_ID,
                    USER_PROFILE.USER_ID,
                    USER_PROFILE.CREATE_DATE_TIME,
                    USER_PROFILE.MOD_DATE_TIME,
                    USER_PROFILE.PAGE_SIZE,
                    USER_PROFILE.VOCOLLECT_WORK_TYPE,
                    USER_PROFILE.CURR_TASK_GRP,
                    USER_PROFILE.CURR_VOCOLLECT_PTS_CASE,
                    USER_PROFILE.CURR_VOCOLLECT_REASON_CODE,
                    USER_PROFILE.TASK_GRP_JUMP_FLAG,
                    USER_PROFILE.AUTO_3PL_LOGIN_FLAG,
                    USER_PROFILE.SECURITY_CONTEXT_ID,
                    USER_PROFILE.SEC_USER_NAME,
                    USER_PROFILE.VOCOLLECT_PUTAWAY_FLAG,
                    USER_PROFILE.VOCOLLECT_REPLEN_FLAG,
                    USER_PROFILE.VOCOLLECT_PACKING_FLAG,
                    USER_PROFILE.CLS_TIMEZONE_ID,
                    USER_PROFILE.DAL_CONNECTION_STRING,
                    USER_PROFILE.USER_PROFILE_ID,
                    USER_PROFILE.WM_VERSION_ID,
                    USER_PROFILE.USER_SECURITY_CONTEXT_ID,
                    USER_PROFILE.DFLT_TASK_INT,
                    USER_PROFILE.CREATED_DTTM,
                    USER_PROFILE.LAST_UPDATED_DTTM,
                    USER_PROFILE.MOBILE_HELP_TEXT,
                    USER_PROFILE.MOB_SPLASH_SCREEN_FLAG,
                    USER_PROFILE.SCREEN_TYPE_ID,
                    USER_PROFILE.MOBILE_MSG_SPEECH_LEVEL,
                    USER_PROFILE.MOBILE_MSG_SPEECH_RATE
                FROM {source_schema}.USER_PROFILE
                WHERE  (TRUNC(USER_PROFILE.CREATE_DATE_TIME)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (TRUNC(USER_PROFILE.MOD_DATE_TIME)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (TRUNC(USER_PROFILE.LAST_UPDATED_DTTM)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (TRUNC(USER_PROFILE.CREATED_DTTM)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1)"""
            

    SQ_Shortcut_to_USER_PROFILE = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_USER_PROFILE is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 51
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_USER_PROFILE_temp = SQ_Shortcut_to_USER_PROFILE.toDF(*["SQ_Shortcut_to_USER_PROFILE___" + col for col in SQ_Shortcut_to_USER_PROFILE.columns])
    
    EXPTRANS = SQ_Shortcut_to_USER_PROFILE_temp.selectExpr( 
    	"SQ_Shortcut_to_USER_PROFILE___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_USER_PROFILE___USER_PROFILE_ID as USER_PROFILE_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___LOGIN_USER_ID as LOGIN_USER_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___MENU_ID as MENU_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___EMPLYE_ID as EMPLYE_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___RESTR_TASK_GRP_TO_DFLT as RESTR_TASK_GRP_TO_DFLT", 
    	"SQ_Shortcut_to_USER_PROFILE___RESTR_MENU_MODE_TO_DFLT as RESTR_MENU_MODE_TO_DFLT", 
    	"SQ_Shortcut_to_USER_PROFILE___DFLT_RF_MENU_MODE as DFLT_RF_MENU_MODE", 
    	"SQ_Shortcut_to_USER_PROFILE___LANG_ID as LANG_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___DATE_MASK as DATE_MASK", 
    	"SQ_Shortcut_to_USER_PROFILE___LAST_TASK as LAST_TASK", 
    	"SQ_Shortcut_to_USER_PROFILE___LAST_LOCN as LAST_LOCN", 
    	"SQ_Shortcut_to_USER_PROFILE___LAST_WORK_GRP as LAST_WORK_GRP", 
    	"SQ_Shortcut_to_USER_PROFILE___LAST_WORK_AREA as LAST_WORK_AREA", 
    	"SQ_Shortcut_to_USER_PROFILE___ALLOW_TASK_INT_CHG as ALLOW_TASK_INT_CHG", 
    	"SQ_Shortcut_to_USER_PROFILE___NBR_OF_TASK_TO_DSP as NBR_OF_TASK_TO_DSP", 
    	"SQ_Shortcut_to_USER_PROFILE___TASK_DSP_MODE as TASK_DSP_MODE", 
    	"SQ_Shortcut_to_USER_PROFILE___DB_USER_ID as DB_USER_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___DB_PSWD as DB_PSWD", 
    	"SQ_Shortcut_to_USER_PROFILE___DB_CONNECT_STRING as DB_CONNECT_STRING", 
    	"SQ_Shortcut_to_USER_PROFILE___PRTR_REQSTR as PRTR_REQSTR", 
    	"SQ_Shortcut_to_USER_PROFILE___IDLE_TIME_BEF_SHTDWN as IDLE_TIME_BEF_SHTDWN", 
    	"SQ_Shortcut_to_USER_PROFILE___RF_MENU_ID as RF_MENU_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___USER_ID as USER_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
    	"SQ_Shortcut_to_USER_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", 
    	"SQ_Shortcut_to_USER_PROFILE___PAGE_SIZE as PAGE_SIZE", 
    	"SQ_Shortcut_to_USER_PROFILE___VOCOLLECT_WORK_TYPE as VOCOLLECT_WORK_TYPE", 
    	"SQ_Shortcut_to_USER_PROFILE___CURR_TASK_GRP as CURR_TASK_GRP", 
    	"SQ_Shortcut_to_USER_PROFILE___CURR_VOCOLLECT_PTS_CASE as CURR_VOCOLLECT_PTS_CASE", 
    	"SQ_Shortcut_to_USER_PROFILE___CURR_VOCOLLECT_REASON_CODE as CURR_VOCOLLECT_REASON_CODE", 
    	"SQ_Shortcut_to_USER_PROFILE___TASK_GRP_JUMP_FLAG as TASK_GRP_JUMP_FLAG", 
    	"SQ_Shortcut_to_USER_PROFILE___AUTO_3PL_LOGIN_FLAG as AUTO_3PL_LOGIN_FLAG", 
    	"SQ_Shortcut_to_USER_PROFILE___SECURITY_CONTEXT_ID as SECURITY_CONTEXT_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___SEC_USER_NAME as SEC_USER_NAME", 
    	"SQ_Shortcut_to_USER_PROFILE___VOCOLLECT_PUTAWAY_FLAG as VOCOLLECT_PUTAWAY_FLAG", 
    	"SQ_Shortcut_to_USER_PROFILE___VOCOLLECT_REPLEN_FLAG as VOCOLLECT_REPLEN_FLAG", 
    	"SQ_Shortcut_to_USER_PROFILE___VOCOLLECT_PACKING_FLAG as VOCOLLECT_PACKING_FLAG", 
    	"SQ_Shortcut_to_USER_PROFILE___CLS_TIMEZONE_ID as CLS_TIMEZONE_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___DAL_CONNECTION_STRING as DAL_CONNECTION_STRING", 
    	"SQ_Shortcut_to_USER_PROFILE___WM_VERSION_ID as WM_VERSION_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___USER_SECURITY_CONTEXT_ID as USER_SECURITY_CONTEXT_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___DFLT_TASK_INT as DFLT_TASK_INT", 
    	"SQ_Shortcut_to_USER_PROFILE___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_USER_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"SQ_Shortcut_to_USER_PROFILE___MOBILE_HELP_TEXT as MOBILE_HELP_TEXT", 
    	"SQ_Shortcut_to_USER_PROFILE___MOB_SPLASH_SCREEN_FLAG as MOB_SPLASH_SCREEN_FLAG", 
    	"SQ_Shortcut_to_USER_PROFILE___SCREEN_TYPE_ID as SCREEN_TYPE_ID", 
    	"SQ_Shortcut_to_USER_PROFILE___MOBILE_MSG_SPEECH_LEVEL as MOBILE_MSG_SPEECH_LEVEL", 
    	"SQ_Shortcut_to_USER_PROFILE___MOBILE_MSG_SPEECH_RATE as MOBILE_MSG_SPEECH_RATE", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_USER_PROFILE_PRE, type TARGET 
    # COLUMN COUNT: 51
    
    
    Shortcut_to_WM_USER_PROFILE_PRE = EXPTRANS.selectExpr( 
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
    	"CAST(USER_PROFILE_ID AS BIGINT) as USER_PROFILE_ID", 
    	"CAST(LOGIN_USER_ID AS STRING) as LOGIN_USER_ID", 
    	"CAST(MENU_ID AS BIGINT) as MENU_ID", 
    	"CAST(EMPLYE_ID AS STRING) as EMPLYE_ID", 
    	"CAST(RESTR_TASK_GRP_TO_DFLT AS STRING) as RESTR_TASK_GRP_TO_DFLT", 
    	"CAST(RESTR_MENU_MODE_TO_DFLT AS STRING) as RESTR_MENU_MODE_TO_DFLT", 
    	"CAST(DFLT_RF_MENU_MODE AS STRING) as DFLT_RF_MENU_MODE", 
    	"CAST(LANG_ID AS STRING) as LANG_ID", 
    	"CAST(DATE_MASK AS STRING) as DATE_MASK", 
    	"CAST(LAST_TASK AS STRING) as LAST_TASK", 
    	"CAST(LAST_LOCN AS STRING) as LAST_LOCN", 
    	"CAST(LAST_WORK_GRP AS STRING) as LAST_WORK_GRP", 
    	"CAST(LAST_WORK_AREA AS STRING) as LAST_WORK_AREA", 
    	"CAST(ALLOW_TASK_INT_CHG AS STRING) as ALLOW_TASK_INT_CHG", 
    	"CAST(NBR_OF_TASK_TO_DSP AS BIGINT) as NBR_OF_TASK_TO_DSP", 
    	"CAST(TASK_DSP_MODE AS STRING) as TASK_DSP_MODE", 
    	"CAST(DB_USER_ID AS STRING) as DB_USER_ID", 
    	"CAST(DB_PSWD AS STRING) as DB_PSWD", 
    	"CAST(DB_CONNECT_STRING AS STRING) as DB_CONNECT_STRING", 
    	"CAST(PRTR_REQSTR AS STRING) as PRTR_REQSTR", 
    	"CAST(IDLE_TIME_BEF_SHTDWN AS BIGINT) as IDLE_TIME_BEF_SHTDWN", 
    	"CAST(RF_MENU_ID AS BIGINT) as RF_MENU_ID", 
    	"CAST(USER_ID AS STRING) as USER_ID", 
    	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
    	"CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
    	"CAST(PAGE_SIZE AS BIGINT) as PAGE_SIZE", 
    	"CAST(VOCOLLECT_WORK_TYPE AS BIGINT) as VOCOLLECT_WORK_TYPE", 
    	"CAST(CURR_TASK_GRP AS STRING) as CURR_TASK_GRP", 
    	"CAST(CURR_VOCOLLECT_PTS_CASE AS STRING) as CURR_VOCOLLECT_PTS_CASE", 
    	"CAST(CURR_VOCOLLECT_REASON_CODE AS STRING) as CURR_VOCOLLECT_REASON_CODE", 
    	"CAST(TASK_GRP_JUMP_FLAG AS STRING) as TASK_GRP_JUMP_FLAG", 
    	"CAST(AUTO_3PL_LOGIN_FLAG AS STRING) as AUTO_3PL_LOGIN_FLAG", 
    	"CAST(SECURITY_CONTEXT_ID AS BIGINT) as SECURITY_CONTEXT_ID", 
    	"CAST(SEC_USER_NAME AS STRING) as SEC_USER_NAME", 
    	"CAST(VOCOLLECT_PUTAWAY_FLAG AS BIGINT) as VOCOLLECT_PUTAWAY_FLAG", 
    	"CAST(VOCOLLECT_REPLEN_FLAG AS BIGINT) as VOCOLLECT_REPLEN_FLAG", 
    	"CAST(VOCOLLECT_PACKING_FLAG AS BIGINT) as VOCOLLECT_PACKING_FLAG", 
    	"CAST(CLS_TIMEZONE_ID AS BIGINT) as CLS_TIMEZONE_ID", 
    	"CAST(DAL_CONNECTION_STRING AS STRING) as DAL_CONNECTION_STRING", 
    	"CAST(WM_VERSION_ID AS BIGINT) as WM_VERSION_ID", 
    	"CAST(USER_SECURITY_CONTEXT_ID AS BIGINT) as USER_SECURITY_CONTEXT_ID", 
    	"CAST(DFLT_TASK_INT AS BIGINT) as DFLT_TASK_INT", 
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
    	"CAST(MOBILE_HELP_TEXT AS STRING) as MOBILE_HELP_TEXT", 
    	"CAST(MOB_SPLASH_SCREEN_FLAG AS STRING) as MOB_SPLASH_SCREEN_FLAG", 
    	"CAST(SCREEN_TYPE_ID AS BIGINT) as SCREEN_TYPE_ID", 
    	"CAST(MOBILE_MSG_SPEECH_LEVEL AS STRING) as MOBILE_MSG_SPEECH_LEVEL", 
    	"CAST(MOBILE_MSG_SPEECH_RATE AS STRING) as MOBILE_MSG_SPEECH_RATE", 
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_USER_PROFILE_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_USER_PROFILE_PRE is written to the target table - " + target_table_name)
