#Code converted on 2023-06-22 20:58:42
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



def m_WM_Sec_User_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Sec_User_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_SEC_USER_PRE"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_SEC_USER"
    prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + prev_run_dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
            SEC_USER.SEC_USER_ID,
            SEC_USER.LOGIN_USER_ID,
            SEC_USER.USER_NAME,
            SEC_USER.USER_DESC,
            SEC_USER.PSWD,
            SEC_USER.PSWD_EXP_DATE,
            SEC_USER.PSWD_CHANGE_AT_LOGIN,
            SEC_USER.CAN_CHNG_PSWD,
            SEC_USER.DISABLED,
            SEC_USER.LOCKED_OUT,
            SEC_USER.LAST_LOGIN,
            SEC_USER.GRACE_LOGINS,
            SEC_USER.LOCKED_OUT_EXPIRATION,
            SEC_USER.FAILED_LOGIN_ATTEMPTS,
            SEC_USER.CREATE_DATE_TIME,
            SEC_USER.MOD_DATE_TIME,
            SEC_USER.USER_ID,
            SEC_USER.SEC_POLICY_SET_ID,
            SEC_USER.WM_VERSION_ID
        FROM SEC_USER
        WHERE {Initial_Load} (TRUNC(CREATE_DATE_TIME) >= TRUNC(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (TRUNC(MOD_DATE_TIME) >=  TRUNC(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1)"""
    

    SQ_Shortcut_to_SEC_USER = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_SEC_USER is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 21
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_SEC_USER_temp = SQ_Shortcut_to_SEC_USER.toDF(*["SQ_Shortcut_to_SEC_USER___" + col for col in SQ_Shortcut_to_SEC_USER.columns])
    
    EXPTRANS = SQ_Shortcut_to_SEC_USER_temp.selectExpr( 
    	"SQ_Shortcut_to_SEC_USER___sys_row_id as sys_row_id", 
    	f"{DC_NBR} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_SEC_USER___SEC_USER_ID as SEC_USER_ID", 
    	"SQ_Shortcut_to_SEC_USER___LOGIN_USER_ID as LOGIN_USER_ID", 
    	"SQ_Shortcut_to_SEC_USER___USER_NAME as USER_NAME", 
    	"SQ_Shortcut_to_SEC_USER___USER_DESC as USER_DESC", 
    	"SQ_Shortcut_to_SEC_USER___PSWD as PSWD", 
    	"SQ_Shortcut_to_SEC_USER___PSWD_EXP_DATE as PSWD_EXP_DATE", 
    	"SQ_Shortcut_to_SEC_USER___PSWD_CHANGE_AT_LOGIN as PSWD_CHANGE_AT_LOGIN", 
    	"SQ_Shortcut_to_SEC_USER___CAN_CHNG_PSWD as CAN_CHNG_PSWD", 
    	"SQ_Shortcut_to_SEC_USER___DISABLED as DISABLED", 
    	"SQ_Shortcut_to_SEC_USER___LOCKED_OUT as LOCKED_OUT", 
    	"SQ_Shortcut_to_SEC_USER___LAST_LOGIN as LAST_LOGIN", 
    	"SQ_Shortcut_to_SEC_USER___GRACE_LOGINS as GRACE_LOGINS", 
    	"SQ_Shortcut_to_SEC_USER___LOCKED_OUT_EXPIRATION as LOCKED_OUT_EXPIRATION", 
    	"SQ_Shortcut_to_SEC_USER___FAILED_LOGIN_ATTEMPTS as FAILED_LOGIN_ATTEMPTS", 
    	"SQ_Shortcut_to_SEC_USER___CREATE_DATE_TIME as CREATE_DATE_TIME", 
    	"SQ_Shortcut_to_SEC_USER___MOD_DATE_TIME as MOD_DATE_TIME", 
    	"SQ_Shortcut_to_SEC_USER___USER_ID as USER_ID", 
    	"SQ_Shortcut_to_SEC_USER___SEC_POLICY_SET_ID as SEC_POLICY_SET_ID", 
    	"SQ_Shortcut_to_SEC_USER___WM_VERSION_ID as WM_VERSION_ID", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_SEC_USER_PRE, type TARGET 
    # COLUMN COUNT: 21
    
    
    Shortcut_to_WM_SEC_USER_PRE = EXPTRANS.selectExpr( 
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
    	"CAST(SEC_USER_ID AS BIGINT) as SEC_USER_ID", 
    	"CAST(LOGIN_USER_ID AS STRING) as LOGIN_USER_ID", 
    	"CAST(USER_NAME AS STRING) as USER_NAME", 
    	"CAST(USER_DESC AS STRING) as USER_DESC", 
    	"CAST(PSWD AS STRING) as PSWD", 
    	"CAST(PSWD_EXP_DATE AS TIMESTAMP) as PSWD_EXP_DATE", 
    	"CAST(PSWD_CHANGE_AT_LOGIN AS STRING) as PSWD_CHANGE_AT_LOGIN", 
    	"CAST(CAN_CHNG_PSWD AS STRING) as CAN_CHNG_PSWD", 
    	"CAST(DISABLED AS STRING) as DISABLED", 
    	"CAST(LOCKED_OUT AS STRING) as LOCKED_OUT", 
    	"CAST(LAST_LOGIN AS TIMESTAMP) as LAST_LOGIN", 
    	"CAST(GRACE_LOGINS AS BIGINT) as GRACE_LOGINS", 
    	"CAST(LOCKED_OUT_EXPIRATION AS TIMESTAMP) as LOCKED_OUT_EXPIRATION", 
    	"CAST(FAILED_LOGIN_ATTEMPTS AS BIGINT) as FAILED_LOGIN_ATTEMPTS", 
    	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
    	"CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
    	"CAST(USER_ID AS STRING) as USER_ID", 
    	"CAST(SEC_POLICY_SET_ID AS BIGINT) as SEC_POLICY_SET_ID", 
    	"CAST(WM_VERSION_ID AS BIGINT) as WM_VERSION_ID", 
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    gu.overwriteDeltaPartition(Shortcut_to_WM_SEC_USER_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_SEC_USER_PRE is written to the target table - " + target_table_name)
