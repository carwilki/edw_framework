#Code converted on 2023-06-24 13:37:00
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
from logging import getLogger, INFO



def m_WM_E_Job_Function_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_E_Job_Function_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_E_JOB_FUNCTION_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_JOB_FUNCTION"


    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now() #start timestamp of the script

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)

    # COMMAND ----------
    # Variable_declaration_comment
    dcnbr = dcnbr.strip()[2:]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)

    # COMMAND ----------
    # Processing node SQ_Shortcut_to_E_JOB_FUNCTION, type SOURCE 
    # COLUMN COUNT: 49

    SQ_Shortcut_to_E_JOB_FUNCTION = jdbcOracleConnection(
        f"""SELECT
            E_JOB_FUNCTION.JOB_FUNC_ID,
            E_JOB_FUNCTION.NAME,
            E_JOB_FUNCTION.DESCRIPTION,
            E_JOB_FUNCTION.STARTUP_TIME,
            E_JOB_FUNCTION.CLEANUP_TIME,
            E_JOB_FUNCTION.CREATE_DATE_TIME,
            E_JOB_FUNCTION.MOD_DATE_TIME,
            E_JOB_FUNCTION.USER_ID,
            E_JOB_FUNCTION.WHSE,
            E_JOB_FUNCTION.TRANSITION_START_TIME,
            E_JOB_FUNCTION.TRANSITION_END_TIME,
            E_JOB_FUNCTION.JF_TYPE,
            E_JOB_FUNCTION.LEVEL_1,
            E_JOB_FUNCTION.LEVEL_2,
            E_JOB_FUNCTION.LEVEL_3,
            E_JOB_FUNCTION.LEVEL_4,
            E_JOB_FUNCTION.LEVEL_5,
            E_JOB_FUNCTION.OPS_CODE_ID,
            E_JOB_FUNCTION.APPLY_TEAM_SETUP_TIME,
            E_JOB_FUNCTION.TEAM_STARTUP_TIME,
            E_JOB_FUNCTION.TEAM_CLEANUP_TIME,
            E_JOB_FUNCTION.TEAM_TRANSITION_START_TIME,
            E_JOB_FUNCTION.TEAM_TRANSITION_END_TIME,
            E_JOB_FUNCTION.MISC_TXT_1,
            E_JOB_FUNCTION.MISC_TXT_2,
            E_JOB_FUNCTION.MISC_NUM_1,
            E_JOB_FUNCTION.MISC_NUM_2,
            E_JOB_FUNCTION.DFLT_PROC_ZONE_ID,
            E_JOB_FUNCTION.PROC_ZONE_TEMPL_ID,
            E_JOB_FUNCTION.MSRMNT_ID,
            E_JOB_FUNCTION.VERSION_ID,
            E_JOB_FUNCTION.OBS_THRESHOLD_EP,
            E_JOB_FUNCTION.TRACK_HIST_TIME,
            E_JOB_FUNCTION.TRAIN_PERIOD,
            E_JOB_FUNCTION.RETRAIN_PERIOD,
            E_JOB_FUNCTION.OS_ONLY,
            E_JOB_FUNCTION.RETRAIN_REQ_DURATION,
            E_JOB_FUNCTION.PERF_GOAL_IND,
            E_JOB_FUNCTION.TRAINING_REQD,
            E_JOB_FUNCTION.USE_JF_TRAIN,
            E_JOB_FUNCTION.PERF_EVAL_PERIOD_ID,
            E_JOB_FUNCTION.DFLT_MAX_OCCUPANCY,
            E_JOB_FUNCTION.UNQ_SEED_ID,
            E_JOB_FUNCTION.DEFAULT_ACT_ID,
            E_JOB_FUNCTION.CREATED_DTTM,
            E_JOB_FUNCTION.LAST_UPDATED_DTTM,
            E_JOB_FUNCTION.PLAN_EP,
            E_JOB_FUNCTION.WHSE_VISIBILITY_GROUP,
            E_JOB_FUNCTION.DEPT_CODE
        FROM E_JOB_FUNCTION
        WHERE (TRUNC( CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (TRUNC( MOD_DATE_TIME) >=  TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (TRUNC( CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (TRUNC( LAST_UPDATED_DTTM) >=  TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 51

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_JOB_FUNCTION_temp = SQ_Shortcut_to_E_JOB_FUNCTION.toDF(*["SQ_Shortcut_to_E_JOB_FUNCTION___" + col for col in SQ_Shortcut_to_E_JOB_FUNCTION.columns])

    EXPTRANS = SQ_Shortcut_to_E_JOB_FUNCTION_temp.selectExpr( 
        "SQ_Shortcut_to_E_JOB_FUNCTION___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___JOB_FUNC_ID as JOB_FUNC_ID", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___NAME as NAME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___DESCRIPTION as DESCRIPTION", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___STARTUP_TIME as STARTUP_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___CLEANUP_TIME as CLEANUP_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___USER_ID as USER_ID", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___WHSE as WHSE", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___TRANSITION_START_TIME as TRANSITION_START_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___TRANSITION_END_TIME as TRANSITION_END_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___JF_TYPE as JF_TYPE", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___LEVEL_1 as LEVEL_1", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___LEVEL_2 as LEVEL_2", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___LEVEL_3 as LEVEL_3", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___LEVEL_4 as LEVEL_4", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___LEVEL_5 as LEVEL_5", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___OPS_CODE_ID as OPS_CODE_ID", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___APPLY_TEAM_SETUP_TIME as APPLY_TEAM_SETUP_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___TEAM_STARTUP_TIME as TEAM_STARTUP_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___TEAM_CLEANUP_TIME as TEAM_CLEANUP_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___TEAM_TRANSITION_START_TIME as TEAM_TRANSITION_START_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___TEAM_TRANSITION_END_TIME as TEAM_TRANSITION_END_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___MISC_TXT_1 as MISC_TXT_1", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___MISC_TXT_2 as MISC_TXT_2", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___MISC_NUM_1 as MISC_NUM_1", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___MISC_NUM_2 as MISC_NUM_2", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___DFLT_PROC_ZONE_ID as DFLT_PROC_ZONE_ID", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___PROC_ZONE_TEMPL_ID as PROC_ZONE_TEMPL_ID", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___MSRMNT_ID as MSRMNT_ID", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___VERSION_ID as VERSION_ID", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___OBS_THRESHOLD_EP as OBS_THRESHOLD_EP", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___TRACK_HIST_TIME as TRACK_HIST_TIME", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___TRAIN_PERIOD as TRAIN_PERIOD", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___RETRAIN_PERIOD as RETRAIN_PERIOD", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___OS_ONLY as OS_ONLY", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___RETRAIN_REQ_DURATION as RETRAIN_REQ_DURATION", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___PERF_GOAL_IND as PERF_GOAL_IND", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___TRAINING_REQD as TRAINING_REQD", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___USE_JF_TRAIN as USE_JF_TRAIN", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___PERF_EVAL_PERIOD_ID as PERF_EVAL_PERIOD_ID", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___DFLT_MAX_OCCUPANCY as DFLT_MAX_OCCUPANCY", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___UNQ_SEED_ID as UNQ_SEED_ID", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___DEFAULT_ACT_ID as DEFAULT_ACT_ID", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___PLAN_EP as PLAN_EP", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___WHSE_VISIBILITY_GROUP as WHSE_VISIBILITY_GROUP", 
        "SQ_Shortcut_to_E_JOB_FUNCTION___DEPT_CODE as DEPT_CODE", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_JOB_FUNCTION_PRE, type TARGET 
    # COLUMN COUNT: 51


    Shortcut_to_WM_E_JOB_FUNCTION_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(JOB_FUNC_ID AS BIGINT) as JOB_FUNC_ID", 
        "CAST(NAME AS STRING) as NAME", 
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION", 
        "CAST(STARTUP_TIME AS BIGINT) as STARTUP_TIME", 
        "CAST(CLEANUP_TIME AS BIGINT) as CLEANUP_TIME", 
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
        "CAST(USER_ID AS STRING) as USER_ID", 
        "CAST(WHSE AS STRING) as WHSE", 
        "CAST(TRANSITION_START_TIME AS BIGINT) as TRANSITION_START_TIME", 
        "CAST(TRANSITION_END_TIME AS BIGINT) as TRANSITION_END_TIME", 
        "CAST(JF_TYPE AS STRING) as JF_TYPE", 
        "CAST(LEVEL_1 AS STRING) as LEVEL_1", 
        "CAST(LEVEL_2 AS STRING) as LEVEL_2", 
        "CAST(LEVEL_3 AS STRING) as LEVEL_3", 
        "CAST(LEVEL_4 AS STRING) as LEVEL_4", 
        "CAST(LEVEL_5 AS STRING) as LEVEL_5", 
        "CAST(OPS_CODE_ID AS BIGINT) as OPS_CODE_ID", 
        "CAST(APPLY_TEAM_SETUP_TIME AS STRING) as APPLY_TEAM_SETUP_TIME", 
        "CAST(TEAM_STARTUP_TIME AS BIGINT) as TEAM_STARTUP_TIME", 
        "CAST(TEAM_CLEANUP_TIME AS BIGINT) as TEAM_CLEANUP_TIME", 
        "CAST(TEAM_TRANSITION_START_TIME AS BIGINT) as TEAM_TRANSITION_START_TIME", 
        "CAST(TEAM_TRANSITION_END_TIME AS BIGINT) as TEAM_TRANSITION_END_TIME", 
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1", 
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2", 
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1", 
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2", 
        "CAST(DFLT_PROC_ZONE_ID AS BIGINT) as DFLT_PROC_ZONE_ID", 
        "CAST(PROC_ZONE_TEMPL_ID AS BIGINT) as PROC_ZONE_TEMPL_ID", 
        "CAST(MSRMNT_ID AS BIGINT) as MSRMNT_ID", 
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID", 
        "CAST(OBS_THRESHOLD_EP AS BIGINT) as OBS_THRESHOLD_EP", 
        "CAST(TRACK_HIST_TIME AS STRING) as TRACK_HIST_TIME", 
        "CAST(TRAIN_PERIOD AS BIGINT) as TRAIN_PERIOD", 
        "CAST(RETRAIN_PERIOD AS BIGINT) as RETRAIN_PERIOD", 
        "CAST(OS_ONLY AS STRING) as OS_ONLY", 
        "CAST(RETRAIN_REQ_DURATION AS BIGINT) as RETRAIN_REQ_DURATION", 
        "CAST(PERF_GOAL_IND AS STRING) as PERF_GOAL_IND", 
        "CAST(TRAINING_REQD AS STRING) as TRAINING_REQD", 
        "CAST(USE_JF_TRAIN AS STRING) as USE_JF_TRAIN", 
        "CAST(PERF_EVAL_PERIOD_ID AS STRING) as PERF_EVAL_PERIOD_ID", 
        "CAST(DFLT_MAX_OCCUPANCY AS BIGINT) as DFLT_MAX_OCCUPANCY", 
        "CAST(UNQ_SEED_ID AS BIGINT) as UNQ_SEED_ID", 
        "CAST(DEFAULT_ACT_ID AS BIGINT) as DEFAULT_ACT_ID", 
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
        "CAST(PLAN_EP AS BIGINT) as PLAN_EP", 
        "CAST(WHSE_VISIBILITY_GROUP AS STRING) as WHSE_VISIBILITY_GROUP", 
        "CAST(DEPT_CODE AS STRING) as DEPT_CODE", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_E_JOB_FUNCTION_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_JOB_FUNCTION_PRE is written to the target table - "
        + target_table_name
    )