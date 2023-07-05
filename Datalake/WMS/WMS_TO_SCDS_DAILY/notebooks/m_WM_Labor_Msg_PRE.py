#Code converted on 2023-06-26 10:18:06
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
from logging import getLogger, INFO



def m_WM_Labor_Msg_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Labor_Msg_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_LABOR_MSG_PRE"

    schemaName = raw
    source_schema = "WMSMIS"


    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]


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
    # Processing node SQ_Shortcut_to_LABOR_MSG, type SOURCE 
    # COLUMN COUNT: 78

    SQ_Shortcut_to_LABOR_MSG = jdbcOracleConnection(  f"""SELECT
    LABOR_MSG.LABOR_MSG_ID,
    LABOR_MSG.STATUS,
    LABOR_MSG.WHSE,
    LABOR_MSG.TRAN_NBR,
    LABOR_MSG.ACT_NAME,
    LABOR_MSG.LOGIN_USER_ID,
    LABOR_MSG.SCHED_START_TIME,
    LABOR_MSG.SCHED_START_DATE,
    LABOR_MSG.VHCL_TYPE,
    LABOR_MSG.REF_CODE,
    LABOR_MSG.REF_NBR,
    LABOR_MSG.TC_COMPANY_ID,
    LABOR_MSG.DIV,
    LABOR_MSG.SHIFT,
    LABOR_MSG.USER_DEF_VAL_1,
    LABOR_MSG.USER_DEF_VAL_2,
    LABOR_MSG.MOD_USER_ID,
    LABOR_MSG.RESEND_TRAN,
    LABOR_MSG.REQ_SAM_REPLY,
    LABOR_MSG.PRIORITY,
    LABOR_MSG.ENGINE_CODE,
    LABOR_MSG.TEAM_STD_GRP,
    LABOR_MSG.MISC,
    LABOR_MSG.MISC_2,
    LABOR_MSG.CREATED_SOURCE_TYPE,
    LABOR_MSG.CREATED_SOURCE,
    LABOR_MSG.CREATED_DTTM,
    LABOR_MSG.LAST_UPDATED_SOURCE_TYPE,
    LABOR_MSG.LAST_UPDATED_SOURCE,
    LABOR_MSG.LAST_UPDATED_DTTM,
    LABOR_MSG.ACTUAL_END_TIME,
    LABOR_MSG.ACTUAL_END_DATE,
    LABOR_MSG.PLAN_ID,
    LABOR_MSG.WAVE_NBR,
    LABOR_MSG.TRANS_TYPE,
    LABOR_MSG.TRANS_VALUE,
    LABOR_MSG.ENGINE_GROUP,
    LABOR_MSG.COMPLETED_WORK,
    LABOR_MSG.WHSE_DATE,
    LABOR_MSG.JOB_FUNCTION,
    LABOR_MSG.LEVEL_1,
    LABOR_MSG.LEVEL_2,
    LABOR_MSG.LEVEL_3,
    LABOR_MSG.LEVEL_4,
    LABOR_MSG.LEVEL_5,
    LABOR_MSG.CARTON_NBR,
    LABOR_MSG.TASK_NBR,
    LABOR_MSG.CASE_NBR,
    LABOR_MSG.ORIG_COMPLETED_WORK,
    LABOR_MSG.TRAN_DATA_ELS_TRAN_ID,
    LABOR_MSG.MISC_TXT_1,
    LABOR_MSG.MISC_TXT_2,
    LABOR_MSG.MISC_NUM_1,
    LABOR_MSG.MISC_NUM_2,
    LABOR_MSG.EVNT_CTGRY_1,
    LABOR_MSG.EVNT_CTGRY_2,
    LABOR_MSG.EVNT_CTGRY_3,
    LABOR_MSG.EVNT_CTGRY_4,
    LABOR_MSG.EVNT_CTGRY_5,
    LABOR_MSG.HIBERNATE_VERSION,
    LABOR_MSG.DEPT,
    LABOR_MSG.MSG_STAT_CODE,
    LABOR_MSG.MONITOR_SMRY_ID,
    LABOR_MSG.ORIG_ACT_NAME,
    LABOR_MSG.SOURCE,
    LABOR_MSG.HAS_MONITOR_MSG,
    LABOR_MSG.TOTAL_QTY,
    LABOR_MSG.ORIG_EVNT_START_TIME,
    LABOR_MSG.ORIG_EVNT_END_TIME,
    LABOR_MSG.ADJ_REASON_CODE,
    LABOR_MSG.ADJ_REF_TRAN_NBR,
    LABOR_MSG.INT_TYPE,
    LABOR_MSG.CONFLICT,
    LABOR_MSG.DISPLAY_UOM,
    LABOR_MSG.UOM_QTY,
    LABOR_MSG.THRUPUT_MIN,
    LABOR_MSG.LOCN_GRP_ATTR,
    LABOR_MSG.RESOURCE_GROUP_ID
    FROM {source_schema}.LABOR_MSG
    WHERE (trunc(LABOR_MSG.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(LABOR_MSG.LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)  AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 80

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LABOR_MSG_temp = SQ_Shortcut_to_LABOR_MSG.toDF(*["SQ_Shortcut_to_LABOR_MSG___" + col for col in SQ_Shortcut_to_LABOR_MSG.columns])

    EXPTRANS = SQ_Shortcut_to_LABOR_MSG_temp.selectExpr( \
        "SQ_Shortcut_to_LABOR_MSG___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_LABOR_MSG___LABOR_MSG_ID as LABOR_MSG_ID", \
        "SQ_Shortcut_to_LABOR_MSG___STATUS as STATUS", \
        "SQ_Shortcut_to_LABOR_MSG___WHSE as WHSE", \
        "SQ_Shortcut_to_LABOR_MSG___TRAN_NBR as TRAN_NBR", \
        "SQ_Shortcut_to_LABOR_MSG___ACT_NAME as ACT_NAME", \
        "SQ_Shortcut_to_LABOR_MSG___LOGIN_USER_ID as LOGIN_USER_ID", \
        "SQ_Shortcut_to_LABOR_MSG___SCHED_START_TIME as SCHED_START_TIME", \
        "SQ_Shortcut_to_LABOR_MSG___SCHED_START_DATE as SCHED_START_DATE", \
        "SQ_Shortcut_to_LABOR_MSG___VHCL_TYPE as VHCL_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG___REF_CODE as REF_CODE", \
        "SQ_Shortcut_to_LABOR_MSG___REF_NBR as REF_NBR", \
        "SQ_Shortcut_to_LABOR_MSG___TC_COMPANY_ID as TC_COMPANY_ID", \
        "SQ_Shortcut_to_LABOR_MSG___DIV as DIV", \
        "SQ_Shortcut_to_LABOR_MSG___SHIFT as SHIFT", \
        "SQ_Shortcut_to_LABOR_MSG___USER_DEF_VAL_1 as USER_DEF_VAL_1", \
        "SQ_Shortcut_to_LABOR_MSG___USER_DEF_VAL_2 as USER_DEF_VAL_2", \
        "SQ_Shortcut_to_LABOR_MSG___MOD_USER_ID as MOD_USER_ID", \
        "SQ_Shortcut_to_LABOR_MSG___RESEND_TRAN as RESEND_TRAN", \
        "SQ_Shortcut_to_LABOR_MSG___REQ_SAM_REPLY as REQ_SAM_REPLY", \
        "SQ_Shortcut_to_LABOR_MSG___PRIORITY as PRIORITY", \
        "SQ_Shortcut_to_LABOR_MSG___ENGINE_CODE as ENGINE_CODE", \
        "SQ_Shortcut_to_LABOR_MSG___TEAM_STD_GRP as TEAM_STD_GRP", \
        "SQ_Shortcut_to_LABOR_MSG___MISC as MISC", \
        "SQ_Shortcut_to_LABOR_MSG___MISC_2 as MISC_2", \
        "SQ_Shortcut_to_LABOR_MSG___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_LABOR_MSG___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_LABOR_MSG___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_LABOR_MSG___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_LABOR_MSG___ACTUAL_END_TIME as ACTUAL_END_TIME", \
        "SQ_Shortcut_to_LABOR_MSG___ACTUAL_END_DATE as ACTUAL_END_DATE", \
        "SQ_Shortcut_to_LABOR_MSG___PLAN_ID as PLAN_ID", \
        "SQ_Shortcut_to_LABOR_MSG___WAVE_NBR as WAVE_NBR", \
        "SQ_Shortcut_to_LABOR_MSG___TRANS_TYPE as TRANS_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG___TRANS_VALUE as TRANS_VALUE", \
        "SQ_Shortcut_to_LABOR_MSG___ENGINE_GROUP as ENGINE_GROUP", \
        "SQ_Shortcut_to_LABOR_MSG___COMPLETED_WORK as COMPLETED_WORK", \
        "SQ_Shortcut_to_LABOR_MSG___WHSE_DATE as WHSE_DATE", \
        "SQ_Shortcut_to_LABOR_MSG___JOB_FUNCTION as JOB_FUNCTION", \
        "SQ_Shortcut_to_LABOR_MSG___LEVEL_1 as LEVEL_1", \
        "SQ_Shortcut_to_LABOR_MSG___LEVEL_2 as LEVEL_2", \
        "SQ_Shortcut_to_LABOR_MSG___LEVEL_3 as LEVEL_3", \
        "SQ_Shortcut_to_LABOR_MSG___LEVEL_4 as LEVEL_4", \
        "SQ_Shortcut_to_LABOR_MSG___LEVEL_5 as LEVEL_5", \
        "SQ_Shortcut_to_LABOR_MSG___CARTON_NBR as CARTON_NBR", \
        "SQ_Shortcut_to_LABOR_MSG___TASK_NBR as TASK_NBR", \
        "SQ_Shortcut_to_LABOR_MSG___CASE_NBR as CASE_NBR", \
        "SQ_Shortcut_to_LABOR_MSG___ORIG_COMPLETED_WORK as ORIG_COMPLETED_WORK", \
        "SQ_Shortcut_to_LABOR_MSG___TRAN_DATA_ELS_TRAN_ID as TRAN_DATA_ELS_TRAN_ID", \
        "SQ_Shortcut_to_LABOR_MSG___MISC_TXT_1 as MISC_TXT_1", \
        "SQ_Shortcut_to_LABOR_MSG___MISC_TXT_2 as MISC_TXT_2", \
        "SQ_Shortcut_to_LABOR_MSG___MISC_NUM_1 as MISC_NUM_1", \
        "SQ_Shortcut_to_LABOR_MSG___MISC_NUM_2 as MISC_NUM_2", \
        "SQ_Shortcut_to_LABOR_MSG___EVNT_CTGRY_1 as EVNT_CTGRY_1", \
        "SQ_Shortcut_to_LABOR_MSG___EVNT_CTGRY_2 as EVNT_CTGRY_2", \
        "SQ_Shortcut_to_LABOR_MSG___EVNT_CTGRY_3 as EVNT_CTGRY_3", \
        "SQ_Shortcut_to_LABOR_MSG___EVNT_CTGRY_4 as EVNT_CTGRY_4", \
        "SQ_Shortcut_to_LABOR_MSG___EVNT_CTGRY_5 as EVNT_CTGRY_5", \
        "SQ_Shortcut_to_LABOR_MSG___HIBERNATE_VERSION as HIBERNATE_VERSION", \
        "SQ_Shortcut_to_LABOR_MSG___DEPT as DEPT", \
        "SQ_Shortcut_to_LABOR_MSG___MSG_STAT_CODE as MSG_STAT_CODE", \
        "SQ_Shortcut_to_LABOR_MSG___MONITOR_SMRY_ID as MONITOR_SMRY_ID", \
        "SQ_Shortcut_to_LABOR_MSG___ORIG_ACT_NAME as ORIG_ACT_NAME", \
        "SQ_Shortcut_to_LABOR_MSG___SOURCE as SOURCE", \
        "SQ_Shortcut_to_LABOR_MSG___HAS_MONITOR_MSG as HAS_MONITOR_MSG", \
        "SQ_Shortcut_to_LABOR_MSG___TOTAL_QTY as TOTAL_QTY", \
        "SQ_Shortcut_to_LABOR_MSG___ORIG_EVNT_START_TIME as ORIG_EVNT_START_TIME", \
        "SQ_Shortcut_to_LABOR_MSG___ORIG_EVNT_END_TIME as ORIG_EVNT_END_TIME", \
        "SQ_Shortcut_to_LABOR_MSG___ADJ_REASON_CODE as ADJ_REASON_CODE", \
        "SQ_Shortcut_to_LABOR_MSG___ADJ_REF_TRAN_NBR as ADJ_REF_TRAN_NBR", \
        "SQ_Shortcut_to_LABOR_MSG___INT_TYPE as INT_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG___CONFLICT as CONFLICT", \
        "SQ_Shortcut_to_LABOR_MSG___DISPLAY_UOM as DISPLAY_UOM", \
        "SQ_Shortcut_to_LABOR_MSG___UOM_QTY as UOM_QTY", \
        "SQ_Shortcut_to_LABOR_MSG___THRUPUT_MIN as THRUPUT_MIN", \
        "SQ_Shortcut_to_LABOR_MSG___LOCN_GRP_ATTR as LOCN_GRP_ATTR", \
        "SQ_Shortcut_to_LABOR_MSG___RESOURCE_GROUP_ID as RESOURCE_GROUP_ID", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_LABOR_MSG_PRE, type TARGET 
    # COLUMN COUNT: 80


    Shortcut_to_WM_LABOR_MSG_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
        "CAST(LABOR_MSG_ID AS BIGINT) as LABOR_MSG_ID", \
        "CAST(STATUS AS BIGINT) as STATUS", \
        "CAST(WHSE AS STRING) as WHSE", \
        "CAST(TRAN_NBR AS BIGINT) as TRAN_NBR", \
        "CAST(ACT_NAME AS STRING) as ACT_NAME", \
        "CAST(LOGIN_USER_ID AS STRING) as LOGIN_USER_ID", \
        "CAST(SCHED_START_TIME AS TIMESTAMP) as SCHED_START_TIME", \
        "CAST(SCHED_START_DATE AS TIMESTAMP) as SCHED_START_DATE", \
        "CAST(VHCL_TYPE AS STRING) as VHCL_TYPE", \
        "CAST(REF_CODE AS STRING) as REF_CODE", \
        "CAST(REF_NBR AS STRING) as REF_NBR", \
        "CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", \
        "CAST(DIV AS STRING) as DIV", \
        "CAST(SHIFT AS STRING) as SHIFT", \
        "CAST(USER_DEF_VAL_1 AS STRING) as USER_DEF_VAL_1", \
        "CAST(USER_DEF_VAL_2 AS STRING) as USER_DEF_VAL_2", \
        "CAST(MOD_USER_ID AS STRING) as MOD_USER_ID", \
        "CAST(RESEND_TRAN AS STRING) as RESEND_TRAN", \
        "CAST(REQ_SAM_REPLY AS STRING) as REQ_SAM_REPLY", \
        "CAST(PRIORITY AS BIGINT) as PRIORITY", \
        "CAST(ENGINE_CODE AS STRING) as ENGINE_CODE", \
        "CAST(TEAM_STD_GRP AS STRING) as TEAM_STD_GRP", \
        "CAST(MISC AS STRING) as MISC", \
        "CAST(MISC_2 AS STRING) as MISC_2", \
        "CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", \
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
        "CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", \
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
        "CAST(ACTUAL_END_TIME AS TIMESTAMP) as ACTUAL_END_TIME", \
        "CAST(ACTUAL_END_DATE AS TIMESTAMP) as ACTUAL_END_DATE", \
        "CAST(PLAN_ID AS STRING) as PLAN_ID", \
        "CAST(WAVE_NBR AS STRING) as WAVE_NBR", \
        "CAST(TRANS_TYPE AS STRING) as TRANS_TYPE", \
        "CAST(TRANS_VALUE AS STRING) as TRANS_VALUE", \
        "CAST(ENGINE_GROUP AS STRING) as ENGINE_GROUP", \
        "CAST(COMPLETED_WORK AS BIGINT) as COMPLETED_WORK", \
        "CAST(WHSE_DATE AS TIMESTAMP) as WHSE_DATE", \
        "CAST(JOB_FUNCTION AS STRING) as JOB_FUNCTION", \
        "CAST(LEVEL_1 AS STRING) as LEVEL_1", \
        "CAST(LEVEL_2 AS STRING) as LEVEL_2", \
        "CAST(LEVEL_3 AS STRING) as LEVEL_3", \
        "CAST(LEVEL_4 AS STRING) as LEVEL_4", \
        "CAST(LEVEL_5 AS STRING) as LEVEL_5", \
        "CAST(CARTON_NBR AS STRING) as CARTON_NBR", \
        "CAST(TASK_NBR AS STRING) as TASK_NBR", \
        "CAST(CASE_NBR AS STRING) as CASE_NBR", \
        "CAST(ORIG_COMPLETED_WORK AS BIGINT) as ORIG_COMPLETED_WORK", \
        "CAST(TRAN_DATA_ELS_TRAN_ID AS BIGINT) as TRAN_DATA_ELS_TRAN_ID", \
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1", \
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2", \
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1", \
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2", \
        "CAST(EVNT_CTGRY_1 AS STRING) as EVNT_CTGRY_1", \
        "CAST(EVNT_CTGRY_2 AS STRING) as EVNT_CTGRY_2", \
        "CAST(EVNT_CTGRY_3 AS STRING) as EVNT_CTGRY_3", \
        "CAST(EVNT_CTGRY_4 AS STRING) as EVNT_CTGRY_4", \
        "CAST(EVNT_CTGRY_5 AS STRING) as EVNT_CTGRY_5", \
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION", \
        "CAST(DEPT AS STRING) as DEPT", \
        "CAST(MSG_STAT_CODE AS BIGINT) as MSG_STAT_CODE", \
        "CAST(MONITOR_SMRY_ID AS BIGINT) as MONITOR_SMRY_ID", \
        "CAST(ORIG_ACT_NAME AS STRING) as ORIG_ACT_NAME", \
        "CAST(SOURCE AS STRING) as SOURCE", \
        "CAST(HAS_MONITOR_MSG AS BIGINT) as HAS_MONITOR_MSG", \
        "CAST(TOTAL_QTY AS BIGINT) as TOTAL_QTY", \
        "CAST(ORIG_EVNT_START_TIME AS TIMESTAMP) as ORIG_EVNT_START_TIME", \
        "CAST(ORIG_EVNT_END_TIME AS TIMESTAMP) as ORIG_EVNT_END_TIME", \
        "CAST(ADJ_REASON_CODE AS STRING) as ADJ_REASON_CODE", \
        "CAST(ADJ_REF_TRAN_NBR AS BIGINT) as ADJ_REF_TRAN_NBR", \
        "CAST(INT_TYPE AS STRING) as INT_TYPE", \
        "CAST(CONFLICT AS STRING) as CONFLICT", \
        "CAST(DISPLAY_UOM AS STRING) as DISPLAY_UOM", \
        "CAST(UOM_QTY AS BIGINT) as UOM_QTY", \
        "CAST(THRUPUT_MIN AS BIGINT) as THRUPUT_MIN", \
        "CAST(LOCN_GRP_ATTR AS STRING) as LOCN_GRP_ATTR", \
        "CAST(RESOURCE_GROUP_ID AS STRING) as RESOURCE_GROUP_ID", \
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_LABOR_MSG_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_LABOR_MSG_PRE is written to the target table - "
        + target_table_name
    )