#Code converted on 2023-06-24 13:37:18
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



def m_WM_E_Evnt_Smry_Hdr_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_E_Evnt_Smry_Hdr_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_E_EVNT_SMRY_HDR_PRE"

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
    # Processing node SQ_Shortcut_to_E_EVNT_SMRY_HDR, type SOURCE 
    # COLUMN COUNT: 79

    SQ_Shortcut_to_E_EVNT_SMRY_HDR = jdbcOracleConnection(
        f"""SELECT
                E_EVNT_SMRY_HDR.ELS_TRAN_ID,
                E_EVNT_SMRY_HDR.WHSE,
                E_EVNT_SMRY_HDR.TRAN_NBR,
                E_EVNT_SMRY_HDR.CO,
                E_EVNT_SMRY_HDR.DIV,
                cast(E_EVNT_SMRY_HDR.SCHED_START_DATE as timestamp) as SCHED_START_DATE,
                E_EVNT_SMRY_HDR.REF_CODE,
                E_EVNT_SMRY_HDR.REF_NBR,
                E_EVNT_SMRY_HDR.LOGIN_USER_ID,
                E_EVNT_SMRY_HDR.DEPT_CODE,
                E_EVNT_SMRY_HDR.VHCL_TYPE,
                E_EVNT_SMRY_HDR.SHIFT_CODE,
                E_EVNT_SMRY_HDR.USER_DEF_FIELD_1,
                E_EVNT_SMRY_HDR.USER_DEF_FIELD_2,
                E_EVNT_SMRY_HDR.STD_TIME_ALLOW,
                E_EVNT_SMRY_HDR.EMP_PERF_ALLOW,
                E_EVNT_SMRY_HDR.PERF_ADJ_AMT,
                E_EVNT_SMRY_HDR.SCHED_ADJ_AMT,
                E_EVNT_SMRY_HDR.SCHED_UNITS_QTY,
                E_EVNT_SMRY_HDR.ACTL_TIME,
                E_EVNT_SMRY_HDR.ERROR_CNT,
                E_EVNT_SMRY_HDR.EVNT_STAT_CODE,
                E_EVNT_SMRY_HDR.CREATE_DATE_TIME,
                E_EVNT_SMRY_HDR.MOD_DATE_TIME,
                E_EVNT_SMRY_HDR.USER_ID,
                E_EVNT_SMRY_HDR.TMU_RECALC_COUNT,
                E_EVNT_SMRY_HDR.MISC,
                E_EVNT_SMRY_HDR.LABOR_TYPE_ID,
                E_EVNT_SMRY_HDR.SOURCE,
                E_EVNT_SMRY_HDR.EVENT_TYPE,
                E_EVNT_SMRY_HDR.TOTAL_WEIGHT,
                E_EVNT_SMRY_HDR.TEAM_STD_SMRY_ID,
                E_EVNT_SMRY_HDR.TEAM_CODE,
                E_EVNT_SMRY_HDR.ORIG_LOGIN_USER_ID,
                E_EVNT_SMRY_HDR.MISC_TXT_1,
                E_EVNT_SMRY_HDR.MISC_TXT_2,
                E_EVNT_SMRY_HDR.MISC_NUM_1,
                E_EVNT_SMRY_HDR.MISC_NUM_2,
                E_EVNT_SMRY_HDR.PROC_ZONE_ID,
                E_EVNT_SMRY_HDR.LAST_MOD_BY,
                E_EVNT_SMRY_HDR.EVNT_CTGRY_1,
                E_EVNT_SMRY_HDR.EVNT_CTGRY_2,
                E_EVNT_SMRY_HDR.EVNT_CTGRY_3,
                E_EVNT_SMRY_HDR.EVNT_CTGRY_4,
                E_EVNT_SMRY_HDR.EVNT_CTGRY_5,
                E_EVNT_SMRY_HDR.SYS_UPDATED_FLAG,
                E_EVNT_SMRY_HDR.APRV_SPVSR,
                E_EVNT_SMRY_HDR.APRV_SPVSR_DATE_TIME,
                E_EVNT_SMRY_HDR.VERSION_ID,
                E_EVNT_SMRY_HDR.TEAM_CHG_ID,
                cast(E_EVNT_SMRY_HDR.ACTUAL_END_DATE as timestamp) as ACTUAL_END_DATE,
                E_EVNT_SMRY_HDR.SPVSR_LOGIN_USER_ID,
                E_EVNT_SMRY_HDR.PAID_BRK_OVERLAP,
                E_EVNT_SMRY_HDR.UNPAID_BRK_OVERLAP,
                E_EVNT_SMRY_HDR.EMP_PERF_SMRY_ID,
                E_EVNT_SMRY_HDR.HDR_MSG_ID,
                E_EVNT_SMRY_HDR.ACT_ID,
                E_EVNT_SMRY_HDR.PERF_SMRY_TRAN_ID,
                cast(E_EVNT_SMRY_HDR.ORIG_EVNT_START_TIME as timestamp) as ORIG_EVNT_START_TIME,
                cast(E_EVNT_SMRY_HDR.ORIG_EVNT_END_TIME as timestamp) as ORIG_EVNT_END_TIME,
                E_EVNT_SMRY_HDR.ADJ_REASON_CODE,
                E_EVNT_SMRY_HDR.ADJ_REF_TRAN_NBR,
                E_EVNT_SMRY_HDR.CONFLICT,
                cast(E_EVNT_SMRY_HDR.TEAM_BEGIN_TIME as timestamp) as TEAM_BEGIN_TIME,
                E_EVNT_SMRY_HDR.THRUPUT_GOAL,
                E_EVNT_SMRY_HDR.THRUPUT_MIN,
                E_EVNT_SMRY_HDR.DISPLAY_UOM,
                E_EVNT_SMRY_HDR.DISPLAY_UOM_QTY,
                E_EVNT_SMRY_HDR.UNIT_MIN,
                E_EVNT_SMRY_HDR.CONSOL_EVNT_TIME,
                E_EVNT_SMRY_HDR.LOCN_GRP_ATTR,
                E_EVNT_SMRY_HDR.RESOURCE_GROUP_ID,
                E_EVNT_SMRY_HDR.JOB_FUNC_ID,
                E_EVNT_SMRY_HDR.COMP_EMPLOYEE_DAY_ID,
                E_EVNT_SMRY_HDR.COMP_EVENT_SUMMARY_HEADER_ID,
                cast(E_EVNT_SMRY_HDR.ASSIGNMENT_START_TIME as timestamp) as ASSIGNMENT_START_TIME,
                cast(E_EVNT_SMRY_HDR.ASSIGNMENT_END_TIME as timestamp) as ASSIGNMENT_END_TIME,
                E_EVNT_SMRY_HDR.REFLECTIVE_CODE,
                E_EVNT_SMRY_HDR.COMP_ASSIGNMENT_ID
            FROM {source_schema}.E_EVNT_SMRY_HDR
            WHERE (TRUNC( E_EVNT_SMRY_HDR.CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (TRUNC( E_EVNT_SMRY_HDR.MOD_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 81

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_EVNT_SMRY_HDR_temp = SQ_Shortcut_to_E_EVNT_SMRY_HDR.toDF(*["SQ_Shortcut_to_E_EVNT_SMRY_HDR___" + col for col in SQ_Shortcut_to_E_EVNT_SMRY_HDR.columns])

    EXPTRANS = SQ_Shortcut_to_E_EVNT_SMRY_HDR_temp.selectExpr( 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ELS_TRAN_ID as ELS_TRAN_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___WHSE as WHSE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___TRAN_NBR as TRAN_NBR", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___CO as CO", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___DIV as DIV", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___SCHED_START_DATE as SCHED_START_DATE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___REF_CODE as REF_CODE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___REF_NBR as REF_NBR", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___LOGIN_USER_ID as LOGIN_USER_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___DEPT_CODE as DEPT_CODE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___VHCL_TYPE as VHCL_TYPE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___SHIFT_CODE as SHIFT_CODE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___USER_DEF_FIELD_1 as USER_DEF_FIELD_1", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___USER_DEF_FIELD_2 as USER_DEF_FIELD_2", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___STD_TIME_ALLOW as STD_TIME_ALLOW", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___EMP_PERF_ALLOW as EMP_PERF_ALLOW", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___PERF_ADJ_AMT as PERF_ADJ_AMT", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___SCHED_ADJ_AMT as SCHED_ADJ_AMT", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___SCHED_UNITS_QTY as SCHED_UNITS_QTY", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ACTL_TIME as ACTL_TIME", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ERROR_CNT as ERROR_CNT", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___EVNT_STAT_CODE as EVNT_STAT_CODE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___USER_ID as USER_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___TMU_RECALC_COUNT as TMU_RECALC_COUNT", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___MISC as MISC", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___LABOR_TYPE_ID as LABOR_TYPE_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___SOURCE as SOURCE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___EVENT_TYPE as EVENT_TYPE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___TOTAL_WEIGHT as TOTAL_WEIGHT", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___TEAM_STD_SMRY_ID as TEAM_STD_SMRY_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___TEAM_CODE as TEAM_CODE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ORIG_LOGIN_USER_ID as ORIG_LOGIN_USER_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___MISC_TXT_1 as MISC_TXT_1", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___MISC_TXT_2 as MISC_TXT_2", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___MISC_NUM_1 as MISC_NUM_1", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___MISC_NUM_2 as MISC_NUM_2", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___PROC_ZONE_ID as PROC_ZONE_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___LAST_MOD_BY as LAST_MOD_BY", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___EVNT_CTGRY_1 as EVNT_CTGRY_1", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___EVNT_CTGRY_2 as EVNT_CTGRY_2", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___EVNT_CTGRY_3 as EVNT_CTGRY_3", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___EVNT_CTGRY_4 as EVNT_CTGRY_4", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___EVNT_CTGRY_5 as EVNT_CTGRY_5", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___SYS_UPDATED_FLAG as SYS_UPDATED_FLAG", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___APRV_SPVSR as APRV_SPVSR", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___APRV_SPVSR_DATE_TIME as APRV_SPVSR_DATE_TIME", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___VERSION_ID as VERSION_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___TEAM_CHG_ID as TEAM_CHG_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ACTUAL_END_DATE as ACTUAL_END_DATE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___SPVSR_LOGIN_USER_ID as SPVSR_LOGIN_USER_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___PAID_BRK_OVERLAP as PAID_BRK_OVERLAP", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___UNPAID_BRK_OVERLAP as UNPAID_BRK_OVERLAP", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___EMP_PERF_SMRY_ID as EMP_PERF_SMRY_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___HDR_MSG_ID as HDR_MSG_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ACT_ID as ACT_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___PERF_SMRY_TRAN_ID as PERF_SMRY_TRAN_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ORIG_EVNT_START_TIME as ORIG_EVNT_START_TIME", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ORIG_EVNT_END_TIME as ORIG_EVNT_END_TIME", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ADJ_REASON_CODE as ADJ_REASON_CODE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ADJ_REF_TRAN_NBR as ADJ_REF_TRAN_NBR", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___CONFLICT as CONFLICT", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___TEAM_BEGIN_TIME as TEAM_BEGIN_TIME", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___THRUPUT_GOAL as THRUPUT_GOAL", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___THRUPUT_MIN as THRUPUT_MIN", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___DISPLAY_UOM as DISPLAY_UOM", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___DISPLAY_UOM_QTY as DISPLAY_UOM_QTY", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___UNIT_MIN as UNIT_MIN", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___CONSOL_EVNT_TIME as CONSOL_EVNT_TIME", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___LOCN_GRP_ATTR as LOCN_GRP_ATTR", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___RESOURCE_GROUP_ID as RESOURCE_GROUP_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___JOB_FUNC_ID as JOB_FUNC_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___COMP_EMPLOYEE_DAY_ID as COMP_EMPLOYEE_DAY_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___COMP_EVENT_SUMMARY_HEADER_ID as COMP_EVENT_SUMMARY_HEADER_ID", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ASSIGNMENT_START_TIME as ASSIGNMENT_START_TIME", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___ASSIGNMENT_END_TIME as ASSIGNMENT_END_TIME", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___REFLECTIVE_CODE as REFLECTIVE_CODE", 
        "SQ_Shortcut_to_E_EVNT_SMRY_HDR___COMP_ASSIGNMENT_ID as COMP_ASSIGNMENT_ID", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_EVNT_SMRY_HDR_PRE, type TARGET 
    # COLUMN COUNT: 81


    Shortcut_to_WM_E_EVNT_SMRY_HDR_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR", 
        "CAST(ELS_TRAN_ID AS DECIMAL(20,0)) as ELS_TRAN_ID", 
        "CAST(WHSE AS STRING) as WHSE", 
        "CAST(TRAN_NBR AS INT) as TRAN_NBR", 
        "CAST(CO AS STRING) as CO", 
        "CAST(DIV AS STRING) as DIV", 
        "CAST(SCHED_START_DATE AS TIMESTAMP) as SCHED_START_DATE", 
        "CAST(REF_CODE AS STRING) as REF_CODE", 
        "CAST(REF_NBR AS STRING) as REF_NBR", 
        "CAST(LOGIN_USER_ID AS STRING) as LOGIN_USER_ID", 
        "CAST(DEPT_CODE AS STRING) as DEPT_CODE", 
        "CAST(VHCL_TYPE AS STRING) as VHCL_TYPE", 
        "CAST(SHIFT_CODE AS STRING) as SHIFT_CODE", 
        "CAST(USER_DEF_FIELD_1 AS STRING) as USER_DEF_FIELD_1", 
        "CAST(USER_DEF_FIELD_2 AS STRING) as USER_DEF_FIELD_2", 
        "CAST(STD_TIME_ALLOW AS DECIMAL(13,5)) as STD_TIME_ALLOW", 
        "CAST(EMP_PERF_ALLOW AS DECIMAL(13,5)) as EMP_PERF_ALLOW", 
        "CAST(PERF_ADJ_AMT AS DECIMAL(13,5)) as PERF_ADJ_AMT", 
        "CAST(SCHED_ADJ_AMT AS DECIMAL(13,5)) as SCHED_ADJ_AMT", 
        "CAST(SCHED_UNITS_QTY AS DECIMAL(20,7)) as SCHED_UNITS_QTY", 
        "CAST(ACTL_TIME AS DECIMAL(20,7)) as ACTL_TIME", 
        "CAST(ERROR_CNT AS INT) as ERROR_CNT", 
        "CAST(EVNT_STAT_CODE AS STRING) as EVNT_STAT_CODE", 
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
        "CAST(USER_ID AS STRING) as USER_ID", 
        "CAST(TMU_RECALC_COUNT AS INT) as TMU_RECALC_COUNT", 
        "CAST(MISC AS STRING) as MISC", 
        "CAST(LABOR_TYPE_ID AS INT) as LABOR_TYPE_ID", 
        "CAST(SOURCE AS STRING) as SOURCE", 
        "CAST(EVENT_TYPE AS STRING) as EVENT_TYPE", 
        "CAST(TOTAL_WEIGHT AS DECIMAL(20,7)) as TOTAL_WEIGHT", 
        "CAST(TEAM_STD_SMRY_ID AS INT) as TEAM_STD_SMRY_ID", 
        "CAST(TEAM_CODE AS STRING) as TEAM_CODE", 
        "CAST(ORIG_LOGIN_USER_ID AS STRING) as ORIG_LOGIN_USER_ID", 
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1", 
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2", 
        "CAST(MISC_NUM_1 AS DECIMAL(20,7)) as MISC_NUM_1", 
        "CAST(MISC_NUM_2 AS DECIMAL(20,7)) as MISC_NUM_2", 
        "CAST(PROC_ZONE_ID AS DECIMAL(20,7)) as PROC_ZONE_ID", 
        "CAST(LAST_MOD_BY AS STRING) as LAST_MOD_BY", 
        "CAST(EVNT_CTGRY_1 AS STRING) as EVNT_CTGRY_1", 
        "CAST(EVNT_CTGRY_2 AS STRING) as EVNT_CTGRY_2", 
        "CAST(EVNT_CTGRY_3 AS STRING) as EVNT_CTGRY_3", 
        "CAST(EVNT_CTGRY_4 AS STRING) as EVNT_CTGRY_4", 
        "CAST(EVNT_CTGRY_5 AS STRING) as EVNT_CTGRY_5", 
        "CAST(SYS_UPDATED_FLAG AS STRING) as SYS_UPDATED_FLAG", 
        "CAST(APRV_SPVSR AS STRING) as APRV_SPVSR", 
        "CAST(APRV_SPVSR_DATE_TIME AS TIMESTAMP) as APRV_SPVSR_DATE_TIME", 
        "CAST(VERSION_ID AS INT) as VERSION_ID", 
        "CAST(TEAM_CHG_ID AS DECIMAL(20,7)) as TEAM_CHG_ID", 
        "CAST(ACTUAL_END_DATE AS TIMESTAMP) as ACTUAL_END_DATE", 
        "CAST(SPVSR_LOGIN_USER_ID AS STRING) as SPVSR_LOGIN_USER_ID", 
        "CAST(PAID_BRK_OVERLAP AS DECIMAL(20,7)) as PAID_BRK_OVERLAP", 
        "CAST(UNPAID_BRK_OVERLAP AS DECIMAL(20,7)) as UNPAID_BRK_OVERLAP", 
        "CAST(EMP_PERF_SMRY_ID AS DECIMAL(20,0)) as EMP_PERF_SMRY_ID", 
        "CAST(HDR_MSG_ID AS DECIMAL(20,0)) as HDR_MSG_ID", 
        "CAST(ACT_ID AS INT) as ACT_ID", 
        "CAST(PERF_SMRY_TRAN_ID AS DECIMAL(20,0)) as PERF_SMRY_TRAN_ID", 
        "CAST(ORIG_EVNT_START_TIME AS TIMESTAMP) as ORIG_EVNT_START_TIME", 
        "CAST(ORIG_EVNT_END_TIME AS TIMESTAMP) as ORIG_EVNT_END_TIME", 
        "CAST(ADJ_REASON_CODE AS STRING) as ADJ_REASON_CODE", 
        "CAST(ADJ_REF_TRAN_NBR AS DECIMAL(20,0)) as ADJ_REF_TRAN_NBR", 
        "CAST(CONFLICT AS STRING) as CONFLICT", 
        "CAST(TEAM_BEGIN_TIME AS TIMESTAMP) as TEAM_BEGIN_TIME", 
        "CAST(THRUPUT_GOAL AS DECIMAL(20,7)) as THRUPUT_GOAL", 
        "CAST(THRUPUT_MIN AS DECIMAL(20,7)) as THRUPUT_MIN", 
        "CAST(DISPLAY_UOM AS STRING) as DISPLAY_UOM", 
        "CAST(DISPLAY_UOM_QTY AS DECIMAL(20,7)) as DISPLAY_UOM_QTY", 
        "CAST(UNIT_MIN AS DECIMAL(20,7)) as UNIT_MIN", 
        "CAST(CONSOL_EVNT_TIME AS DECIMAL(20,7)) as CONSOL_EVNT_TIME", 
        "CAST(LOCN_GRP_ATTR AS STRING) as LOCN_GRP_ATTR", 
        "CAST(RESOURCE_GROUP_ID AS STRING) as RESOURCE_GROUP_ID", 
        "CAST(JOB_FUNC_ID AS INT) as JOB_FUNC_ID", 
        "CAST(COMP_EMPLOYEE_DAY_ID AS STRING) as COMP_EMPLOYEE_DAY_ID", 
        "CAST(COMP_EVENT_SUMMARY_HEADER_ID AS STRING) as COMP_EVENT_SUMMARY_HEADER_ID", 
        "CAST(ASSIGNMENT_START_TIME AS TIMESTAMP) as ASSIGNMENT_START_TIME", 
        "CAST(ASSIGNMENT_END_TIME AS TIMESTAMP) as ASSIGNMENT_END_TIME", 
        "CAST(REFLECTIVE_CODE AS STRING) as REFLECTIVE_CODE", 
        "CAST(COMP_ASSIGNMENT_ID AS STRING) as COMP_ASSIGNMENT_ID", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_E_EVNT_SMRY_HDR_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_EVNT_SMRY_HDR_PRE is written to the target table - "
        + target_table_name
    )