#Code converted on 2023-06-24 13:37:55
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



def m_WM_E_Emp_Dtl_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_E_Emp_Dtl_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_E_EMP_DTL_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_EMP_DTL"


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
    # Processing node SQ_Shortcut_to_E_EMP_DTL, type SOURCE 
    # COLUMN COUNT: 30

    SQ_Shortcut_to_E_EMP_DTL = jdbcOracleConnection(
        f"""SELECT
                E_EMP_DTL.EMP_ID,
                E_EMP_DTL.EFF_DATE_TIME,
                E_EMP_DTL.EMP_STAT_ID,
                E_EMP_DTL.PAY_RATE,
                E_EMP_DTL.PAY_SCALE_ID,
                E_EMP_DTL.SPVSR_EMP_ID,
                E_EMP_DTL.DEPT_ID,
                E_EMP_DTL.SHIFT_ID,
                E_EMP_DTL.ROLE_ID,
                E_EMP_DTL.USER_DEF_FIELD_1,
                E_EMP_DTL.USER_DEF_FIELD_2,
                E_EMP_DTL.CMNT,
                E_EMP_DTL.CREATE_DATE_TIME,
                E_EMP_DTL.MOD_DATE_TIME,
                E_EMP_DTL.USER_ID,
                E_EMP_DTL.WHSE,
                E_EMP_DTL.JOB_FUNC_ID,
                E_EMP_DTL.STARTUP_TIME,
                E_EMP_DTL.CLEANUP_TIME,
                E_EMP_DTL.MISC_TXT_1,
                E_EMP_DTL.MISC_TXT_2,
                E_EMP_DTL.MISC_NUM_1,
                E_EMP_DTL.MISC_NUM_2,
                E_EMP_DTL.DFLT_PERF_GOAL,
                E_EMP_DTL.VERSION_ID,
                E_EMP_DTL.IS_SUPER,
                E_EMP_DTL.EMP_DTL_ID,
                E_EMP_DTL.CREATED_DTTM,
                E_EMP_DTL.LAST_UPDATED_DTTM,
                E_EMP_DTL.EXCLUDE_AUTO_CICO
            FROM E_EMP_DTL
            WHERE (TRUNC( E_EMP_DTL.CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC( E_EMP_DTL.MOD_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC( E_EMP_DTL.CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC( E_EMP_DTL.LAST_UPDATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 32

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_EMP_DTL_temp = SQ_Shortcut_to_E_EMP_DTL.toDF(*["SQ_Shortcut_to_E_EMP_DTL___" + col for col in SQ_Shortcut_to_E_EMP_DTL.columns])

    EXPTRANS = SQ_Shortcut_to_E_EMP_DTL_temp.selectExpr( 
        "SQ_Shortcut_to_E_EMP_DTL___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_E_EMP_DTL___EMP_ID as EMP_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___EFF_DATE_TIME as EFF_DATE_TIME", 
        "SQ_Shortcut_to_E_EMP_DTL___EMP_STAT_ID as EMP_STAT_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___PAY_RATE as PAY_RATE", 
        "SQ_Shortcut_to_E_EMP_DTL___PAY_SCALE_ID as PAY_SCALE_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___SPVSR_EMP_ID as SPVSR_EMP_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___DEPT_ID as DEPT_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___SHIFT_ID as SHIFT_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___ROLE_ID as ROLE_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___USER_DEF_FIELD_1 as USER_DEF_FIELD_1", 
        "SQ_Shortcut_to_E_EMP_DTL___USER_DEF_FIELD_2 as USER_DEF_FIELD_2", 
        "SQ_Shortcut_to_E_EMP_DTL___CMNT as CMNT", 
        "SQ_Shortcut_to_E_EMP_DTL___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_E_EMP_DTL___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_E_EMP_DTL___USER_ID as USER_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___WHSE as WHSE", 
        "SQ_Shortcut_to_E_EMP_DTL___JOB_FUNC_ID as JOB_FUNC_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___STARTUP_TIME as STARTUP_TIME", 
        "SQ_Shortcut_to_E_EMP_DTL___CLEANUP_TIME as CLEANUP_TIME", 
        "SQ_Shortcut_to_E_EMP_DTL___MISC_TXT_1 as MISC_TXT_1", 
        "SQ_Shortcut_to_E_EMP_DTL___MISC_TXT_2 as MISC_TXT_2", 
        "SQ_Shortcut_to_E_EMP_DTL___MISC_NUM_1 as MISC_NUM_1", 
        "SQ_Shortcut_to_E_EMP_DTL___MISC_NUM_2 as MISC_NUM_2", 
        "SQ_Shortcut_to_E_EMP_DTL___DFLT_PERF_GOAL as DFLT_PERF_GOAL", 
        "SQ_Shortcut_to_E_EMP_DTL___VERSION_ID as VERSION_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___IS_SUPER as IS_SUPER", 
        "SQ_Shortcut_to_E_EMP_DTL___EMP_DTL_ID as EMP_DTL_ID", 
        "SQ_Shortcut_to_E_EMP_DTL___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_E_EMP_DTL___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "SQ_Shortcut_to_E_EMP_DTL___EXCLUDE_AUTO_CICO as EXCLUDE_AUTO_CICO", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_EMP_DTL_PRE, type TARGET 
    # COLUMN COUNT: 32


    Shortcut_to_WM_E_EMP_DTL_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(EMP_DTL_ID AS BIGINT) as EMP_DTL_ID", 
        "CAST(EMP_ID AS BIGINT) as EMP_ID", 
        "CAST(EFF_DATE_TIME AS TIMESTAMP) as EFF_DATE_TIME", 
        "CAST(EMP_STAT_ID AS BIGINT) as EMP_STAT_ID", 
        "CAST(PAY_RATE AS BIGINT) as PAY_RATE", 
        "CAST(PAY_SCALE_ID AS BIGINT) as PAY_SCALE_ID", 
        "CAST(SPVSR_EMP_ID AS BIGINT) as SPVSR_EMP_ID", 
        "CAST(DEPT_ID AS BIGINT) as DEPT_ID", 
        "CAST(SHIFT_ID AS BIGINT) as SHIFT_ID", 
        "CAST(ROLE_ID AS BIGINT) as ROLE_ID", 
        "CAST(USER_DEF_FIELD_1 AS STRING) as USER_DEF_FIELD_1", 
        "CAST(USER_DEF_FIELD_2 AS STRING) as USER_DEF_FIELD_2", 
        "CAST(CMNT AS STRING) as CMNT", 
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
        "CAST(USER_ID AS STRING) as USER_ID", 
        "CAST(WHSE AS STRING) as WHSE", 
        "CAST(JOB_FUNC_ID AS BIGINT) as JOB_FUNC_ID", 
        "CAST(STARTUP_TIME AS BIGINT) as STARTUP_TIME", 
        "CAST(CLEANUP_TIME AS BIGINT) as CLEANUP_TIME", 
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1", 
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2", 
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1", 
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2", 
        "CAST(DFLT_PERF_GOAL AS BIGINT) as DFLT_PERF_GOAL", 
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID", 
        "CAST(IS_SUPER AS STRING) as IS_SUPER", 
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
        "CAST(EXCLUDE_AUTO_CICO AS STRING) as EXCLUDE_AUTO_CICO", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    overwriteDeltaPartition(Shortcut_to_WM_E_EMP_DTL_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_EMP_DTL_PRE is written to the target table - "
        + target_table_name
    )