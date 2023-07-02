# Code converted on 2023-06-24 13:38:47
from datetime import datetime

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DecimalType, StringType, TimestampType

from Datalake.utils.configs import getConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from logging import getLogger, INFO
from Datalake.utils import genericUtilities as gu


def m_WM_E_Dept_PRE(dcnbr, env):
    logger = getLogger()
    logger.info("inside m_WM_E_Dept_PRE function")

    spark = SparkSession.getActiveSession()

    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_E_DEPT_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_DEPT"

    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now()  # start timestamp of the script

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)

    # COMMAND ----------
    # Variable_declaration_comment
    dcnbr = dcnbr.strip()[2:]
    Prev_Run_Dt = gu.genPrevRunDt(refine_table_name, refine, raw)

    # COMMAND ----------
    # Processing node SQ_Shortcut_to_E_DEPT, type SOURCE
    # COLUMN COUNT: 15

    SQ_Shortcut_to_E_DEPT = gu.jdbcOracleConnection(
        f"""SELECT
                E_DEPT.DEPT_ID,
                E_DEPT.DEPT_CODE,
                E_DEPT.DESCRIPTION,
                E_DEPT.CREATE_DATE_TIME,
                E_DEPT.MOD_DATE_TIME,
                E_DEPT.USER_ID,
                E_DEPT.WHSE,
                E_DEPT.MISC_TXT_1,
                E_DEPT.MISC_TXT_2,
                E_DEPT.MISC_NUM_1,
                E_DEPT.MISC_NUM_2,
                E_DEPT.PERF_GOAL,
                E_DEPT.VERSION_ID,
                E_DEPT.CREATED_DTTM,
                E_DEPT.LAST_UPDATED_DTTM
            FROM E_DEPT
            WHERE (TRUNC( CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (TRUNC( MOD_DATE_TIME) >=  TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (TRUNC( CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (TRUNC( LAST_UPDATED_DTTM) >=  TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1)""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 17

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_DEPT_temp = SQ_Shortcut_to_E_DEPT.toDF(
        *["SQ_Shortcut_to_E_DEPT___" + col for col in SQ_Shortcut_to_E_DEPT.columns]
    )

    EXPTRANS = SQ_Shortcut_to_E_DEPT_temp.selectExpr(
        "SQ_Shortcut_to_E_DEPT___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_E_DEPT___DEPT_ID as DEPT_ID",
        "SQ_Shortcut_to_E_DEPT___DEPT_CODE as DEPT_CODE",
        "SQ_Shortcut_to_E_DEPT___DESCRIPTION as DESCRIPTION",
        "SQ_Shortcut_to_E_DEPT___CREATE_DATE_TIME as CREATE_DATE_TIME",
        "SQ_Shortcut_to_E_DEPT___MOD_DATE_TIME as MOD_DATE_TIME",
        "SQ_Shortcut_to_E_DEPT___USER_ID as USER_ID",
        "SQ_Shortcut_to_E_DEPT___WHSE as WHSE",
        "SQ_Shortcut_to_E_DEPT___MISC_TXT_1 as MISC_TXT_1",
        "SQ_Shortcut_to_E_DEPT___MISC_TXT_2 as MISC_TXT_2",
        "SQ_Shortcut_to_E_DEPT___MISC_NUM_1 as MISC_NUM_1",
        "SQ_Shortcut_to_E_DEPT___MISC_NUM_2 as MISC_NUM_2",
        "SQ_Shortcut_to_E_DEPT___PERF_GOAL as PERF_GOAL",
        "SQ_Shortcut_to_E_DEPT___VERSION_ID as VERSION_ID",
        "SQ_Shortcut_to_E_DEPT___CREATED_DTTM as CREATED_DTTM",
        "SQ_Shortcut_to_E_DEPT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_DEPT_PRE, type TARGET
    # COLUMN COUNT: 17

    Shortcut_to_WM_E_DEPT_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
        "CAST(DEPT_ID AS BIGINT) as DEPT_ID",
        "CAST(DEPT_CODE AS STRING) as DEPT_CODE",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(WHSE AS STRING) as WHSE",
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1",
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2",
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1",
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2",
        "CAST(PERF_GOAL AS BIGINT) as PERF_GOAL",
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP",
    )
    gu.overwriteDeltaPartition(
        Shortcut_to_WM_E_DEPT_PRE, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_E_DEPT_PRE is written to the target table - "
        + target_table_name
    )
