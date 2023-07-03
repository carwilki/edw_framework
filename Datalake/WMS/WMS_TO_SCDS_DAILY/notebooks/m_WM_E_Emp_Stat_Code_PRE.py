# Code converted on 2023-06-26 09:57:38
from datetime import datetime

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DecimalType, StringType, TimestampType

from Datalake.utils.configs import getConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from logging import getLogger, INFO
from Datalake.utils import genericUtilities as gu


def m_WM_E_Emp_Stat_Code_PRE(dcnbr, env):
    logger = getLogger()
    logger.info("inside m_WM_E_Emp_Stat_Code_PRE function")

    spark = SparkSession.getActiveSession()
    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_E_EMP_STAT_CODE_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "TDB"

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
    # Processing node SQ_Shortcut_to_E_EMP_STAT_CODE, type SOURCE
    # COLUMN COUNT: 14

    SQ_Shortcut_to_E_EMP_STAT_CODE = gu.jdbcOracleConnection(
        f"""SELECT
    E_EMP_STAT_CODE.EMP_STAT_ID,
    E_EMP_STAT_CODE.EMP_STAT_CODE,
    E_EMP_STAT_CODE.DESCRIPTION,
    E_EMP_STAT_CODE.CREATE_DATE_TIME,
    E_EMP_STAT_CODE.MOD_DATE_TIME,
    E_EMP_STAT_CODE.USER_ID,
    E_EMP_STAT_CODE.MISC_TXT_1,
    E_EMP_STAT_CODE.MISC_TXT_2,
    E_EMP_STAT_CODE.MISC_NUM_1,
    E_EMP_STAT_CODE.MISC_NUM_2,
    E_EMP_STAT_CODE.VERSION_ID,
    E_EMP_STAT_CODE.UNQ_SEED_ID,
    E_EMP_STAT_CODE.CREATED_DTTM,
    E_EMP_STAT_CODE.LAST_UPDATED_DTTM
    FROM E_EMP_STAT_CODE
    WHERE (trunc(CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-14) OR (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-14)  AND
    1=1""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 16

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_EMP_STAT_CODE_temp = SQ_Shortcut_to_E_EMP_STAT_CODE.toDF(
        *[
            "SQ_Shortcut_to_E_EMP_STAT_CODE___" + col
            for col in SQ_Shortcut_to_E_EMP_STAT_CODE.columns
        ]
    )

    EXPTRANS = SQ_Shortcut_to_E_EMP_STAT_CODE_temp.selectExpr(
        "SQ_Shortcut_to_E_EMP_STAT_CODE___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___EMP_STAT_ID as EMP_STAT_ID",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___EMP_STAT_CODE as EMP_STAT_CODE",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___DESCRIPTION as DESCRIPTION",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___CREATE_DATE_TIME as CREATE_DATE_TIME",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___MOD_DATE_TIME as MOD_DATE_TIME",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___USER_ID as USER_ID",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___MISC_TXT_1 as MISC_TXT_1",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___MISC_TXT_2 as MISC_TXT_2",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___MISC_NUM_1 as MISC_NUM_1",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___MISC_NUM_2 as MISC_NUM_2",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___VERSION_ID as VERSION_ID",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___UNQ_SEED_ID as UNQ_SEED_ID",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___CREATED_DTTM as CREATED_DTTM",
        "SQ_Shortcut_to_E_EMP_STAT_CODE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_EMP_STAT_CODE_PRE, type TARGET
    # COLUMN COUNT: 16

    Shortcut_to_WM_E_EMP_STAT_CODE_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
        "CAST(EMP_STAT_ID AS BIGINT) as EMP_STAT_ID",
        "CAST(EMP_STAT_CODE AS STRING) as EMP_STAT_CODE",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1",
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2",
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1",
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2",
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID",
        "CAST(UNQ_SEED_ID AS BIGINT) as UNQ_SEED_ID",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
    )
    gu.overwriteDeltaPartition(
        Shortcut_to_WM_E_EMP_STAT_CODE_PRE, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_E_EMP_STAT_CODE_PRE is written to the target table - "
        + target_table_name
    )
