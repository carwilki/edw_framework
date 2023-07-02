# Code converted on 2023-06-24 13:38:12
from datetime import datetime

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DecimalType, StringType, TimestampType

from Datalake.utils.configs import getConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from logging import getLogger, INFO
from Datalake.utils import genericUtilities as gu


def m_WM_E_Elm_PRE(dcnbr, env):
    logger = getLogger()
    logger.info("inside m_WM_E_Elm_PRE function")

    spark = SparkSession.getActiveSession()

    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_E_ELM_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_ELM"

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
    # Processing node SQ_Shortcut_to_E_ELM, type SOURCE
    # COLUMN COUNT: 18

    SQ_Shortcut_to_E_ELM = gu.jdbcOracleConnection(
        f"""SELECT
                E_ELM.ELM_ID,
                E_ELM.NAME,
                E_ELM.DESCRIPTION,
                E_ELM.CORE_FLAG,
                E_ELM.MSRMNT_ID,
                E_ELM.TIME_ALLOW,
                E_ELM.ELM_GRP_ID,
                E_ELM.CREATE_DATE_TIME,
                E_ELM.MOD_DATE_TIME,
                E_ELM.USER_ID,
                E_ELM.MISC_TXT_1,
                E_ELM.MISC_TXT_2,
                E_ELM.MISC_NUM_1,
                E_ELM.MISC_NUM_2,
                E_ELM.VERSION_ID,
                E_ELM.UNQ_SEED_ID,
                E_ELM.SIM_WHSE,
                E_ELM.ORIG_NAME
            FROM E_ELM
            WHERE (TRUNC( CREATE_DATE_TIME)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14) OR (TRUNC( MOD_DATE_TIME)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14)""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 20

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_ELM_temp = SQ_Shortcut_to_E_ELM.toDF(
        *["SQ_Shortcut_to_E_ELM___" + col for col in SQ_Shortcut_to_E_ELM.columns]
    )

    EXPTRANS = SQ_Shortcut_to_E_ELM_temp.selectExpr(
        "SQ_Shortcut_to_E_ELM___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_E_ELM___ELM_ID as ELM_ID",
        "SQ_Shortcut_to_E_ELM___NAME as NAME",
        "SQ_Shortcut_to_E_ELM___DESCRIPTION as DESCRIPTION",
        "SQ_Shortcut_to_E_ELM___CORE_FLAG as CORE_FLAG",
        "SQ_Shortcut_to_E_ELM___MSRMNT_ID as MSRMNT_ID",
        "SQ_Shortcut_to_E_ELM___TIME_ALLOW as TIME_ALLOW",
        "SQ_Shortcut_to_E_ELM___ELM_GRP_ID as ELM_GRP_ID",
        "SQ_Shortcut_to_E_ELM___CREATE_DATE_TIME as CREATE_DATE_TIME",
        "SQ_Shortcut_to_E_ELM___MOD_DATE_TIME as MOD_DATE_TIME",
        "SQ_Shortcut_to_E_ELM___USER_ID as USER_ID",
        "SQ_Shortcut_to_E_ELM___MISC_TXT_1 as MISC_TXT_1",
        "SQ_Shortcut_to_E_ELM___MISC_TXT_2 as MISC_TXT_2",
        "SQ_Shortcut_to_E_ELM___MISC_NUM_1 as MISC_NUM_1",
        "SQ_Shortcut_to_E_ELM___MISC_NUM_2 as MISC_NUM_2",
        "SQ_Shortcut_to_E_ELM___VERSION_ID as VERSION_ID",
        "SQ_Shortcut_to_E_ELM___UNQ_SEED_ID as UNQ_SEED_ID",
        "SQ_Shortcut_to_E_ELM___SIM_WHSE as SIM_WHSE",
        "SQ_Shortcut_to_E_ELM___ORIG_NAME as ORIG_NAME",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_ELM_PRE, type TARGET
    # COLUMN COUNT: 20

    Shortcut_to_WM_E_ELM_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
        "CAST(ELM_ID AS BIGINT) as ELM_ID",
        "CAST(NAME AS STRING) as NAME",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(CORE_FLAG AS STRING) as CORE_FLAG",
        "CAST(MSRMNT_ID AS BIGINT) as MSRMNT_ID",
        "CAST(TIME_ALLOW AS BIGINT) as TIME_ALLOW",
        "CAST(ELM_GRP_ID AS BIGINT) as ELM_GRP_ID",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1",
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2",
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1",
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2",
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID",
        "CAST(UNQ_SEED_ID AS BIGINT) as UNQ_SEED_ID",
        "CAST(SIM_WHSE AS STRING) as SIM_WHSE",
        "CAST(ORIG_NAME AS STRING) as ORIG_NAME",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP",
    )
    gu.overwriteDeltaPartition(
        Shortcut_to_WM_E_ELM_PRE, "DC_NBR", dcnbr, target_table_name
    )

    logger.info(
        "Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE is written to the target table - "
        + target_table_name
    )
