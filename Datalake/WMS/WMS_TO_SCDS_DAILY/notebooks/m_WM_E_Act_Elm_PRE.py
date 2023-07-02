# Code converted on 2023-06-24 13:40:05
from datetime import datetime

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DecimalType, StringType, TimestampType

from Datalake.utils.configs import getConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from logging import getLogger, INFO
from Datalake.utils import genericUtilities as gu


def m_WM_E_Act_Elm_PRE(dcnbr, env):
    logger = getLogger()
    logger.info("inside m_WM_E_Act_Elm_PRE function")

    spark = SparkSession.getActiveSession()

    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_E_ACT_ELM_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_ACT_ELM"

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
    # Processing node SQ_Shortcut_to_E_ACT_ELM, type SOURCE
    # COLUMN COUNT: 15

    SQ_Shortcut_to_E_ACT_ELM = gu.jdbcOracleConnection(
        f"""SELECT
                E_ACT_ELM.ACT_ID,
                E_ACT_ELM.ELM_ID,
                E_ACT_ELM.TIME_ALLOW,
                E_ACT_ELM.THRUPUT_MSRMNT,
                E_ACT_ELM.SEQ_NBR,
                E_ACT_ELM.CREATE_DATE_TIME,
                E_ACT_ELM.MOD_DATE_TIME,
                E_ACT_ELM.USER_ID,
                E_ACT_ELM.MISC_TXT_1,
                E_ACT_ELM.MISC_TXT_2,
                E_ACT_ELM.MISC_NUM_1,
                E_ACT_ELM.MISC_NUM_2,
                E_ACT_ELM.VERSION_ID,
                E_ACT_ELM.AVG_ACT_ID,
                E_ACT_ELM.AVG_BY
            FROM E_ACT_ELM
            WHERE (TRUNC( E_ACT_ELM.CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14) OR \
                (TRUNC( E_ACT_ELM.MOD_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14)""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 17

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_ACT_ELM_temp = SQ_Shortcut_to_E_ACT_ELM.toDF(
        *[
            "SQ_Shortcut_to_E_ACT_ELM___" + col
            for col in SQ_Shortcut_to_E_ACT_ELM.columns
        ]
    )

    EXPTRANS = SQ_Shortcut_to_E_ACT_ELM_temp.selectExpr(
        "SQ_Shortcut_to_E_ACT_ELM___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_E_ACT_ELM___ACT_ID as ACT_ID",
        "SQ_Shortcut_to_E_ACT_ELM___ELM_ID as ELM_ID",
        "SQ_Shortcut_to_E_ACT_ELM___TIME_ALLOW as TIME_ALLOW",
        "SQ_Shortcut_to_E_ACT_ELM___THRUPUT_MSRMNT as THRUPUT_MSRMNT",
        "SQ_Shortcut_to_E_ACT_ELM___SEQ_NBR as SEQ_NBR",
        "SQ_Shortcut_to_E_ACT_ELM___CREATE_DATE_TIME as CREATE_DATE_TIME",
        "SQ_Shortcut_to_E_ACT_ELM___MOD_DATE_TIME as MOD_DATE_TIME",
        "SQ_Shortcut_to_E_ACT_ELM___USER_ID as USER_ID",
        "SQ_Shortcut_to_E_ACT_ELM___MISC_TXT_1 as MISC_TXT_1",
        "SQ_Shortcut_to_E_ACT_ELM___MISC_TXT_2 as MISC_TXT_2",
        "SQ_Shortcut_to_E_ACT_ELM___MISC_NUM_1 as MISC_NUM_1",
        "SQ_Shortcut_to_E_ACT_ELM___MISC_NUM_2 as MISC_NUM_2",
        "SQ_Shortcut_to_E_ACT_ELM___VERSION_ID as VERSION_ID",
        "SQ_Shortcut_to_E_ACT_ELM___AVG_ACT_ID as AVG_ACT_ID",
        "SQ_Shortcut_to_E_ACT_ELM___AVG_BY as AVG_BY",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_ACT_ELM_PRE, type TARGET
    # COLUMN COUNT: 17

    Shortcut_to_WM_E_ACT_ELM_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
        "CAST(ACT_ID AS BIGINT) as ACT_ID",
        "CAST(ELM_ID AS BIGINT) as ELM_ID",
        "CAST(TIME_ALLOW AS BIGINT) as TIME_ALLOW",
        "CAST(THRUPUT_MSRMNT AS STRING) as THRUPUT_MSRMNT",
        "CAST(SEQ_NBR AS BIGINT) as SEQ_NBR",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1",
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2",
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1",
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2",
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID",
        "CAST(AVG_ACT_ID AS BIGINT) as AVG_ACT_ID",
        "CAST(AVG_BY AS STRING) as AVG_BY",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP",
    )

    gu.overwriteDeltaPartition(
        Shortcut_to_WM_E_ACT_ELM_PRE, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_E_ACT_ELM_PRE is written to the target table - "
        + target_table_name
    )
