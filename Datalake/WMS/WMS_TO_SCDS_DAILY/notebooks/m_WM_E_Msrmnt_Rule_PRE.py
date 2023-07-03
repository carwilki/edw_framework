# Code converted on 2023-06-24 13:35:36
from datetime import datetime

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DecimalType, StringType, TimestampType

from Datalake.utils.configs import getConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from logging import getLogger
from Datalake.utils import genericUtilities as gu


def m_WM_E_Msrmnt_Rule_PRE(dcnbr, env):
    logger = getLogger()
    logger.info("inside m_WM_E_Msrmnt_Rule_PRE function")

    spark = SparkSession.getActiveSession()

    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_E_MSRMNT_RULE_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_MSRMNT_RULE"

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
    # Processing node SQ_Shortcut_to_E_MSRMNT_RULE, type SOURCE
    # COLUMN COUNT: 16

    SQ_Shortcut_to_E_MSRMNT_RULE = gu.jdbcOracleConnection(
        f"""SELECT
                E_MSRMNT_RULE.MSRMNT_ID,
                E_MSRMNT_RULE.RULE_NBR,
                E_MSRMNT_RULE.DESCRIPTION,
                E_MSRMNT_RULE.STATUS_FLAG,
                E_MSRMNT_RULE.THEN_STATEMENT,
                E_MSRMNT_RULE.ELSE_STATEMENT,
                E_MSRMNT_RULE.NOTE,
                E_MSRMNT_RULE.MISC,
                E_MSRMNT_RULE.CREATE_DATE_TIME,
                E_MSRMNT_RULE.MOD_DATE_TIME,
                E_MSRMNT_RULE.USER_ID,
                E_MSRMNT_RULE.MISC_TXT_1,
                E_MSRMNT_RULE.MISC_TXT_2,
                E_MSRMNT_RULE.MISC_NUM_1,
                E_MSRMNT_RULE.MISC_NUM_2,
                E_MSRMNT_RULE.VERSION_ID
            FROM E_MSRMNT_RULE
            WHERE (TRUNC( E_MSRMNT_RULE.CREATE_DATE_TIME) >=TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC( E_MSRMNT_RULE.MOD_DATE_TIME) >=TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 18

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_MSRMNT_RULE_temp = SQ_Shortcut_to_E_MSRMNT_RULE.toDF(
        *[
            "SQ_Shortcut_to_E_MSRMNT_RULE___" + col
            for col in SQ_Shortcut_to_E_MSRMNT_RULE.columns
        ]
    )

    EXPTRANS = SQ_Shortcut_to_E_MSRMNT_RULE_temp.selectExpr(
        "SQ_Shortcut_to_E_MSRMNT_RULE___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_E_MSRMNT_RULE___MSRMNT_ID as MSRMNT_ID",
        "SQ_Shortcut_to_E_MSRMNT_RULE___RULE_NBR as RULE_NBR",
        "SQ_Shortcut_to_E_MSRMNT_RULE___DESCRIPTION as DESCRIPTION",
        "SQ_Shortcut_to_E_MSRMNT_RULE___STATUS_FLAG as STATUS_FLAG",
        "SQ_Shortcut_to_E_MSRMNT_RULE___THEN_STATEMENT as THEN_STATEMENT",
        "SQ_Shortcut_to_E_MSRMNT_RULE___ELSE_STATEMENT as ELSE_STATEMENT",
        "SQ_Shortcut_to_E_MSRMNT_RULE___NOTE as NOTE",
        "SQ_Shortcut_to_E_MSRMNT_RULE___MISC as MISC",
        "SQ_Shortcut_to_E_MSRMNT_RULE___CREATE_DATE_TIME as CREATE_DATE_TIME",
        "SQ_Shortcut_to_E_MSRMNT_RULE___MOD_DATE_TIME as MOD_DATE_TIME",
        "SQ_Shortcut_to_E_MSRMNT_RULE___USER_ID as USER_ID",
        "SQ_Shortcut_to_E_MSRMNT_RULE___MISC_TXT_1 as MISC_TXT_1",
        "SQ_Shortcut_to_E_MSRMNT_RULE___MISC_TXT_2 as MISC_TXT_2",
        "SQ_Shortcut_to_E_MSRMNT_RULE___MISC_NUM_1 as MISC_NUM_1",
        "SQ_Shortcut_to_E_MSRMNT_RULE___MISC_NUM_2 as MISC_NUM_2",
        "SQ_Shortcut_to_E_MSRMNT_RULE___VERSION_ID as VERSION_ID",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_MSRMNT_RULE_PRE, type TARGET
    # COLUMN COUNT: 18

    Shortcut_to_WM_E_MSRMNT_RULE_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
        "CAST(MSRMNT_ID AS BIGINT) as MSRMNT_ID",
        "CAST(RULE_NBR AS BIGINT) as RULE_NBR",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(STATUS_FLAG AS STRING) as STATUS_FLAG",
        "CAST(THEN_STATEMENT AS STRING) as THEN_STATEMENT",
        "CAST(ELSE_STATEMENT AS STRING) as ELSE_STATEMENT",
        "CAST(NOTE AS STRING) as NOTE",
        "CAST(MISC AS STRING) as MISC",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1",
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2",
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1",
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2",
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP",
    )

    gu.overwriteDeltaPartition(
        Shortcut_to_WM_E_MSRMNT_RULE_PRE, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_E_MSRMNT_RULE_PRE is written to the target table - "
        + target_table_name
    )
