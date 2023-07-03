# Code converted on 2023-06-24 13:35:43
from datetime import datetime

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DecimalType, StringType, TimestampType

from Datalake.utils.configs import getConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from logging import getLogger
from Datalake.utils import genericUtilities as gu


def m_WM_E_Msrmnt_Rule_Condition_PRE(dcnbr, env):
    logger = getLogger()
    logger.info("inside m_WM_E_Msrmnt_Rule_Condition_PRE function")

    spark = SparkSession.getActiveSession()

    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_E_MSRMNT_RULE_CONDITION_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_MSRMNT_RULE_CONDITION"

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
    # Processing node SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION, type SOURCE
    # COLUMN COUNT: 20

    SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION = gu.jdbcOracleConnection(
        f"""SELECT
                E_MSRMNT_RULE_CONDITION.MSRMNT_ID,
                E_MSRMNT_RULE_CONDITION.RULE_NBR,
                E_MSRMNT_RULE_CONDITION.RULE_SEQ_NBR,
                E_MSRMNT_RULE_CONDITION.OPEN_PARAN,
                E_MSRMNT_RULE_CONDITION.AND_OR,
                E_MSRMNT_RULE_CONDITION.FIELD_NAME,
                E_MSRMNT_RULE_CONDITION.OPERATOR,
                E_MSRMNT_RULE_CONDITION.RULE_COMPARE_VALUE,
                E_MSRMNT_RULE_CONDITION.CLOSE_PARAN,
                E_MSRMNT_RULE_CONDITION.FREE_FORM_TEXT,
                E_MSRMNT_RULE_CONDITION.DESCRIPTION,
                E_MSRMNT_RULE_CONDITION.MISC,
                E_MSRMNT_RULE_CONDITION.CREATE_DATE_TIME,
                E_MSRMNT_RULE_CONDITION.MOD_DATE_TIME,
                E_MSRMNT_RULE_CONDITION.USER_ID,
                E_MSRMNT_RULE_CONDITION.MISC_TXT_1,
                E_MSRMNT_RULE_CONDITION.MISC_TXT_2,
                E_MSRMNT_RULE_CONDITION.MISC_NUM_1,
                E_MSRMNT_RULE_CONDITION.MISC_NUM_2,
                E_MSRMNT_RULE_CONDITION.VERSION_ID
            FROM E_MSRMNT_RULE_CONDITION
            WHERE (TRUNC( CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14) OR (TRUNC( MOD_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14)""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 22

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION_temp = (
        SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION.toDF(
            *[
                "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___" + col
                for col in SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION.columns
            ]
        )
    )

    EXPTRANS = SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION_temp.selectExpr(
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___MSRMNT_ID as MSRMNT_ID",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___RULE_NBR as RULE_NBR",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___RULE_SEQ_NBR as RULE_SEQ_NBR",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___OPEN_PARAN as OPEN_PARAN",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___AND_OR as AND_OR",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___FIELD_NAME as FIELD_NAME",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___OPERATOR as OPERATOR",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___RULE_COMPARE_VALUE as RULE_COMPARE_VALUE",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___CLOSE_PARAN as CLOSE_PARAN",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___FREE_FORM_TEXT as FREE_FORM_TEXT",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___DESCRIPTION as DESCRIPTION",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___MISC as MISC",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___CREATE_DATE_TIME as CREATE_DATE_TIME",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___MOD_DATE_TIME as MOD_DATE_TIME",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___USER_ID as USER_ID",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___MISC_TXT_1 as MISC_TXT_1",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___MISC_TXT_2 as MISC_TXT_2",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___MISC_NUM_1 as MISC_NUM_1",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___MISC_NUM_2 as MISC_NUM_2",
        "SQ_Shortcut_to_E_MSRMNT_RULE_CONDITION___VERSION_ID as VERSION_ID",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_MSRMNT_RULE_CONDITION_PRE, type TARGET
    # COLUMN COUNT: 22

    Shortcut_to_WM_E_MSRMNT_RULE_CONDITION_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
        "CAST(MSRMNT_ID AS BIGINT) as MSRMNT_ID",
        "CAST(RULE_NBR AS BIGINT) as RULE_NBR",
        "CAST(RULE_SEQ_NBR AS BIGINT) as RULE_SEQ_NBR",
        "CAST(OPEN_PARAN AS STRING) as OPEN_PARAN",
        "CAST(AND_OR AS STRING) as AND_OR",
        "CAST(FIELD_NAME AS STRING) as FIELD_NAME",
        "CAST(OPERATOR AS STRING) as OPERATOR",
        "CAST(RULE_COMPARE_VALUE AS STRING) as RULE_COMPARE_VALUE",
        "CAST(CLOSE_PARAN AS STRING) as CLOSE_PARAN",
        "CAST(FREE_FORM_TEXT AS STRING) as FREE_FORM_TEXT",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(MISC AS STRING) as MISC",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1",
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2",
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1",
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2",
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID",
        "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
    )

    gu.overwriteDeltaPartition(
        Shortcut_to_WM_E_MSRMNT_RULE_CONDITION_PRE, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_E_MSRMNT_RULE_CONDITION_PRE is written to the target table - "
        + target_table_name
    )
