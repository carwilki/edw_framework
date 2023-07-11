# Code converted on 2023-06-24 13:35:19
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


def m_WM_E_Shift_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    logger.info("inside m_WM_E_Shift_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_E_SHIFT_PRE"

    schemaName = raw
    source_schema = "WMSMIS"

    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]

    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now()  # start timestamp of the script

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)

    # COMMAND ----------
    # Variable_declaration_comment
    dcnbr = dcnbr.strip()[2:]
    Prev_Run_Dt = genPrevRunDt(refine_table_name, refine, raw)

    # COMMAND ----------
    # Processing node SQ_Shortcut_to_E_SHIFT, type SOURCE
    # COLUMN COUNT: 23

    SQ_Shortcut_to_E_SHIFT = jdbcOracleConnection(
        f"""SELECT
                E_SHIFT.SHIFT_ID,
                E_SHIFT.EFF_DATE,
                E_SHIFT.SHIFT_CODE,
                E_SHIFT.DESCRIPTION,
                E_SHIFT.OT_INDIC,
                E_SHIFT.OT_PAY_FCT,
                E_SHIFT.WEEK_MIN_OT_HRS,
                E_SHIFT.CREATE_DATE_TIME,
                E_SHIFT.MOD_DATE_TIME,
                E_SHIFT.USER_ID,
                E_SHIFT.WHSE,
                E_SHIFT.REPORT_SHIFT,
                E_SHIFT.MISC_TXT_1,
                E_SHIFT.MISC_TXT_2,
                E_SHIFT.MISC_NUM_1,
                E_SHIFT.MISC_NUM_2,
                E_SHIFT.VERSION_ID,
                E_SHIFT.ROT_SHIFT_IND,
                E_SHIFT.ROT_SHIFT_NUM_DAYS,
                E_SHIFT.ROT_SHIFT_START_DATE,
                E_SHIFT.SCHED_SHIFT,
                E_SHIFT.CREATED_DTTM,
                E_SHIFT.LAST_UPDATED_DTTM
            FROM {source_schema}.E_SHIFT
            WHERE (TRUNC( CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (TRUNC( LAST_UPDATED_DTTM) >=  TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (TRUNC( CREATE_DATE_TIME) >=  TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14 )OR (TRUNC( MOD_DATE_TIME) >=  TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14)""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 25

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_SHIFT_temp = SQ_Shortcut_to_E_SHIFT.toDF(
        *["SQ_Shortcut_to_E_SHIFT___" + col for col in SQ_Shortcut_to_E_SHIFT.columns]
    )

    EXPTRANS = SQ_Shortcut_to_E_SHIFT_temp.selectExpr(
        "SQ_Shortcut_to_E_SHIFT___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_E_SHIFT___SHIFT_ID as SHIFT_ID",
        "SQ_Shortcut_to_E_SHIFT___EFF_DATE as EFF_DATE",
        "SQ_Shortcut_to_E_SHIFT___SHIFT_CODE as SHIFT_CODE",
        "SQ_Shortcut_to_E_SHIFT___DESCRIPTION as DESCRIPTION",
        "SQ_Shortcut_to_E_SHIFT___OT_INDIC as OT_INDIC",
        "SQ_Shortcut_to_E_SHIFT___OT_PAY_FCT as OT_PAY_FCT",
        "SQ_Shortcut_to_E_SHIFT___WEEK_MIN_OT_HRS as WEEK_MIN_OT_HRS",
        "SQ_Shortcut_to_E_SHIFT___CREATE_DATE_TIME as CREATE_DATE_TIME",
        "SQ_Shortcut_to_E_SHIFT___MOD_DATE_TIME as MOD_DATE_TIME",
        "SQ_Shortcut_to_E_SHIFT___USER_ID as USER_ID",
        "SQ_Shortcut_to_E_SHIFT___WHSE as WHSE",
        "SQ_Shortcut_to_E_SHIFT___REPORT_SHIFT as REPORT_SHIFT",
        "SQ_Shortcut_to_E_SHIFT___MISC_TXT_1 as MISC_TXT_1",
        "SQ_Shortcut_to_E_SHIFT___MISC_TXT_2 as MISC_TXT_2",
        "SQ_Shortcut_to_E_SHIFT___MISC_NUM_1 as MISC_NUM_1",
        "SQ_Shortcut_to_E_SHIFT___MISC_NUM_2 as MISC_NUM_2",
        "SQ_Shortcut_to_E_SHIFT___VERSION_ID as VERSION_ID",
        "SQ_Shortcut_to_E_SHIFT___ROT_SHIFT_IND as ROT_SHIFT_IND",
        "SQ_Shortcut_to_E_SHIFT___ROT_SHIFT_NUM_DAYS as ROT_SHIFT_NUM_DAYS",
        "SQ_Shortcut_to_E_SHIFT___ROT_SHIFT_START_DATE as ROT_SHIFT_START_DATE",
        "SQ_Shortcut_to_E_SHIFT___SCHED_SHIFT as SCHED_SHIFT",
        "SQ_Shortcut_to_E_SHIFT___CREATED_DTTM as CREATED_DTTM",
        "SQ_Shortcut_to_E_SHIFT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_SHIFT_PRE1, type TARGET
    # COLUMN COUNT: 25

    Shortcut_to_WM_E_SHIFT_PRE1 = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(SHIFT_ID AS BIGINT) as SHIFT_ID",
        "CAST(EFF_DATE AS TIMESTAMP) as EFF_DATE",
        "CAST(SHIFT_CODE AS STRING) as SHIFT_CODE",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(OT_INDIC AS STRING) as OT_INDIC",
        "CAST(OT_PAY_FCT AS BIGINT) as OT_PAY_FCT",
        "CAST(WEEK_MIN_OT_HRS AS BIGINT) as WEEK_MIN_OT_HRS",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(WHSE AS STRING) as WHSE",
        "CAST(REPORT_SHIFT AS STRING) as REPORT_SHIFT",
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1",
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2",
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1",
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2",
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID",
        "CAST(ROT_SHIFT_IND AS STRING) as ROT_SHIFT_IND",
        "CAST(ROT_SHIFT_NUM_DAYS AS BIGINT) as ROT_SHIFT_NUM_DAYS",
        "CAST(ROT_SHIFT_START_DATE AS TIMESTAMP) as ROT_SHIFT_START_DATE",
        "CAST(SCHED_SHIFT AS STRING) as SCHED_SHIFT",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
    )

    overwriteDeltaPartition(
        Shortcut_to_WM_E_SHIFT_PRE1, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_E_SHIFT_PRE1Shortcut_to_WM_E_SHIFT_PRE1 is written to the target table - "
        + target_table_name
    )
