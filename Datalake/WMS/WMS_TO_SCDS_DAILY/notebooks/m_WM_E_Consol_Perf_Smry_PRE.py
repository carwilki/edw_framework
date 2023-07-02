# Code converted on 2023-06-24 13:39:21
from datetime import datetime

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DecimalType, StringType, TimestampType

from Datalake.utils.configs import getConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from logging import getLogger, INFO
from Datalake.utils import genericUtilities as gu


def m_WM_E_Consol_Perf_Smry_PRE(dcnbr, env):
    logger = getLogger()
    logger.info("inside m_WM_E_Consol_Perf_Smry_PRE function")

    spark = SparkSession.getActiveSession()

    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_E_CONSOL_PERF_SMRY_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_CONSOL_PERF_SMRY"

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
    # Processing node SQ_Shortcut_to_E_CONSOL_PERF_SMRY, type SOURCE
    # COLUMN COUNT: 84

    SQ_Shortcut_to_E_CONSOL_PERF_SMRY = gu.jdbcOracleConnection(
        f"""SELECT
                E_CONSOL_PERF_SMRY.PERF_SMRY_TRAN_ID,
                E_CONSOL_PERF_SMRY.WHSE,
                E_CONSOL_PERF_SMRY.LOGIN_USER_ID,
                E_CONSOL_PERF_SMRY.JOB_FUNCTION_NAME,
                E_CONSOL_PERF_SMRY.SPVSR_LOGIN_USER_ID,
                E_CONSOL_PERF_SMRY.DEPT_CODE,
                E_CONSOL_PERF_SMRY.CLOCK_IN_DATE,
                E_CONSOL_PERF_SMRY.CLOCK_IN_STATUS,
                E_CONSOL_PERF_SMRY.TOTAL_SAM,
                E_CONSOL_PERF_SMRY.TOTAL_PAM,
                E_CONSOL_PERF_SMRY.TOTAL_TIME,
                E_CONSOL_PERF_SMRY.OSDL,
                E_CONSOL_PERF_SMRY.OSIL,
                E_CONSOL_PERF_SMRY.NSDL,
                E_CONSOL_PERF_SMRY.SIL,
                E_CONSOL_PERF_SMRY.UDIL,
                E_CONSOL_PERF_SMRY.UIL,
                E_CONSOL_PERF_SMRY.ADJ_OSDL,
                E_CONSOL_PERF_SMRY.ADJ_OSIL,
                E_CONSOL_PERF_SMRY.ADJ_UDIL,
                E_CONSOL_PERF_SMRY.ADJ_NSDL,
                E_CONSOL_PERF_SMRY.PAID_BRK,
                E_CONSOL_PERF_SMRY.UNPAID_BRK,
                E_CONSOL_PERF_SMRY.REF_OSDL,
                E_CONSOL_PERF_SMRY.REF_OSIL,
                E_CONSOL_PERF_SMRY.REF_UDIL,
                E_CONSOL_PERF_SMRY.REF_NSDL,
                E_CONSOL_PERF_SMRY.REF_ADJ_OSDL,
                E_CONSOL_PERF_SMRY.REF_ADJ_OSIL,
                E_CONSOL_PERF_SMRY.REF_ADJ_UDIL,
                E_CONSOL_PERF_SMRY.REF_ADJ_NSDL,
                E_CONSOL_PERF_SMRY.MISC_NUMBER_1,
                E_CONSOL_PERF_SMRY.CREATE_DATE_TIME,
                E_CONSOL_PERF_SMRY.MOD_DATE_TIME,
                E_CONSOL_PERF_SMRY.USER_ID,
                E_CONSOL_PERF_SMRY.MISC_1,
                E_CONSOL_PERF_SMRY.MISC_2,
                E_CONSOL_PERF_SMRY.CLOCK_OUT_DATE,
                E_CONSOL_PERF_SMRY.SHIFT_CODE,
                E_CONSOL_PERF_SMRY.EVENT_COUNT,
                E_CONSOL_PERF_SMRY.START_DATE_TIME,
                E_CONSOL_PERF_SMRY.END_DATE_TIME,
                E_CONSOL_PERF_SMRY.LEVEL_1,
                E_CONSOL_PERF_SMRY.LEVEL_2,
                E_CONSOL_PERF_SMRY.LEVEL_3,
                E_CONSOL_PERF_SMRY.LEVEL_4,
                E_CONSOL_PERF_SMRY.LEVEL_5,
                E_CONSOL_PERF_SMRY.WHSE_DATE,
                E_CONSOL_PERF_SMRY.OPS_CODE,
                E_CONSOL_PERF_SMRY.REF_SAM,
                E_CONSOL_PERF_SMRY.REF_PAM,
                E_CONSOL_PERF_SMRY.REPORT_SHIFT,
                E_CONSOL_PERF_SMRY.MISC_TXT_1,
                E_CONSOL_PERF_SMRY.MISC_TXT_2,
                E_CONSOL_PERF_SMRY.MISC_NUM_1,
                E_CONSOL_PERF_SMRY.MISC_NUM_2,
                E_CONSOL_PERF_SMRY.EVNT_CTGRY_1,
                E_CONSOL_PERF_SMRY.EVNT_CTGRY_2,
                E_CONSOL_PERF_SMRY.EVNT_CTGRY_3,
                E_CONSOL_PERF_SMRY.EVNT_CTGRY_4,
                E_CONSOL_PERF_SMRY.EVNT_CTGRY_5,
                E_CONSOL_PERF_SMRY.LABOR_COST_RATE,
                E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSDL,
                E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSDL,
                E_CONSOL_PERF_SMRY.PAID_OVERLAP_NSDL,
                E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_NSDL,
                E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSIL,
                E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSIL,
                E_CONSOL_PERF_SMRY.PAID_OVERLAP_UDIL,
                E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_UDIL,
                E_CONSOL_PERF_SMRY.VERSION_ID,
                E_CONSOL_PERF_SMRY.TEAM_CODE,
                E_CONSOL_PERF_SMRY.DEFAULT_JF_FLAG,
                E_CONSOL_PERF_SMRY.EMP_PERF_SMRY_ID,
                E_CONSOL_PERF_SMRY.TOTAL_QTY,
                E_CONSOL_PERF_SMRY.REF_NBR,
                E_CONSOL_PERF_SMRY.TEAM_BEGIN_TIME,
                E_CONSOL_PERF_SMRY.THRUPUT_MIN,
                E_CONSOL_PERF_SMRY.DISPLAY_UOM_QTY,
                E_CONSOL_PERF_SMRY.DISPLAY_UOM,
                E_CONSOL_PERF_SMRY.LOCN_GRP_ATTR,
                E_CONSOL_PERF_SMRY.RESOURCE_GROUP_ID,
                E_CONSOL_PERF_SMRY.COMP_ASSIGNMENT_ID,
                E_CONSOL_PERF_SMRY.REFLECTIVE_CODE
            FROM E_CONSOL_PERF_SMRY
            WHERE (TRUNC( E_CONSOL_PERF_SMRY.CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1 ) OR (TRUNC( E_CONSOL_PERF_SMRY.MOD_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 86

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_CONSOL_PERF_SMRY_temp = SQ_Shortcut_to_E_CONSOL_PERF_SMRY.toDF(
        *[
            "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___" + col
            for col in SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns
        ]
    )

    EXPTRANS = SQ_Shortcut_to_E_CONSOL_PERF_SMRY_temp.selectExpr(
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___PERF_SMRY_TRAN_ID as PERF_SMRY_TRAN_ID",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___WHSE as WHSE",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___LOGIN_USER_ID as LOGIN_USER_ID",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___JOB_FUNCTION_NAME as JOB_FUNCTION_NAME",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___SPVSR_LOGIN_USER_ID as SPVSR_LOGIN_USER_ID",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___DEPT_CODE as DEPT_CODE",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___CLOCK_IN_DATE as CLOCK_IN_DATE",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___CLOCK_IN_STATUS as CLOCK_IN_STATUS",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___TOTAL_SAM as TOTAL_SAM",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___TOTAL_PAM as TOTAL_PAM",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___TOTAL_TIME as TOTAL_TIME",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___OSDL as OSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___OSIL as OSIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___NSDL as NSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___SIL as SIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___UDIL as UDIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___UIL as UIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___ADJ_OSDL as ADJ_OSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___ADJ_OSIL as ADJ_OSIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___ADJ_UDIL as ADJ_UDIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___ADJ_NSDL as ADJ_NSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___PAID_BRK as PAID_BRK",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___UNPAID_BRK as UNPAID_BRK",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_OSDL as REF_OSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_OSIL as REF_OSIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_UDIL as REF_UDIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_NSDL as REF_NSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_ADJ_OSDL as REF_ADJ_OSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_ADJ_OSIL as REF_ADJ_OSIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_ADJ_UDIL as REF_ADJ_UDIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_ADJ_NSDL as REF_ADJ_NSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___MISC_NUMBER_1 as MISC_NUMBER_1",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___CREATE_DATE_TIME as CREATE_DATE_TIME",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___MOD_DATE_TIME as MOD_DATE_TIME",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___USER_ID as USER_ID",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___MISC_1 as MISC_1",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___MISC_2 as MISC_2",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___CLOCK_OUT_DATE as CLOCK_OUT_DATE",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___SHIFT_CODE as SHIFT_CODE",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___EVENT_COUNT as EVENT_COUNT",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___START_DATE_TIME as START_DATE_TIME",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___END_DATE_TIME as END_DATE_TIME",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___LEVEL_1 as LEVEL_1",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___LEVEL_2 as LEVEL_2",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___LEVEL_3 as LEVEL_3",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___LEVEL_4 as LEVEL_4",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___LEVEL_5 as LEVEL_5",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___WHSE_DATE as WHSE_DATE",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___OPS_CODE as OPS_CODE",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_SAM as REF_SAM",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_PAM as REF_PAM",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REPORT_SHIFT as REPORT_SHIFT",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___MISC_TXT_1 as MISC_TXT_1",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___MISC_TXT_2 as MISC_TXT_2",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___MISC_NUM_1 as MISC_NUM_1",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___MISC_NUM_2 as MISC_NUM_2",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___EVNT_CTGRY_1 as EVNT_CTGRY_1",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___EVNT_CTGRY_2 as EVNT_CTGRY_2",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___EVNT_CTGRY_3 as EVNT_CTGRY_3",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___EVNT_CTGRY_4 as EVNT_CTGRY_4",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___EVNT_CTGRY_5 as EVNT_CTGRY_5",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___LABOR_COST_RATE as LABOR_COST_RATE",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___PAID_OVERLAP_OSDL as PAID_OVERLAP_OSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___UNPAID_OVERLAP_OSDL as UNPAID_OVERLAP_OSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___PAID_OVERLAP_NSDL as PAID_OVERLAP_NSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___UNPAID_OVERLAP_NSDL as UNPAID_OVERLAP_NSDL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___PAID_OVERLAP_OSIL as PAID_OVERLAP_OSIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___UNPAID_OVERLAP_OSIL as UNPAID_OVERLAP_OSIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___PAID_OVERLAP_UDIL as PAID_OVERLAP_UDIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___UNPAID_OVERLAP_UDIL as UNPAID_OVERLAP_UDIL",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___VERSION_ID as VERSION_ID",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___TEAM_CODE as TEAM_CODE",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___DEFAULT_JF_FLAG as DEFAULT_JF_FLAG",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___EMP_PERF_SMRY_ID as EMP_PERF_SMRY_ID",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___TOTAL_QTY as TOTAL_QTY",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REF_NBR as REF_NBR",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___TEAM_BEGIN_TIME as TEAM_BEGIN_TIME",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___THRUPUT_MIN as THRUPUT_MIN",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___DISPLAY_UOM_QTY as DISPLAY_UOM_QTY",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___DISPLAY_UOM as DISPLAY_UOM",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___LOCN_GRP_ATTR as LOCN_GRP_ATTR",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___RESOURCE_GROUP_ID as RESOURCE_GROUP_ID",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___COMP_ASSIGNMENT_ID as COMP_ASSIGNMENT_ID",
        "SQ_Shortcut_to_E_CONSOL_PERF_SMRY___REFLECTIVE_CODE as REFLECTIVE_CODE",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE, type TARGET
    # COLUMN COUNT: 86

    Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
        "CAST(PERF_SMRY_TRAN_ID AS BIGINT) as PERF_SMRY_TRAN_ID",
        "CAST(WHSE AS STRING) as WHSE",
        "CAST(LOGIN_USER_ID AS STRING) as LOGIN_USER_ID",
        "CAST(JOB_FUNCTION_NAME AS STRING) as JOB_FUNCTION_NAME",
        "CAST(SPVSR_LOGIN_USER_ID AS STRING) as SPVSR_LOGIN_USER_ID",
        "CAST(DEPT_CODE AS STRING) as DEPT_CODE",
        "CAST(CLOCK_IN_DATE AS TIMESTAMP) as CLOCK_IN_DATE",
        "CAST(CLOCK_IN_STATUS AS BIGINT) as CLOCK_IN_STATUS",
        "CAST(TOTAL_SAM AS BIGINT) as TOTAL_SAM",
        "CAST(TOTAL_PAM AS BIGINT) as TOTAL_PAM",
        "CAST(TOTAL_TIME AS BIGINT) as TOTAL_TIME",
        "CAST(OSDL AS BIGINT) as OSDL",
        "CAST(OSIL AS BIGINT) as OSIL",
        "CAST(NSDL AS BIGINT) as NSDL",
        "CAST(SIL AS BIGINT) as SIL",
        "CAST(UDIL AS BIGINT) as UDIL",
        "CAST(UIL AS BIGINT) as UIL",
        "CAST(ADJ_OSDL AS BIGINT) as ADJ_OSDL",
        "CAST(ADJ_OSIL AS BIGINT) as ADJ_OSIL",
        "CAST(ADJ_UDIL AS BIGINT) as ADJ_UDIL",
        "CAST(ADJ_NSDL AS BIGINT) as ADJ_NSDL",
        "CAST(PAID_BRK AS BIGINT) as PAID_BRK",
        "CAST(UNPAID_BRK AS BIGINT) as UNPAID_BRK",
        "CAST(REF_OSDL AS BIGINT) as REF_OSDL",
        "CAST(REF_OSIL AS BIGINT) as REF_OSIL",
        "CAST(REF_UDIL AS BIGINT) as REF_UDIL",
        "CAST(REF_NSDL AS BIGINT) as REF_NSDL",
        "CAST(REF_ADJ_OSDL AS BIGINT) as REF_ADJ_OSDL",
        "CAST(REF_ADJ_OSIL AS BIGINT) as REF_ADJ_OSIL",
        "CAST(REF_ADJ_UDIL AS BIGINT) as REF_ADJ_UDIL",
        "CAST(REF_ADJ_NSDL AS BIGINT) as REF_ADJ_NSDL",
        "CAST(MISC_NUMBER_1 AS BIGINT) as MISC_NUMBER_1",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(MISC_1 AS STRING) as MISC_1",
        "CAST(MISC_2 AS STRING) as MISC_2",
        "CAST(CLOCK_OUT_DATE AS TIMESTAMP) as CLOCK_OUT_DATE",
        "CAST(SHIFT_CODE AS STRING) as SHIFT_CODE",
        "CAST(EVENT_COUNT AS BIGINT) as EVENT_COUNT",
        "CAST(START_DATE_TIME AS TIMESTAMP) as START_DATE_TIME",
        "CAST(END_DATE_TIME AS TIMESTAMP) as END_DATE_TIME",
        "CAST(LEVEL_1 AS STRING) as LEVEL_1",
        "CAST(LEVEL_2 AS STRING) as LEVEL_2",
        "CAST(LEVEL_3 AS STRING) as LEVEL_3",
        "CAST(LEVEL_4 AS STRING) as LEVEL_4",
        "CAST(LEVEL_5 AS STRING) as LEVEL_5",
        "CAST(WHSE_DATE AS TIMESTAMP) as WHSE_DATE",
        "CAST(OPS_CODE AS STRING) as OPS_CODE",
        "CAST(REF_SAM AS BIGINT) as REF_SAM",
        "CAST(REF_PAM AS BIGINT) as REF_PAM",
        "CAST(REPORT_SHIFT AS STRING) as REPORT_SHIFT",
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1",
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2",
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1",
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2",
        "CAST(EVNT_CTGRY_1 AS STRING) as EVNT_CTGRY_1",
        "CAST(EVNT_CTGRY_2 AS STRING) as EVNT_CTGRY_2",
        "CAST(EVNT_CTGRY_3 AS STRING) as EVNT_CTGRY_3",
        "CAST(EVNT_CTGRY_4 AS STRING) as EVNT_CTGRY_4",
        "CAST(EVNT_CTGRY_5 AS STRING) as EVNT_CTGRY_5",
        "CAST(LABOR_COST_RATE AS BIGINT) as LABOR_COST_RATE",
        "CAST(PAID_OVERLAP_OSDL AS BIGINT) as PAID_OVERLAP_OSDL",
        "CAST(UNPAID_OVERLAP_OSDL AS BIGINT) as UNPAID_OVERLAP_OSDL",
        "CAST(PAID_OVERLAP_NSDL AS BIGINT) as PAID_OVERLAP_NSDL",
        "CAST(UNPAID_OVERLAP_NSDL AS BIGINT) as UNPAID_OVERLAP_NSDL",
        "CAST(PAID_OVERLAP_OSIL AS BIGINT) as PAID_OVERLAP_OSIL",
        "CAST(UNPAID_OVERLAP_OSIL AS BIGINT) as UNPAID_OVERLAP_OSIL",
        "CAST(PAID_OVERLAP_UDIL AS BIGINT) as PAID_OVERLAP_UDIL",
        "CAST(UNPAID_OVERLAP_UDIL AS BIGINT) as UNPAID_OVERLAP_UDIL",
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID",
        "CAST(TEAM_CODE AS STRING) as TEAM_CODE",
        "CAST(DEFAULT_JF_FLAG AS BIGINT) as DEFAULT_JF_FLAG",
        "CAST(EMP_PERF_SMRY_ID AS BIGINT) as EMP_PERF_SMRY_ID",
        "CAST(TOTAL_QTY AS BIGINT) as TOTAL_QTY",
        "CAST(REF_NBR AS STRING) as REF_NBR",
        "CAST(TEAM_BEGIN_TIME AS TIMESTAMP) as TEAM_BEGIN_TIME",
        "CAST(THRUPUT_MIN AS BIGINT) as THRUPUT_MIN",
        "CAST(DISPLAY_UOM_QTY AS BIGINT) as DISPLAY_UOM_QTY",
        "CAST(DISPLAY_UOM AS STRING) as DISPLAY_UOM",
        "CAST(LOCN_GRP_ATTR AS STRING) as LOCN_GRP_ATTR",
        "CAST(RESOURCE_GROUP_ID AS STRING) as RESOURCE_GROUP_ID",
        "CAST(COMP_ASSIGNMENT_ID AS STRING) as COMP_ASSIGNMENT_ID",
        "CAST(REFLECTIVE_CODE AS STRING) as REFLECTIVE_CODE",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP",
    )

    gu.overwriteDeltaPartition(
        Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE is written to the target table - "
        + target_table_name
    )
