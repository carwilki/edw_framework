#Code converted on 2023-06-24 13:39:57
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



def m_WM_E_Act_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_E_Act_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_E_ACT_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_ACT"


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
    # Processing node SQ_Shortcut_to_E_ACT, type SOURCE 
    # COLUMN COUNT: 25

    SQ_Shortcut_to_E_ACT = jdbcOracleConnection(
        f"""SELECT
                E_ACT.ACT_ID,
                E_ACT.CREATE_DATE_TIME,
                E_ACT.MOD_DATE_TIME,
                E_ACT.USER_ID,
                E_ACT.JOB_FUNC_ID,
                E_ACT.WHSE,
                E_ACT.LABOR_TYPE_ID,
                E_ACT.MONITOR_UOM,
                E_ACT.MISC_TXT_1,
                E_ACT.MISC_TXT_2,
                E_ACT.MISC_NUM_1,
                E_ACT.MISC_NUM_2,
                E_ACT.OVERRIDE_PROC_ZONE_ID,
                E_ACT.PROC_ZONE_LOCN_IND,
                E_ACT.LM_MAN_EVNT_REQ_APRV,
                E_ACT.LM_KIOSK_REQ_APRV,
                E_ACT.WM_REQ_APRV,
                E_ACT.VERSION_ID,
                E_ACT.LABOR_ACTIVITY_ID,
                E_ACT.VHCL_TYPE_ID,
                E_ACT.UNQ_SEED_ID,
                E_ACT.CREATED_DTTM,
                E_ACT.LAST_UPDATED_DTTM,
                E_ACT.DISPLAY_UOM,
                E_ACT.THRUPUT_GOAL
            FROM E_ACT
            WHERE (TRUNC( E_ACT.CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14 ) OR (TRUNC( E_ACT.MOD_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14 ) OR (TRUNC( E_ACT.CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14) OR (TRUNC( E_ACT.LAST_UPDATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 27

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_ACT_temp = SQ_Shortcut_to_E_ACT.toDF(*["SQ_Shortcut_to_E_ACT___" + col for col in SQ_Shortcut_to_E_ACT.columns])

    EXPTRANS = SQ_Shortcut_to_E_ACT_temp.selectExpr( 
        "SQ_Shortcut_to_E_ACT___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_E_ACT___ACT_ID as ACT_ID", 
        "SQ_Shortcut_to_E_ACT___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_E_ACT___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_E_ACT___USER_ID as USER_ID", 
        "SQ_Shortcut_to_E_ACT___JOB_FUNC_ID as JOB_FUNC_ID", 
        "SQ_Shortcut_to_E_ACT___WHSE as WHSE", 
        "SQ_Shortcut_to_E_ACT___LABOR_TYPE_ID as LABOR_TYPE_ID", 
        "SQ_Shortcut_to_E_ACT___MONITOR_UOM as MONITOR_UOM", 
        "SQ_Shortcut_to_E_ACT___MISC_TXT_1 as MISC_TXT_1", 
        "SQ_Shortcut_to_E_ACT___MISC_TXT_2 as MISC_TXT_2", 
        "SQ_Shortcut_to_E_ACT___MISC_NUM_1 as MISC_NUM_1", 
        "SQ_Shortcut_to_E_ACT___MISC_NUM_2 as MISC_NUM_2", 
        "SQ_Shortcut_to_E_ACT___OVERRIDE_PROC_ZONE_ID as OVERRIDE_PROC_ZONE_ID", 
        "SQ_Shortcut_to_E_ACT___PROC_ZONE_LOCN_IND as PROC_ZONE_LOCN_IND", 
        "SQ_Shortcut_to_E_ACT___LM_MAN_EVNT_REQ_APRV as LM_MAN_EVNT_REQ_APRV", 
        "SQ_Shortcut_to_E_ACT___LM_KIOSK_REQ_APRV as LM_KIOSK_REQ_APRV", 
        "SQ_Shortcut_to_E_ACT___WM_REQ_APRV as WM_REQ_APRV", 
        "SQ_Shortcut_to_E_ACT___VERSION_ID as VERSION_ID", 
        "SQ_Shortcut_to_E_ACT___LABOR_ACTIVITY_ID as LABOR_ACTIVITY_ID", 
        "SQ_Shortcut_to_E_ACT___VHCL_TYPE_ID as VHCL_TYPE_ID", 
        "SQ_Shortcut_to_E_ACT___UNQ_SEED_ID as UNQ_SEED_ID", 
        "SQ_Shortcut_to_E_ACT___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_E_ACT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "SQ_Shortcut_to_E_ACT___DISPLAY_UOM as DISPLAY_UOM", 
        "SQ_Shortcut_to_E_ACT___THRUPUT_GOAL as THRUPUT_GOAL", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_ACT_PRE, type TARGET 
    # COLUMN COUNT: 27


    Shortcut_to_WM_E_ACT_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(ACT_ID AS BIGINT) as ACT_ID", 
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
        "CAST(USER_ID AS STRING) as USER_ID", 
        "CAST(JOB_FUNC_ID AS BIGINT) as JOB_FUNC_ID", 
        "CAST(WHSE AS STRING) as WHSE", 
        "CAST(LABOR_TYPE_ID AS BIGINT) as LABOR_TYPE_ID", 
        "CAST(MONITOR_UOM AS STRING) as MONITOR_UOM", 
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1", 
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2", 
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1", 
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2", 
        "CAST(OVERRIDE_PROC_ZONE_ID AS BIGINT) as OVERRIDE_PROC_ZONE_ID", 
        "CAST(PROC_ZONE_LOCN_IND AS BIGINT) as PROC_ZONE_LOCN_IND", 
        "CAST(LM_MAN_EVNT_REQ_APRV AS STRING) as LM_MAN_EVNT_REQ_APRV", 
        "CAST(LM_KIOSK_REQ_APRV AS STRING) as LM_KIOSK_REQ_APRV", 
        "CAST(WM_REQ_APRV AS STRING) as WM_REQ_APRV", 
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID", 
        "CAST(LABOR_ACTIVITY_ID AS BIGINT) as LABOR_ACTIVITY_ID", 
        "CAST(VHCL_TYPE_ID AS BIGINT) as VHCL_TYPE_ID", 
        "CAST(UNQ_SEED_ID AS BIGINT) as UNQ_SEED_ID", 
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
        "CAST(DISPLAY_UOM AS STRING) as DISPLAY_UOM", 
        "CAST(THRUPUT_GOAL AS BIGINT) as THRUPUT_GOAL", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    overwriteDeltaPartition(Shortcut_to_WM_E_ACT_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_ACT_PRE is written to the target table - "
        + target_table_name
    )