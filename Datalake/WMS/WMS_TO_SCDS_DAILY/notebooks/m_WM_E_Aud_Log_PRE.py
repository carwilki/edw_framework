#Code converted on 2023-06-24 13:39:40
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



def m_WM_E_Aud_Log_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_E_Aud_Log_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_E_AUD_LOG_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_AUD_LOG"


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
    # Processing node SQ_Shortcut_to_E_AUD_LOG, type SOURCE 
    # COLUMN COUNT: 42

    SQ_Shortcut_to_E_AUD_LOG = jdbcOracleConnection(
        f"""SELECT
                E_AUD_LOG.AUD_ID,
                E_AUD_LOG.WHSE,
                E_AUD_LOG.TRAN_NBR,
                E_AUD_LOG.SKU_ID,
                E_AUD_LOG.SKU_HNDL_ATTR,
                E_AUD_LOG.CRITERIA,
                E_AUD_LOG.SLOT_ATTR,
                E_AUD_LOG.FACTOR_TIME,
                E_AUD_LOG.CURR_SLOT,
                E_AUD_LOG.NEXT_SLOT,
                E_AUD_LOG.DISTANCE,
                E_AUD_LOG.HEIGHT,
                E_AUD_LOG.TRVL_DIR,
                E_AUD_LOG.ELEM_TIME,
                E_AUD_LOG.UOM,
                E_AUD_LOG.CURR_LOCN_CLASS,
                E_AUD_LOG.NEXT_LOCN_CLASS,
                E_AUD_LOG.TOT_TIME,
                E_AUD_LOG.TOT_UNITS,
                E_AUD_LOG.UNITS_PER_GRAB,
                E_AUD_LOG.LPN,
                E_AUD_LOG.ELEM_DESC,
                E_AUD_LOG.CREATE_DATE_TIME,
                E_AUD_LOG.TA_MULTIPLIER,
                E_AUD_LOG.ELS_TRAN_ID,
                E_AUD_LOG.CRIT_VAL,
                E_AUD_LOG.CRIT_TIME,
                E_AUD_LOG.ELM_ID,
                E_AUD_LOG.FACTOR,
                E_AUD_LOG.THRUPUT_MSRMNT,
                E_AUD_LOG.MODULE_TYPE,
                E_AUD_LOG.MISC_TXT_1,
                E_AUD_LOG.MISC_TXT_2,
                E_AUD_LOG.MISC_NUM_1,
                E_AUD_LOG.MISC_NUM_2,
                E_AUD_LOG.ADDTL_TIME_ALLOW,
                E_AUD_LOG.SLOT_TYPE_TIME_ALLOW,
                E_AUD_LOG.UNIT_PICK_TIME_ALLOW,
                E_AUD_LOG.PFD_TIME,
                E_AUD_LOG.VERSION_ID,
                E_AUD_LOG.ACT_ID,
                E_AUD_LOG.COMPONENT_BK
            FROM E_AUD_LOG
            WHERE (TRUNC( E_AUD_LOG.CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 44

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_AUD_LOG_temp = SQ_Shortcut_to_E_AUD_LOG.toDF(*["SQ_Shortcut_to_E_AUD_LOG___" + col for col in SQ_Shortcut_to_E_AUD_LOG.columns])

    EXPTRANS = SQ_Shortcut_to_E_AUD_LOG_temp.selectExpr( 
        "SQ_Shortcut_to_E_AUD_LOG___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_E_AUD_LOG___AUD_ID as AUD_ID", 
        "SQ_Shortcut_to_E_AUD_LOG___WHSE as WHSE", 
        "SQ_Shortcut_to_E_AUD_LOG___TRAN_NBR as TRAN_NBR", 
        "SQ_Shortcut_to_E_AUD_LOG___SKU_ID as SKU_ID", 
        "SQ_Shortcut_to_E_AUD_LOG___SKU_HNDL_ATTR as SKU_HNDL_ATTR", 
        "SQ_Shortcut_to_E_AUD_LOG___CRITERIA as CRITERIA", 
        "SQ_Shortcut_to_E_AUD_LOG___SLOT_ATTR as SLOT_ATTR", 
        "SQ_Shortcut_to_E_AUD_LOG___FACTOR_TIME as FACTOR_TIME", 
        "SQ_Shortcut_to_E_AUD_LOG___CURR_SLOT as CURR_SLOT", 
        "SQ_Shortcut_to_E_AUD_LOG___NEXT_SLOT as NEXT_SLOT", 
        "SQ_Shortcut_to_E_AUD_LOG___DISTANCE as DISTANCE", 
        "SQ_Shortcut_to_E_AUD_LOG___HEIGHT as HEIGHT", 
        "SQ_Shortcut_to_E_AUD_LOG___TRVL_DIR as TRVL_DIR", 
        "SQ_Shortcut_to_E_AUD_LOG___ELEM_TIME as ELEM_TIME", 
        "SQ_Shortcut_to_E_AUD_LOG___UOM as UOM", 
        "SQ_Shortcut_to_E_AUD_LOG___CURR_LOCN_CLASS as CURR_LOCN_CLASS", 
        "SQ_Shortcut_to_E_AUD_LOG___NEXT_LOCN_CLASS as NEXT_LOCN_CLASS", 
        "SQ_Shortcut_to_E_AUD_LOG___TOT_TIME as TOT_TIME", 
        "SQ_Shortcut_to_E_AUD_LOG___TOT_UNITS as TOT_UNITS", 
        "SQ_Shortcut_to_E_AUD_LOG___UNITS_PER_GRAB as UNITS_PER_GRAB", 
        "SQ_Shortcut_to_E_AUD_LOG___LPN as LPN", 
        "SQ_Shortcut_to_E_AUD_LOG___ELEM_DESC as ELEM_DESC", 
        "SQ_Shortcut_to_E_AUD_LOG___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_E_AUD_LOG___TA_MULTIPLIER as TA_MULTIPLIER", 
        "SQ_Shortcut_to_E_AUD_LOG___ELS_TRAN_ID as ELS_TRAN_ID", 
        "SQ_Shortcut_to_E_AUD_LOG___CRIT_VAL as CRIT_VAL", 
        "SQ_Shortcut_to_E_AUD_LOG___CRIT_TIME as CRIT_TIME", 
        "SQ_Shortcut_to_E_AUD_LOG___ELM_ID as ELM_ID", 
        "SQ_Shortcut_to_E_AUD_LOG___FACTOR as FACTOR", 
        "SQ_Shortcut_to_E_AUD_LOG___THRUPUT_MSRMNT as THRUPUT_MSRMNT", 
        "SQ_Shortcut_to_E_AUD_LOG___MODULE_TYPE as MODULE_TYPE", 
        "SQ_Shortcut_to_E_AUD_LOG___MISC_TXT_1 as MISC_TXT_1", 
        "SQ_Shortcut_to_E_AUD_LOG___MISC_TXT_2 as MISC_TXT_2", 
        "SQ_Shortcut_to_E_AUD_LOG___MISC_NUM_1 as MISC_NUM_1", 
        "SQ_Shortcut_to_E_AUD_LOG___MISC_NUM_2 as MISC_NUM_2", 
        "SQ_Shortcut_to_E_AUD_LOG___ADDTL_TIME_ALLOW as ADDTL_TIME_ALLOW", 
        "SQ_Shortcut_to_E_AUD_LOG___SLOT_TYPE_TIME_ALLOW as SLOT_TYPE_TIME_ALLOW", 
        "SQ_Shortcut_to_E_AUD_LOG___UNIT_PICK_TIME_ALLOW as UNIT_PICK_TIME_ALLOW", 
        "SQ_Shortcut_to_E_AUD_LOG___PFD_TIME as PFD_TIME", 
        "SQ_Shortcut_to_E_AUD_LOG___VERSION_ID as VERSION_ID", 
        "SQ_Shortcut_to_E_AUD_LOG___ACT_ID as ACT_ID", 
        "SQ_Shortcut_to_E_AUD_LOG___COMPONENT_BK as COMPONENT_BK", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_AUD_LOG_PRE, type TARGET 
    # COLUMN COUNT: 44


    Shortcut_to_WM_E_AUD_LOG_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(AUD_ID AS BIGINT) as AUD_ID", 
        "CAST(WHSE AS STRING) as WHSE", 
        "CAST(TRAN_NBR AS BIGINT) as TRAN_NBR", 
        "CAST(SKU_ID AS STRING) as SKU_ID", 
        "CAST(SKU_HNDL_ATTR AS STRING) as SKU_HNDL_ATTR", 
        "CAST(CRITERIA AS STRING) as CRITERIA", 
        "CAST(SLOT_ATTR AS STRING) as SLOT_ATTR", 
        "CAST(FACTOR_TIME AS BIGINT) as FACTOR_TIME", 
        "CAST(CURR_SLOT AS STRING) as CURR_SLOT", 
        "CAST(NEXT_SLOT AS STRING) as NEXT_SLOT", 
        "CAST(DISTANCE AS BIGINT) as DISTANCE", 
        "CAST(HEIGHT AS BIGINT) as HEIGHT", 
        "CAST(TRVL_DIR AS STRING) as TRVL_DIR", 
        "CAST(ELEM_TIME AS BIGINT) as ELEM_TIME", 
        "CAST(UOM AS STRING) as UOM", 
        "CAST(CURR_LOCN_CLASS AS STRING) as CURR_LOCN_CLASS", 
        "CAST(NEXT_LOCN_CLASS AS STRING) as NEXT_LOCN_CLASS", 
        "CAST(TOT_TIME AS BIGINT) as TOT_TIME", 
        "CAST(TOT_UNITS AS BIGINT) as TOT_UNITS", 
        "CAST(UNITS_PER_GRAB AS BIGINT) as UNITS_PER_GRAB", 
        "CAST(LPN AS STRING) as LPN", 
        "CAST(ELEM_DESC AS STRING) as ELEM_DESC", 
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
        "CAST(TA_MULTIPLIER AS BIGINT) as TA_MULTIPLIER", 
        "CAST(ELS_TRAN_ID AS BIGINT) as ELS_TRAN_ID", 
        "CAST(CRIT_VAL AS STRING) as CRIT_VAL", 
        "CAST(CRIT_TIME AS BIGINT) as CRIT_TIME", 
        "CAST(ELM_ID AS BIGINT) as ELM_ID", 
        "CAST(FACTOR AS STRING) as FACTOR", 
        "CAST(THRUPUT_MSRMNT AS STRING) as THRUPUT_MSRMNT", 
        "CAST(MODULE_TYPE AS STRING) as MODULE_TYPE", 
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1", 
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2", 
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1", 
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2", 
        "CAST(ADDTL_TIME_ALLOW AS BIGINT) as ADDTL_TIME_ALLOW", 
        "CAST(SLOT_TYPE_TIME_ALLOW AS BIGINT) as SLOT_TYPE_TIME_ALLOW", 
        "CAST(UNIT_PICK_TIME_ALLOW AS BIGINT) as UNIT_PICK_TIME_ALLOW", 
        "CAST(PFD_TIME AS BIGINT) as PFD_TIME", 
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID", 
        "CAST(ACT_ID AS BIGINT) as ACT_ID", 
        "CAST(COMPONENT_BK AS STRING) as COMPONENT_BK", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_E_AUD_LOG_PRE,"DC_NBR",dcnbr,target_table_name)
    
    logger.info(
        "Shortcut_to_WM_E_AUD_LOG_PRE is written to the target table - "
        + target_table_name
    )