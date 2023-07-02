#Code converted on 2023-06-24 13:36:26
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



def m_WM_E_Msrmnt_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_E_Msrmnt_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_E_MSRMNT_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_MSRMNT"


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
    # Processing node SQ_Shortcut_to_E_MSRMNT, type SOURCE 
    # COLUMN COUNT: 19

    SQ_Shortcut_to_E_MSRMNT = jdbcOracleConnection(
        f"""SELECT
            E_MSRMNT.MSRMNT_ID,
            E_MSRMNT.MSRMNT_CODE,
            E_MSRMNT.NAME,
            E_MSRMNT.STATUS_FLAG,
            E_MSRMNT.SYS_CREATED,
            E_MSRMNT.CREATE_DATE_TIME,
            E_MSRMNT.MOD_DATE_TIME,
            E_MSRMNT.USER_ID,
            E_MSRMNT.MISC_TXT_1,
            E_MSRMNT.MISC_TXT_2,
            E_MSRMNT.MISC_NUM_1,
            E_MSRMNT.MISC_NUM_2,
            E_MSRMNT.VERSION_ID,
            E_MSRMNT.UNQ_SEED_ID,
            E_MSRMNT.SIM_WHSE,
            E_MSRMNT.ORIG_MSRMNT_CODE,
            E_MSRMNT.ORIG_NAME,
            E_MSRMNT.CREATED_DTTM,
            E_MSRMNT.LAST_UPDATED_DTTM
        FROM E_MSRMNT
        WHERE (TRUNC( E_MSRMNT.CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (TRUNC( E_MSRMNT.LAST_UPDATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 21

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_MSRMNT_temp = SQ_Shortcut_to_E_MSRMNT.toDF(*["SQ_Shortcut_to_E_MSRMNT___" + col for col in SQ_Shortcut_to_E_MSRMNT.columns])

    EXPTRANS = SQ_Shortcut_to_E_MSRMNT_temp.selectExpr( 
        "SQ_Shortcut_to_E_MSRMNT___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NMBR_EXP", 
        "SQ_Shortcut_to_E_MSRMNT___MSRMNT_ID as MSRMNT_ID", 
        "SQ_Shortcut_to_E_MSRMNT___MSRMNT_CODE as MSRMNT_CODE", 
        "SQ_Shortcut_to_E_MSRMNT___NAME as NAME", 
        "SQ_Shortcut_to_E_MSRMNT___STATUS_FLAG as STATUS_FLAG", 
        "SQ_Shortcut_to_E_MSRMNT___SYS_CREATED as SYS_CREATED", 
        "SQ_Shortcut_to_E_MSRMNT___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_E_MSRMNT___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_E_MSRMNT___USER_ID as USER_ID", 
        "SQ_Shortcut_to_E_MSRMNT___MISC_TXT_1 as MISC_TXT_1", 
        "SQ_Shortcut_to_E_MSRMNT___MISC_TXT_2 as MISC_TXT_2", 
        "SQ_Shortcut_to_E_MSRMNT___MISC_NUM_1 as MISC_NUM_1", 
        "SQ_Shortcut_to_E_MSRMNT___MISC_NUM_2 as MISC_NUM_2", 
        "SQ_Shortcut_to_E_MSRMNT___VERSION_ID as VERSION_ID", 
        "SQ_Shortcut_to_E_MSRMNT___UNQ_SEED_ID as UNQ_SEED_ID", 
        "SQ_Shortcut_to_E_MSRMNT___SIM_WHSE as SIM_WHSE", 
        "SQ_Shortcut_to_E_MSRMNT___ORIG_MSRMNT_CODE as ORIG_MSRMNT_CODE", 
        "SQ_Shortcut_to_E_MSRMNT___ORIG_NAME as ORIG_NAME", 
        "SQ_Shortcut_to_E_MSRMNT___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_E_MSRMNT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_MSRMNT_PRE, type TARGET 
    # COLUMN COUNT: 21


    Shortcut_to_WM_E_MSRMNT_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NMBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(MSRMNT_ID AS BIGINT) as MSRMNT_ID", 
        "CAST(MSRMNT_CODE AS STRING) as MSRMNT_CODE", 
        "CAST(NAME AS STRING) as NAME", 
        "CAST(STATUS_FLAG AS STRING) as STATUS_FLAG", 
        "CAST(SYS_CREATED AS STRING) as SYS_CREATED", 
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
        "CAST(ORIG_MSRMNT_CODE AS STRING) as ORIG_MSRMNT_CODE", 
        "CAST(ORIG_NAME AS STRING) as ORIG_NAME", 
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_E_MSRMNT_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_MSRMNT_PRE is written to the target table - "
        + target_table_name
    )