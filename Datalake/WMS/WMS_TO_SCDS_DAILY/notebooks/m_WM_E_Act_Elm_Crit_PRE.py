#Code converted on 2023-06-24 13:40:14
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



def m_WM_E_Act_Elm_Crit_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_E_Act_Elm_Crit_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_E_ACT_ELM_CRIT_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_ACT_ELM_CRIT"


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
    # Processing node SQ_Shortcut_to_E_ACT_ELM_CRIT, type SOURCE 
    # COLUMN COUNT: 13

    SQ_Shortcut_to_E_ACT_ELM_CRIT = jdbcOracleConnection(
        f"""SELECT
                E_ACT_ELM_CRIT.ACT_ID,
                E_ACT_ELM_CRIT.ELM_ID,
                E_ACT_ELM_CRIT.CRIT_VAL_ID,
                E_ACT_ELM_CRIT.TIME_ALLOW,
                E_ACT_ELM_CRIT.CREATE_DATE_TIME,
                E_ACT_ELM_CRIT.MOD_DATE_TIME,
                E_ACT_ELM_CRIT.USER_ID,
                E_ACT_ELM_CRIT.CRIT_ID,
                E_ACT_ELM_CRIT.MISC_TXT_1,
                E_ACT_ELM_CRIT.MISC_TXT_2,
                E_ACT_ELM_CRIT.MISC_NUM_1,
                E_ACT_ELM_CRIT.MISC_NUM_2,
                E_ACT_ELM_CRIT.VERSION_ID
            FROM E_ACT_ELM_CRIT
            WHERE (TRUNC( E_ACT_ELM_CRIT.CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14) OR (TRUNC( E_ACT_ELM_CRIT.MOD_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 15

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_ACT_ELM_CRIT_temp = SQ_Shortcut_to_E_ACT_ELM_CRIT.toDF(*["SQ_Shortcut_to_E_ACT_ELM_CRIT___" + col for col in SQ_Shortcut_to_E_ACT_ELM_CRIT.columns])

    EXPTRANS = SQ_Shortcut_to_E_ACT_ELM_CRIT_temp.selectExpr( 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___ACT_ID as ACT_ID", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___ELM_ID as ELM_ID", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___CRIT_ID as CRIT_ID", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___CRIT_VAL_ID as CRIT_VAL_ID", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___TIME_ALLOW as TIME_ALLOW", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___USER_ID as USER_ID", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___MISC_TXT_1 as MISC_TXT_1", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___MISC_TXT_2 as MISC_TXT_2", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___MISC_NUM_1 as MISC_NUM_1", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___MISC_NUM_2 as MISC_NUM_2", 
        "SQ_Shortcut_to_E_ACT_ELM_CRIT___VERSION_ID as VERSION_ID", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_ACT_ELM_CRIT_PRE, type TARGET 
    # COLUMN COUNT: 15


    Shortcut_to_WM_E_ACT_ELM_CRIT_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(ACT_ID AS BIGINT) as ACT_ID", 
        "CAST(ELM_ID AS BIGINT) as ELM_ID", 
        "CAST(CRIT_ID AS BIGINT) as CRIT_ID", 
        "CAST(CRIT_VAL_ID AS BIGINT) as CRIT_VAL_ID", 
        "CAST(TIME_ALLOW AS BIGINT) as TIME_ALLOW", 
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
        "CAST(USER_ID AS STRING) as USER_ID", 
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1", 
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2", 
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1", 
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2", 
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    overwriteDeltaPartition(Shortcut_to_WM_E_ACT_ELM_CRIT_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_ACT_ELM_CRIT_PRE is written to the target table - "
        + target_table_name
    )