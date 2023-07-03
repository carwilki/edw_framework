#Code converted on 2023-06-24 13:39:03
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



def m_WM_E_Crit_Val_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_E_Crit_Val_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_E_CRIT_VAL_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "E_CRIT_VAL"


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
    # Processing node SQ_Shortcut_to_E_CRIT_VAL, type SOURCE 
    # COLUMN COUNT: 11

    SQ_Shortcut_to_E_CRIT_VAL = jdbcOracleConnection(
        f"""SELECT
                E_CRIT_VAL.CRIT_VAL_ID,
                E_CRIT_VAL.CRIT_ID,
                E_CRIT_VAL.CRIT_VAL,
                E_CRIT_VAL.CREATE_DATE_TIME,
                E_CRIT_VAL.MOD_DATE_TIME,
                E_CRIT_VAL.USER_ID,
                E_CRIT_VAL.MISC_TXT_1,
                E_CRIT_VAL.MISC_TXT_2,
                E_CRIT_VAL.MISC_NUM_1,
                E_CRIT_VAL.MISC_NUM_2,
                E_CRIT_VAL.VERSION_ID
            FROM E_CRIT_VAL
            WHERE (TRUNC( CREATE_DATE_TIME)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14) OR (TRUNC( MOD_DATE_TIME)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 13

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_CRIT_VAL_temp = SQ_Shortcut_to_E_CRIT_VAL.toDF(*["SQ_Shortcut_to_E_CRIT_VAL___" + col for col in SQ_Shortcut_to_E_CRIT_VAL.columns])

    EXPTRANS = SQ_Shortcut_to_E_CRIT_VAL_temp.selectExpr( 
        "SQ_Shortcut_to_E_CRIT_VAL___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_E_CRIT_VAL___CRIT_VAL_ID as CRIT_VAL_ID", 
        "SQ_Shortcut_to_E_CRIT_VAL___CRIT_ID as CRIT_ID", 
        "SQ_Shortcut_to_E_CRIT_VAL___CRIT_VAL as CRIT_VAL", 
        "SQ_Shortcut_to_E_CRIT_VAL___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_E_CRIT_VAL___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_E_CRIT_VAL___USER_ID as USER_ID", 
        "SQ_Shortcut_to_E_CRIT_VAL___MISC_TXT_1 as MISC_TXT_1", 
        "SQ_Shortcut_to_E_CRIT_VAL___MISC_TXT_2 as MISC_TXT_2", 
        "SQ_Shortcut_to_E_CRIT_VAL___MISC_NUM_1 as MISC_NUM_1", 
        "SQ_Shortcut_to_E_CRIT_VAL___MISC_NUM_2 as MISC_NUM_2", 
        "SQ_Shortcut_to_E_CRIT_VAL___VERSION_ID as VERSION_ID", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_CRIT_VAL_PRE, type TARGET 
    # COLUMN COUNT: 13


    Shortcut_to_WM_E_CRIT_VAL_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(CRIT_VAL_ID AS BIGINT) as CRIT_VAL_ID", 
        "CAST(CRIT_ID AS BIGINT) as CRIT_ID", 
        "CAST(CRIT_VAL AS STRING) as CRIT_VAL", 
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
    overwriteDeltaPartition(Shortcut_to_WM_E_CRIT_VAL_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_CRIT_VAL_PRE is written to the target table - "
        + target_table_name
    )