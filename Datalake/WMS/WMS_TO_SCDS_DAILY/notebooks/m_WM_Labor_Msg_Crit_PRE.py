#Code converted on 2023-06-26 10:19:23
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



def m_WM_Labor_Msg_Crit_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Labor_Msg_Crit_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_LABOR_MSG_CRIT_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "LABOR_MSG_CRIT"


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
    # Processing node SQ_Shortcut_to_LABOR_MSG_CRIT, type SOURCE 
    # COLUMN COUNT: 19

    SQ_Shortcut_to_LABOR_MSG_CRIT = jdbcOracleConnection(  f"""SELECT
    LABOR_MSG_CRIT.LABOR_MSG_CRIT_ID,
    LABOR_MSG_CRIT.LABOR_MSG_ID,
    LABOR_MSG_CRIT.TRAN_NBR,
    LABOR_MSG_CRIT.CRIT_SEQ_NBR,
    LABOR_MSG_CRIT.MSG_STAT_CODE,
    LABOR_MSG_CRIT.CRIT_TYPE,
    LABOR_MSG_CRIT.CRIT_VAL,
    LABOR_MSG_CRIT.CREATED_SOURCE_TYPE,
    LABOR_MSG_CRIT.CREATED_SOURCE,
    LABOR_MSG_CRIT.CREATED_DTTM,
    LABOR_MSG_CRIT.LAST_UPDATED_SOURCE_TYPE,
    LABOR_MSG_CRIT.LAST_UPDATED_SOURCE,
    LABOR_MSG_CRIT.LAST_UPDATED_DTTM,
    LABOR_MSG_CRIT.WHSE,
    LABOR_MSG_CRIT.MISC_TXT_1,
    LABOR_MSG_CRIT.MISC_TXT_2,
    LABOR_MSG_CRIT.MISC_NUM_1,
    LABOR_MSG_CRIT.MISC_NUM_2,
    LABOR_MSG_CRIT.HIBERNATE_VERSION
    FROM LABOR_MSG_CRIT
    WHERE (trunc(LABOR_MSG_CRIT.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (trunc(LABOR_MSG_CRIT.LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1)  AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 21

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LABOR_MSG_CRIT_temp = SQ_Shortcut_to_LABOR_MSG_CRIT.toDF(*["SQ_Shortcut_to_LABOR_MSG_CRIT___" + col for col in SQ_Shortcut_to_LABOR_MSG_CRIT.columns])

    EXPTRANS = SQ_Shortcut_to_LABOR_MSG_CRIT_temp.selectExpr( \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___LABOR_MSG_CRIT_ID as LABOR_MSG_CRIT_ID", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___LABOR_MSG_ID as LABOR_MSG_ID", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___TRAN_NBR as TRAN_NBR", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___CRIT_SEQ_NBR as CRIT_SEQ_NBR", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___MSG_STAT_CODE as MSG_STAT_CODE", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___CRIT_TYPE as CRIT_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___CRIT_VAL as CRIT_VAL", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___WHSE as WHSE", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___MISC_TXT_1 as MISC_TXT_1", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___MISC_TXT_2 as MISC_TXT_2", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___MISC_NUM_1 as MISC_NUM_1", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___MISC_NUM_2 as MISC_NUM_2", \
        "SQ_Shortcut_to_LABOR_MSG_CRIT___HIBERNATE_VERSION as HIBERNATE_VERSION", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_LABOR_MSG_CRIT_PRE, type TARGET 
    # COLUMN COUNT: 21


    Shortcut_to_WM_LABOR_MSG_CRIT_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
        "CAST(LABOR_MSG_CRIT_ID AS BIGINT) as LABOR_MSG_CRIT_ID", \
        "CAST(LABOR_MSG_ID AS BIGINT) as LABOR_MSG_ID", \
        "CAST(TRAN_NBR AS BIGINT) as TRAN_NBR", \
        "CAST(CRIT_SEQ_NBR AS BIGINT) as CRIT_SEQ_NBR", \
        "CAST(MSG_STAT_CODE AS STRING) as MSG_STAT_CODE", \
        "CAST(CRIT_TYPE AS STRING) as CRIT_TYPE", \
        "CAST(CRIT_VAL AS STRING) as CRIT_VAL", \
        "CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", \
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
        "CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", \
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
        "CAST(WHSE AS STRING) as WHSE", \
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1", \
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2", \
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1", \
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2", \
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION", \
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )

    overwriteDeltaPartition(Shortcut_to_WM_LABOR_MSG_CRIT_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_LABOR_MSG_CRIT_PRE is written to the target table - "
        + target_table_name
    )