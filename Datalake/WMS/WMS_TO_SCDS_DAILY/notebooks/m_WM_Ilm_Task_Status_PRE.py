#Code converted on 2023-06-21 18:23:00
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from utils.genericUtilities import *
from utils.configs import *
from utils.mergeUtils import *
from logging import getLogger, INFO



def m_WM_Ilm_Task_Status_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Ilm_Task_Status_PRE function")
    
    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ILM_TASK_STATUS_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "ILM_TASK_STATUS"


    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now() #start timestamp of the script


    # COMMAND ----------
    # Variable_declaration_comment
    dcnbr = dcnbr.strip()[2:]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)
    # COMMAND ----------
    # Processing node SQ_Shortcut_to_ILM_TASK_STATUS, type SOURCE 
    # COLUMN COUNT: 4

    SQ_Shortcut_to_ILM_TASK_STATUS = jdbcOracleConnection(  f"""SELECT
    ILM_TASK_STATUS.TASK_STATUS,
    ILM_TASK_STATUS.DESCRIPTION,
    ILM_TASK_STATUS.CREATED_DTTM,
    ILM_TASK_STATUS.LAST_UPDATED_DTTM
    FROM ILM_TASK_STATUS
    WHERE (date_trunc('DD', CREATED_DTTM)>= date_trunc('DD', to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (date_trunc('DD', LAST_UPDATED_DTTM)>= date_trunc('DD', to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) AND 
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 6

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ILM_TASK_STATUS_temp = SQ_Shortcut_to_ILM_TASK_STATUS.toDF(*["SQ_Shortcut_to_ILM_TASK_STATUS___" + col for col in SQ_Shortcut_to_ILM_TASK_STATUS.columns])

    EXPTRANS = SQ_Shortcut_to_ILM_TASK_STATUS_temp.selectExpr( \
        "SQ_Shortcut_to_ILM_TASK_STATUS___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_ILM_TASK_STATUS___TASK_STATUS as TASK_STATUS", \
        "SQ_Shortcut_to_ILM_TASK_STATUS___DESCRIPTION as DESCRIPTION", \
        "SQ_Shortcut_to_ILM_TASK_STATUS___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_ILM_TASK_STATUS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ILM_TASK_STATUS_PRE, type TARGET 
    # COLUMN COUNT: 6


    Shortcut_to_WM_ILM_TASK_STATUS_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
        "CAST(TASK_STATUS AS BIGINT) as TASK_STATUS", \
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION", \
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )

    overwriteDeltaPartition(Shortcut_to_WM_ILM_TASK_STATUS_PRE,"DC_NBR",dcnbr,target_table_name)
    
    logger.info(
        "Shortcut_to_WM_ILM_TASK_STATUS_PRE is written to the target table - "
        + target_table_name
    )