#Code converted on 2023-06-22 11:02:27
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



def m_WM_Labor_Activity_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Labor_Activity_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_LABOR_ACTIVITY_PRE"

    schemaName = raw
    source_schema = "WMSMIS"


    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]


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
    # Processing node SQ_Shortcut_to_LABOR_ACTIVITY, type SOURCE 
    # COLUMN COUNT: 18

    SQ_Shortcut_to_LABOR_ACTIVITY = jdbcOracleConnection(  f"""SELECT
    LABOR_ACTIVITY.LABOR_ACTIVITY_ID,
    LABOR_ACTIVITY.NAME,
    LABOR_ACTIVITY.DESCRIPTION,
    LABOR_ACTIVITY.AIL_ACT,
    LABOR_ACTIVITY.ACT_TYPE,
    LABOR_ACTIVITY.CREATED_SOURCE_TYPE,
    LABOR_ACTIVITY.CREATED_SOURCE,
    LABOR_ACTIVITY.CREATED_DTTM,
    LABOR_ACTIVITY.LAST_UPDATED_SOURCE_TYPE,
    LABOR_ACTIVITY.LAST_UPDATED_SOURCE,
    LABOR_ACTIVITY.LAST_UPDATED_DTTM,
    LABOR_ACTIVITY.HIBERNATE_VERSION,
    LABOR_ACTIVITY.PROMPT_LOCN,
    LABOR_ACTIVITY.DISPL_EPP,
    LABOR_ACTIVITY.INCLD_TRVL,
    LABOR_ACTIVITY.CRIT_RULE_TYPE,
    LABOR_ACTIVITY.PERMISSION_ID,
    LABOR_ACTIVITY.COMPANY_ID
    FROM {source_schema}.LABOR_ACTIVITY
    WHERE  (trunc(CREATED_DTTM)>= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (trunc(LAST_UPDATED_DTTM)>= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) AND 
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 20

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LABOR_ACTIVITY_temp = SQ_Shortcut_to_LABOR_ACTIVITY.toDF(*["SQ_Shortcut_to_LABOR_ACTIVITY___" + col for col in SQ_Shortcut_to_LABOR_ACTIVITY.columns])

    EXPTRANS = SQ_Shortcut_to_LABOR_ACTIVITY_temp.selectExpr( \
        "SQ_Shortcut_to_LABOR_ACTIVITY___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___LABOR_ACTIVITY_ID as LABOR_ACTIVITY_ID", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___NAME as NAME", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___DESCRIPTION as DESCRIPTION", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___AIL_ACT as AIL_ACT", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___ACT_TYPE as ACT_TYPE", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___HIBERNATE_VERSION as HIBERNATE_VERSION", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___PROMPT_LOCN as PROMPT_LOCN", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___DISPL_EPP as DISPL_EPP", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___INCLD_TRVL as INCLD_TRVL", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___CRIT_RULE_TYPE as CRIT_RULE_TYPE", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___PERMISSION_ID as PERMISSION_ID", \
        "SQ_Shortcut_to_LABOR_ACTIVITY___COMPANY_ID as COMPANY_ID", \
        "CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_LABOR_ACTIVITY_PRE, type TARGET 
    # COLUMN COUNT: 20


    Shortcut_to_WM_LABOR_ACTIVITY_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
        "CAST(LABOR_ACTIVITY_ID AS BIGINT) as LABOR_ACTIVITY_ID", \
        "CAST(NAME AS STRING) as NAME", \
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION", \
        "CAST(AIL_ACT AS STRING) as AIL_ACT", \
        "CAST(ACT_TYPE AS STRING) as ACT_TYPE", \
        "CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", \
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
        "CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", \
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION", \
        "CAST(PROMPT_LOCN AS STRING) as PROMPT_LOCN", \
        "CAST(DISPL_EPP AS STRING) as DISPL_EPP", \
        "CAST(INCLD_TRVL AS STRING) as INCLD_TRVL", \
        "CAST(CRIT_RULE_TYPE AS STRING) as CRIT_RULE_TYPE", \
        "CAST(PERMISSION_ID AS BIGINT) as PERMISSION_ID", \
        "CAST(COMPANY_ID AS BIGINT) as COMPANY_ID", \
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_LABOR_ACTIVITY_PRE,"DC_NBR",dcnbr,target_table_name)
    
    logger.info(
        "Shortcut_to_WM_LABOR_ACTIVITY_PRE is written to the target table - "
        + target_table_name
    )