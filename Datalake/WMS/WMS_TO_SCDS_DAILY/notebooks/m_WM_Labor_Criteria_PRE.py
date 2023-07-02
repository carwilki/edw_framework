#Code converted on 2023-06-22 11:02:21
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



def m_WM_Labor_Criteria_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Labor_Criteria_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_LABOR_CRITERIA_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "LABOR_CRITERIA"


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
    # Processing node SQ_Shortcut_to_LABOR_CRITERIA, type SOURCE 
    # COLUMN COUNT: 14

    SQ_Shortcut_to_LABOR_CRITERIA = jdbcOracleConnection(  f"""SELECT
    LABOR_CRITERIA.CRIT_ID,
    LABOR_CRITERIA.CRIT_CODE,
    LABOR_CRITERIA.DESCRIPTION,
    LABOR_CRITERIA.RULE_FILTER,
    LABOR_CRITERIA.DATA_TYPE,
    LABOR_CRITERIA.DATA_SIZE,
    LABOR_CRITERIA.HIBERNATE_VERSION,
    LABOR_CRITERIA.CREATED_SOURCE_TYPE,
    LABOR_CRITERIA.CREATED_SOURCE,
    LABOR_CRITERIA.CREATED_DTTM,
    LABOR_CRITERIA.LAST_UPDATED_SOURCE_TYPE,
    LABOR_CRITERIA.LAST_UPDATED_SOURCE,
    LABOR_CRITERIA.LAST_UPDATED_DTTM,
    LABOR_CRITERIA.COMPANY_ID
    FROM LABOR_CRITERIA
    WHERE  (trunc(CREATED_DTTM)>= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14) OR (trunc(LAST_UPDATED_DTTM)>= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 14) AND 
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 16

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LABOR_CRITERIA_temp = SQ_Shortcut_to_LABOR_CRITERIA.toDF(*["SQ_Shortcut_to_LABOR_CRITERIA___" + col for col in SQ_Shortcut_to_LABOR_CRITERIA.columns])

    EXPTRANS = SQ_Shortcut_to_LABOR_CRITERIA_temp.selectExpr( \
        "SQ_Shortcut_to_LABOR_CRITERIA___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_LABOR_CRITERIA___CRIT_ID as CRIT_ID", \
        "SQ_Shortcut_to_LABOR_CRITERIA___CRIT_CODE as CRIT_CODE", \
        "SQ_Shortcut_to_LABOR_CRITERIA___DESCRIPTION as DESCRIPTION", \
        "SQ_Shortcut_to_LABOR_CRITERIA___RULE_FILTER as RULE_FILTER", \
        "SQ_Shortcut_to_LABOR_CRITERIA___DATA_TYPE as DATA_TYPE", \
        "SQ_Shortcut_to_LABOR_CRITERIA___DATA_SIZE as DATA_SIZE", \
        "SQ_Shortcut_to_LABOR_CRITERIA___HIBERNATE_VERSION as HIBERNATE_VERSION", \
        "SQ_Shortcut_to_LABOR_CRITERIA___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LABOR_CRITERIA___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_LABOR_CRITERIA___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_LABOR_CRITERIA___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LABOR_CRITERIA___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_LABOR_CRITERIA___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_LABOR_CRITERIA___COMPANY_ID as COMPANY_ID", \
        "CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_LABOR_CRITERIA_PRE, type TARGET 
    # COLUMN COUNT: 16


    Shortcut_to_WM_LABOR_CRITERIA_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
        "CAST(CRIT_ID AS BIGINT) as CRIT_ID", \
        "CAST(CRIT_CODE AS STRING) as CRIT_CODE", \
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION", \
        "CAST(RULE_FILTER AS STRING) as RULE_FILTER", \
        "CAST(DATA_TYPE AS STRING) as DATA_TYPE", \
        "CAST(DATA_SIZE AS BIGINT) as DATA_SIZE", \
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION", \
        "CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", \
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
        "CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", \
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
        "CAST(COMPANY_ID AS BIGINT) as COMPANY_ID", \
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_LABOR_CRITERIA_PRE,"DC_NBR",dcnbr,target_table_name)
    
    logger.info(
        "Shortcut_to_WM_LABOR_CRITERIA_PRE is written to the target table - "
        + target_table_name
    )