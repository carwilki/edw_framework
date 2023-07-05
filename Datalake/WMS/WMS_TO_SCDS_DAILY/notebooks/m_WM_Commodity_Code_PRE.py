#Code converted on 2023-06-24 13:42:44
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



def m_WM_Commodity_Code_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Commodity_Code_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_COMMODITY_CODE_PRE"

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
    # Processing node SQ_Shortcut_to_COMMODITY_CODE, type SOURCE 
    # COLUMN COUNT: 13

    SQ_Shortcut_to_COMMODITY_CODE = jdbcOracleConnection(
        f"""SELECT
                COMMODITY_CODE.COMMODITY_CODE_ID,
                COMMODITY_CODE.COMM_CODE_SECTION,
                COMMODITY_CODE.COMM_CODE_CHAPTER,
                COMMODITY_CODE.TC_COMPANY_ID,
                COMMODITY_CODE.DESCRIPTION_SHORT,
                COMMODITY_CODE.DESCRIPTION_LONG,
                COMMODITY_CODE.MARK_FOR_DELETION,
                COMMODITY_CODE.CREATED_SOURCE_TYPE,
                COMMODITY_CODE.CREATED_SOURCE,
                COMMODITY_CODE.CREATED_DTTM,
                COMMODITY_CODE.LAST_UPDATED_SOURCE_TYPE,
                COMMODITY_CODE.LAST_UPDATED_SOURCE,
                COMMODITY_CODE.LAST_UPDATED_DTTM
            FROM {source_schema}.COMMODITY_CODE
            WHERE (TRUNC( CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC( LAST_UPDATED_DTTM) >=  TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 15

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_COMMODITY_CODE_temp = SQ_Shortcut_to_COMMODITY_CODE.toDF(*["SQ_Shortcut_to_COMMODITY_CODE___" + col for col in SQ_Shortcut_to_COMMODITY_CODE.columns])

    EXPTRANS = SQ_Shortcut_to_COMMODITY_CODE_temp.selectExpr( 
        "SQ_Shortcut_to_COMMODITY_CODE___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR", 
        "SQ_Shortcut_to_COMMODITY_CODE___COMMODITY_CODE_ID as COMMODITY_CODE_ID", 
        "SQ_Shortcut_to_COMMODITY_CODE___COMM_CODE_SECTION as COMM_CODE_SECTION", 
        "SQ_Shortcut_to_COMMODITY_CODE___COMM_CODE_CHAPTER as COMM_CODE_CHAPTER", 
        "SQ_Shortcut_to_COMMODITY_CODE___TC_COMPANY_ID as TC_COMPANY_ID", 
        "SQ_Shortcut_to_COMMODITY_CODE___DESCRIPTION_SHORT as DESCRIPTION_SHORT", 
        "SQ_Shortcut_to_COMMODITY_CODE___DESCRIPTION_LONG as DESCRIPTION_LONG", 
        "SQ_Shortcut_to_COMMODITY_CODE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
        "SQ_Shortcut_to_COMMODITY_CODE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_COMMODITY_CODE___CREATED_SOURCE as CREATED_SOURCE", 
        "SQ_Shortcut_to_COMMODITY_CODE___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_COMMODITY_CODE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_COMMODITY_CODE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
        "SQ_Shortcut_to_COMMODITY_CODE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_COMMODITY_CODE_PRE, type TARGET 
    # COLUMN COUNT: 15


    Shortcut_to_WM_COMMODITY_CODE_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR AS BIGINT) as DC_NBR", 
        "CAST(COMMODITY_CODE_ID AS BIGINT) as COMMODITY_CODE_ID", 
        "CAST(COMM_CODE_SECTION AS BIGINT) as COMM_CODE_SECTION", 
        "CAST(COMM_CODE_CHAPTER AS BIGINT) as COMM_CODE_CHAPTER", 
        "CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", 
        "CAST(DESCRIPTION_SHORT AS STRING) as DESCRIPTION_SHORT", 
        "CAST(DESCRIPTION_LONG AS STRING) as DESCRIPTION_LONG", 
        "CAST(MARK_FOR_DELETION AS BIGINT) as MARK_FOR_DELETION", 
        "CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", 
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", 
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
        "CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", 
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", 
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
        "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    overwriteDeltaPartition(Shortcut_to_WM_COMMODITY_CODE_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_COMMODITY_CODE_PRE is written to the target table - "
        + target_table_name
    )