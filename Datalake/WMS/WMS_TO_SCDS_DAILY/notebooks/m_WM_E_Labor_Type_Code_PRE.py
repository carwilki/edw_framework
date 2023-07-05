#Code converted on 2023-06-24 13:36:42
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



def m_WM_E_Labor_Type_Code_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_E_Labor_Type_Code_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_E_LABOR_TYPE_CODE_PRE"

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
    # Processing node SQ_Shortcut_to_E_LABOR_TYPE_CODE, type SOURCE 
    # COLUMN COUNT: 14

    SQ_Shortcut_to_E_LABOR_TYPE_CODE = jdbcOracleConnection(
        f"""SELECT
                E_LABOR_TYPE_CODE.LABOR_TYPE_ID,
                E_LABOR_TYPE_CODE.LABOR_TYPE_CODE,
                E_LABOR_TYPE_CODE.DESCRIPTION,
                E_LABOR_TYPE_CODE.USER_ID,
                E_LABOR_TYPE_CODE.CREATE_DATE_TIME,
                E_LABOR_TYPE_CODE.MOD_DATE_TIME,
                E_LABOR_TYPE_CODE.MISC_TXT_1,
                E_LABOR_TYPE_CODE.MISC_TXT_2,
                E_LABOR_TYPE_CODE.MISC_NUM_1,
                E_LABOR_TYPE_CODE.MISC_NUM_2,
                E_LABOR_TYPE_CODE.VERSION_ID,
                E_LABOR_TYPE_CODE.SPVSR_AUTH_REQUIRED,
                E_LABOR_TYPE_CODE.CREATED_DTTM,
                E_LABOR_TYPE_CODE.LAST_UPDATED_DTTM
            FROM {source_schema}.E_LABOR_TYPE_CODE
            WHERE (TRUNC( CREATE_DATE_TIME)>= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (TRUNC( MOD_DATE_TIME)>= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (TRUNC( CREATED_DTTM)>= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (TRUNC( LAST_UPDATED_DTTM)>= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 16

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_E_LABOR_TYPE_CODE_temp = SQ_Shortcut_to_E_LABOR_TYPE_CODE.toDF(*["SQ_Shortcut_to_E_LABOR_TYPE_CODE___" + col for col in SQ_Shortcut_to_E_LABOR_TYPE_CODE.columns])

    EXPTRANS = SQ_Shortcut_to_E_LABOR_TYPE_CODE_temp.selectExpr( 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___LABOR_TYPE_ID as LABOR_TYPE_ID", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___LABOR_TYPE_CODE as LABOR_TYPE_CODE", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___DESCRIPTION as DESCRIPTION", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___USER_ID as USER_ID", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___MISC_TXT_1 as MISC_TXT_1", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___MISC_TXT_2 as MISC_TXT_2", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___MISC_NUM_1 as MISC_NUM_1", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___MISC_NUM_2 as MISC_NUM_2", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___VERSION_ID as VERSION_ID", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___SPVSR_AUTH_REQUIRED as SPVSR_AUTH_REQUIRED", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_E_LABOR_TYPE_CODE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE, type TARGET 
    # COLUMN COUNT: 16


    Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(LABOR_TYPE_ID AS BIGINT) as LABOR_TYPE_ID", 
        "CAST(LABOR_TYPE_CODE AS STRING) as LABOR_TYPE_CODE", 
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION", 
        "CAST(USER_ID AS STRING) as USER_ID", 
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
        "CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1", 
        "CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2", 
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1", 
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2", 
        "CAST(VERSION_ID AS BIGINT) as VERSION_ID", 
        "CAST(SPVSR_AUTH_REQUIRED AS STRING) as SPVSR_AUTH_REQUIRED", 
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE is written to the target table - "
        + target_table_name
    )