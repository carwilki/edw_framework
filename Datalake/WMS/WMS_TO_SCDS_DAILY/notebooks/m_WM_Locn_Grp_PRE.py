#Code converted on 2023-06-26 10:17:04
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



def m_WM_Locn_Grp_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Locn_Grp_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_LOCN_GRP_PRE"

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
    # Processing node SQ_Shortcut_to_LOCN_GRP, type SOURCE 
    # COLUMN COUNT: 11

    SQ_Shortcut_to_LOCN_GRP = jdbcOracleConnection(  f"""SELECT
    LOCN_GRP.GRP_TYPE,
    LOCN_GRP.LOCN_ID,
    LOCN_GRP.GRP_ATTR,
    LOCN_GRP.CREATE_DATE_TIME,
    LOCN_GRP.MOD_DATE_TIME,
    LOCN_GRP.USER_ID,
    LOCN_GRP.LOCN_GRP_ID,
    LOCN_GRP.LOCN_HDR_ID,
    LOCN_GRP.WM_VERSION_ID,
    LOCN_GRP.CREATED_DTTM,
    LOCN_GRP.LAST_UPDATED_DTTM
    FROM {source_schema}.LOCN_GRP
    WHERE (trunc(CREATE_DATE_TIME) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (trunc(MOD_DATE_TIME) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (trunc(CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 13

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LOCN_GRP_temp = SQ_Shortcut_to_LOCN_GRP.toDF(*["SQ_Shortcut_to_LOCN_GRP___" + col for col in SQ_Shortcut_to_LOCN_GRP.columns])

    EXPTRANS = SQ_Shortcut_to_LOCN_GRP_temp.selectExpr( \
        "SQ_Shortcut_to_LOCN_GRP___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_LOCN_GRP___LOCN_GRP_ID as LOCN_GRP_ID", \
        "SQ_Shortcut_to_LOCN_GRP___GRP_TYPE as GRP_TYPE", \
        "SQ_Shortcut_to_LOCN_GRP___LOCN_ID as LOCN_ID", \
        "SQ_Shortcut_to_LOCN_GRP___GRP_ATTR as GRP_ATTR", \
        "SQ_Shortcut_to_LOCN_GRP___CREATE_DATE_TIME as CREATE_DATE_TIME", \
        "SQ_Shortcut_to_LOCN_GRP___MOD_DATE_TIME as MOD_DATE_TIME", \
        "SQ_Shortcut_to_LOCN_GRP___USER_ID as USER_ID", \
        "SQ_Shortcut_to_LOCN_GRP___LOCN_HDR_ID as LOCN_HDR_ID", \
        "SQ_Shortcut_to_LOCN_GRP___WM_VERSION_ID as WM_VERSION_ID", \
        "SQ_Shortcut_to_LOCN_GRP___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_LOCN_GRP___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_LOCN_GRP_PRE, type TARGET 
    # COLUMN COUNT: 13


    Shortcut_to_WM_LOCN_GRP_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(LOCN_GRP_ID AS INT) as LOCN_GRP_ID",
        "CAST(GRP_TYPE AS SMALLINT) as GRP_TYPE",
        "CAST(LOCN_ID AS STRING) as LOCN_ID",
        "CAST(GRP_ATTR AS STRING) as GRP_ATTR",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(LOCN_HDR_ID AS INT) as LOCN_HDR_ID",
        "CAST(WM_VERSION_ID AS INT) as WM_VERSION_ID",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )


    overwriteDeltaPartition(Shortcut_to_WM_LOCN_GRP_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE is written to the target table - "
        + target_table_name
    )    