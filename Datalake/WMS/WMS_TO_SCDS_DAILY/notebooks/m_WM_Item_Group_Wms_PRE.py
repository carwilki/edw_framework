#Code converted on 2023-06-22 10:47:25
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from logging import getLogger, INFO



def m_WM_Item_Group_Wms_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Item_Group_Wms_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ITEM_GROUP_WMS_PRE"

    schemaName = raw
    source_schema = "WMSMIS"


    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]


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
    # Processing node SQ_Shortcut_to_ITEM_GROUP_WMS, type SOURCE 
    # COLUMN COUNT: 12

    SQ_Shortcut_to_ITEM_GROUP_WMS = jdbcOracleConnection(  f"""SELECT
    ITEM_GROUP_WMS.ITEM_GROUP_ID,
    ITEM_GROUP_WMS.ITEM_ID,
    ITEM_GROUP_WMS.GROUP_TYPE,
    ITEM_GROUP_WMS.GROUP_CODE,
    ITEM_GROUP_WMS.GROUP_ATTRIBUTE,
    ITEM_GROUP_WMS.AUDIT_CREATED_SOURCE_TYPE,
    ITEM_GROUP_WMS.AUDIT_CREATED_DTTM,
    ITEM_GROUP_WMS.AUDIT_LAST_UPDATED_SOURCE_TYPE,
    ITEM_GROUP_WMS.AUDIT_LAST_UPDATED_DTTM,
    ITEM_GROUP_WMS.MARK_FOR_DELETION,
    ITEM_GROUP_WMS.AUDIT_CREATED_SOURCE,
    ITEM_GROUP_WMS.AUDIT_LAST_UPDATED_SOURCE
    FROM {source_schema}.ITEM_GROUP_WMS
    WHERE (trunc(AUDIT_CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(AUDIT_LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)  AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 14

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ITEM_GROUP_WMS_temp = SQ_Shortcut_to_ITEM_GROUP_WMS.toDF(*["SQ_Shortcut_to_ITEM_GROUP_WMS___" + col for col in SQ_Shortcut_to_ITEM_GROUP_WMS.columns])

    EXPTRANS = SQ_Shortcut_to_ITEM_GROUP_WMS_temp.selectExpr( \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___ITEM_GROUP_ID as ITEM_GROUP_ID", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___ITEM_ID as ITEM_ID", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___GROUP_TYPE as GROUP_TYPE", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___GROUP_CODE as GROUP_CODE", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___GROUP_ATTRIBUTE as GROUP_ATTRIBUTE", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___MARK_FOR_DELETION as MARK_FOR_DELETION", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
        "SQ_Shortcut_to_ITEM_GROUP_WMS___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
        "CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ITEM_GROUP_WMS_PRE, type TARGET 
    # COLUMN COUNT: 14


    Shortcut_to_WM_ITEM_GROUP_WMS_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
        "CAST(ITEM_GROUP_ID AS BIGINT) as ITEM_GROUP_ID", \
        "CAST(ITEM_ID AS BIGINT) as ITEM_ID", \
        "CAST(GROUP_TYPE AS STRING) as GROUP_TYPE", \
        "CAST(GROUP_CODE AS STRING) as GROUP_CODE", \
        "CAST(GROUP_ATTRIBUTE AS STRING) as GROUP_ATTRIBUTE", \
        "CAST(AUDIT_CREATED_SOURCE_TYPE AS BIGINT) as AUDIT_CREATED_SOURCE_TYPE", \
        "CAST(AUDIT_CREATED_DTTM AS TIMESTAMP) as AUDIT_CREATED_DTTM", \
        "CAST(AUDIT_LAST_UPDATED_SOURCE_TYPE AS BIGINT) as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
        "CAST(AUDIT_LAST_UPDATED_DTTM AS TIMESTAMP) as AUDIT_LAST_UPDATED_DTTM", \
        "CAST(MARK_FOR_DELETION AS BIGINT) as MARK_FOR_DELETION", \
        "CAST(AUDIT_CREATED_SOURCE AS STRING) as AUDIT_CREATED_SOURCE", \
        "CAST(AUDIT_LAST_UPDATED_SOURCE AS STRING) as AUDIT_LAST_UPDATED_SOURCE", \
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_ITEM_GROUP_WMS_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_ITEM_GROUP_WMS_PRE is written to the target table - "
        + target_table_name
    )