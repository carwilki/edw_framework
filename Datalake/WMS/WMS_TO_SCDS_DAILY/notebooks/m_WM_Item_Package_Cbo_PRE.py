#Code converted on 2023-06-22 10:47:21
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from logging import getLogger, INFO



def m_WM_Item_Package_Cbo_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Item_Package_Cbo_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ITEM_PACKAGE_CBO_PRE"

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
    # Processing node SQ_Shortcut_to_ITEM_PACKAGE_CBO, type SOURCE 
    # COLUMN COUNT: 23

    SQ_Shortcut_to_ITEM_PACKAGE_CBO = jdbcOracleConnection(  f"""SELECT
    ITEM_PACKAGE_CBO.ITEM_PACKAGE_ID,
    ITEM_PACKAGE_CBO.ITEM_ID,
    ITEM_PACKAGE_CBO.PACKAGE_UOM_ID,
    ITEM_PACKAGE_CBO.QUANTITY,
    ITEM_PACKAGE_CBO.WEIGHT,
    ITEM_PACKAGE_CBO.WEIGHT_UOM_ID,
    ITEM_PACKAGE_CBO.GTIN,
    ITEM_PACKAGE_CBO.AUDIT_CREATED_SOURCE,
    ITEM_PACKAGE_CBO.AUDIT_CREATED_SOURCE_TYPE,
    ITEM_PACKAGE_CBO.AUDIT_CREATED_DTTM,
    ITEM_PACKAGE_CBO.AUDIT_LAST_UPDATED_SOURCE,
    ITEM_PACKAGE_CBO.AUDIT_LAST_UPDATED_SOURCE_TYPE,
    ITEM_PACKAGE_CBO.AUDIT_LAST_UPDATED_DTTM,
    ITEM_PACKAGE_CBO.MARK_FOR_DELETION,
    ITEM_PACKAGE_CBO.DIMENSION_UOM_ID,
    ITEM_PACKAGE_CBO.VOLUME,
    ITEM_PACKAGE_CBO.VOLUME_UOM_ID,
    ITEM_PACKAGE_CBO.LENGTH,
    ITEM_PACKAGE_CBO.HEIGHT,
    ITEM_PACKAGE_CBO.WIDTH,
    ITEM_PACKAGE_CBO.HIBERNATE_VERSION,
    ITEM_PACKAGE_CBO.IS_STD,
    ITEM_PACKAGE_CBO.BUSINESS_PARTNER_ID
    FROM {source_schema}.ITEM_PACKAGE_CBO
    WHERE (trunc(AUDIT_CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(AUDIT_LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)  AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 25

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ITEM_PACKAGE_CBO_temp = SQ_Shortcut_to_ITEM_PACKAGE_CBO.toDF(*["SQ_Shortcut_to_ITEM_PACKAGE_CBO___" + col for col in SQ_Shortcut_to_ITEM_PACKAGE_CBO.columns])

    EXPTRANS = SQ_Shortcut_to_ITEM_PACKAGE_CBO_temp.selectExpr( \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___ITEM_PACKAGE_ID as ITEM_PACKAGE_ID", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___ITEM_ID as ITEM_ID", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___PACKAGE_UOM_ID as PACKAGE_UOM_ID", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___QUANTITY as QUANTITY", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___WEIGHT as WEIGHT", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___WEIGHT_UOM_ID as WEIGHT_UOM_ID", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___GTIN as GTIN", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___MARK_FOR_DELETION as MARK_FOR_DELETION", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___DIMENSION_UOM_ID as DIMENSION_UOM_ID", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___VOLUME as VOLUME", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___VOLUME_UOM_ID as VOLUME_UOM_ID", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___LENGTH as LENGTH", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___HEIGHT as HEIGHT", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___WIDTH as WIDTH", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___HIBERNATE_VERSION as HIBERNATE_VERSION", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___IS_STD as IS_STD", \
        "SQ_Shortcut_to_ITEM_PACKAGE_CBO___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ITEM_PACKAGE_CBO_PRE, type TARGET 
    # COLUMN COUNT: 25


    Shortcut_to_WM_ITEM_PACKAGE_CBO_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(ITEM_PACKAGE_ID AS INT) as ITEM_PACKAGE_ID",
        "CAST(ITEM_ID AS INT) as ITEM_ID",
        "CAST(PACKAGE_UOM_ID AS INT) as PACKAGE_UOM_ID",
        "CAST(QUANTITY AS DECIMAL(16,4)) as QUANTITY",
        "CAST(WEIGHT AS DECIMAL(16,4)) as WEIGHT",
        "CAST(WEIGHT_UOM_ID AS INT) as WEIGHT_UOM_ID",
        "CAST(GTIN AS STRING) as GTIN",
        "CAST(AUDIT_CREATED_SOURCE AS STRING) as AUDIT_CREATED_SOURCE",
        "CAST(AUDIT_CREATED_SOURCE_TYPE AS TINYINT) as AUDIT_CREATED_SOURCE_TYPE",
        "CAST(AUDIT_CREATED_DTTM AS TIMESTAMP) as AUDIT_CREATED_DTTM",
        "CAST(AUDIT_LAST_UPDATED_SOURCE AS STRING) as AUDIT_LAST_UPDATED_SOURCE",
        "CAST(AUDIT_LAST_UPDATED_SOURCE_TYPE AS TINYINT) as AUDIT_LAST_UPDATED_SOURCE_TYPE",
        "CAST(AUDIT_LAST_UPDATED_DTTM AS TIMESTAMP) as AUDIT_LAST_UPDATED_DTTM",
        "CAST(MARK_FOR_DELETION AS TINYINT) as MARK_FOR_DELETION",
        "CAST(DIMENSION_UOM_ID AS INT) as DIMENSION_UOM_ID",
        "CAST(VOLUME AS DECIMAL(16,4)) as VOLUME",
        "CAST(VOLUME_UOM_ID AS DECIMAL(16,4)) as VOLUME_UOM_ID",
        "CAST(LENGTH AS DECIMAL(16,4)) as LENGTH",
        "CAST(HEIGHT AS DECIMAL(16,4)) as HEIGHT",
        "CAST(WIDTH AS DECIMAL(16,4)) as WIDTH",
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION",
        "CAST(IS_STD AS STRING) as IS_STD",
        "CAST(BUSINESS_PARTNER_ID AS STRING) as BUSINESS_PARTNER_ID",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )


    overwriteDeltaPartition(Shortcut_to_WM_ITEM_PACKAGE_CBO_PRE,"DC_NBR",dcnbr,target_table_name)
    
    logger.info(
        "Shortcut_to_WM_ITEM_PACKAGE_CBO_PRE is written to the target table - "
        + target_table_name
    )    