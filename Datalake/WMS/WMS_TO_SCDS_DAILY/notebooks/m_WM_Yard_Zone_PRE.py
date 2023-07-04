# Code converted on 2023-06-22 21:04:41
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
from Datalake.utils.logger import *


def m_WM_Yard_Zone_PRE(dcnbr, env):
    
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Yard_Zone_PRE")

    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"

    tableName = "WM_YARD_ZONE_PRE"
    schemaName = raw
    schema = 'WMSMIS'

    target_table_name = schemaName + "." + tableName
    refine_table_name = tableName[:-4]
    # prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    prev_run_dt = genPrevRunDt(refine_table_name, refine, raw)
    print("The prev run date is " + prev_run_dt)

    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")

    dcnbr = dcnbr.strip()[2:]

    query = f"""SELECT
                    YARD_ZONE.YARD_ID,
                    YARD_ZONE.YARD_ZONE_ID,
                    YARD_ZONE.YARD_ZONE_NAME,
                    YARD_ZONE.MARK_FOR_DELETION,
                    YARD_ZONE.PUTAWAY_ELIGIBLE,
                    YARD_ZONE.LOCATION_ID,
                    YARD_ZONE.CREATED_DTTM,
                    YARD_ZONE.LAST_UPDATED_DTTM,
                    YARD_ZONE.CREATED_SOURCE,
                    YARD_ZONE.CREATED_SOURCE_TYPE,
                    YARD_ZONE.LAST_UPDATED_SOURCE,
                    YARD_ZONE.LAST_UPDATED_SOURCE_TYPE
                FROM {schema}.YARD_ZONE
                WHERE  (TRUNC( CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM-DD-YYYY HH24:MI:SS'))-14) OR (TRUNC( LAST_UPDATED_DTTM) >=  TRUNC( to_date('{Prev_Run_Dt}','MM-DD-YYYY HH24:MI:SS'))-14)"""

    # SQ_Shortcut_to_YARD_ZONE = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    SQ_Shortcut_to_YARD_ZONE = jdbcOracleConnection(
        query, username, password, connection_string
    ).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info(
        "SQL query for SQ_Shortcut_to_YARD_ZONE is executed and data is loaded using jdbc"
    )

    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 14

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_YARD_ZONE_temp = SQ_Shortcut_to_YARD_ZONE.toDF(
        *[
            "SQ_Shortcut_to_YARD_ZONE___" + col
            for col in SQ_Shortcut_to_YARD_ZONE.columns
        ]
    )

    EXPTRANS = SQ_Shortcut_to_YARD_ZONE_temp.selectExpr(
        "SQ_Shortcut_to_YARD_ZONE___sys_row_id as sys_row_id",
        f"{DC_NBR}as DC_NBR",
        "SQ_Shortcut_to_YARD_ZONE___YARD_ID as YARD_ID",
        "SQ_Shortcut_to_YARD_ZONE___YARD_ZONE_ID as YARD_ZONE_ID",
        "SQ_Shortcut_to_YARD_ZONE___YARD_ZONE_NAME as YARD_ZONE_NAME",
        "SQ_Shortcut_to_YARD_ZONE___MARK_FOR_DELETION as MARK_FOR_DELETION",
        "SQ_Shortcut_to_YARD_ZONE___PUTAWAY_ELIGIBLE as PUTAWAY_ELIGIBLE",
        "SQ_Shortcut_to_YARD_ZONE___LOCATION_ID as LOCATION_ID",
        "SQ_Shortcut_to_YARD_ZONE___CREATED_DTTM as CREATED_DTTM",
        "SQ_Shortcut_to_YARD_ZONE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
        "SQ_Shortcut_to_YARD_ZONE___CREATED_SOURCE as CREATED_SOURCE",
        "SQ_Shortcut_to_YARD_ZONE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE",
        "SQ_Shortcut_to_YARD_ZONE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE",
        "SQ_Shortcut_to_YARD_ZONE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP",
    )

    # Processing node Shortcut_to_WM_YARD_ZONE_PRE, type TARGET
    # COLUMN COUNT: 14

    Shortcut_to_WM_YARD_ZONE_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR AS BIGINT) as DC_NBR",
        "CAST(YARD_ID AS BIGINT) as YARD_ID",
        "CAST(YARD_ZONE_ID AS BIGINT) as YARD_ZONE_ID",
        "CAST(YARD_ZONE_NAME AS STRING) as YARD_ZONE_NAME",
        "CAST(MARK_FOR_DELETION AS BIGINT) as MARK_FOR_DELETION",
        "CAST(PUTAWAY_ELIGIBLE AS BIGINT) as PUTAWAY_ELIGIBLE",
        "CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE",
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
        "CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP",
    )

    # gu.overwriteDeltaPartition(Shortcut_to_WM_YARD_ZONE_PRE, "DC_NBR", dcnbr, target_table_name)
    overwriteDeltaPartition(
        Shortcut_to_WM_YARD_ZONE_PRE, "DC_NBR", dcnbr, target_table_name
    )

    logger.info(
        "Shortcut_to_WM_YARD_ZONE_PRE is written to the target table - "
        + target_table_name
    )
