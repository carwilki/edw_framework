#Code converted on 2023-06-24 13:41:50
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



def m_WM_Dock_Door_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Dock_Door_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_DOCK_DOOR_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "DOCK_DOOR"


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
    # Processing node SQ_Shortcut_to_DOCK_DOOR, type SOURCE 
    # COLUMN COUNT: 26

    SQ_Shortcut_to_DOCK_DOOR = jdbcOracleConnection(
        f"""SELECT
                DOCK_DOOR.FACILITY_ID,
                DOCK_DOOR.DOCK_ID,
                DOCK_DOOR.DOCK_DOOR_ID,
                DOCK_DOOR.TC_COMPANY_ID,
                DOCK_DOOR.DOCK_DOOR_NAME,
                DOCK_DOOR.DOCK_DOOR_STATUS,
                DOCK_DOOR.DESCRIPTION,
                DOCK_DOOR.MARK_FOR_DELETION,
                DOCK_DOOR.CREATED_SOURCE_TYPE,
                DOCK_DOOR.CREATED_SOURCE,
                DOCK_DOOR.CREATED_DTTM,
                DOCK_DOOR.LAST_UPDATED_SOURCE_TYPE,
                DOCK_DOOR.LAST_UPDATED_SOURCE,
                DOCK_DOOR.LAST_UPDATED_DTTM,
                DOCK_DOOR.OLD_DOCK_DOOR_STATUS,
                DOCK_DOOR.ACTIVITY_TYPE,
                DOCK_DOOR.APPOINTMENT_TYPE,
                DOCK_DOOR.BARCODE,
                DOCK_DOOR.TIME_FROM_INDUCTION,
                DOCK_DOOR.PALLETIZATION_SPUR,
                DOCK_DOOR.SORT_ZONE,
                DOCK_DOOR.ILM_APPOINTMENT_NUMBER,
                DOCK_DOOR.FLOWTHRU_ALLOC_SORT_PRTY,
                DOCK_DOOR.LOCN_HDR_ID,
                DOCK_DOOR.DOCK_DOOR_LOCN_ID,
                DOCK_DOOR.OUTBD_STAGING_LOCN_ID
            FROM DOCK_DOOR
            WHERE (TRUNC( DOCK_DOOR.CREATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC( DOCK_DOOR.LAST_UPDATED_DTTM) >= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 28

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_DOCK_DOOR_temp = SQ_Shortcut_to_DOCK_DOOR.toDF(*["SQ_Shortcut_to_DOCK_DOOR___" + col for col in SQ_Shortcut_to_DOCK_DOOR.columns])

    EXPTRANS = SQ_Shortcut_to_DOCK_DOOR_temp.selectExpr( 
        "SQ_Shortcut_to_DOCK_DOOR___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_DOCK_DOOR___DOCK_DOOR_ID as DOCK_DOOR_ID", 
        "SQ_Shortcut_to_DOCK_DOOR___FACILITY_ID as FACILITY_ID", 
        "SQ_Shortcut_to_DOCK_DOOR___DOCK_ID as DOCK_ID", 
        "SQ_Shortcut_to_DOCK_DOOR___TC_COMPANY_ID as TC_COMPANY_ID", 
        "SQ_Shortcut_to_DOCK_DOOR___DOCK_DOOR_NAME as DOCK_DOOR_NAME", 
        "SQ_Shortcut_to_DOCK_DOOR___DOCK_DOOR_STATUS as DOCK_DOOR_STATUS", 
        "SQ_Shortcut_to_DOCK_DOOR___DESCRIPTION as DESCRIPTION", 
        "SQ_Shortcut_to_DOCK_DOOR___MARK_FOR_DELETION as MARK_FOR_DELETION", 
        "SQ_Shortcut_to_DOCK_DOOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_DOCK_DOOR___CREATED_SOURCE as CREATED_SOURCE", 
        "SQ_Shortcut_to_DOCK_DOOR___CREATED_DTTM as CREATED_DTTM", 
        "SQ_Shortcut_to_DOCK_DOOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
        "SQ_Shortcut_to_DOCK_DOOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
        "SQ_Shortcut_to_DOCK_DOOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
        "SQ_Shortcut_to_DOCK_DOOR___OLD_DOCK_DOOR_STATUS as OLD_DOCK_DOOR_STATUS", 
        "SQ_Shortcut_to_DOCK_DOOR___ACTIVITY_TYPE as ACTIVITY_TYPE", 
        "SQ_Shortcut_to_DOCK_DOOR___APPOINTMENT_TYPE as APPOINTMENT_TYPE", 
        "SQ_Shortcut_to_DOCK_DOOR___BARCODE as BARCODE", 
        "SQ_Shortcut_to_DOCK_DOOR___TIME_FROM_INDUCTION as TIME_FROM_INDUCTION", 
        "SQ_Shortcut_to_DOCK_DOOR___PALLETIZATION_SPUR as PALLETIZATION_SPUR", 
        "SQ_Shortcut_to_DOCK_DOOR___SORT_ZONE as SORT_ZONE", 
        "SQ_Shortcut_to_DOCK_DOOR___ILM_APPOINTMENT_NUMBER as ILM_APPOINTMENT_NUMBER", 
        "SQ_Shortcut_to_DOCK_DOOR___FLOWTHRU_ALLOC_SORT_PRTY as FLOWTHRU_ALLOC_SORT_PRTY", 
        "SQ_Shortcut_to_DOCK_DOOR___LOCN_HDR_ID as LOCN_HDR_ID", 
        "SQ_Shortcut_to_DOCK_DOOR___DOCK_DOOR_LOCN_ID as DOCK_DOOR_LOCN_ID", 
        "SQ_Shortcut_to_DOCK_DOOR___OUTBD_STAGING_LOCN_ID as OUTBD_STAGING_LOCN_ID", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_DOCK_DOOR_PRE, type TARGET 
    # COLUMN COUNT: 28


    Shortcut_to_WM_DOCK_DOOR_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(DOCK_DOOR_ID AS BIGINT) as DOCK_DOOR_ID", 
        "CAST(FACILITY_ID AS BIGINT) as FACILITY_ID", 
        "CAST(DOCK_ID AS STRING) as DOCK_ID", 
        "CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", 
        "CAST(DOCK_DOOR_NAME AS STRING) as DOCK_DOOR_NAME", 
        "CAST(DOCK_DOOR_STATUS AS BIGINT) as DOCK_DOOR_STATUS", 
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION", 
        "CAST(MARK_FOR_DELETION AS BIGINT) as MARK_FOR_DELETION", 
        "CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", 
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", 
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
        "CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", 
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", 
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
        "CAST(OLD_DOCK_DOOR_STATUS AS BIGINT) as OLD_DOCK_DOOR_STATUS", 
        "CAST(ACTIVITY_TYPE AS STRING) as ACTIVITY_TYPE", 
        "CAST(APPOINTMENT_TYPE AS STRING) as APPOINTMENT_TYPE", 
        "CAST(BARCODE AS STRING) as BARCODE", 
        "CAST(TIME_FROM_INDUCTION AS BIGINT) as TIME_FROM_INDUCTION", 
        "CAST(PALLETIZATION_SPUR AS STRING) as PALLETIZATION_SPUR", 
        "CAST(SORT_ZONE AS STRING) as SORT_ZONE", 
        "CAST(ILM_APPOINTMENT_NUMBER AS STRING) as ILM_APPOINTMENT_NUMBER", 
        "CAST(FLOWTHRU_ALLOC_SORT_PRTY AS STRING) as FLOWTHRU_ALLOC_SORT_PRTY", 
        "CAST(LOCN_HDR_ID AS BIGINT) as LOCN_HDR_ID", 
        "CAST(DOCK_DOOR_LOCN_ID AS STRING) as DOCK_DOOR_LOCN_ID", 
        "CAST(OUTBD_STAGING_LOCN_ID AS STRING) as OUTBD_STAGING_LOCN_ID", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )

    overwriteDeltaPartition(Shortcut_to_WM_DOCK_DOOR_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_DOCK_DOOR_PRE is written to the target table - "
        + target_table_name
    )