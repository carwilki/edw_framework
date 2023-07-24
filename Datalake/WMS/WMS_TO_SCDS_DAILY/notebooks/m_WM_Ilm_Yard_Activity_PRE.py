#Code converted on 2023-06-21 18:22:57
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



def m_WM_Ilm_Yard_Activity_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Ilm_Yard_Activity_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ILM_YARD_ACTIVITY_PRE"

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
    
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)
    dcnbr = dcnbr.strip()[2:]
    # COMMAND ----------
    # Processing node SQ_Shortcut_to_ILM_YARD_ACTIVITY, type SOURCE 
    # COLUMN COUNT: 16

    SQ_Shortcut_to_ILM_YARD_ACTIVITY = jdbcOracleConnection(  f"""SELECT
    ILM_YARD_ACTIVITY.ACTIVITY_ID,
    ILM_YARD_ACTIVITY.COMPANY_ID,
    ILM_YARD_ACTIVITY.APPOINTMENT_ID,
    ILM_YARD_ACTIVITY.EQUIPMENT_ID1,
    ILM_YARD_ACTIVITY.ACTIVITY_TYPE,
    ILM_YARD_ACTIVITY.ACTIVITY_SOURCE,
    ILM_YARD_ACTIVITY.ACTIVITY_DTTM,
    ILM_YARD_ACTIVITY.DRIVER_ID,
    ILM_YARD_ACTIVITY.EQUIPMENT_ID2,
    ILM_YARD_ACTIVITY.TASK_ID,
    ILM_YARD_ACTIVITY.LOCATION_ID,
    ILM_YARD_ACTIVITY.NO_OF_PALLETS,
    ILM_YARD_ACTIVITY.EQUIP_INS_STATUS,
    ILM_YARD_ACTIVITY.FACILITY_ID,
    ILM_YARD_ACTIVITY.VISIT_DETAIL_ID,
    ILM_YARD_ACTIVITY.LOCN_ID
    FROM {source_schema}.ILM_YARD_ACTIVITY""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 18

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ILM_YARD_ACTIVITY_temp = SQ_Shortcut_to_ILM_YARD_ACTIVITY.toDF(*["SQ_Shortcut_to_ILM_YARD_ACTIVITY___" + col for col in SQ_Shortcut_to_ILM_YARD_ACTIVITY.columns])

    EXPTRANS = SQ_Shortcut_to_ILM_YARD_ACTIVITY_temp.selectExpr( \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_exp", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___ACTIVITY_ID as ACTIVITY_ID", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___COMPANY_ID as COMPANY_ID", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___APPOINTMENT_ID as APPOINTMENT_ID", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___EQUIPMENT_ID1 as EQUIPMENT_ID1", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___ACTIVITY_TYPE as ACTIVITY_TYPE", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___ACTIVITY_SOURCE as ACTIVITY_SOURCE", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___ACTIVITY_DTTM as ACTIVITY_DTTM", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___DRIVER_ID as DRIVER_ID", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___EQUIPMENT_ID2 as EQUIPMENT_ID2", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___TASK_ID as TASK_ID", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___LOCATION_ID as LOCATION_ID", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___NO_OF_PALLETS as NO_OF_PALLETS", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___EQUIP_INS_STATUS as EQUIP_INS_STATUS", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___FACILITY_ID as FACILITY_ID", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___VISIT_DETAIL_ID as VISIT_DETAIL_ID", \
        "SQ_Shortcut_to_ILM_YARD_ACTIVITY___LOCN_ID as LOCN_ID", \
        "CURRENT_TIMESTAMP() as LOADTSTMP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE, type TARGET 
    # COLUMN COUNT: 18


    Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_exp AS SMALLINT) as DC_NBR",
        "CAST(ACTIVITY_ID AS INT) as ACTIVITY_ID",
        "CAST(COMPANY_ID AS INT) as COMPANY_ID",
        "CAST(APPOINTMENT_ID AS INT) as APPOINTMENT_ID",
        "CAST(EQUIPMENT_ID1 AS INT) as EQUIPMENT_ID1",
        "CAST(ACTIVITY_TYPE AS SMALLINT) as ACTIVITY_TYPE",
        "CAST(ACTIVITY_SOURCE AS STRING) as ACTIVITY_SOURCE",
        "CAST(ACTIVITY_DTTM AS TIMESTAMP) as ACTIVITY_DTTM",
        "CAST(DRIVER_ID AS INT) as DRIVER_ID",
        "CAST(EQUIPMENT_ID2 AS INT) as EQUIPMENT_ID2",
        "CAST(TASK_ID AS INT) as TASK_ID",
        "CAST(LOCATION_ID AS INT) as LOCATION_ID",
        "CAST(NO_OF_PALLETS AS INT) as NO_OF_PALLETS",
        "CAST(EQUIP_INS_STATUS AS SMALLINT) as EQUIP_INS_STATUS",
        "CAST(FACILITY_ID AS INT) as FACILITY_ID",
        "CAST(VISIT_DETAIL_ID AS INT) as VISIT_DETAIL_ID",
        "CAST(LOCN_ID AS STRING) as LOCN_ID",
        "CAST(LOADTSTMP AS TIMESTAMP) as LOAD_TSTMP"
    )

    overwriteDeltaPartition(Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE,"DC_NBR",dcnbr,target_table_name)

    logger.info(
        "Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE is written to the target table - "
        + target_table_name
    )    