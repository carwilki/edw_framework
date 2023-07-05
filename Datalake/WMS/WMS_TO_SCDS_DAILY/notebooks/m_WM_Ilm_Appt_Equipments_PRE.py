#Code converted on 2023-06-21 18:23:02
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



def m_WM_Ilm_Appt_Equipments_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Ilm_Appt_Equipments_PRE function")
    
    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)


    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ILM_APPT_EQUIPMENTS_PRE"

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
    # Processing node SQ_Shortcut_to_ILM_APPT_EQUIPMENTS, type SOURCE 
    # COLUMN COUNT: 7

    SQ_Shortcut_to_ILM_APPT_EQUIPMENTS = jdbcOracleConnection(  f"""SELECT
    ILM_APPT_EQUIPMENTS.APPOINTMENT_ID,
    ILM_APPT_EQUIPMENTS.COMPANY_ID,
    ILM_APPT_EQUIPMENTS.EQUIPMENT_INSTANCE_ID,
    ILM_APPT_EQUIPMENTS.EQUIPMENT_NUMBER,
    ILM_APPT_EQUIPMENTS.EQUIPMENT_LICENSE_NUMBER,
    ILM_APPT_EQUIPMENTS.EQUIPMENT_LICENSE_STATE,
    ILM_APPT_EQUIPMENTS.EQUIPMENT_TYPE
    FROM {source_schema}.ILM_APPT_EQUIPMENTS""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 9

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ILM_APPT_EQUIPMENTS_temp = SQ_Shortcut_to_ILM_APPT_EQUIPMENTS.toDF(*["SQ_Shortcut_to_ILM_APPT_EQUIPMENTS___" + col for col in SQ_Shortcut_to_ILM_APPT_EQUIPMENTS.columns])

    EXPTRANS = SQ_Shortcut_to_ILM_APPT_EQUIPMENTS_temp.selectExpr( \
        "SQ_Shortcut_to_ILM_APPT_EQUIPMENTS___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_ILM_APPT_EQUIPMENTS___APPOINTMENT_ID as APPOINTMENT_ID", \
        "SQ_Shortcut_to_ILM_APPT_EQUIPMENTS___COMPANY_ID as COMPANY_ID", \
        "SQ_Shortcut_to_ILM_APPT_EQUIPMENTS___EQUIPMENT_INSTANCE_ID as EQUIPMENT_INSTANCE_ID", \
        "SQ_Shortcut_to_ILM_APPT_EQUIPMENTS___EQUIPMENT_NUMBER as EQUIPMENT_NUMBER", \
        "SQ_Shortcut_to_ILM_APPT_EQUIPMENTS___EQUIPMENT_LICENSE_NUMBER as EQUIPMENT_LICENSE_NUMBER", \
        "SQ_Shortcut_to_ILM_APPT_EQUIPMENTS___EQUIPMENT_LICENSE_STATE as EQUIPMENT_LICENSE_STATE", \
        "SQ_Shortcut_to_ILM_APPT_EQUIPMENTS___EQUIPMENT_TYPE as EQUIPMENT_TYPE", \
        "CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE, type TARGET 
    # COLUMN COUNT: 9


    Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
        "CAST(APPOINTMENT_ID AS BIGINT) as APPOINTMENT_ID", \
        "CAST(COMPANY_ID AS BIGINT) as COMPANY_ID", \
        "CAST(EQUIPMENT_INSTANCE_ID AS BIGINT) as EQUIPMENT_INSTANCE_ID", \
        "CAST(EQUIPMENT_NUMBER AS STRING) as EQUIPMENT_NUMBER", \
        "CAST(EQUIPMENT_LICENSE_NUMBER AS STRING) as EQUIPMENT_LICENSE_NUMBER", \
        "CAST(EQUIPMENT_LICENSE_STATE AS STRING) as EQUIPMENT_LICENSE_STATE", \
        "CAST(EQUIPMENT_TYPE AS BIGINT) as EQUIPMENT_TYPE", \
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )

    overwriteDeltaPartition(Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE,"DC_NBR",dcnbr,target_table_name)
    
    logger.info(
        "Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE is written to the target table - "
        + target_table_name
    )