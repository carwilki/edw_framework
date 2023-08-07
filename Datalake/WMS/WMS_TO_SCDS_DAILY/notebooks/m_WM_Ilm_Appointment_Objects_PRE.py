#Code converted on 2023-06-21 15:27:57
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



def m_WM_Ilm_Appointment_Objects_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Ilm_Appointment_Objects_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ILM_APPOINTMENT_OBJECTS_PRE"

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
    # Processing node SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS, type SOURCE 
    # COLUMN COUNT: 8

    SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS = jdbcOracleConnection(  f"""SELECT
    ILM_APPOINTMENT_OBJECTS.ID,
    ILM_APPOINTMENT_OBJECTS.APPT_OBJ_TYPE,
    ILM_APPOINTMENT_OBJECTS.APPT_OBJ_ID,
    ILM_APPOINTMENT_OBJECTS.COMPANY_ID,
    ILM_APPOINTMENT_OBJECTS.APPOINTMENT_ID,
    ILM_APPOINTMENT_OBJECTS.STOP_SEQ,
    ILM_APPOINTMENT_OBJECTS.CREATED_DTTM,
    ILM_APPOINTMENT_OBJECTS.LAST_UPDATED_DTTM
    FROM {source_schema}.ILM_APPOINTMENT_OBJECTS
    WHERE  (trunc(CREATED_DTTM)>= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (trunc(LAST_UPDATED_DTTM)>= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) AND 
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 10

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS_temp = SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS.toDF(*["SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS___" + col for col in SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS.columns])

    EXPTRANS = SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS_temp.selectExpr( \
        "SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS___sys_row_id as sys_row_id", \
        f"{dcnbr}  as DC_NBR_EXP", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS___ID as ID", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS___APPT_OBJ_TYPE as APPT_OBJ_TYPE", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS___APPT_OBJ_ID as APPT_OBJ_ID", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS___COMPANY_ID as COMPANY_ID", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS___APPOINTMENT_ID as APPOINTMENT_ID", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS___STOP_SEQ as STOP_SEQ", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_OBJECTS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE, type TARGET 
    # COLUMN COUNT: 10


    Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(ID AS INT) as ID",
        "CAST(APPT_OBJ_TYPE AS SMALLINT) as APPT_OBJ_TYPE",
        "CAST(APPT_OBJ_ID AS INT) as APPT_OBJ_ID",
        "CAST(COMPANY_ID AS INT) as COMPANY_ID",
        "CAST(APPOINTMENT_ID AS INT) as APPOINTMENT_ID",
        "CAST(STOP_SEQ AS SMALLINT) as STOP_SEQ",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )

    overwriteDeltaPartition(Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE is written to the target table - "
        + target_table_name
    )