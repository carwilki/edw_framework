#Code converted on 2023-06-21 15:27:53
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



def m_WM_Ilm_Appointment_Type_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Ilm_Appointment_Type_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ILM_APPOINTMENT_TYPE_PRE"

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
    # Processing node SQ_Shortcut_to_ILM_APPOINTMENT_TYPE, type SOURCE 
    # COLUMN COUNT: 4

    SQ_Shortcut_to_ILM_APPOINTMENT_TYPE = jdbcOracleConnection(  f"""SELECT
    ILM_APPOINTMENT_TYPE.APPT_TYPE,
    ILM_APPOINTMENT_TYPE.DESCRIPTION,
    ILM_APPOINTMENT_TYPE.CREATED_DTTM,
    ILM_APPOINTMENT_TYPE.LAST_UPDATED_DTTM
    FROM {source_schema}.ILM_APPOINTMENT_TYPE
    WHERE (trunc(CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 6

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ILM_APPOINTMENT_TYPE_temp = SQ_Shortcut_to_ILM_APPOINTMENT_TYPE.toDF(*["SQ_Shortcut_to_ILM_APPOINTMENT_TYPE___" + col for col in SQ_Shortcut_to_ILM_APPOINTMENT_TYPE.columns])

    EXPTRANS = SQ_Shortcut_to_ILM_APPOINTMENT_TYPE_temp.selectExpr( \
        "SQ_Shortcut_to_ILM_APPOINTMENT_TYPE___sys_row_id as sys_row_id", \
        f"{dcnbr}  as DC_NBR_exp", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_TYPE___APPT_TYPE as APPT_TYPE", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_TYPE___DESCRIPTION as DESCRIPTION", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_TYPE___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_ILM_APPOINTMENT_TYPE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "CURRENT_TIMESTAMP () as LOADTSTMP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE, type TARGET 
    # COLUMN COUNT: 6


    Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_exp AS BIGINT) as DC_NBR", \
        "CAST(APPT_TYPE AS BIGINT) as APPT_TYPE", \
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION", \
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
        "CAST(LOADTSTMP AS TIMESTAMP) as LOAD_TSTMP" \
    )

    overwriteDeltaPartition(Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE is written to the target table - "
        + target_table_name
    )