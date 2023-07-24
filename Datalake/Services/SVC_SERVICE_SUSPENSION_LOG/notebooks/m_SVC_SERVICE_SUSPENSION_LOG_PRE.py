#Code converted on 2023-07-11 16:28:36
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
#from pyspark.dbutils import DBUtils
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
# COMMAND ----------

def m_SVC_SERVICE_SUSPENSION_LOG_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_SRC_Services_Reservation_Pre function")
    
    spark = SparkSession.getActiveSession()
    #dbutils = DBUtils(spark)

    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    legacy = getEnvPrefix(env) + 'legacy'
    
    tableName = "SVC_SERVICE_SUSPENSION_LOG_PRE"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName

    # Set global variables
    starttime = datetime.now() #start timestamp of the script

    # Read in relation source variables
    (username, password, connection_string) = getConfig(DC_NBR, env)

    # COMMAND ----------
    # Processing node SQ_Shortcut_to_ServiceSuspensionLog, type SOURCE 
    # COLUMN COUNT: 9

    SQ_Shortcut_to_ServiceSuspensionLog = jdbcOracleConnection(f"""SELECT
                ServiceSuspensionLog.DayDt,
                ServiceSuspensionLog.StoreNumber,
                ServiceSuspensionLog.ServiceArea,
                ServiceSuspensionLog.SuspensionReason,
                ServiceSuspensionLog.SubmittedBy,
                ServiceSuspensionLog.SubmittedByPosition,
                ServiceSuspensionLog.Comments,
                ServiceSuspensionLog.ReversalDt,
                ServiceSuspensionLog.CreateDt
            FROM ServiceSuspensionLog
            WHERE ServiceSuspensionLog.CreateDt > SYSDATE - INTERVAL 1 DAY OR ServiceSuspensionLog.ModifyDt > SYSDATE - INTERVAL 1 DAY""",  
       username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXP_FIELDS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
    # COLUMN COUNT: 12

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ServiceSuspensionLog_temp = SQ_Shortcut_to_ServiceSuspensionLog.toDF(*["SQ_Shortcut_to_ServiceSuspensionLog___" + col for col in SQ_Shortcut_to_ServiceSuspensionLog.columns])

    EXP_FIELDS = SQ_Shortcut_to_ServiceSuspensionLog_temp.selectExpr( \
        "SQ_Shortcut_to_ServiceSuspensionLog___DayDt as DAY_DT", \
        "SQ_Shortcut_to_ServiceSuspensionLog___StoreNumber as IN_STORE_NUMBER", \
        "SQ_Shortcut_to_ServiceSuspensionLog___ServiceArea as SERVICE_AREA", \
        "SQ_Shortcut_to_ServiceSuspensionLog___SuspensionReason as SUSPENSION_REASON", \
        "SQ_Shortcut_to_ServiceSuspensionLog___SubmittedBy as SUBMITTED_BY", \
        "SQ_Shortcut_to_ServiceSuspensionLog___SubmittedByPosition as SUBMITTED_BY_POSITION", \
        "SQ_Shortcut_to_ServiceSuspensionLog___Comments as COMMENTS", \
        "SQ_Shortcut_to_ServiceSuspensionLog___ReversalDt as REVERSAL_DT", \
        "SQ_Shortcut_to_ServiceSuspensionLog___CreateDt as CREATE_TSTMP") \
        .withColumn('UPDATE_TSTMP', lit(None)) \
        .selectExpr( \
        "SQ_Shortcut_to_ServiceSuspensionLog___sys_row_id as sys_row_id", \
        "SQ_Shortcut_to_ServiceSuspensionLog___DAY_DT as DAY_DT", \
        "SQ_Shortcut_to_ServiceSuspensionLog___IN_STORE_NUMBER as IN_STORE_NUMBER", \
        "cast(SQ_Shortcut_to_ServiceSuspensionLog___IN_STORE_NUMBER as int) as STORE_NUMBER", \
        "SQ_Shortcut_to_ServiceSuspensionLog___SERVICE_AREA as SERVICE_AREA", \
        "SQ_Shortcut_to_ServiceSuspensionLog___SUSPENSION_REASON as SUSPENSION_REASON", \
        "SQ_Shortcut_to_ServiceSuspensionLog___SUBMITTED_BY as SUBMITTED_BY", \
        "SQ_Shortcut_to_ServiceSuspensionLog___SUBMITTED_BY_POSITION as SUBMITTED_BY_POSITION", \
        "SQ_Shortcut_to_ServiceSuspensionLog___COMMENTS as COMMENTS", \
        "SQ_Shortcut_to_ServiceSuspensionLog___REVERSAL_DT as REVERSAL_DT", \
        "SQ_Shortcut_to_ServiceSuspensionLog___CREATE_TSTMP as CREATE_TSTMP", \
        "SQ_Shortcut_to_ServiceSuspensionLog___UPDATE_TSTMP as UPDATE_TSTMP", \
        "CURRENT_TIMESTAMP as LOAD_TSTMP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE, type TARGET 
    # COLUMN COUNT: 11


    Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE = EXP_FIELDS.selectExpr( \
        "CAST(DAY_DT AS DATE) as DAY_DT", \
        "CAST(STORE_NUMBER AS BIGINT) as STORE_NUMBER", \
        "CAST(SERVICE_AREA AS STRING) as SERVICE_AREA", \
        "CAST(SUSPENSION_REASON AS STRING) as SUSPENSION_REASON", \
        "CAST(SUBMITTED_BY AS STRING) as SUBMITTED_BY", \
        "CAST(SUBMITTED_BY_POSITION AS STRING) as SUBMITTED_BY_POSITION", \
        "CAST(COMMENTS AS STRING) as COMMENTS", \
        "CAST(REVERSAL_DT AS TIMESTAMP) as REVERSAL_TSTMP", \
        "CAST(CREATE_TSTMP AS TIMESTAMP) as CREATE_TSTMP", \
        "CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
        "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE is written to the target table - " + target_table_name)
    
    # Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE.write.saveAsTable(f'{raw}.SVC_SERVICE_SUSPENSION_LOG_PRE')