#Code converted on 2023-06-24 13:42:27
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



def m_WM_C_Leader_Audit_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_C_Leader_Audit_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_C_LEADER_AUDIT_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "C_LEADER_AUDIT"


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
    # Processing node SQ_Shortcut_to_C_LEADER_AUDIT, type SOURCE 
    # COLUMN COUNT: 10

    SQ_Shortcut_to_C_LEADER_AUDIT = jdbcOracleConnection(
        f"""SELECT
                C_LEADER_AUDIT.C_LEADER_AUDIT_ID,
                C_LEADER_AUDIT.LEADER_USER_ID,
                C_LEADER_AUDIT.PICKER_USER_ID,
                C_LEADER_AUDIT.STATUS,
                C_LEADER_AUDIT.ITEM_NAME,
                C_LEADER_AUDIT.LPN,
                C_LEADER_AUDIT.EXPECTED_QTY,
                C_LEADER_AUDIT.ACTUAL_QTY,
                C_LEADER_AUDIT.CREATE_DATE_TIME,
                C_LEADER_AUDIT.MOD_DATE_TIME
            FROM C_LEADER_AUDIT
            WHERE (TRUNC( CREATE_DATE_TIME)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC( MOD_DATE_TIME)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 12

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_C_LEADER_AUDIT_temp = SQ_Shortcut_to_C_LEADER_AUDIT.toDF(*["SQ_Shortcut_to_C_LEADER_AUDIT___" + col for col in SQ_Shortcut_to_C_LEADER_AUDIT.columns])

    EXPTRANS = SQ_Shortcut_to_C_LEADER_AUDIT_temp.selectExpr( 
        "SQ_Shortcut_to_C_LEADER_AUDIT___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_C_LEADER_AUDIT___C_LEADER_AUDIT_ID as C_LEADER_AUDIT_ID", 
        "SQ_Shortcut_to_C_LEADER_AUDIT___LEADER_USER_ID as LEADER_USER_ID", 
        "SQ_Shortcut_to_C_LEADER_AUDIT___PICKER_USER_ID as PICKER_USER_ID", 
        "SQ_Shortcut_to_C_LEADER_AUDIT___STATUS as STATUS", 
        "SQ_Shortcut_to_C_LEADER_AUDIT___ITEM_NAME as ITEM_NAME", 
        "SQ_Shortcut_to_C_LEADER_AUDIT___LPN as LPN", 
        "SQ_Shortcut_to_C_LEADER_AUDIT___EXPECTED_QTY as EXPECTED_QTY", 
        "SQ_Shortcut_to_C_LEADER_AUDIT___ACTUAL_QTY as ACTUAL_QTY", 
        "SQ_Shortcut_to_C_LEADER_AUDIT___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_C_LEADER_AUDIT___MOD_DATE_TIME as MOD_DATE_TIME", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_C_LEADER_AUDIT_PRE, type TARGET 
    # COLUMN COUNT: 12


    Shortcut_to_WM_C_LEADER_AUDIT_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
        "CAST(C_LEADER_AUDIT_ID AS BIGINT) as C_LEADER_AUDIT_ID", 
        "CAST(LEADER_USER_ID AS STRING) as LEADER_USER_ID", 
        "CAST(PICKER_USER_ID AS STRING) as PICKER_USER_ID", 
        "CAST(STATUS AS BIGINT) as STATUS", 
        "CAST(ITEM_NAME AS STRING) as ITEM_NAME", 
        "CAST(LPN AS STRING) as LPN", 
        "CAST(EXPECTED_QTY AS BIGINT) as EXPECTED_QTY", 
        "CAST(ACTUAL_QTY AS BIGINT) as ACTUAL_QTY", 
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_C_LEADER_AUDIT_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_C_LEADER_AUDIT_PRE is written to the target table - "
        + target_table_name
    )