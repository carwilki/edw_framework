#Code converted on 2023-06-24 13:44:06
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
from logging import getLogger, INFO



def m_WM_Asn_Detail_Status_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Asn_Detail_Status_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ASN_DETAIL_STATUS_PRE"

    schemaName = raw
    source_schema = "WMSMIS"


    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]


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
    # Processing node SQ_Shortcut_to_ASN_DETAIL_STATUS, type SOURCE 
    # COLUMN COUNT: 2

    SQ_Shortcut_to_ASN_DETAIL_STATUS = jdbcOracleConnection(
        f"""SELECT
                ASN_DETAIL_STATUS.ASN_DETAIL_STATUS,
                ASN_DETAIL_STATUS.DESCRIPTION
            FROM {source_schema}.ASN_DETAIL_STATUS""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 4

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ASN_DETAIL_STATUS_temp = SQ_Shortcut_to_ASN_DETAIL_STATUS.toDF(*["SQ_Shortcut_to_ASN_DETAIL_STATUS___" + col for col in SQ_Shortcut_to_ASN_DETAIL_STATUS.columns])

    EXPTRANS = SQ_Shortcut_to_ASN_DETAIL_STATUS_temp.selectExpr( 
        "SQ_Shortcut_to_ASN_DETAIL_STATUS___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_ASN_DETAIL_STATUS___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", 
        "SQ_Shortcut_to_ASN_DETAIL_STATUS___DESCRIPTION as DESCRIPTION", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ASN_DETAIL_STATUS_PRE, type TARGET 
    # COLUMN COUNT: 4


    Shortcut_to_WM_ASN_DETAIL_STATUS_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(ASN_DETAIL_STATUS AS SMALLINT) as ASN_DETAIL_STATUS",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_ASN_DETAIL_STATUS_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_ASN_DETAIL_STATUS_PRE is written to the target table - "
        + target_table_name
    )