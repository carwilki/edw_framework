# Code converted on 2023-06-24 13:43:40
from datetime import datetime

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DecimalType, StringType, TimestampType

from Datalake.utils.configs import getConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from logging import getLogger, INFO
from Datalake.utils import genericUtilities as gu


def m_WM_Asn_Status_PRE(dcnbr, env):
    logger = getLogger()
    logger.info("inside m_WM_Asn_Status_PRE function")

    spark = SparkSession.getActiveSession()

    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_ASN_STATUS_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "ASN_STATUS"

    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now()  # start timestamp of the script

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)

    # COMMAND ----------
    # Variable_declaration_comment
    dcnbr = dcnbr.strip()[2:]
    Prev_Run_Dt = gu.genPrevRunDt(refine_table_name, refine, raw)

    # COMMAND ----------
    # Processing node SQ_Shortcut_to_ASN_STATUS, type SOURCE
    # COLUMN COUNT: 4

    SQ_Shortcut_to_ASN_STATUS = gu.jdbcOracleConnection(
        f"""SELECT
            ASN_STATUS.ASN_STATUS,
            ASN_STATUS.DESCRIPTION,
            ASN_STATUS.CREATED_DTTM,
            ASN_STATUS.LAST_UPDATED_DTTM
        FROM ASN_STATUS
        WHERE  (TRUNC( CREATED_DTTM)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC( LAST_UPDATED_DTTM)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 6

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ASN_STATUS_temp = SQ_Shortcut_to_ASN_STATUS.toDF(
        *[
            "SQ_Shortcut_to_ASN_STATUS___" + col
            for col in SQ_Shortcut_to_ASN_STATUS.columns
        ]
    )

    EXPTRANS = SQ_Shortcut_to_ASN_STATUS_temp.selectExpr(
        "SQ_Shortcut_to_ASN_STATUS___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_ASN_STATUS___ASN_STATUS as ASN_STATUS",
        "SQ_Shortcut_to_ASN_STATUS___DESCRIPTION as DESCRIPTION",
        "SQ_Shortcut_to_ASN_STATUS___CREATED_DTTM as CREATED_DTTM",
        "SQ_Shortcut_to_ASN_STATUS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ASN_STATUS_PRE, type TARGET
    # COLUMN COUNT: 6

    Shortcut_to_WM_ASN_STATUS_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
        "CAST(ASN_STATUS AS BIGINT) as ASN_STATUS",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP",
    )

    gu.overwriteDeltaPartition(
        Shortcut_to_WM_ASN_STATUS_PRE, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_ASN_STATUS_PRE is written to the target table - "
        + target_table_name
    )
