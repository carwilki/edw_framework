# Code converted on 2023-06-24 13:44:06
from datetime import datetime

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DecimalType, StringType, TimestampType

from Datalake.utils.configs import getConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from logging import getLogger
from Datalake.utils import genericUtilities as gu


def m_WM_Asn_Detail_Status_PRE(dcnbr, env):
    logger = getLogger()
    logger.info("inside m_WM_Asn_Detail_Status_PRE function")

    spark = SparkSession.getActiveSession()

    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_ASN_DETAIL_STATUS_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "ASN_DETAIL_STATUS"

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
    # Processing node SQ_Shortcut_to_ASN_DETAIL_STATUS, type SOURCE
    # COLUMN COUNT: 2

    SQ_Shortcut_to_ASN_DETAIL_STATUS = gu.jdbcOracleConnection(
        """SELECT
                ASN_DETAIL_STATUS.ASN_DETAIL_STATUS,
                ASN_DETAIL_STATUS.DESCRIPTION
            FROM ASN_DETAIL_STATUS""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 4

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ASN_DETAIL_STATUS_temp = SQ_Shortcut_to_ASN_DETAIL_STATUS.toDF(
        *[
            "SQ_Shortcut_to_ASN_DETAIL_STATUS___" + col
            for col in SQ_Shortcut_to_ASN_DETAIL_STATUS.columns
        ]
    )

    EXPTRANS = SQ_Shortcut_to_ASN_DETAIL_STATUS_temp.selectExpr(
        "SQ_Shortcut_to_ASN_DETAIL_STATUS___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_ASN_DETAIL_STATUS___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS",
        "SQ_Shortcut_to_ASN_DETAIL_STATUS___DESCRIPTION as DESCRIPTION",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ASN_DETAIL_STATUS_PRE, type TARGET
    # COLUMN COUNT: 4

    Shortcut_to_WM_ASN_DETAIL_STATUS_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
        "CAST(ASN_DETAIL_STATUS AS BIGINT) as ASN_DETAIL_STATUS",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP",
    )
    gu.overwriteDeltaPartition(
        Shortcut_to_WM_ASN_DETAIL_STATUS_PRE, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_ASN_DETAIL_STATUS_PRE is written to the target table - "
        + target_table_name
    )
