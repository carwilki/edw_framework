# Code converted on 2023-06-24 13:43:23
from datetime import datetime

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DecimalType, StringType, TimestampType

from Datalake.utils.configs import getConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from logging import getLogger, INFO
from Datalake.utils import genericUtilities as gu


def m_WM_Business_Partner_PRE(dcnbr, env):
    logger = getLogger()
    logger.info("inside m_WM_Business_Partner_PRE function")

    spark = SparkSession.getActiveSession()

    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    tableName = "WM_BUSINESS_PARTNER_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "BUSINESS_PARTNER"

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
    # Processing node SQ_Shortcut_to_BUSINESS_PARTNER, type SOURCE
    # COLUMN COUNT: 29

    SQ_Shortcut_to_BUSINESS_PARTNER = gu.jdbcOracleConnection(
        f"""SELECT
                BUSINESS_PARTNER.TC_COMPANY_ID,
                BUSINESS_PARTNER.BUSINESS_PARTNER_ID,
                BUSINESS_PARTNER.DESCRIPTION,
                BUSINESS_PARTNER.MARK_FOR_DELETION,
                BUSINESS_PARTNER.BP_ID,
                BUSINESS_PARTNER.BP_COMPANY_ID,
                BUSINESS_PARTNER.ACCREDITED_BP,
                BUSINESS_PARTNER.BUSINESS_NUMBER,
                BUSINESS_PARTNER.ADDRESS_1,
                BUSINESS_PARTNER.CITY,
                BUSINESS_PARTNER.STATE_PROV,
                BUSINESS_PARTNER.POSTAL_CODE,
                BUSINESS_PARTNER.COUNTY,
                BUSINESS_PARTNER.COUNTRY_CODE,
                BUSINESS_PARTNER.COMMENTS,
                BUSINESS_PARTNER.TEL_NBR,
                BUSINESS_PARTNER.LAST_UPDATED_SOURCE,
                BUSINESS_PARTNER.CREATED_DTTM,
                BUSINESS_PARTNER.LAST_UPDATED_DTTM,
                BUSINESS_PARTNER.CREATED_SOURCE,
                BUSINESS_PARTNER.ADDRESS_2,
                BUSINESS_PARTNER.ADDRESS_3,
                BUSINESS_PARTNER.HIBERNATE_VERSION,
                BUSINESS_PARTNER.PREFIX,
                BUSINESS_PARTNER.ATTRIBUTE_1,
                BUSINESS_PARTNER.ATTRIBUTE_2,
                BUSINESS_PARTNER.ATTRIBUTE_3,
                BUSINESS_PARTNER.ATTRIBUTE_4,
                BUSINESS_PARTNER.ATTRIBUTE_5
            FROM BUSINESS_PARTNER
            WHERE (TRUNC( BUSINESS_PARTNER.CREATED_DTTM)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC( BUSINESS_PARTNER.LAST_UPDATED_DTTM)>= TRUNC( to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)""",
        username,
        password,
        connection_string,
    ).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 31

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_BUSINESS_PARTNER_temp = SQ_Shortcut_to_BUSINESS_PARTNER.toDF(
        *[
            "SQ_Shortcut_to_BUSINESS_PARTNER___" + col
            for col in SQ_Shortcut_to_BUSINESS_PARTNER.columns
        ]
    )

    EXPTRANS = SQ_Shortcut_to_BUSINESS_PARTNER_temp.selectExpr(
        "SQ_Shortcut_to_BUSINESS_PARTNER___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_EXP",
        "SQ_Shortcut_to_BUSINESS_PARTNER___TC_COMPANY_ID as TC_COMPANY_ID",
        "SQ_Shortcut_to_BUSINESS_PARTNER___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID",
        "SQ_Shortcut_to_BUSINESS_PARTNER___DESCRIPTION as DESCRIPTION",
        "SQ_Shortcut_to_BUSINESS_PARTNER___MARK_FOR_DELETION as MARK_FOR_DELETION",
        "SQ_Shortcut_to_BUSINESS_PARTNER___BP_ID as BP_ID",
        "SQ_Shortcut_to_BUSINESS_PARTNER___BP_COMPANY_ID as BP_COMPANY_ID",
        "SQ_Shortcut_to_BUSINESS_PARTNER___ACCREDITED_BP as ACCREDITED_BP",
        "SQ_Shortcut_to_BUSINESS_PARTNER___BUSINESS_NUMBER as BUSINESS_NUMBER",
        "SQ_Shortcut_to_BUSINESS_PARTNER___ADDRESS_1 as ADDRESS_1",
        "SQ_Shortcut_to_BUSINESS_PARTNER___CITY as CITY",
        "SQ_Shortcut_to_BUSINESS_PARTNER___STATE_PROV as STATE_PROV",
        "SQ_Shortcut_to_BUSINESS_PARTNER___POSTAL_CODE as POSTAL_CODE",
        "SQ_Shortcut_to_BUSINESS_PARTNER___COUNTY as COUNTY",
        "SQ_Shortcut_to_BUSINESS_PARTNER___COUNTRY_CODE as COUNTRY_CODE",
        "SQ_Shortcut_to_BUSINESS_PARTNER___COMMENTS as COMMENTS",
        "SQ_Shortcut_to_BUSINESS_PARTNER___TEL_NBR as TEL_NBR",
        "SQ_Shortcut_to_BUSINESS_PARTNER___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE",
        "SQ_Shortcut_to_BUSINESS_PARTNER___CREATED_DTTM as CREATED_DTTM",
        "SQ_Shortcut_to_BUSINESS_PARTNER___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
        "SQ_Shortcut_to_BUSINESS_PARTNER___CREATED_SOURCE as CREATED_SOURCE",
        "SQ_Shortcut_to_BUSINESS_PARTNER___ADDRESS_2 as ADDRESS_2",
        "SQ_Shortcut_to_BUSINESS_PARTNER___ADDRESS_3 as ADDRESS_3",
        "SQ_Shortcut_to_BUSINESS_PARTNER___HIBERNATE_VERSION as HIBERNATE_VERSION",
        "SQ_Shortcut_to_BUSINESS_PARTNER___PREFIX as PREFIX",
        "SQ_Shortcut_to_BUSINESS_PARTNER___ATTRIBUTE_1 as ATTRIBUTE_1",
        "SQ_Shortcut_to_BUSINESS_PARTNER___ATTRIBUTE_2 as ATTRIBUTE_2",
        "SQ_Shortcut_to_BUSINESS_PARTNER___ATTRIBUTE_3 as ATTRIBUTE_3",
        "SQ_Shortcut_to_BUSINESS_PARTNER___ATTRIBUTE_4 as ATTRIBUTE_4",
        "SQ_Shortcut_to_BUSINESS_PARTNER___ATTRIBUTE_5 as ATTRIBUTE_5",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP",
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_BUSINESS_PARTNER_PRE, type TARGET
    # COLUMN COUNT: 31

    Shortcut_to_WM_BUSINESS_PARTNER_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
        "CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID",
        "CAST(BUSINESS_PARTNER_ID AS STRING) as BUSINESS_PARTNER_ID",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(MARK_FOR_DELETION AS BIGINT) as MARK_FOR_DELETION",
        "CAST(BP_ID AS BIGINT) as BP_ID",
        "CAST(BP_COMPANY_ID AS BIGINT) as BP_COMPANY_ID",
        "CAST(ACCREDITED_BP AS BIGINT) as ACCREDITED_BP",
        "CAST(BUSINESS_NUMBER AS STRING) as BUSINESS_NUMBER",
        "CAST(ADDRESS_1 AS STRING) as ADDRESS_1",
        "CAST(CITY AS STRING) as CITY",
        "CAST(STATE_PROV AS STRING) as STATE_PROV",
        "CAST(POSTAL_CODE AS STRING) as POSTAL_CODE",
        "CAST(COUNTY AS STRING) as COUNTY",
        "CAST(COUNTRY_CODE AS STRING) as COUNTRY_CODE",
        "CAST(COMMENTS AS STRING) as COMMENTS",
        "CAST(TEL_NBR AS STRING) as TEL_NBR",
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(ADDRESS_2 AS STRING) as ADDRESS_2",
        "CAST(ADDRESS_3 AS STRING) as ADDRESS_3",
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION",
        "CAST(PREFIX AS STRING) as PREFIX",
        "CAST(ATTRIBUTE_1 AS BIGINT) as ATTRIBUTE_1",
        "CAST(ATTRIBUTE_2 AS STRING) as ATTRIBUTE_2",
        "CAST(ATTRIBUTE_3 AS STRING) as ATTRIBUTE_3",
        "CAST(ATTRIBUTE_4 AS STRING) as ATTRIBUTE_4",
        "CAST(ATTRIBUTE_5 AS STRING) as ATTRIBUTE_5",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP",
    )
    gu.overwriteDeltaPartition(
        Shortcut_to_WM_BUSINESS_PARTNER_PRE, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_BUSINESS_PARTNER_PRE is written to the target table - "
        + target_table_name
    )
