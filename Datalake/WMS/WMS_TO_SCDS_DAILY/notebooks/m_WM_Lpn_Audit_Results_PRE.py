#Code converted on 2023-06-27 09:40:48
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



def m_WM_Lpn_Audit_Results_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Lpn_Audit_Results_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_LPN_AUDIT_RESULTS_PRE"

    schemaName = raw

    target_table_name = schemaName + "." + tableName

    refine_table_name = "LPN_AUDIT_RESULTS"


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
    # Processing node SQ_Shortcut_to_LPN_AUDIT_RESULTS, type SOURCE 
    # COLUMN COUNT: 40

    SQ_Shortcut_to_LPN_AUDIT_RESULTS = jdbcOracleConnection(  f"""SELECT
    LPN_AUDIT_RESULTS.LPN_AUDIT_RESULTS_ID,
    LPN_AUDIT_RESULTS.AUDIT_TRANSACTION_ID,
    LPN_AUDIT_RESULTS.AUDIT_COUNT,
    LPN_AUDIT_RESULTS.TC_COMPANY_ID,
    LPN_AUDIT_RESULTS.FACILITY_ALIAS_ID,
    LPN_AUDIT_RESULTS.TC_SHIPMENT_ID,
    LPN_AUDIT_RESULTS.STOP_SEQ,
    LPN_AUDIT_RESULTS.TC_ORDER_ID,
    LPN_AUDIT_RESULTS.TC_PARENT_LPN_ID,
    LPN_AUDIT_RESULTS.TC_LPN_ID,
    LPN_AUDIT_RESULTS.INBOUND_OUTBOUND_INDICATOR,
    LPN_AUDIT_RESULTS.DEST_FACILITY_ALIAS_ID,
    LPN_AUDIT_RESULTS.STATIC_ROUTE_ID,
    LPN_AUDIT_RESULTS.ITEM_ID,
    LPN_AUDIT_RESULTS.GTIN,
    LPN_AUDIT_RESULTS.CNTRY_OF_ORGN,
    LPN_AUDIT_RESULTS.INVENTORY_TYPE,
    LPN_AUDIT_RESULTS.PRODUCT_STATUS,
    LPN_AUDIT_RESULTS.BATCH_NBR,
    LPN_AUDIT_RESULTS.ITEM_ATTR_1,
    LPN_AUDIT_RESULTS.ITEM_ATTR_2,
    LPN_AUDIT_RESULTS.ITEM_ATTR_3,
    LPN_AUDIT_RESULTS.ITEM_ATTR_4,
    LPN_AUDIT_RESULTS.ITEM_ATTR_5,
    LPN_AUDIT_RESULTS.CREATED_SOURCE_TYPE,
    LPN_AUDIT_RESULTS.CREATED_SOURCE,
    LPN_AUDIT_RESULTS.CREATED_DTTM,
    LPN_AUDIT_RESULTS.LAST_UPDATED_SOURCE_TYPE,
    LPN_AUDIT_RESULTS.LAST_UPDATED_SOURCE,
    LPN_AUDIT_RESULTS.LAST_UPDATED_DTTM,
    LPN_AUDIT_RESULTS.AUDITOR_USERID,
    LPN_AUDIT_RESULTS.PICKER_USERID,
    LPN_AUDIT_RESULTS.PACKER_USERID,
    LPN_AUDIT_RESULTS.QUAL_AUD_STAT_CODE,
    LPN_AUDIT_RESULTS.QA_FLAG,
    LPN_AUDIT_RESULTS.COUNT_QUANTITY,
    LPN_AUDIT_RESULTS.EXPECTED_QUANTITY,
    LPN_AUDIT_RESULTS.VALIDATION_LEVEL,
    LPN_AUDIT_RESULTS.TRAN_NAME,
    LPN_AUDIT_RESULTS.FACILITY_ID
    FROM LPN_AUDIT_RESULTS
    WHERE (trunc(CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1)  AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 42

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LPN_AUDIT_RESULTS_temp = SQ_Shortcut_to_LPN_AUDIT_RESULTS.toDF(*["SQ_Shortcut_to_LPN_AUDIT_RESULTS___" + col for col in SQ_Shortcut_to_LPN_AUDIT_RESULTS.columns])

    EXPTRANS = SQ_Shortcut_to_LPN_AUDIT_RESULTS_temp.selectExpr( \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___LPN_AUDIT_RESULTS_ID as LPN_AUDIT_RESULTS_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___AUDIT_TRANSACTION_ID as AUDIT_TRANSACTION_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___AUDIT_COUNT as AUDIT_COUNT", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___TC_COMPANY_ID as TC_COMPANY_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___FACILITY_ALIAS_ID as FACILITY_ALIAS_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___STOP_SEQ as STOP_SEQ", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___TC_ORDER_ID as TC_ORDER_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___TC_PARENT_LPN_ID as TC_PARENT_LPN_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___TC_LPN_ID as TC_LPN_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___DEST_FACILITY_ALIAS_ID as DEST_FACILITY_ALIAS_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___STATIC_ROUTE_ID as STATIC_ROUTE_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___ITEM_ID as ITEM_ID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___GTIN as GTIN", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___INVENTORY_TYPE as INVENTORY_TYPE", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___PRODUCT_STATUS as PRODUCT_STATUS", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___BATCH_NBR as BATCH_NBR", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___ITEM_ATTR_1 as ITEM_ATTR_1", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___ITEM_ATTR_2 as ITEM_ATTR_2", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___ITEM_ATTR_3 as ITEM_ATTR_3", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___ITEM_ATTR_4 as ITEM_ATTR_4", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___ITEM_ATTR_5 as ITEM_ATTR_5", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___AUDITOR_USERID as AUDITOR_USERID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___PICKER_USERID as PICKER_USERID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___PACKER_USERID as PACKER_USERID", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___QUAL_AUD_STAT_CODE as QUAL_AUD_STAT_CODE", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___QA_FLAG as QA_FLAG", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___COUNT_QUANTITY as COUNT_QUANTITY", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___EXPECTED_QUANTITY as EXPECTED_QUANTITY", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___VALIDATION_LEVEL as VALIDATION_LEVEL", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___TRAN_NAME as TRAN_NAME", \
        "SQ_Shortcut_to_LPN_AUDIT_RESULTS___FACILITY_ID as FACILITY_ID", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE, type TARGET 
    # COLUMN COUNT: 42


    Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
        "CAST(LPN_AUDIT_RESULTS_ID AS BIGINT) as LPN_AUDIT_RESULTS_ID", \
        "CAST(AUDIT_TRANSACTION_ID AS BIGINT) as AUDIT_TRANSACTION_ID", \
        "CAST(AUDIT_COUNT AS BIGINT) as AUDIT_COUNT", \
        "CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", \
        "CAST(FACILITY_ALIAS_ID AS STRING) as FACILITY_ALIAS_ID", \
        "CAST(TC_SHIPMENT_ID AS STRING) as TC_SHIPMENT_ID", \
        "CAST(STOP_SEQ AS BIGINT) as STOP_SEQ", \
        "CAST(TC_ORDER_ID AS STRING) as TC_ORDER_ID", \
        "CAST(TC_PARENT_LPN_ID AS STRING) as TC_PARENT_LPN_ID", \
        "CAST(TC_LPN_ID AS STRING) as TC_LPN_ID", \
        "CAST(INBOUND_OUTBOUND_INDICATOR AS STRING) as INBOUND_OUTBOUND_INDICATOR", \
        "CAST(DEST_FACILITY_ALIAS_ID AS STRING) as DEST_FACILITY_ALIAS_ID", \
        "CAST(STATIC_ROUTE_ID AS BIGINT) as STATIC_ROUTE_ID", \
        "CAST(ITEM_ID AS BIGINT) as ITEM_ID", \
        "CAST(GTIN AS STRING) as GTIN", \
        "CAST(CNTRY_OF_ORGN AS STRING) as CNTRY_OF_ORGN", \
        "CAST(INVENTORY_TYPE AS STRING) as INVENTORY_TYPE", \
        "CAST(PRODUCT_STATUS AS STRING) as PRODUCT_STATUS", \
        "CAST(BATCH_NBR AS STRING) as BATCH_NBR", \
        "CAST(ITEM_ATTR_1 AS STRING) as ITEM_ATTR_1", \
        "CAST(ITEM_ATTR_2 AS STRING) as ITEM_ATTR_2", \
        "CAST(ITEM_ATTR_3 AS STRING) as ITEM_ATTR_3", \
        "CAST(ITEM_ATTR_4 AS STRING) as ITEM_ATTR_4", \
        "CAST(ITEM_ATTR_5 AS STRING) as ITEM_ATTR_5", \
        "CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", \
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
        "CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", \
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
        "CAST(AUDITOR_USERID AS STRING) as AUDITOR_USERID", \
        "CAST(PICKER_USERID AS STRING) as PICKER_USERID", \
        "CAST(PACKER_USERID AS STRING) as PACKER_USERID", \
        "CAST(QUAL_AUD_STAT_CODE AS BIGINT) as QUAL_AUD_STAT_CODE", \
        "CAST(QA_FLAG AS STRING) as QA_FLAG", \
        "CAST(COUNT_QUANTITY AS BIGINT) as COUNT_QUANTITY", \
        "CAST(EXPECTED_QUANTITY AS BIGINT) as EXPECTED_QUANTITY", \
        "CAST(VALIDATION_LEVEL AS STRING) as VALIDATION_LEVEL", \
        "CAST(TRAN_NAME AS STRING) as TRAN_NAME", \
        "CAST(FACILITY_ID AS BIGINT) as FACILITY_ID", \
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )

    overwriteDeltaPartition(Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE is written to the target table - "
        + target_table_name
    )