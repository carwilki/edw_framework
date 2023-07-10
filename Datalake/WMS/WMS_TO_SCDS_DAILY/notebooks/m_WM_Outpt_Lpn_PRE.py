#Code converted on 2023-06-26 17:05:25
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
from Datalake.utils.logger import *


def m_WM_Outpt_Lpn_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Outpt_Lpn_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_OUTPT_LPN_PRE"
    schemaName = raw
    source_schema = "WMSMIS"

    
    target_table_name = schemaName + "." + tableName
    refine_table_name = tableName[:-4]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + Prev_Run_Dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    query = f"""SELECT
                    OUTPT_LPN.ACTUAL_CHARGE,
                    OUTPT_LPN.ACTUAL_CHARGE_CURRENCY,
                    OUTPT_LPN.ACTUAL_MONETARY_VALUE,
                    OUTPT_LPN.ACTUAL_VOLUME,
                    OUTPT_LPN.ADDNL_OPTION_CHARGE,
                    OUTPT_LPN.ADDNL_OPTION_CHARGE_CURRENCY,
                    OUTPT_LPN.ADF_TRANSMIT_FLAG,
                    OUTPT_LPN.AUDITOR_USERID,
                    OUTPT_LPN.BASE_CHARGE,
                    OUTPT_LPN.BASE_CHARGE_CURRENCY,
                    OUTPT_LPN.BILL_OF_LADING_NUMBER,
                    OUTPT_LPN.C_FACILITY_ALIAS_ID,
                    OUTPT_LPN.COD_AMOUNT,
                    OUTPT_LPN.COD_PAYMENT_METHOD,
                    OUTPT_LPN.CONTAINER_SIZE,
                    OUTPT_LPN.CONTAINER_TYPE,
                    OUTPT_LPN.CREATED_DTTM,
                    OUTPT_LPN.CREATED_SOURCE,
                    OUTPT_LPN.CREATED_SOURCE_TYPE,
                    OUTPT_LPN.DECLARED_MONETARY_VALUE,
                    OUTPT_LPN.DIST_CHARGE,
                    OUTPT_LPN.DIST_CHARGE_CURRENCY,
                    OUTPT_LPN.EPC_MATCH_FLAG,
                    OUTPT_LPN.ESTIMATED_WEIGHT,
                    OUTPT_LPN.FINAL_DEST_FACILITY_ALIAS_ID,
                    OUTPT_LPN.FREIGHT_CHARGE,
                    OUTPT_LPN.FREIGHT_CHARGE_CURRENCY,
                    OUTPT_LPN.INSUR_CHARGE,
                    OUTPT_LPN.INSUR_CHARGE_CURRENCY,
                    OUTPT_LPN.INVC_BATCH_NBR,
                    OUTPT_LPN.LAST_UPDATED_DTTM,
                    OUTPT_LPN.LOADED_DTTM,
                    OUTPT_LPN.LOADED_POSN,
                    OUTPT_LPN.LPN_EPC,
                    OUTPT_LPN.LPN_NBR_X_OF_Y,
                    OUTPT_LPN.MANIFEST_NBR,
                    OUTPT_LPN.MASTER_BOL_NBR,
                    OUTPT_LPN.MISC_INSTR_CODE_1,
                    OUTPT_LPN.MISC_INSTR_CODE_2,
                    OUTPT_LPN.MISC_INSTR_CODE_3,
                    OUTPT_LPN.MISC_INSTR_CODE_4,
                    OUTPT_LPN.MISC_INSTR_CODE_5,
                    OUTPT_LPN.MISC_NUM_1,
                    OUTPT_LPN.MISC_NUM_2,
                    OUTPT_LPN.NON_INVENTORY_LPN_FLAG,
                    OUTPT_LPN.NON_MACHINEABLE,
                    OUTPT_LPN.OUTPT_LPN_ID,
                    OUTPT_LPN.PACKAGE_DESCRIPTION,
                    OUTPT_LPN.PACKER_USERID,
                    OUTPT_LPN.PALLET_EPC,
                    OUTPT_LPN.PALLET_MASTER_LPN_FLAG,
                    OUTPT_LPN.PARCEL_SERVICE_MODE,
                    OUTPT_LPN.PARCEL_SHIPMENT_NBR,
                    OUTPT_LPN.PICKER_USERID,
                    OUTPT_LPN.PRE_BULK_ADD_OPT_CHR,
                    OUTPT_LPN.PRE_BULK_ADD_OPT_CHR_CURRENCY,
                    OUTPT_LPN.PRE_BULK_BASE_CHARGE,
                    OUTPT_LPN.PROC_DTTM,
                    OUTPT_LPN.PROC_STAT_CODE,
                    OUTPT_LPN.QTY_UOM,
                    OUTPT_LPN.RATE_ZONE,
                    OUTPT_LPN.RATED_WEIGHT,
                    OUTPT_LPN.RATED_WEIGHT_UOM,
                    OUTPT_LPN.SERVICE_LEVEL,
                    OUTPT_LPN.SHIP_VIA,
                    OUTPT_LPN.SHIPPED_DTTM,
                    OUTPT_LPN.STATIC_ROUTE_ID,
                    OUTPT_LPN.TC_COMPANY_ID,
                    OUTPT_LPN.TC_LPN_ID,
                    OUTPT_LPN.TC_ORDER_ID,
                    OUTPT_LPN.TC_PARENT_LPN_ID,
                    OUTPT_LPN.TC_SHIPMENT_ID,
                    OUTPT_LPN.TOTAL_LPN_QTY,
                    OUTPT_LPN.TRACKING_NBR,
                    OUTPT_LPN.ALT_TRACKING_NBR,
                    OUTPT_LPN.VERSION_NBR,
                    OUTPT_LPN.VOLUME_UOM,
                    OUTPT_LPN.WEIGHT,
                    OUTPT_LPN.WEIGHT_UOM,
                    OUTPT_LPN.LOAD_SEQUENCE,
                    OUTPT_LPN.TC_PURCHASE_ORDER_ID,
                    OUTPT_LPN.RETURN_TRACKING_NBR,
                    OUTPT_LPN.XREF_OLPN,
                    OUTPT_LPN.RETURN_REFERENCE_NUMBER,
                    OUTPT_LPN.EPI_PACKAGE_ID,
                    OUTPT_LPN.EPI_SHIPMENT_ID,
                    OUTPT_LPN.REF_FIELD_2,
                    OUTPT_LPN.REF_FIELD_3,
                    OUTPT_LPN.REF_FIELD_4,
                    OUTPT_LPN.REF_FIELD_5,
                    OUTPT_LPN.REF_FIELD_6,
                    OUTPT_LPN.REF_FIELD_7,
                    OUTPT_LPN.REF_FIELD_8,
                    OUTPT_LPN.REF_FIELD_9,
                    OUTPT_LPN.REF_FIELD_10,
                    OUTPT_LPN.REF_FIELD_1
                FROM {source_schema}.OUTPT_LPN
                WHERE  (trunc(OUTPT_LPN.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(OUTPT_LPN.LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""

    SQ_Shortcut_to_OUTPT_LPN = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_OUTPT_LPN is executed and data is loaded using jdbc")

    # Processing node EXP_TRN, type EXPRESSION 
    # COLUMN COUNT: 98
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_OUTPT_LPN_temp = SQ_Shortcut_to_OUTPT_LPN.toDF(*["SQ_Shortcut_to_OUTPT_LPN___" + col for col in SQ_Shortcut_to_OUTPT_LPN.columns])
    
    EXP_TRN = SQ_Shortcut_to_OUTPT_LPN_temp.selectExpr( \
        "SQ_Shortcut_to_OUTPT_LPN___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_OUTPT_LPN___ACTUAL_CHARGE as ACTUAL_CHARGE", \
        "SQ_Shortcut_to_OUTPT_LPN___ACTUAL_CHARGE_CURRENCY as ACTUAL_CHARGE_CURRENCY", \
        "SQ_Shortcut_to_OUTPT_LPN___ACTUAL_MONETARY_VALUE as ACTUAL_MONETARY_VALUE", \
        "SQ_Shortcut_to_OUTPT_LPN___ACTUAL_VOLUME as ACTUAL_VOLUME", \
        "SQ_Shortcut_to_OUTPT_LPN___ADDNL_OPTION_CHARGE as ADDNL_OPTION_CHARGE", \
        "SQ_Shortcut_to_OUTPT_LPN___ADDNL_OPTION_CHARGE_CURRENCY as ADDNL_OPTION_CHARGE_CURRENCY", \
        "SQ_Shortcut_to_OUTPT_LPN___ADF_TRANSMIT_FLAG as ADF_TRANSMIT_FLAG", \
        "SQ_Shortcut_to_OUTPT_LPN___AUDITOR_USERID as AUDITOR_USERID", \
        "SQ_Shortcut_to_OUTPT_LPN___BASE_CHARGE as BASE_CHARGE", \
        "SQ_Shortcut_to_OUTPT_LPN___BASE_CHARGE_CURRENCY as BASE_CHARGE_CURRENCY", \
        "SQ_Shortcut_to_OUTPT_LPN___BILL_OF_LADING_NUMBER as BILL_OF_LADING_NUMBER", \
        "SQ_Shortcut_to_OUTPT_LPN___C_FACILITY_ALIAS_ID as C_FACILITY_ALIAS_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___COD_AMOUNT as COD_AMOUNT", \
        "SQ_Shortcut_to_OUTPT_LPN___COD_PAYMENT_METHOD as COD_PAYMENT_METHOD", \
        "SQ_Shortcut_to_OUTPT_LPN___CONTAINER_SIZE as CONTAINER_SIZE", \
        "SQ_Shortcut_to_OUTPT_LPN___CONTAINER_TYPE as CONTAINER_TYPE", \
        "SQ_Shortcut_to_OUTPT_LPN___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_OUTPT_LPN___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_OUTPT_LPN___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_OUTPT_LPN___DECLARED_MONETARY_VALUE as DECLARED_MONETARY_VALUE", \
        "SQ_Shortcut_to_OUTPT_LPN___DIST_CHARGE as DIST_CHARGE", \
        "SQ_Shortcut_to_OUTPT_LPN___DIST_CHARGE_CURRENCY as DIST_CHARGE_CURRENCY", \
        "SQ_Shortcut_to_OUTPT_LPN___EPC_MATCH_FLAG as EPC_MATCH_FLAG", \
        "SQ_Shortcut_to_OUTPT_LPN___ESTIMATED_WEIGHT as ESTIMATED_WEIGHT", \
        "SQ_Shortcut_to_OUTPT_LPN___FINAL_DEST_FACILITY_ALIAS_ID as FINAL_DEST_FACILITY_ALIAS_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___FREIGHT_CHARGE as FREIGHT_CHARGE", \
        "SQ_Shortcut_to_OUTPT_LPN___FREIGHT_CHARGE_CURRENCY as FREIGHT_CHARGE_CURRENCY", \
        "SQ_Shortcut_to_OUTPT_LPN___INSUR_CHARGE as INSUR_CHARGE", \
        "SQ_Shortcut_to_OUTPT_LPN___INSUR_CHARGE_CURRENCY as INSUR_CHARGE_CURRENCY", \
        "SQ_Shortcut_to_OUTPT_LPN___INVC_BATCH_NBR as INVC_BATCH_NBR", \
        "SQ_Shortcut_to_OUTPT_LPN___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_OUTPT_LPN___LOADED_DTTM as LOADED_DTTM", \
        "SQ_Shortcut_to_OUTPT_LPN___LOADED_POSN as LOADED_POSN", \
        "SQ_Shortcut_to_OUTPT_LPN___LPN_EPC as LPN_EPC", \
        "SQ_Shortcut_to_OUTPT_LPN___LPN_NBR_X_OF_Y as LPN_NBR_X_OF_Y", \
        "SQ_Shortcut_to_OUTPT_LPN___MANIFEST_NBR as MANIFEST_NBR", \
        "SQ_Shortcut_to_OUTPT_LPN___MASTER_BOL_NBR as MASTER_BOL_NBR", \
        "SQ_Shortcut_to_OUTPT_LPN___MISC_INSTR_CODE_1 as MISC_INSTR_CODE_1", \
        "SQ_Shortcut_to_OUTPT_LPN___MISC_INSTR_CODE_2 as MISC_INSTR_CODE_2", \
        "SQ_Shortcut_to_OUTPT_LPN___MISC_INSTR_CODE_3 as MISC_INSTR_CODE_3", \
        "SQ_Shortcut_to_OUTPT_LPN___MISC_INSTR_CODE_4 as MISC_INSTR_CODE_4", \
        "SQ_Shortcut_to_OUTPT_LPN___MISC_INSTR_CODE_5 as MISC_INSTR_CODE_5", \
        "SQ_Shortcut_to_OUTPT_LPN___MISC_NUM_1 as MISC_NUM_1", \
        "SQ_Shortcut_to_OUTPT_LPN___MISC_NUM_2 as MISC_NUM_2", \
        "SQ_Shortcut_to_OUTPT_LPN___NON_INVENTORY_LPN_FLAG as NON_INVENTORY_LPN_FLAG", \
        "SQ_Shortcut_to_OUTPT_LPN___NON_MACHINEABLE as NON_MACHINEABLE", \
        "SQ_Shortcut_to_OUTPT_LPN___OUTPT_LPN_ID as OUTPT_LPN_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___PACKAGE_DESCRIPTION as PACKAGE_DESCRIPTION", \
        "SQ_Shortcut_to_OUTPT_LPN___PACKER_USERID as PACKER_USERID", \
        "SQ_Shortcut_to_OUTPT_LPN___PALLET_EPC as PALLET_EPC", \
        "SQ_Shortcut_to_OUTPT_LPN___PALLET_MASTER_LPN_FLAG as PALLET_MASTER_LPN_FLAG", \
        "SQ_Shortcut_to_OUTPT_LPN___PARCEL_SERVICE_MODE as PARCEL_SERVICE_MODE", \
        "SQ_Shortcut_to_OUTPT_LPN___PARCEL_SHIPMENT_NBR as PARCEL_SHIPMENT_NBR", \
        "SQ_Shortcut_to_OUTPT_LPN___PICKER_USERID as PICKER_USERID", \
        "SQ_Shortcut_to_OUTPT_LPN___PRE_BULK_ADD_OPT_CHR as PRE_BULK_ADD_OPT_CHR", \
        "SQ_Shortcut_to_OUTPT_LPN___PRE_BULK_ADD_OPT_CHR_CURRENCY as PRE_BULK_ADD_OPT_CHR_CURRENCY", \
        "SQ_Shortcut_to_OUTPT_LPN___PRE_BULK_BASE_CHARGE as PRE_BULK_BASE_CHARGE", \
        "SQ_Shortcut_to_OUTPT_LPN___PROC_DTTM as PROC_DTTM", \
        "SQ_Shortcut_to_OUTPT_LPN___PROC_STAT_CODE as PROC_STAT_CODE", \
        "SQ_Shortcut_to_OUTPT_LPN___QTY_UOM as QTY_UOM", \
        "SQ_Shortcut_to_OUTPT_LPN___RATE_ZONE as RATE_ZONE", \
        "SQ_Shortcut_to_OUTPT_LPN___RATED_WEIGHT as RATED_WEIGHT", \
        "SQ_Shortcut_to_OUTPT_LPN___RATED_WEIGHT_UOM as RATED_WEIGHT_UOM", \
        "SQ_Shortcut_to_OUTPT_LPN___SERVICE_LEVEL as SERVICE_LEVEL", \
        "SQ_Shortcut_to_OUTPT_LPN___SHIP_VIA as SHIP_VIA", \
        "SQ_Shortcut_to_OUTPT_LPN___SHIPPED_DTTM as SHIPPED_DTTM", \
        "SQ_Shortcut_to_OUTPT_LPN___STATIC_ROUTE_ID as STATIC_ROUTE_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___TC_COMPANY_ID as TC_COMPANY_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___TC_LPN_ID as TC_LPN_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___TC_ORDER_ID as TC_ORDER_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___TC_PARENT_LPN_ID as TC_PARENT_LPN_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___TOTAL_LPN_QTY as TOTAL_LPN_QTY", \
        "SQ_Shortcut_to_OUTPT_LPN___TRACKING_NBR as TRACKING_NBR", \
        "SQ_Shortcut_to_OUTPT_LPN___ALT_TRACKING_NBR as ALT_TRACKING_NBR", \
        "SQ_Shortcut_to_OUTPT_LPN___VERSION_NBR as VERSION_NBR", \
        "SQ_Shortcut_to_OUTPT_LPN___VOLUME_UOM as VOLUME_UOM", \
        "SQ_Shortcut_to_OUTPT_LPN___WEIGHT as WEIGHT", \
        "SQ_Shortcut_to_OUTPT_LPN___WEIGHT_UOM as WEIGHT_UOM", \
        "SQ_Shortcut_to_OUTPT_LPN___LOAD_SEQUENCE as LOAD_SEQUENCE", \
        "SQ_Shortcut_to_OUTPT_LPN___TC_PURCHASE_ORDER_ID as TC_PURCHASE_ORDER_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___RETURN_TRACKING_NBR as RETURN_TRACKING_NBR", \
        "SQ_Shortcut_to_OUTPT_LPN___XREF_OLPN as XREF_OLPN", \
        "SQ_Shortcut_to_OUTPT_LPN___RETURN_REFERENCE_NUMBER as RETURN_REFERENCE_NUMBER", \
        "SQ_Shortcut_to_OUTPT_LPN___EPI_PACKAGE_ID as EPI_PACKAGE_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___EPI_SHIPMENT_ID as EPI_SHIPMENT_ID", \
        "SQ_Shortcut_to_OUTPT_LPN___REF_FIELD_2 as REF_FIELD_2", \
        "SQ_Shortcut_to_OUTPT_LPN___REF_FIELD_3 as REF_FIELD_3", \
        "SQ_Shortcut_to_OUTPT_LPN___REF_FIELD_4 as REF_FIELD_4", \
        "SQ_Shortcut_to_OUTPT_LPN___REF_FIELD_5 as REF_FIELD_5", \
        "SQ_Shortcut_to_OUTPT_LPN___REF_FIELD_6 as REF_FIELD_6", \
        "SQ_Shortcut_to_OUTPT_LPN___REF_FIELD_7 as REF_FIELD_7", \
        "SQ_Shortcut_to_OUTPT_LPN___REF_FIELD_8 as REF_FIELD_8", \
        "SQ_Shortcut_to_OUTPT_LPN___REF_FIELD_9 as REF_FIELD_9", \
        "SQ_Shortcut_to_OUTPT_LPN___REF_FIELD_10 as REF_FIELD_10", \
        "SQ_Shortcut_to_OUTPT_LPN___REF_FIELD_1 as REF_FIELD_1", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_OUTPT_LPN_PRE, type TARGET 
    # COLUMN COUNT: 98
    
    
    Shortcut_to_WM_OUTPT_LPN_PRE = EXP_TRN.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(OUTPT_LPN_ID AS BIGINT) as OUTPT_LPN_ID",
        "CAST(ACTUAL_CHARGE AS DECIMAL(9,2)) as ACTUAL_CHARGE",
        "CAST(ACTUAL_CHARGE_CURRENCY AS STRING) as ACTUAL_CHARGE_CURRENCY",
        "CAST(ACTUAL_MONETARY_VALUE AS DECIMAL(13,4)) as ACTUAL_MONETARY_VALUE",
        "CAST(ACTUAL_VOLUME AS DECIMAL(13,4)) as ACTUAL_VOLUME",
        "CAST(ADDNL_OPTION_CHARGE AS DECIMAL(9,2)) as ADDNL_OPTION_CHARGE",
        "CAST(ADDNL_OPTION_CHARGE_CURRENCY AS STRING) as ADDNL_OPTION_CHARGE_CURRENCY",
        "CAST(ADF_TRANSMIT_FLAG AS STRING) as ADF_TRANSMIT_FLAG",
        "CAST(AUDITOR_USERID AS STRING) as AUDITOR_USERID",
        "CAST(BASE_CHARGE AS DECIMAL(13,4)) as BASE_CHARGE",
        "CAST(BASE_CHARGE_CURRENCY AS STRING) as BASE_CHARGE_CURRENCY",
        "CAST(BILL_OF_LADING_NUMBER AS STRING) as BILL_OF_LADING_NUMBER",
        "CAST(C_FACILITY_ALIAS_ID AS STRING) as C_FACILITY_ALIAS_ID",
        "CAST(COD_AMOUNT AS DECIMAL(13,4)) as COD_AMOUNT",
        "CAST(COD_PAYMENT_METHOD AS INT) as COD_PAYMENT_METHOD",
        "CAST(CONTAINER_SIZE AS STRING) as CONTAINER_SIZE",
        "CAST(CONTAINER_TYPE AS STRING) as CONTAINER_TYPE",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(CREATED_SOURCE_TYPE AS SMALLINT) as CREATED_SOURCE_TYPE",
        "CAST(DECLARED_MONETARY_VALUE AS DECIMAL(13,4)) as DECLARED_MONETARY_VALUE",
        "CAST(DIST_CHARGE AS DECIMAL(11,2)) as DIST_CHARGE",
        "CAST(DIST_CHARGE_CURRENCY AS STRING) as DIST_CHARGE_CURRENCY",
        "CAST(EPC_MATCH_FLAG AS STRING) as EPC_MATCH_FLAG",
        "CAST(ESTIMATED_WEIGHT AS DECIMAL(13,4)) as ESTIMATED_WEIGHT",
        "CAST(FINAL_DEST_FACILITY_ALIAS_ID AS STRING) as FINAL_DEST_FACILITY_ALIAS_ID",
        "CAST(FREIGHT_CHARGE AS DECIMAL(11,2)) as FREIGHT_CHARGE",
        "CAST(FREIGHT_CHARGE_CURRENCY AS STRING) as FREIGHT_CHARGE_CURRENCY",
        "CAST(INSUR_CHARGE AS DECIMAL(9,2)) as INSUR_CHARGE",
        "CAST(INSUR_CHARGE_CURRENCY AS STRING) as INSUR_CHARGE_CURRENCY",
        "CAST(INVC_BATCH_NBR AS INT) as INVC_BATCH_NBR",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOADED_DTTM AS TIMESTAMP) as LOADED_DTTM",
        "CAST(LOADED_POSN AS STRING) as LOADED_POSN",
        "CAST(LPN_EPC AS STRING) as LPN_EPC",
        "CAST(LPN_NBR_X_OF_Y AS INT) as LPN_NBR_X_OF_Y",
        "CAST(MANIFEST_NBR AS STRING) as MANIFEST_NBR",
        "CAST(MASTER_BOL_NBR AS STRING) as MASTER_BOL_NBR",
        "CAST(MISC_INSTR_CODE_1 AS STRING) as MISC_INSTR_CODE_1",
        "CAST(MISC_INSTR_CODE_2 AS STRING) as MISC_INSTR_CODE_2",
        "CAST(MISC_INSTR_CODE_3 AS STRING) as MISC_INSTR_CODE_3",
        "CAST(MISC_INSTR_CODE_4 AS STRING) as MISC_INSTR_CODE_4",
        "CAST(MISC_INSTR_CODE_5 AS STRING) as MISC_INSTR_CODE_5",
        "CAST(MISC_NUM_1 AS DECIMAL(9,2)) as MISC_NUM_1",
        "CAST(MISC_NUM_2 AS DECIMAL(9,2)) as MISC_NUM_2",
        "CAST(NON_INVENTORY_LPN_FLAG AS SMALLINT) as NON_INVENTORY_LPN_FLAG",
        "CAST(NON_MACHINEABLE AS INT) as NON_MACHINEABLE",
        "CAST(PACKAGE_DESCRIPTION AS STRING) as PACKAGE_DESCRIPTION",
        "CAST(PACKER_USERID AS STRING) as PACKER_USERID",
        "CAST(PALLET_EPC AS STRING) as PALLET_EPC",
        "CAST(PALLET_MASTER_LPN_FLAG AS SMALLINT) as PALLET_MASTER_LPN_FLAG",
        "CAST(PARCEL_SERVICE_MODE AS STRING) as PARCEL_SERVICE_MODE",
        "CAST(PARCEL_SHIPMENT_NBR AS STRING) as PARCEL_SHIPMENT_NBR",
        "CAST(PICKER_USERID AS STRING) as PICKER_USERID",
        "CAST(PRE_BULK_ADD_OPT_CHR AS DECIMAL(9,2)) as PRE_BULK_ADD_OPT_CHR",
        "CAST(PRE_BULK_ADD_OPT_CHR_CURRENCY AS STRING) as PRE_BULK_ADD_OPT_CHR_CURRENCY",
        "CAST(PRE_BULK_BASE_CHARGE AS DECIMAL(9,2)) as PRE_BULK_BASE_CHARGE",
        "CAST(PROC_DTTM AS TIMESTAMP) as PROC_DTTM",
        "CAST(PROC_STAT_CODE AS SMALLINT) as PROC_STAT_CODE",
        "CAST(QTY_UOM AS STRING) as QTY_UOM",
        "CAST(RATE_ZONE AS STRING) as RATE_ZONE",
        "CAST(RATED_WEIGHT AS DECIMAL(13,4)) as RATED_WEIGHT",
        "CAST(RATED_WEIGHT_UOM AS STRING) as RATED_WEIGHT_UOM",
        "CAST(SERVICE_LEVEL AS STRING) as SERVICE_LEVEL",
        "CAST(SHIP_VIA AS STRING) as SHIP_VIA",
        "CAST(SHIPPED_DTTM AS TIMESTAMP) as SHIPPED_DTTM",
        "CAST(STATIC_ROUTE_ID AS INT) as STATIC_ROUTE_ID",
        "CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID",
        "CAST(TC_LPN_ID AS STRING) as TC_LPN_ID",
        "CAST(TC_ORDER_ID AS STRING) as TC_ORDER_ID",
        "CAST(TC_PARENT_LPN_ID AS STRING) as TC_PARENT_LPN_ID",
        "CAST(TC_SHIPMENT_ID AS STRING) as TC_SHIPMENT_ID",
        "CAST(TOTAL_LPN_QTY AS DECIMAL(13,4)) as TOTAL_LPN_QTY",
        "CAST(TRACKING_NBR AS STRING) as TRACKING_NBR",
        "CAST(ALT_TRACKING_NBR AS STRING) as ALT_TRACKING_NBR",
        "CAST(VERSION_NBR AS BIGINT) as VERSION_NBR",
        "CAST(VOLUME_UOM AS STRING) as VOLUME_UOM",
        "CAST(WEIGHT AS DECIMAL(16,4)) as WEIGHT",
        "CAST(WEIGHT_UOM AS STRING) as WEIGHT_UOM",
        "CAST(LOAD_SEQUENCE AS INT) as LOAD_SEQUENCE",
        "CAST(TC_PURCHASE_ORDER_ID AS STRING) as TC_PURCHASE_ORDER_ID",
        "CAST(RETURN_TRACKING_NBR AS STRING) as RETURN_TRACKING_NBR",
        "CAST(XREF_OLPN AS STRING) as XREF_OLPN",
        "CAST(RETURN_REFERENCE_NUMBER AS STRING) as RETURN_REFERENCE_NUMBER",
        "CAST(EPI_PACKAGE_ID AS STRING) as EPI_PACKAGE_ID",
        "CAST(EPI_SHIPMENT_ID AS STRING) as EPI_SHIPMENT_ID",
        "CAST(REF_FIELD_2 AS STRING) as REF_FIELD_2",
        "CAST(REF_FIELD_3 AS STRING) as REF_FIELD_3",
        "CAST(REF_FIELD_4 AS STRING) as REF_FIELD_4",
        "CAST(REF_FIELD_5 AS STRING) as REF_FIELD_5",
        "CAST(REF_FIELD_6 AS STRING) as REF_FIELD_6",
        "CAST(REF_FIELD_7 AS STRING) as REF_FIELD_7",
        "CAST(REF_FIELD_8 AS STRING) as REF_FIELD_8",
        "CAST(REF_FIELD_9 AS STRING) as REF_FIELD_9",
        "CAST(REF_FIELD_10 AS STRING) as REF_FIELD_10",
        "CAST(REF_FIELD_1 AS STRING) as REF_FIELD_1",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )    
    overwriteDeltaPartition(Shortcut_to_WM_OUTPT_LPN_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_OUTPT_LPN_PRE is written to the target table - " + target_table_name)
