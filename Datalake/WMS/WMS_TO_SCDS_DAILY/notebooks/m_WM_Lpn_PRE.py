#Code converted on 2023-06-20 18:04:27
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
from Datalake.utils.logger import *


def m_WM_Lpn_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Lpn_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_LPN_PRE"
    schemaName = raw
    source_schema = "WMSMIS"

    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_LPN"
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + Prev_Run_Dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
                    LPN.LPN_ID,
                    LPN.TC_LPN_ID,
                    LPN.BUSINESS_PARTNER_ID,
                    LPN.TC_COMPANY_ID,
                    LPN.PARENT_LPN_ID,
                    LPN.TC_PARENT_LPN_ID,
                    LPN.LPN_TYPE,
                    LPN.VERSION_NBR,
                    LPN.LPN_MONETARY_VALUE,
                    LPN.LPN_MONETARY_VALUE_CURR_CODE,
                    LPN.TIER_QTY,
                    LPN.STD_LPN_QTY,
                    LPN.C_FACILITY_ID,
                    LPN.O_FACILITY_ID,
                    LPN.O_FACILITY_ALIAS_ID,
                    LPN.FINAL_DEST_FACILITY_ALIAS_ID,
                    LPN.FINAL_DEST_FACILITY_ID,
                    LPN.LPN_STATUS,
                    LPN.LPN_STATUS_UPDATED_DTTM,
                    LPN.LPN_FACILITY_STATUS,
                    LPN.TC_REFERENCE_LPN_ID,
                    LPN.TC_ORDER_ID,
                    LPN.ORIGINAL_TC_SHIPMENT_ID,
                    LPN.PARCEL_SHIPMENT_NBR,
                    LPN.TRACKING_NBR,
                    LPN.MANIFEST_NBR,
                    LPN.SHIP_VIA,
                    LPN.MASTER_BOL_NBR,
                    LPN.BOL_NBR,
                    LPN.INIT_SHIP_VIA,
                    LPN.PATH_ID,
                    LPN.ROUTE_RULE_ID,
                    LPN.IDEAL_PICK_DTTM,
                    LPN.REPRINT_COUNT,
                    LPN.LAST_FROZEN_DTTM,
                    LPN.LAST_COUNTED_DTTM,
                    LPN.BASE_CHARGE,
                    LPN.BASE_CHARGE_CURR_CODE,
                    LPN.ADDNL_OPTION_CHARGE,
                    LPN.ADDNL_OPTION_CHARGE_CURR_CODE,
                    LPN.INSUR_CHARGE,
                    LPN.INSUR_CHARGE_CURR_CODE,
                    LPN.ACTUAL_CHARGE,
                    LPN.ACTUAL_CHARGE_CURR_CODE,
                    LPN.PRE_BULK_BASE_CHARGE,
                    LPN.PRE_BULK_BASE_CHARGE_CURR_CODE,
                    LPN.PRE_BULK_ADD_OPT_CHR,
                    LPN.PRE_BULK_ADD_OPT_CHR_CURR_CODE,
                    LPN.DIST_CHARGE,
                    LPN.DIST_CHARGE_CURR_CODE,
                    LPN.FREIGHT_CHARGE,
                    LPN.FREIGHT_CHARGE_CURR_CODE,
                    LPN.RECEIVED_TC_SHIPMENT_ID,
                    LPN.MANUFACTURED_DTTM,
                    LPN.MANUFACT_PLANT_FACILITY_ALIAS,
                    LPN.RCVD_DTTM,
                    LPN.TC_PURCHASE_ORDERS_ID,
                    LPN.PURCHASE_ORDERS_ID,
                    LPN.TC_SHIPMENT_ID,
                    LPN.SHIPMENT_ID,
                    LPN.TC_ASN_ID,
                    LPN.ASN_ID,
                    LPN.RECEIPT_VARIANCE_INDICATOR,
                    LPN.ESTIMATED_VOLUME,
                    LPN.WEIGHT,
                    LPN.ACTUAL_VOLUME,
                    LPN.LPN_LABEL_TYPE,
                    LPN.PACKAGE_TYPE_ID,
                    LPN.MISC_INSTR_CODE_1,
                    LPN.MISC_INSTR_CODE_2,
                    LPN.MISC_INSTR_CODE_3,
                    LPN.MISC_INSTR_CODE_4,
                    LPN.MISC_INSTR_CODE_5,
                    LPN.MISC_NUM_1,
                    LPN.MISC_NUM_2,
                    LPN.HIBERNATE_VERSION,
                    LPN.CUBE_UOM,
                    LPN.ERROR_INDICATOR,
                    LPN.WARNING_INDICATOR,
                    LPN.QA_FLAG,
                    LPN.PALLET_OPEN_FLAG,
                    LPN.QTY_UOM_ID_BASE,
                    LPN.WEIGHT_UOM_ID_BASE,
                    LPN.VOLUME_UOM_ID_BASE,
                    LPN.SPLIT_LPN_ID,
                    LPN.CREATED_SOURCE_TYPE,
                    LPN.CREATED_SOURCE,
                    LPN.CREATED_DTTM,
                    LPN.EXT_CREATED_DTTM,
                    LPN.LAST_UPDATED_SOURCE_TYPE,
                    LPN.LAST_UPDATED_SOURCE,
                    LPN.LAST_UPDATED_DTTM,
                    LPN.CONVEYABLE_LPN_FLAG,
                    LPN.ACTIVE_LPN_FLAG,
                    LPN.LPN_SHIPPED_FLAG,
                    LPN.STD_QTY_FLAG,
                    LPN.C_FACILITY_ALIAS_ID,
                    LPN.PALLET_MASTER_LPN_FLAG,
                    LPN.MASTER_LPN_FLAG,
                    LPN.LPN_SIZE_TYPE_ID,
                    LPN.PICK_SUB_LOCN_ID,
                    LPN.CURR_SUB_LOCN_ID,
                    LPN.PREV_SUB_LOCN_ID,
                    LPN.DEST_SUB_LOCN_ID,
                    LPN.INBOUND_OUTBOUND_INDICATOR,
                    LPN.WORK_ORD_NBR,
                    LPN.INTERNAL_ORDER_ID,
                    LPN.SHIPPED_DTTM,
                    LPN.TRAILER_STOP_SEQ_NBR,
                    LPN.PACK_WAVE_NBR,
                    LPN.WAVE_NBR,
                    LPN.WAVE_SEQ_NBR,
                    LPN.WAVE_STAT_CODE,
                    LPN.DIRECTED_QTY,
                    LPN.TRANSITIONAL_INVENTORY_TYPE,
                    LPN.PICKER_USERID,
                    LPN.PACKER_USERID,
                    LPN.CHUTE_ID,
                    LPN.CHUTE_ASSIGN_TYPE,
                    LPN.STAGE_INDICATOR,
                    LPN.OUT_OF_ZONE_INDICATOR,
                    LPN.LABEL_PRINT_REQD,
                    LPN.CONSUMPTION_SEQUENCE,
                    LPN.CONSUMPTION_PRIORITY,
                    LPN.CONSUMPTION_PRIORITY_DTTM,
                    LPN.PUTAWAY_TYPE,
                    LPN.RETURN_DISPOSITION_CODE,
                    LPN.FINAL_DISPOSITION_CODE,
                    LPN.LOADED_DTTM,
                    LPN.LOADER_USERID,
                    LPN.LOADED_POSN,
                    LPN.SHIP_BY_DATE,
                    LPN.PICK_DELIVERY_DURATION,
                    LPN.LPN_BREAK_ATTR,
                    LPN.LPN_PRINT_GROUP_CODE,
                    LPN.SEQ_RULE_PRIORITY,
                    LPN.SPUR_LANE,
                    LPN.SPUR_POSITION,
                    LPN.FIRST_ZONE,
                    LPN.LAST_ZONE,
                    LPN.NBR_OF_ZONES,
                    LPN.LPN_DIVERT_CODE,
                    LPN.INTERNAL_ORDER_CONSOL_ATTR,
                    LPN.WHSE_INTERNAL_EVENT_CODE,
                    LPN.SELECTION_RULE_ID,
                    LPN.VOCO_INTRN_REVERSE_ID,
                    LPN.VOCO_INTRN_REVERSE_PALLET_ID,
                    LPN.LPN_CREATION_CODE,
                    LPN.PALLET_X_OF_Y,
                    LPN.INCUBATION_DATE,
                    LPN.CONTAINER_TYPE,
                    LPN.CONTAINER_SIZE,
                    LPN.LPN_NBR_X_OF_Y,
                    LPN.LOAD_SEQUENCE,
                    LPN.MASTER_PACK_ID,
                    LPN.SINGLE_LINE_LPN,
                    LPN.NON_INVENTORY_LPN_FLAG,
                    LPN.EPC_MATCH_FLAG,
                    LPN.AUDITOR_USERID,
                    LPN.PRINTING_RULE_ID,
                    LPN.EXPIRATION_DATE,
                    LPN.QUAL_AUD_STAT_CODE,
                    LPN.DELVRECIPIENTNAME,
                    LPN.DELVRECEIPTDATETIME,
                    LPN.DELVONTIMEFLAG,
                    LPN.DELVCOMPLFLAG,
                    LPN.ITEM_ID,
                    LPN.LPN_FACILITY_STAT_UPDATED_DTTM,
                    LPN.LPN_DISP_STATUS,
                    LPN.OVERSIZE_LENGTH,
                    LPN.BILLING_METHOD,
                    LPN.CUSTOMER_ID,
                    LPN.IS_ZONE_SKIPPED,
                    LPN.ZONESKIP_HUB_FACILITY_ALIAS_ID,
                    LPN.ZONESKIP_HUB_FACILITY_ID,
                    LPN.PLANING_DEST_FACILITY_ALIAS_ID,
                    LPN.PLANING_DEST_FACILITY_ID,
                    LPN.SCHED_SHIP_DTTM,
                    LPN.DISTRIBUTION_LEG_CARRIER_ID,
                    LPN.DISTRIBUTION_LEG_MODE_ID,
                    LPN.DISTRIBUTION_LEV_SVCE_LEVEL_ID,
                    LPN.SERVICE_LEVEL_INDICATOR,
                    LPN.COD_FLAG,
                    LPN.COD_AMOUNT,
                    LPN.COD_PAYMENT_METHOD,
                    LPN.RATED_WEIGHT,
                    LPN.RATE_WEIGHT_TYPE,
                    LPN.RATE_ZONE,
                    LPN.FRT_FORWARDER_ACCT_NBR,
                    LPN.INTL_GOODS_DESC,
                    LPN.SHIPMENT_PRINT_SED,
                    LPN.EXPORT_LICENSE_NUMBER,
                    LPN.EXPORT_LICENSE_SYMBOL,
                    LPN.EXPORT_INFO_CODE,
                    LPN.REPRINT_SHIPPING_LABEL,
                    LPN.PLAN_LOAD_ID,
                    LPN.DOCUMENTS_ONLY,
                    LPN.SCHEDULED_DELIVERY_DTTM,
                    LPN.NON_MACHINEABLE,
                    LPN.D_FACILITY_ID,
                    LPN.D_FACILITY_ALIAS_ID,
                    LPN.VENDOR_LPN_NBR,
                    LPN.PLANNED_TC_ASN_ID,
                    LPN.ESTIMATED_WEIGHT,
                    LPN.ORDER_ID,
                    LPN.MARK_FOR,
                    LPN.PRE_RECEIPT_STATUS,
                    LPN.CROSSDOCK_TC_ORDER_ID,
                    LPN.PHYSICAL_ENTITY_CODE,
                    LPN.PROCESS_IMMEDIATE_NEEDS,
                    LPN.LENGTH,
                    LPN.WIDTH,
                    LPN.HEIGHT,
                    LPN.TOTAL_LPN_QTY,
                    LPN.STATIC_ROUTE_ID,
                    LPN.HAS_ALERTS,
                    LPN.ORDER_SPLIT_ID,
                    LPN.MHE_LOADED,
                    LPN.ITEM_NAME,
                    LPN.ALT_TRACKING_NBR,
                    LPN.RECEIPT_TYPE,
                    LPN.VARIANCE_TYPE,
                    LPN.RETURN_TRACKING_NBR,
                    LPN.CONTAINS_DRY_ICE,
                    LPN.RETURN_REFERENCE_NUMBER,
                    LPN.HAS_NOTES,
                    LPN.RETURN_TRACKING_NBR_2,
                    LPN.DELIVERY_TYPE,
                    LPN.REF_FIELD_1,
                    LPN.REF_FIELD_2,
                    LPN.REF_FIELD_3,
                    LPN.REF_FIELD_4,
                    LPN.REF_FIELD_5,
                    LPN.REF_FIELD_6,
                    LPN.REF_FIELD_7,
                    LPN.REF_FIELD_8,
                    LPN.REF_FIELD_9,
                    LPN.REF_FIELD_10,
                    LPN.REF_NUM1,
                    LPN.REF_NUM2,
                    LPN.REF_NUM3,
                    LPN.REF_NUM4,
                    LPN.REF_NUM5,
                    LPN.REGULATION_SET,
                    LPN.DRY_ICE_WT,
                    LPN.OVERPACK,
                    LPN.SALVAGE_PACK,
                    LPN.SALVAGE_PACK_QTY,
                    LPN.Q_VALUE,
                    LPN.ALL_PAKD_IN_ONE,
                    LPN.SPECIAL_PERMITS,
                    LPN.CN22_NBR,
                    LPN.DISPOSITION_TYPE,
                    LPN.DISPOSITION_SOURCE_ID,
                    LPN.END_OLPN_DTTM,
                    LPN.VOCOLLECT_ASSIGN_ID,
                    LPN.CALCULATED_CUT_OFF_DTTM,
                    LPN.FAILED_GUARANTEED_DELIVERY,
                    LPN.EPI_PACKAGE_ID,
                    LPN.EPI_SHIPMENT_ID,
                    LPN.EPI_MANIFEST_ID,
                    LPN.EPI_PACKAGE_STATUS,
                    LPN.XREF_OLPN,
                    LPN.PALLET_CONTENT_CODE,
                    LPN.EPI_DOC_OUTPUT_TYPE,
                    LPN.ORIGINAL_ASSIGNED_SHIP_VIA,
                    LPN.REF_SHIPMENT_NBR,
                    LPN.RTS_ID,
                    LPN.IS_CHASE_CREATED,
                    LPN.LPN_LEVEL_ROUTING,
                    LPN.PACKWAVE_RULE_ID,
                    LPN.CONS_RUN_ID,
                    LPN.PROCESSED_FOR_TRLR_MOVES,
                    LPN.EXCEPTION_OLPN,
                    LPN.BUILD_LPN_BATCH_ID,
                    LPN.ROUTING_LANE_ID,
                    LPN.ROUTING_LANE_DETAIL_ID,
                    LPN.RATING_LANE_ID,
                    LPN.RATING_LANE_DETAIL_ID,
                    LPN.PENDING_CANCELLATION
                    FROM {source_schema}.LPN
                    WHERE  (trunc(LPN.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (trunc(LPN.EXT_CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (trunc(LPN.LAST_UPDATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1)"""

    SQ_Shortcut_to_LPN = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_LPN is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 282
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LPN_temp = SQ_Shortcut_to_LPN.toDF(*["SQ_Shortcut_to_LPN___" + col for col in SQ_Shortcut_to_LPN.columns])
    
    EXPTRANS = SQ_Shortcut_to_LPN_temp.selectExpr( \
    	"SQ_Shortcut_to_LPN___sys_row_id as sys_row_id", \
    	f"{dcnbr} as DC_NBR_EXP", \
    	"SQ_Shortcut_to_LPN___LPN_ID as LPN_ID", \
    	"SQ_Shortcut_to_LPN___TC_LPN_ID as TC_LPN_ID", \
    	"SQ_Shortcut_to_LPN___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", \
    	"SQ_Shortcut_to_LPN___TC_COMPANY_ID as TC_COMPANY_ID", \
    	"SQ_Shortcut_to_LPN___PARENT_LPN_ID as PARENT_LPN_ID", \
    	"SQ_Shortcut_to_LPN___TC_PARENT_LPN_ID as TC_PARENT_LPN_ID", \
    	"SQ_Shortcut_to_LPN___LPN_TYPE as LPN_TYPE", \
    	"SQ_Shortcut_to_LPN___VERSION_NBR as VERSION_NBR", \
    	"SQ_Shortcut_to_LPN___LPN_MONETARY_VALUE as LPN_MONETARY_VALUE", \
    	"SQ_Shortcut_to_LPN___LPN_MONETARY_VALUE_CURR_CODE as LPN_MONETARY_VALUE_CURR_CODE", \
    	"SQ_Shortcut_to_LPN___TIER_QTY as TIER_QTY", \
    	"SQ_Shortcut_to_LPN___STD_LPN_QTY as STD_LPN_QTY", \
    	"SQ_Shortcut_to_LPN___C_FACILITY_ID as C_FACILITY_ID", \
    	"SQ_Shortcut_to_LPN___O_FACILITY_ID as O_FACILITY_ID", \
    	"SQ_Shortcut_to_LPN___O_FACILITY_ALIAS_ID as O_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_LPN___FINAL_DEST_FACILITY_ALIAS_ID as FINAL_DEST_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_LPN___FINAL_DEST_FACILITY_ID as FINAL_DEST_FACILITY_ID", \
    	"SQ_Shortcut_to_LPN___LPN_STATUS as LPN_STATUS", \
    	"SQ_Shortcut_to_LPN___LPN_STATUS_UPDATED_DTTM as LPN_STATUS_UPDATED_DTTM", \
    	"SQ_Shortcut_to_LPN___LPN_FACILITY_STATUS as LPN_FACILITY_STATUS", \
    	"SQ_Shortcut_to_LPN___TC_REFERENCE_LPN_ID as TC_REFERENCE_LPN_ID", \
    	"SQ_Shortcut_to_LPN___TC_ORDER_ID as TC_ORDER_ID", \
    	"SQ_Shortcut_to_LPN___ORIGINAL_TC_SHIPMENT_ID as ORIGINAL_TC_SHIPMENT_ID", \
    	"SQ_Shortcut_to_LPN___PARCEL_SHIPMENT_NBR as PARCEL_SHIPMENT_NBR", \
    	"SQ_Shortcut_to_LPN___TRACKING_NBR as TRACKING_NBR", \
    	"SQ_Shortcut_to_LPN___MANIFEST_NBR as MANIFEST_NBR", \
    	"SQ_Shortcut_to_LPN___SHIP_VIA as SHIP_VIA", \
    	"SQ_Shortcut_to_LPN___MASTER_BOL_NBR as MASTER_BOL_NBR", \
    	"SQ_Shortcut_to_LPN___BOL_NBR as BOL_NBR", \
    	"SQ_Shortcut_to_LPN___INIT_SHIP_VIA as INIT_SHIP_VIA", \
    	"SQ_Shortcut_to_LPN___PATH_ID as PATH_ID", \
    	"SQ_Shortcut_to_LPN___ROUTE_RULE_ID as ROUTE_RULE_ID", \
    	"SQ_Shortcut_to_LPN___IDEAL_PICK_DTTM as IDEAL_PICK_DTTM", \
    	"SQ_Shortcut_to_LPN___REPRINT_COUNT as REPRINT_COUNT", \
    	"SQ_Shortcut_to_LPN___LAST_FROZEN_DTTM as LAST_FROZEN_DTTM", \
    	"SQ_Shortcut_to_LPN___LAST_COUNTED_DTTM as LAST_COUNTED_DTTM", \
    	"SQ_Shortcut_to_LPN___BASE_CHARGE as BASE_CHARGE", \
    	"SQ_Shortcut_to_LPN___BASE_CHARGE_CURR_CODE as BASE_CHARGE_CURR_CODE", \
    	"SQ_Shortcut_to_LPN___ADDNL_OPTION_CHARGE as ADDNL_OPTION_CHARGE", \
    	"SQ_Shortcut_to_LPN___ADDNL_OPTION_CHARGE_CURR_CODE as ADDNL_OPTION_CHARGE_CURR_CODE", \
    	"SQ_Shortcut_to_LPN___INSUR_CHARGE as INSUR_CHARGE", \
    	"SQ_Shortcut_to_LPN___INSUR_CHARGE_CURR_CODE as INSUR_CHARGE_CURR_CODE", \
    	"SQ_Shortcut_to_LPN___ACTUAL_CHARGE as ACTUAL_CHARGE", \
    	"SQ_Shortcut_to_LPN___ACTUAL_CHARGE_CURR_CODE as ACTUAL_CHARGE_CURR_CODE", \
    	"SQ_Shortcut_to_LPN___PRE_BULK_BASE_CHARGE as PRE_BULK_BASE_CHARGE", \
    	"SQ_Shortcut_to_LPN___PRE_BULK_BASE_CHARGE_CURR_CODE as PRE_BULK_BASE_CHARGE_CURR_CODE", \
    	"SQ_Shortcut_to_LPN___PRE_BULK_ADD_OPT_CHR as PRE_BULK_ADD_OPT_CHR", \
    	"SQ_Shortcut_to_LPN___PRE_BULK_ADD_OPT_CHR_CURR_CODE as PRE_BULK_ADD_OPT_CHR_CURR_CODE", \
    	"SQ_Shortcut_to_LPN___DIST_CHARGE as DIST_CHARGE", \
    	"SQ_Shortcut_to_LPN___DIST_CHARGE_CURR_CODE as DIST_CHARGE_CURR_CODE", \
    	"SQ_Shortcut_to_LPN___FREIGHT_CHARGE as FREIGHT_CHARGE", \
    	"SQ_Shortcut_to_LPN___FREIGHT_CHARGE_CURR_CODE as FREIGHT_CHARGE_CURR_CODE", \
    	"SQ_Shortcut_to_LPN___RECEIVED_TC_SHIPMENT_ID as RECEIVED_TC_SHIPMENT_ID", \
    	"SQ_Shortcut_to_LPN___MANUFACTURED_DTTM as MANUFACTURED_DTTM", \
    	"SQ_Shortcut_to_LPN___MANUFACT_PLANT_FACILITY_ALIAS as MANUFACT_PLANT_FACILITY_ALIAS", \
    	"SQ_Shortcut_to_LPN___RCVD_DTTM as RCVD_DTTM", \
    	"SQ_Shortcut_to_LPN___TC_PURCHASE_ORDERS_ID as TC_PURCHASE_ORDERS_ID", \
    	"SQ_Shortcut_to_LPN___PURCHASE_ORDERS_ID as PURCHASE_ORDERS_ID", \
    	"SQ_Shortcut_to_LPN___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
    	"SQ_Shortcut_to_LPN___SHIPMENT_ID as SHIPMENT_ID", \
    	"SQ_Shortcut_to_LPN___TC_ASN_ID as TC_ASN_ID", \
    	"SQ_Shortcut_to_LPN___ASN_ID as ASN_ID", \
    	"SQ_Shortcut_to_LPN___RECEIPT_VARIANCE_INDICATOR as RECEIPT_VARIANCE_INDICATOR", \
    	"SQ_Shortcut_to_LPN___ESTIMATED_VOLUME as ESTIMATED_VOLUME", \
    	"SQ_Shortcut_to_LPN___WEIGHT as WEIGHT", \
    	"SQ_Shortcut_to_LPN___ACTUAL_VOLUME as ACTUAL_VOLUME", \
    	"SQ_Shortcut_to_LPN___LPN_LABEL_TYPE as LPN_LABEL_TYPE", \
    	"SQ_Shortcut_to_LPN___PACKAGE_TYPE_ID as PACKAGE_TYPE_ID", \
    	"SQ_Shortcut_to_LPN___MISC_INSTR_CODE_1 as MISC_INSTR_CODE_1", \
    	"SQ_Shortcut_to_LPN___MISC_INSTR_CODE_2 as MISC_INSTR_CODE_2", \
    	"SQ_Shortcut_to_LPN___MISC_INSTR_CODE_3 as MISC_INSTR_CODE_3", \
    	"SQ_Shortcut_to_LPN___MISC_INSTR_CODE_4 as MISC_INSTR_CODE_4", \
    	"SQ_Shortcut_to_LPN___MISC_INSTR_CODE_5 as MISC_INSTR_CODE_5", \
    	"SQ_Shortcut_to_LPN___MISC_NUM_1 as MISC_NUM_1", \
    	"SQ_Shortcut_to_LPN___MISC_NUM_2 as MISC_NUM_2", \
    	"SQ_Shortcut_to_LPN___HIBERNATE_VERSION as HIBERNATE_VERSION", \
    	"SQ_Shortcut_to_LPN___CUBE_UOM as CUBE_UOM", \
    	"SQ_Shortcut_to_LPN___ERROR_INDICATOR as ERROR_INDICATOR", \
    	"SQ_Shortcut_to_LPN___WARNING_INDICATOR as WARNING_INDICATOR", \
    	"SQ_Shortcut_to_LPN___QA_FLAG as QA_FLAG", \
    	"SQ_Shortcut_to_LPN___PALLET_OPEN_FLAG as PALLET_OPEN_FLAG", \
    	"SQ_Shortcut_to_LPN___QTY_UOM_ID_BASE as QTY_UOM_ID_BASE", \
    	"SQ_Shortcut_to_LPN___WEIGHT_UOM_ID_BASE as WEIGHT_UOM_ID_BASE", \
    	"SQ_Shortcut_to_LPN___VOLUME_UOM_ID_BASE as VOLUME_UOM_ID_BASE", \
    	"SQ_Shortcut_to_LPN___SPLIT_LPN_ID as SPLIT_LPN_ID", \
    	"SQ_Shortcut_to_LPN___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
    	"SQ_Shortcut_to_LPN___CREATED_SOURCE as CREATED_SOURCE", \
    	"SQ_Shortcut_to_LPN___CREATED_DTTM as CREATED_DTTM", \
    	"SQ_Shortcut_to_LPN___EXT_CREATED_DTTM as EXT_CREATED_DTTM", \
    	"SQ_Shortcut_to_LPN___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
    	"SQ_Shortcut_to_LPN___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
    	"SQ_Shortcut_to_LPN___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
    	"SQ_Shortcut_to_LPN___CONVEYABLE_LPN_FLAG as CONVEYABLE_LPN_FLAG", \
    	"SQ_Shortcut_to_LPN___ACTIVE_LPN_FLAG as ACTIVE_LPN_FLAG", \
    	"SQ_Shortcut_to_LPN___LPN_SHIPPED_FLAG as LPN_SHIPPED_FLAG", \
    	"SQ_Shortcut_to_LPN___STD_QTY_FLAG as STD_QTY_FLAG", \
    	"SQ_Shortcut_to_LPN___C_FACILITY_ALIAS_ID as C_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_LPN___PALLET_MASTER_LPN_FLAG as PALLET_MASTER_LPN_FLAG", \
    	"SQ_Shortcut_to_LPN___MASTER_LPN_FLAG as MASTER_LPN_FLAG", \
    	"SQ_Shortcut_to_LPN___LPN_SIZE_TYPE_ID as LPN_SIZE_TYPE_ID", \
    	"SQ_Shortcut_to_LPN___PICK_SUB_LOCN_ID as PICK_SUB_LOCN_ID", \
    	"SQ_Shortcut_to_LPN___CURR_SUB_LOCN_ID as CURR_SUB_LOCN_ID", \
    	"SQ_Shortcut_to_LPN___PREV_SUB_LOCN_ID as PREV_SUB_LOCN_ID", \
    	"SQ_Shortcut_to_LPN___DEST_SUB_LOCN_ID as DEST_SUB_LOCN_ID", \
    	"SQ_Shortcut_to_LPN___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
    	"SQ_Shortcut_to_LPN___WORK_ORD_NBR as WORK_ORD_NBR", \
    	"SQ_Shortcut_to_LPN___INTERNAL_ORDER_ID as INTERNAL_ORDER_ID", \
    	"SQ_Shortcut_to_LPN___SHIPPED_DTTM as SHIPPED_DTTM", \
    	"SQ_Shortcut_to_LPN___TRAILER_STOP_SEQ_NBR as TRAILER_STOP_SEQ_NBR", \
    	"SQ_Shortcut_to_LPN___PACK_WAVE_NBR as PACK_WAVE_NBR", \
    	"SQ_Shortcut_to_LPN___WAVE_NBR as WAVE_NBR", \
    	"SQ_Shortcut_to_LPN___WAVE_SEQ_NBR as WAVE_SEQ_NBR", \
    	"SQ_Shortcut_to_LPN___WAVE_STAT_CODE as WAVE_STAT_CODE", \
    	"SQ_Shortcut_to_LPN___DIRECTED_QTY as DIRECTED_QTY", \
    	"SQ_Shortcut_to_LPN___TRANSITIONAL_INVENTORY_TYPE as TRANSITIONAL_INVENTORY_TYPE", \
    	"SQ_Shortcut_to_LPN___PICKER_USERID as PICKER_USERID", \
    	"SQ_Shortcut_to_LPN___PACKER_USERID as PACKER_USERID", \
    	"SQ_Shortcut_to_LPN___CHUTE_ID as CHUTE_ID", \
    	"SQ_Shortcut_to_LPN___CHUTE_ASSIGN_TYPE as CHUTE_ASSIGN_TYPE", \
    	"SQ_Shortcut_to_LPN___STAGE_INDICATOR as STAGE_INDICATOR", \
    	"SQ_Shortcut_to_LPN___OUT_OF_ZONE_INDICATOR as OUT_OF_ZONE_INDICATOR", \
    	"SQ_Shortcut_to_LPN___LABEL_PRINT_REQD as LABEL_PRINT_REQD", \
    	"SQ_Shortcut_to_LPN___CONSUMPTION_SEQUENCE as CONSUMPTION_SEQUENCE", \
    	"SQ_Shortcut_to_LPN___CONSUMPTION_PRIORITY as CONSUMPTION_PRIORITY", \
    	"SQ_Shortcut_to_LPN___CONSUMPTION_PRIORITY_DTTM as CONSUMPTION_PRIORITY_DTTM", \
    	"SQ_Shortcut_to_LPN___PUTAWAY_TYPE as PUTAWAY_TYPE", \
    	"SQ_Shortcut_to_LPN___RETURN_DISPOSITION_CODE as RETURN_DISPOSITION_CODE", \
    	"SQ_Shortcut_to_LPN___FINAL_DISPOSITION_CODE as FINAL_DISPOSITION_CODE", \
    	"SQ_Shortcut_to_LPN___LOADED_DTTM as LOADED_DTTM", \
    	"SQ_Shortcut_to_LPN___LOADER_USERID as LOADER_USERID", \
    	"SQ_Shortcut_to_LPN___LOADED_POSN as LOADED_POSN", \
    	"SQ_Shortcut_to_LPN___SHIP_BY_DATE as SHIP_BY_DATE", \
    	"SQ_Shortcut_to_LPN___PICK_DELIVERY_DURATION as PICK_DELIVERY_DURATION", \
    	"SQ_Shortcut_to_LPN___LPN_BREAK_ATTR as LPN_BREAK_ATTR", \
    	"SQ_Shortcut_to_LPN___LPN_PRINT_GROUP_CODE as LPN_PRINT_GROUP_CODE", \
    	"SQ_Shortcut_to_LPN___SEQ_RULE_PRIORITY as SEQ_RULE_PRIORITY", \
    	"SQ_Shortcut_to_LPN___SPUR_LANE as SPUR_LANE", \
    	"SQ_Shortcut_to_LPN___SPUR_POSITION as SPUR_POSITION", \
    	"SQ_Shortcut_to_LPN___FIRST_ZONE as FIRST_ZONE", \
    	"SQ_Shortcut_to_LPN___LAST_ZONE as LAST_ZONE", \
    	"SQ_Shortcut_to_LPN___NBR_OF_ZONES as NBR_OF_ZONES", \
    	"SQ_Shortcut_to_LPN___LPN_DIVERT_CODE as LPN_DIVERT_CODE", \
    	"SQ_Shortcut_to_LPN___INTERNAL_ORDER_CONSOL_ATTR as INTERNAL_ORDER_CONSOL_ATTR", \
    	"SQ_Shortcut_to_LPN___WHSE_INTERNAL_EVENT_CODE as WHSE_INTERNAL_EVENT_CODE", \
    	"SQ_Shortcut_to_LPN___SELECTION_RULE_ID as SELECTION_RULE_ID", \
    	"SQ_Shortcut_to_LPN___VOCO_INTRN_REVERSE_ID as VOCO_INTRN_REVERSE_ID", \
    	"SQ_Shortcut_to_LPN___VOCO_INTRN_REVERSE_PALLET_ID as VOCO_INTRN_REVERSE_PALLET_ID", \
    	"SQ_Shortcut_to_LPN___LPN_CREATION_CODE as LPN_CREATION_CODE", \
    	"SQ_Shortcut_to_LPN___PALLET_X_OF_Y as PALLET_X_OF_Y", \
    	"SQ_Shortcut_to_LPN___INCUBATION_DATE as INCUBATION_DATE", \
    	"SQ_Shortcut_to_LPN___CONTAINER_TYPE as CONTAINER_TYPE", \
    	"SQ_Shortcut_to_LPN___CONTAINER_SIZE as CONTAINER_SIZE", \
    	"SQ_Shortcut_to_LPN___LPN_NBR_X_OF_Y as LPN_NBR_X_OF_Y", \
    	"SQ_Shortcut_to_LPN___LOAD_SEQUENCE as LOAD_SEQUENCE", \
    	"SQ_Shortcut_to_LPN___MASTER_PACK_ID as MASTER_PACK_ID", \
    	"SQ_Shortcut_to_LPN___SINGLE_LINE_LPN as SINGLE_LINE_LPN", \
    	"SQ_Shortcut_to_LPN___NON_INVENTORY_LPN_FLAG as NON_INVENTORY_LPN_FLAG", \
    	"SQ_Shortcut_to_LPN___EPC_MATCH_FLAG as EPC_MATCH_FLAG", \
    	"SQ_Shortcut_to_LPN___AUDITOR_USERID as AUDITOR_USERID", \
    	"SQ_Shortcut_to_LPN___PRINTING_RULE_ID as PRINTING_RULE_ID", \
    	"SQ_Shortcut_to_LPN___EXPIRATION_DATE as EXPIRATION_DATE", \
    	"SQ_Shortcut_to_LPN___QUAL_AUD_STAT_CODE as QUAL_AUD_STAT_CODE", \
    	"SQ_Shortcut_to_LPN___DELVRECIPIENTNAME as DELVRECIPIENTNAME", \
    	"SQ_Shortcut_to_LPN___DELVRECEIPTDATETIME as DELVRECEIPTDATETIME", \
    	"SQ_Shortcut_to_LPN___DELVONTIMEFLAG as DELVONTIMEFLAG", \
    	"SQ_Shortcut_to_LPN___DELVCOMPLFLAG as DELVCOMPLFLAG", \
    	"SQ_Shortcut_to_LPN___ITEM_ID as ITEM_ID", \
    	"SQ_Shortcut_to_LPN___LPN_FACILITY_STAT_UPDATED_DTTM as LPN_FACILITY_STAT_UPDATED_DTTM", \
    	"SQ_Shortcut_to_LPN___LPN_DISP_STATUS as LPN_DISP_STATUS", \
    	"SQ_Shortcut_to_LPN___OVERSIZE_LENGTH as OVERSIZE_LENGTH", \
    	"SQ_Shortcut_to_LPN___BILLING_METHOD as BILLING_METHOD", \
    	"SQ_Shortcut_to_LPN___CUSTOMER_ID as CUSTOMER_ID", \
    	"SQ_Shortcut_to_LPN___IS_ZONE_SKIPPED as IS_ZONE_SKIPPED", \
    	"SQ_Shortcut_to_LPN___ZONESKIP_HUB_FACILITY_ALIAS_ID as ZONESKIP_HUB_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_LPN___ZONESKIP_HUB_FACILITY_ID as ZONESKIP_HUB_FACILITY_ID", \
    	"SQ_Shortcut_to_LPN___PLANING_DEST_FACILITY_ALIAS_ID as PLANING_DEST_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_LPN___PLANING_DEST_FACILITY_ID as PLANING_DEST_FACILITY_ID", \
    	"SQ_Shortcut_to_LPN___SCHED_SHIP_DTTM as SCHED_SHIP_DTTM", \
    	"SQ_Shortcut_to_LPN___DISTRIBUTION_LEG_CARRIER_ID as DISTRIBUTION_LEG_CARRIER_ID", \
    	"SQ_Shortcut_to_LPN___DISTRIBUTION_LEG_MODE_ID as DISTRIBUTION_LEG_MODE_ID", \
    	"SQ_Shortcut_to_LPN___DISTRIBUTION_LEV_SVCE_LEVEL_ID as DISTRIBUTION_LEV_SVCE_LEVEL_ID", \
    	"SQ_Shortcut_to_LPN___SERVICE_LEVEL_INDICATOR as SERVICE_LEVEL_INDICATOR", \
    	"SQ_Shortcut_to_LPN___COD_FLAG as COD_FLAG", \
    	"SQ_Shortcut_to_LPN___COD_AMOUNT as COD_AMOUNT", \
    	"SQ_Shortcut_to_LPN___COD_PAYMENT_METHOD as COD_PAYMENT_METHOD", \
    	"SQ_Shortcut_to_LPN___RATED_WEIGHT as RATED_WEIGHT", \
    	"SQ_Shortcut_to_LPN___RATE_WEIGHT_TYPE as RATE_WEIGHT_TYPE", \
    	"SQ_Shortcut_to_LPN___RATE_ZONE as RATE_ZONE", \
    	"SQ_Shortcut_to_LPN___FRT_FORWARDER_ACCT_NBR as FRT_FORWARDER_ACCT_NBR", \
    	"SQ_Shortcut_to_LPN___INTL_GOODS_DESC as INTL_GOODS_DESC", \
    	"SQ_Shortcut_to_LPN___SHIPMENT_PRINT_SED as SHIPMENT_PRINT_SED", \
    	"SQ_Shortcut_to_LPN___EXPORT_LICENSE_NUMBER as EXPORT_LICENSE_NUMBER", \
    	"SQ_Shortcut_to_LPN___EXPORT_LICENSE_SYMBOL as EXPORT_LICENSE_SYMBOL", \
    	"SQ_Shortcut_to_LPN___EXPORT_INFO_CODE as EXPORT_INFO_CODE", \
    	"SQ_Shortcut_to_LPN___REPRINT_SHIPPING_LABEL as REPRINT_SHIPPING_LABEL", \
    	"SQ_Shortcut_to_LPN___PLAN_LOAD_ID as PLAN_LOAD_ID", \
    	"SQ_Shortcut_to_LPN___DOCUMENTS_ONLY as DOCUMENTS_ONLY", \
    	"SQ_Shortcut_to_LPN___SCHEDULED_DELIVERY_DTTM as SCHEDULED_DELIVERY_DTTM", \
    	"SQ_Shortcut_to_LPN___NON_MACHINEABLE as NON_MACHINEABLE", \
    	"SQ_Shortcut_to_LPN___D_FACILITY_ID as D_FACILITY_ID", \
    	"SQ_Shortcut_to_LPN___D_FACILITY_ALIAS_ID as D_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_LPN___VENDOR_LPN_NBR as VENDOR_LPN_NBR", \
    	"SQ_Shortcut_to_LPN___PLANNED_TC_ASN_ID as PLANNED_TC_ASN_ID", \
    	"SQ_Shortcut_to_LPN___ESTIMATED_WEIGHT as ESTIMATED_WEIGHT", \
    	"SQ_Shortcut_to_LPN___ORDER_ID as ORDER_ID", \
    	"SQ_Shortcut_to_LPN___MARK_FOR as MARK_FOR", \
    	"SQ_Shortcut_to_LPN___PRE_RECEIPT_STATUS as PRE_RECEIPT_STATUS", \
    	"SQ_Shortcut_to_LPN___CROSSDOCK_TC_ORDER_ID as CROSSDOCK_TC_ORDER_ID", \
    	"SQ_Shortcut_to_LPN___PHYSICAL_ENTITY_CODE as PHYSICAL_ENTITY_CODE", \
    	"SQ_Shortcut_to_LPN___PROCESS_IMMEDIATE_NEEDS as PROCESS_IMMEDIATE_NEEDS", \
    	"SQ_Shortcut_to_LPN___LENGTH as LENGTH", \
    	"SQ_Shortcut_to_LPN___WIDTH as WIDTH", \
    	"SQ_Shortcut_to_LPN___HEIGHT as HEIGHT", \
    	"SQ_Shortcut_to_LPN___TOTAL_LPN_QTY as TOTAL_LPN_QTY", \
    	"SQ_Shortcut_to_LPN___STATIC_ROUTE_ID as STATIC_ROUTE_ID", \
    	"SQ_Shortcut_to_LPN___HAS_ALERTS as HAS_ALERTS", \
    	"SQ_Shortcut_to_LPN___ORDER_SPLIT_ID as ORDER_SPLIT_ID", \
    	"SQ_Shortcut_to_LPN___MHE_LOADED as MHE_LOADED", \
    	"SQ_Shortcut_to_LPN___ITEM_NAME as ITEM_NAME", \
    	"SQ_Shortcut_to_LPN___ALT_TRACKING_NBR as ALT_TRACKING_NBR", \
    	"SQ_Shortcut_to_LPN___RECEIPT_TYPE as RECEIPT_TYPE", \
    	"SQ_Shortcut_to_LPN___VARIANCE_TYPE as VARIANCE_TYPE", \
    	"SQ_Shortcut_to_LPN___RETURN_TRACKING_NBR as RETURN_TRACKING_NBR", \
    	"SQ_Shortcut_to_LPN___CONTAINS_DRY_ICE as CONTAINS_DRY_ICE", \
    	"SQ_Shortcut_to_LPN___RETURN_REFERENCE_NUMBER as RETURN_REFERENCE_NUMBER", \
    	"SQ_Shortcut_to_LPN___HAS_NOTES as HAS_NOTES", \
    	"SQ_Shortcut_to_LPN___RETURN_TRACKING_NBR_2 as RETURN_TRACKING_NBR_2", \
    	"SQ_Shortcut_to_LPN___DELIVERY_TYPE as DELIVERY_TYPE", \
    	"SQ_Shortcut_to_LPN___REF_FIELD_1 as REF_FIELD_1", \
    	"SQ_Shortcut_to_LPN___REF_FIELD_2 as REF_FIELD_2", \
    	"SQ_Shortcut_to_LPN___REF_FIELD_3 as REF_FIELD_3", \
    	"SQ_Shortcut_to_LPN___REF_FIELD_4 as REF_FIELD_4", \
    	"SQ_Shortcut_to_LPN___REF_FIELD_5 as REF_FIELD_5", \
    	"SQ_Shortcut_to_LPN___REF_FIELD_6 as REF_FIELD_6", \
    	"SQ_Shortcut_to_LPN___REF_FIELD_7 as REF_FIELD_7", \
    	"SQ_Shortcut_to_LPN___REF_FIELD_8 as REF_FIELD_8", \
    	"SQ_Shortcut_to_LPN___REF_FIELD_9 as REF_FIELD_9", \
    	"SQ_Shortcut_to_LPN___REF_FIELD_10 as REF_FIELD_10", \
    	"SQ_Shortcut_to_LPN___REF_NUM1 as REF_NUM1", \
    	"SQ_Shortcut_to_LPN___REF_NUM2 as REF_NUM2", \
    	"SQ_Shortcut_to_LPN___REF_NUM3 as REF_NUM3", \
    	"SQ_Shortcut_to_LPN___REF_NUM4 as REF_NUM4", \
    	"SQ_Shortcut_to_LPN___REF_NUM5 as REF_NUM5", \
    	"SQ_Shortcut_to_LPN___REGULATION_SET as REGULATION_SET", \
    	"SQ_Shortcut_to_LPN___DRY_ICE_WT as DRY_ICE_WT", \
    	"SQ_Shortcut_to_LPN___OVERPACK as OVERPACK", \
    	"SQ_Shortcut_to_LPN___SALVAGE_PACK as SALVAGE_PACK", \
    	"SQ_Shortcut_to_LPN___SALVAGE_PACK_QTY as SALVAGE_PACK_QTY", \
    	"SQ_Shortcut_to_LPN___Q_VALUE as Q_VALUE", \
    	"SQ_Shortcut_to_LPN___ALL_PAKD_IN_ONE as ALL_PAKD_IN_ONE", \
    	"SQ_Shortcut_to_LPN___SPECIAL_PERMITS as SPECIAL_PERMITS", \
    	"SQ_Shortcut_to_LPN___CN22_NBR as CN22_NBR", \
    	"SQ_Shortcut_to_LPN___DISPOSITION_TYPE as DISPOSITION_TYPE", \
    	"SQ_Shortcut_to_LPN___DISPOSITION_SOURCE_ID as DISPOSITION_SOURCE_ID", \
    	"SQ_Shortcut_to_LPN___END_OLPN_DTTM as END_OLPN_DTTM", \
    	"SQ_Shortcut_to_LPN___VOCOLLECT_ASSIGN_ID as VOCOLLECT_ASSIGN_ID", \
    	"SQ_Shortcut_to_LPN___CALCULATED_CUT_OFF_DTTM as CALCULATED_CUT_OFF_DTTM", \
    	"SQ_Shortcut_to_LPN___FAILED_GUARANTEED_DELIVERY as FAILED_GUARANTEED_DELIVERY", \
    	"SQ_Shortcut_to_LPN___EPI_PACKAGE_ID as EPI_PACKAGE_ID", \
    	"SQ_Shortcut_to_LPN___EPI_SHIPMENT_ID as EPI_SHIPMENT_ID", \
    	"SQ_Shortcut_to_LPN___EPI_MANIFEST_ID as EPI_MANIFEST_ID", \
    	"SQ_Shortcut_to_LPN___EPI_PACKAGE_STATUS as EPI_PACKAGE_STATUS", \
    	"SQ_Shortcut_to_LPN___XREF_OLPN as XREF_OLPN", \
    	"SQ_Shortcut_to_LPN___PALLET_CONTENT_CODE as PALLET_CONTENT_CODE", \
    	"SQ_Shortcut_to_LPN___EPI_DOC_OUTPUT_TYPE as EPI_DOC_OUTPUT_TYPE", \
    	"SQ_Shortcut_to_LPN___ORIGINAL_ASSIGNED_SHIP_VIA as ORIGINAL_ASSIGNED_SHIP_VIA", \
    	"SQ_Shortcut_to_LPN___REF_SHIPMENT_NBR as REF_SHIPMENT_NBR", \
    	"SQ_Shortcut_to_LPN___RTS_ID as RTS_ID", \
    	"SQ_Shortcut_to_LPN___IS_CHASE_CREATED as IS_CHASE_CREATED", \
    	"SQ_Shortcut_to_LPN___LPN_LEVEL_ROUTING as LPN_LEVEL_ROUTING", \
    	"SQ_Shortcut_to_LPN___PACKWAVE_RULE_ID as PACKWAVE_RULE_ID", \
    	"SQ_Shortcut_to_LPN___CONS_RUN_ID as CONS_RUN_ID", \
    	"SQ_Shortcut_to_LPN___PROCESSED_FOR_TRLR_MOVES as PROCESSED_FOR_TRLR_MOVES", \
    	"SQ_Shortcut_to_LPN___EXCEPTION_OLPN as EXCEPTION_OLPN", \
    	"SQ_Shortcut_to_LPN___BUILD_LPN_BATCH_ID as BUILD_LPN_BATCH_ID", \
    	"SQ_Shortcut_to_LPN___ROUTING_LANE_ID as ROUTING_LANE_ID", \
    	"SQ_Shortcut_to_LPN___ROUTING_LANE_DETAIL_ID as ROUTING_LANE_DETAIL_ID", \
    	"SQ_Shortcut_to_LPN___RATING_LANE_ID as RATING_LANE_ID", \
    	"SQ_Shortcut_to_LPN___RATING_LANE_DETAIL_ID as RATING_LANE_DETAIL_ID", \
    	"SQ_Shortcut_to_LPN___PENDING_CANCELLATION as PENDING_CANCELLATION", \
    	"CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_LPN_PRE, type TARGET 
    # COLUMN COUNT: 282


    Shortcut_to_WM_LPN_PRE = EXPTRANS.selectExpr(
    "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
    "CAST(LPN_ID AS BIGINT) as LPN_ID",
    "CAST(TC_LPN_ID AS STRING) as TC_LPN_ID",
    "CAST(BUSINESS_PARTNER_ID AS STRING) as BUSINESS_PARTNER_ID",
    "CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID",
    "CAST(PARENT_LPN_ID AS BIGINT) as PARENT_LPN_ID",
    "CAST(TC_PARENT_LPN_ID AS STRING) as TC_PARENT_LPN_ID",
    "CAST(LPN_TYPE AS SMALLINT) as LPN_TYPE",
    "CAST(VERSION_NBR AS BIGINT) as VERSION_NBR",
    "CAST(LPN_MONETARY_VALUE AS DECIMAL(11,2)) as LPN_MONETARY_VALUE",
    "CAST(LPN_MONETARY_VALUE_CURR_CODE AS STRING) as LPN_MONETARY_VALUE_CURR_CODE",
    "CAST(TIER_QTY AS DECIMAL(9,2)) as TIER_QTY",
    "CAST(STD_LPN_QTY AS DECIMAL(13,4)) as STD_LPN_QTY",
    "CAST(C_FACILITY_ID AS BIGINT) as C_FACILITY_ID",
    "CAST(O_FACILITY_ID AS BIGINT) as O_FACILITY_ID",
    "CAST(O_FACILITY_ALIAS_ID AS STRING) as O_FACILITY_ALIAS_ID",
    "CAST(FINAL_DEST_FACILITY_ALIAS_ID AS STRING) as FINAL_DEST_FACILITY_ALIAS_ID",
    "CAST(FINAL_DEST_FACILITY_ID AS BIGINT) as FINAL_DEST_FACILITY_ID",
    "CAST(LPN_STATUS AS SMALLINT) as LPN_STATUS",
    "CAST(LPN_STATUS_UPDATED_DTTM AS TIMESTAMP) as LPN_STATUS_UPDATED_DTTM",
    "CAST(LPN_FACILITY_STATUS AS SMALLINT) as LPN_FACILITY_STATUS",
    "CAST(TC_REFERENCE_LPN_ID AS STRING) as TC_REFERENCE_LPN_ID",
    "CAST(TC_ORDER_ID AS STRING) as TC_ORDER_ID",
    "CAST(ORIGINAL_TC_SHIPMENT_ID AS STRING) as ORIGINAL_TC_SHIPMENT_ID",
    "CAST(PARCEL_SHIPMENT_NBR AS STRING) as PARCEL_SHIPMENT_NBR",
    "CAST(TRACKING_NBR AS STRING) as TRACKING_NBR",
    "CAST(MANIFEST_NBR AS STRING) as MANIFEST_NBR",
    "CAST(SHIP_VIA AS STRING) as SHIP_VIA",
    "CAST(MASTER_BOL_NBR AS STRING) as MASTER_BOL_NBR",
    "CAST(BOL_NBR AS STRING) as BOL_NBR",
    "CAST(INIT_SHIP_VIA AS STRING) as INIT_SHIP_VIA",
    "CAST(PATH_ID AS BIGINT) as PATH_ID",
    "CAST(ROUTE_RULE_ID AS INT) as ROUTE_RULE_ID",
    "CAST(IDEAL_PICK_DTTM AS TIMESTAMP) as IDEAL_PICK_DTTM",
    "CAST(REPRINT_COUNT AS TINYINT) as REPRINT_COUNT",
    "CAST(LAST_FROZEN_DTTM AS TIMESTAMP) as LAST_FROZEN_DTTM",
    "CAST(LAST_COUNTED_DTTM AS TIMESTAMP) as LAST_COUNTED_DTTM",
    "CAST(BASE_CHARGE AS DECIMAL(9,2)) as BASE_CHARGE",
    "CAST(BASE_CHARGE_CURR_CODE AS STRING) as BASE_CHARGE_CURR_CODE",
    "CAST(ADDNL_OPTION_CHARGE AS DECIMAL(9,2)) as ADDNL_OPTION_CHARGE",
    "CAST(ADDNL_OPTION_CHARGE_CURR_CODE AS STRING) as ADDNL_OPTION_CHARGE_CURR_CODE",
    "CAST(INSUR_CHARGE AS DECIMAL(9,2)) as INSUR_CHARGE",
    "CAST(INSUR_CHARGE_CURR_CODE AS STRING) as INSUR_CHARGE_CURR_CODE",
    "CAST(ACTUAL_CHARGE AS DECIMAL(9,2)) as ACTUAL_CHARGE",
    "CAST(ACTUAL_CHARGE_CURR_CODE AS STRING) as ACTUAL_CHARGE_CURR_CODE",
    "CAST(PRE_BULK_BASE_CHARGE AS DECIMAL(9,2)) as PRE_BULK_BASE_CHARGE",
    "CAST(PRE_BULK_BASE_CHARGE_CURR_CODE AS STRING) as PRE_BULK_BASE_CHARGE_CURR_CODE",
    "CAST(PRE_BULK_ADD_OPT_CHR AS DECIMAL(9,2)) as PRE_BULK_ADD_OPT_CHR",
    "CAST(PRE_BULK_ADD_OPT_CHR_CURR_CODE AS STRING) as PRE_BULK_ADD_OPT_CHR_CURR_CODE",
    "CAST(DIST_CHARGE AS DECIMAL(11,2)) as DIST_CHARGE",
    "CAST(DIST_CHARGE_CURR_CODE AS STRING) as DIST_CHARGE_CURR_CODE",
    "CAST(FREIGHT_CHARGE AS DECIMAL(11,2)) as FREIGHT_CHARGE",
    "CAST(FREIGHT_CHARGE_CURR_CODE AS STRING) as FREIGHT_CHARGE_CURR_CODE",
    "CAST(RECEIVED_TC_SHIPMENT_ID AS STRING) as RECEIVED_TC_SHIPMENT_ID",
    "CAST(MANUFACTURED_DTTM AS TIMESTAMP) as MANUFACTURED_DTTM",
    "CAST(MANUFACT_PLANT_FACILITY_ALIAS AS STRING) as MANUFACT_PLANT_FACILITY_ALIAS",
    "CAST(RCVD_DTTM AS TIMESTAMP) as RCVD_DTTM",
    "CAST(TC_PURCHASE_ORDERS_ID AS STRING) as TC_PURCHASE_ORDERS_ID",
    "CAST(PURCHASE_ORDERS_ID AS BIGINT) as PURCHASE_ORDERS_ID",
    "CAST(TC_SHIPMENT_ID AS STRING) as TC_SHIPMENT_ID",
    "CAST(SHIPMENT_ID AS BIGINT) as SHIPMENT_ID",
    "CAST(TC_ASN_ID AS STRING) as TC_ASN_ID",
    "CAST(ASN_ID AS BIGINT) as ASN_ID",
    "CAST(RECEIPT_VARIANCE_INDICATOR AS SMALLINT) as RECEIPT_VARIANCE_INDICATOR",
    "CAST(ESTIMATED_VOLUME AS DECIMAL(13,4)) as ESTIMATED_VOLUME",
    "CAST(WEIGHT AS DECIMAL(16,4)) as WEIGHT",
    "CAST(ACTUAL_VOLUME AS DECIMAL(13,4)) as ACTUAL_VOLUME",
    "CAST(LPN_LABEL_TYPE AS STRING) as LPN_LABEL_TYPE",
    "CAST(PACKAGE_TYPE_ID AS INT) as PACKAGE_TYPE_ID",
    "CAST(MISC_INSTR_CODE_1 AS STRING) as MISC_INSTR_CODE_1",
    "CAST(MISC_INSTR_CODE_2 AS STRING) as MISC_INSTR_CODE_2",
    "CAST(MISC_INSTR_CODE_3 AS STRING) as MISC_INSTR_CODE_3",
    "CAST(MISC_INSTR_CODE_4 AS STRING) as MISC_INSTR_CODE_4",
    "CAST(MISC_INSTR_CODE_5 AS STRING) as MISC_INSTR_CODE_5",
    "CAST(MISC_NUM_1 AS DECIMAL(9,2)) as MISC_NUM_1",
    "CAST(MISC_NUM_2 AS DECIMAL(9,2)) as MISC_NUM_2",
    "CAST(HIBERNATE_VERSION AS INT) as HIBERNATE_VERSION",
    "CAST(CUBE_UOM AS STRING) as CUBE_UOM",
    "CAST(ERROR_INDICATOR AS SMALLINT) as ERROR_INDICATOR",
    "CAST(WARNING_INDICATOR AS SMALLINT) as WARNING_INDICATOR",
    "CAST(QA_FLAG AS STRING) as QA_FLAG",
    "CAST(PALLET_OPEN_FLAG AS TINYINT) as PALLET_OPEN_FLAG",
    "CAST(QTY_UOM_ID_BASE AS INT) as QTY_UOM_ID_BASE",
    "CAST(WEIGHT_UOM_ID_BASE AS INT) as WEIGHT_UOM_ID_BASE",
    "CAST(VOLUME_UOM_ID_BASE AS INT) as VOLUME_UOM_ID_BASE",
    "CAST(SPLIT_LPN_ID AS BIGINT) as SPLIT_LPN_ID",
    "CAST(CREATED_SOURCE_TYPE AS SMALLINT) as CREATED_SOURCE_TYPE",
    "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
    "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
    "CAST(EXT_CREATED_DTTM AS TIMESTAMP) as EXT_CREATED_DTTM",
    "CAST(LAST_UPDATED_SOURCE_TYPE AS SMALLINT) as LAST_UPDATED_SOURCE_TYPE",
    "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
    "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
    "CAST(CONVEYABLE_LPN_FLAG AS SMALLINT) as CONVEYABLE_LPN_FLAG",
    "CAST(ACTIVE_LPN_FLAG AS SMALLINT) as ACTIVE_LPN_FLAG",
    "CAST(LPN_SHIPPED_FLAG AS SMALLINT) as LPN_SHIPPED_FLAG",
    "CAST(STD_QTY_FLAG AS SMALLINT) as STD_QTY_FLAG",
    "CAST(C_FACILITY_ALIAS_ID AS STRING) as C_FACILITY_ALIAS_ID",
    "CAST(PALLET_MASTER_LPN_FLAG AS TINYINT) as PALLET_MASTER_LPN_FLAG",
    "CAST(MASTER_LPN_FLAG AS STRING) as MASTER_LPN_FLAG",
    "CAST(LPN_SIZE_TYPE_ID AS INT) as LPN_SIZE_TYPE_ID",
    "CAST(PICK_SUB_LOCN_ID AS STRING) as PICK_SUB_LOCN_ID",
    "CAST(CURR_SUB_LOCN_ID AS STRING) as CURR_SUB_LOCN_ID",
    "CAST(PREV_SUB_LOCN_ID AS STRING) as PREV_SUB_LOCN_ID",
    "CAST(DEST_SUB_LOCN_ID AS STRING) as DEST_SUB_LOCN_ID",
    "CAST(INBOUND_OUTBOUND_INDICATOR AS STRING) as INBOUND_OUTBOUND_INDICATOR",
    "CAST(WORK_ORD_NBR AS STRING) as WORK_ORD_NBR",
    "CAST(INTERNAL_ORDER_ID AS STRING) as INTERNAL_ORDER_ID",
    "CAST(SHIPPED_DTTM AS TIMESTAMP) as SHIPPED_DTTM",
    "CAST(TRAILER_STOP_SEQ_NBR AS INT) as TRAILER_STOP_SEQ_NBR",
    "CAST(PACK_WAVE_NBR AS STRING) as PACK_WAVE_NBR",
    "CAST(WAVE_NBR AS STRING) as WAVE_NBR",
    "CAST(WAVE_SEQ_NBR AS INT) as WAVE_SEQ_NBR",
    "CAST(WAVE_STAT_CODE AS TINYINT) as WAVE_STAT_CODE",
    "CAST(DIRECTED_QTY AS DECIMAL(9,2)) as DIRECTED_QTY",
    "CAST(TRANSITIONAL_INVENTORY_TYPE AS SMALLINT) as TRANSITIONAL_INVENTORY_TYPE",
    "CAST(PICKER_USERID AS STRING) as PICKER_USERID",
    "CAST(PACKER_USERID AS STRING) as PACKER_USERID",
    "CAST(CHUTE_ID AS STRING) as CHUTE_ID",
    "CAST(CHUTE_ASSIGN_TYPE AS STRING) as CHUTE_ASSIGN_TYPE",
    "CAST(STAGE_INDICATOR AS TINYINT) as STAGE_INDICATOR",
    "CAST(OUT_OF_ZONE_INDICATOR AS STRING) as OUT_OF_ZONE_INDICATOR",
    "CAST(LABEL_PRINT_REQD AS STRING) as LABEL_PRINT_REQD",
    "CAST(CONSUMPTION_SEQUENCE AS STRING) as CONSUMPTION_SEQUENCE",
    "CAST(CONSUMPTION_PRIORITY AS STRING) as CONSUMPTION_PRIORITY",
    "CAST(CONSUMPTION_PRIORITY_DTTM AS TIMESTAMP) as CONSUMPTION_PRIORITY_DTTM",
    "CAST(PUTAWAY_TYPE AS STRING) as PUTAWAY_TYPE",
    "CAST(RETURN_DISPOSITION_CODE AS STRING) as RETURN_DISPOSITION_CODE",
    "CAST(FINAL_DISPOSITION_CODE AS STRING) as FINAL_DISPOSITION_CODE",
    "CAST(LOADED_DTTM AS TIMESTAMP) as LOADED_DTTM",
    "CAST(LOADER_USERID AS STRING) as LOADER_USERID",
    "CAST(LOADED_POSN AS STRING) as LOADED_POSN",
    "CAST(SHIP_BY_DATE AS TIMESTAMP) as SHIP_BY_DATE",
    "CAST(PICK_DELIVERY_DURATION AS DECIMAL(5,2)) as PICK_DELIVERY_DURATION",
    "CAST(LPN_BREAK_ATTR AS STRING) as LPN_BREAK_ATTR",
    "CAST(LPN_PRINT_GROUP_CODE AS STRING) as LPN_PRINT_GROUP_CODE",
    "CAST(SEQ_RULE_PRIORITY AS INT) as SEQ_RULE_PRIORITY",
    "CAST(SPUR_LANE AS STRING) as SPUR_LANE",
    "CAST(SPUR_POSITION AS STRING) as SPUR_POSITION",
    "CAST(FIRST_ZONE AS STRING) as FIRST_ZONE",
    "CAST(LAST_ZONE AS STRING) as LAST_ZONE",
    "CAST(NBR_OF_ZONES AS SMALLINT) as NBR_OF_ZONES",
    "CAST(LPN_DIVERT_CODE AS STRING) as LPN_DIVERT_CODE",
    "CAST(INTERNAL_ORDER_CONSOL_ATTR AS STRING) as INTERNAL_ORDER_CONSOL_ATTR",
    "CAST(WHSE_INTERNAL_EVENT_CODE AS STRING) as WHSE_INTERNAL_EVENT_CODE",
    "CAST(SELECTION_RULE_ID AS INT) as SELECTION_RULE_ID",
    "CAST(VOCO_INTRN_REVERSE_ID AS STRING) as VOCO_INTRN_REVERSE_ID",
    "CAST(VOCO_INTRN_REVERSE_PALLET_ID AS STRING) as VOCO_INTRN_REVERSE_PALLET_ID",
    "CAST(LPN_CREATION_CODE AS TINYINT) as LPN_CREATION_CODE",
    "CAST(PALLET_X_OF_Y AS INT) as PALLET_X_OF_Y",
    "CAST(INCUBATION_DATE AS TIMESTAMP) as INCUBATION_DATE",
    "CAST(CONTAINER_TYPE AS STRING) as CONTAINER_TYPE",
    "CAST(CONTAINER_SIZE AS STRING) as CONTAINER_SIZE",
    "CAST(LPN_NBR_X_OF_Y AS INT) as LPN_NBR_X_OF_Y",
    "CAST(LOAD_SEQUENCE AS INT) as LOAD_SEQUENCE",
    "CAST(MASTER_PACK_ID AS STRING) as MASTER_PACK_ID",
    "CAST(SINGLE_LINE_LPN AS STRING) as SINGLE_LINE_LPN",
    "CAST(NON_INVENTORY_LPN_FLAG AS TINYINT) as NON_INVENTORY_LPN_FLAG",
    "CAST(EPC_MATCH_FLAG AS STRING) as EPC_MATCH_FLAG",
    "CAST(AUDITOR_USERID AS STRING) as AUDITOR_USERID",
    "CAST(PRINTING_RULE_ID AS INT) as PRINTING_RULE_ID",
    "CAST(EXPIRATION_DATE AS TIMESTAMP) as EXPIRATION_DATE",
    "CAST(QUAL_AUD_STAT_CODE AS TINYINT) as QUAL_AUD_STAT_CODE",
    "CAST(DELVRECIPIENTNAME AS STRING) as DELVRECIPIENTNAME",
    "CAST(DELVRECEIPTDATETIME AS TIMESTAMP) as DELVRECEIPTDATETIME",
    "CAST(DELVONTIMEFLAG AS STRING) as DELVONTIMEFLAG",
    "CAST(DELVCOMPLFLAG AS STRING) as DELVCOMPLFLAG",
    "CAST(ITEM_ID AS INT) as ITEM_ID",
    "CAST(LPN_FACILITY_STAT_UPDATED_DTTM AS TIMESTAMP) as LPN_FACILITY_STAT_UPDATED_DTTM",
    "CAST(LPN_DISP_STATUS AS STRING) as LPN_DISP_STATUS",
    "CAST(OVERSIZE_LENGTH AS SMALLINT) as OVERSIZE_LENGTH",
    "CAST(BILLING_METHOD AS INT) as BILLING_METHOD",
    "CAST(CUSTOMER_ID AS BIGINT) as CUSTOMER_ID",
    "CAST(IS_ZONE_SKIPPED AS SMALLINT) as IS_ZONE_SKIPPED",
    "CAST(ZONESKIP_HUB_FACILITY_ALIAS_ID AS STRING) as ZONESKIP_HUB_FACILITY_ALIAS_ID",
    "CAST(ZONESKIP_HUB_FACILITY_ID AS INT) as ZONESKIP_HUB_FACILITY_ID",
    "CAST(PLANING_DEST_FACILITY_ALIAS_ID AS INT) as PLANING_DEST_FACILITY_ALIAS_ID",
    "CAST(PLANING_DEST_FACILITY_ID AS INT) as PLANING_DEST_FACILITY_ID",
    "CAST(SCHED_SHIP_DTTM AS TIMESTAMP) as SCHED_SHIP_DTTM",
    "CAST(DISTRIBUTION_LEG_CARRIER_ID AS BIGINT) as DISTRIBUTION_LEG_CARRIER_ID",
    "CAST(DISTRIBUTION_LEG_MODE_ID AS INT) as DISTRIBUTION_LEG_MODE_ID",
    "CAST(DISTRIBUTION_LEV_SVCE_LEVEL_ID AS INT) as DISTRIBUTION_LEV_SVCE_LEVEL_ID",
    "CAST(SERVICE_LEVEL_INDICATOR AS STRING) as SERVICE_LEVEL_INDICATOR",
    "CAST(COD_FLAG AS SMALLINT) as COD_FLAG",
    "CAST(COD_AMOUNT AS DECIMAL(9,2)) as COD_AMOUNT",
    "CAST(COD_PAYMENT_METHOD AS INT) as COD_PAYMENT_METHOD",
    "CAST(RATED_WEIGHT AS DECIMAL(13,4)) as RATED_WEIGHT",
    "CAST(RATE_WEIGHT_TYPE AS SMALLINT) as RATE_WEIGHT_TYPE",
    "CAST(RATE_ZONE AS STRING) as RATE_ZONE",
    "CAST(FRT_FORWARDER_ACCT_NBR AS STRING) as FRT_FORWARDER_ACCT_NBR",
    "CAST(INTL_GOODS_DESC AS STRING) as INTL_GOODS_DESC",
    "CAST(SHIPMENT_PRINT_SED AS SMALLINT) as SHIPMENT_PRINT_SED",
    "CAST(EXPORT_LICENSE_NUMBER AS STRING) as EXPORT_LICENSE_NUMBER",
    "CAST(EXPORT_LICENSE_SYMBOL AS STRING) as EXPORT_LICENSE_SYMBOL",
    "CAST(EXPORT_INFO_CODE AS STRING) as EXPORT_INFO_CODE",
    "CAST(REPRINT_SHIPPING_LABEL AS SMALLINT) as REPRINT_SHIPPING_LABEL",
    "CAST(PLAN_LOAD_ID AS BIGINT) as PLAN_LOAD_ID",
    "CAST(DOCUMENTS_ONLY AS SMALLINT) as DOCUMENTS_ONLY",
    "CAST(SCHEDULED_DELIVERY_DTTM AS TIMESTAMP) as SCHEDULED_DELIVERY_DTTM",
    "CAST(NON_MACHINEABLE AS SMALLINT) as NON_MACHINEABLE",
    "CAST(D_FACILITY_ID AS BIGINT) as D_FACILITY_ID",
    "CAST(D_FACILITY_ALIAS_ID AS STRING) as D_FACILITY_ALIAS_ID",
    "CAST(VENDOR_LPN_NBR AS STRING) as VENDOR_LPN_NBR",
    "CAST(PLANNED_TC_ASN_ID AS STRING) as PLANNED_TC_ASN_ID",
    "CAST(ESTIMATED_WEIGHT AS DECIMAL(13,4)) as ESTIMATED_WEIGHT",
    "CAST(ORDER_ID AS BIGINT) as ORDER_ID",
    "CAST(MARK_FOR AS STRING) as MARK_FOR",
    "CAST(PRE_RECEIPT_STATUS AS STRING) as PRE_RECEIPT_STATUS",
    "CAST(CROSSDOCK_TC_ORDER_ID AS STRING) as CROSSDOCK_TC_ORDER_ID",
    "CAST(PHYSICAL_ENTITY_CODE AS STRING) as PHYSICAL_ENTITY_CODE",
    "CAST(PROCESS_IMMEDIATE_NEEDS AS STRING) as PROCESS_IMMEDIATE_NEEDS",
    "CAST(LENGTH AS DECIMAL(16,4)) as LENGTH",
    "CAST(WIDTH AS DECIMAL(16,4)) as WIDTH",
    "CAST(HEIGHT AS DECIMAL(16,4)) as HEIGHT",
    "CAST(TOTAL_LPN_QTY AS DECIMAL(13,4)) as TOTAL_LPN_QTY",
    "CAST(STATIC_ROUTE_ID AS INT) as STATIC_ROUTE_ID",
    "CAST(HAS_ALERTS AS TINYINT) as HAS_ALERTS",
    "CAST(ORDER_SPLIT_ID AS BIGINT) as ORDER_SPLIT_ID",
    "CAST(MHE_LOADED AS STRING) as MHE_LOADED",
    "CAST(ITEM_NAME AS STRING) as ITEM_NAME",
    "CAST(ALT_TRACKING_NBR AS STRING) as ALT_TRACKING_NBR",
    "CAST(RECEIPT_TYPE AS TINYINT) as RECEIPT_TYPE",
    "CAST(VARIANCE_TYPE AS SMALLINT) as VARIANCE_TYPE",
    "CAST(RETURN_TRACKING_NBR AS STRING) as RETURN_TRACKING_NBR",
    "CAST(CONTAINS_DRY_ICE AS STRING) as CONTAINS_DRY_ICE",
    "CAST(RETURN_REFERENCE_NUMBER AS STRING) as RETURN_REFERENCE_NUMBER",
    "CAST(HAS_NOTES AS SMALLINT) as HAS_NOTES",
    "CAST(RETURN_TRACKING_NBR_2 AS STRING) as RETURN_TRACKING_NBR_2",
    "CAST(DELIVERY_TYPE AS STRING) as DELIVERY_TYPE",
    "CAST(REF_FIELD_1 AS STRING) as REF_FIELD_1",
    "CAST(REF_FIELD_2 AS STRING) as REF_FIELD_2",
    "CAST(REF_FIELD_3 AS STRING) as REF_FIELD_3",
    "CAST(REF_FIELD_4 AS STRING) as REF_FIELD_4",
    "CAST(REF_FIELD_5 AS STRING) as REF_FIELD_5",
    "CAST(REF_FIELD_6 AS STRING) as REF_FIELD_6",
    "CAST(REF_FIELD_7 AS STRING) as REF_FIELD_7",
    "CAST(REF_FIELD_8 AS STRING) as REF_FIELD_8",
    "CAST(REF_FIELD_9 AS STRING) as REF_FIELD_9",
    "CAST(REF_FIELD_10 AS STRING) as REF_FIELD_10",
    "CAST(REF_NUM1 AS DECIMAL(13,5)) as REF_NUM1",
    "CAST(REF_NUM2 AS DECIMAL(13,5)) as REF_NUM2",
    "CAST(REF_NUM3 AS DECIMAL(13,5)) as REF_NUM3",
    "CAST(REF_NUM4 AS DECIMAL(13,5)) as REF_NUM4",
    "CAST(REF_NUM5 AS DECIMAL(13,5)) as REF_NUM5",
    "CAST(REGULATION_SET AS STRING) as REGULATION_SET",
    "CAST(DRY_ICE_WT AS DECIMAL(13,4)) as DRY_ICE_WT",
    "CAST(OVERPACK AS TINYINT) as OVERPACK",
    "CAST(SALVAGE_PACK AS TINYINT) as SALVAGE_PACK",
    "CAST(SALVAGE_PACK_QTY AS DECIMAL(13,4)) as SALVAGE_PACK_QTY",
    "CAST(Q_VALUE AS DECIMAL(2,1)) as Q_VALUE",
    "CAST(ALL_PAKD_IN_ONE AS TINYINT) as ALL_PAKD_IN_ONE",
    "CAST(SPECIAL_PERMITS AS STRING) as SPECIAL_PERMITS",
    "CAST(CN22_NBR AS STRING) as CN22_NBR",
    "CAST(DISPOSITION_TYPE AS STRING) as DISPOSITION_TYPE",
    "CAST(DISPOSITION_SOURCE_ID AS INT) as DISPOSITION_SOURCE_ID",
    "CAST(END_OLPN_DTTM AS TIMESTAMP) as END_OLPN_DTTM",
    "CAST(VOCOLLECT_ASSIGN_ID AS INT) as VOCOLLECT_ASSIGN_ID",
    "CAST(CALCULATED_CUT_OFF_DTTM AS TIMESTAMP) as CALCULATED_CUT_OFF_DTTM",
    "CAST(FAILED_GUARANTEED_DELIVERY AS STRING) as FAILED_GUARANTEED_DELIVERY",
    "CAST(EPI_PACKAGE_ID AS STRING) as EPI_PACKAGE_ID",
    "CAST(EPI_SHIPMENT_ID AS STRING) as EPI_SHIPMENT_ID",
    "CAST(EPI_MANIFEST_ID AS STRING) as EPI_MANIFEST_ID",
    "CAST(EPI_PACKAGE_STATUS AS STRING) as EPI_PACKAGE_STATUS",
    "CAST(XREF_OLPN AS STRING) as XREF_OLPN",
    "CAST(PALLET_CONTENT_CODE AS STRING) as PALLET_CONTENT_CODE",
    "CAST(EPI_DOC_OUTPUT_TYPE AS STRING) as EPI_DOC_OUTPUT_TYPE",
    "CAST(ORIGINAL_ASSIGNED_SHIP_VIA AS STRING) as ORIGINAL_ASSIGNED_SHIP_VIA",
    "CAST(REF_SHIPMENT_NBR AS STRING) as REF_SHIPMENT_NBR",
    "CAST(RTS_ID AS BIGINT) as RTS_ID",
    "CAST(IS_CHASE_CREATED AS TINYINT) as IS_CHASE_CREATED",
    "CAST(LPN_LEVEL_ROUTING AS TINYINT) as LPN_LEVEL_ROUTING",
    "CAST(PACKWAVE_RULE_ID AS INT) as PACKWAVE_RULE_ID",
    "CAST(CONS_RUN_ID AS BIGINT) as CONS_RUN_ID",
    "CAST(PROCESSED_FOR_TRLR_MOVES AS TINYINT) as PROCESSED_FOR_TRLR_MOVES",
    "CAST(EXCEPTION_OLPN AS TINYINT) as EXCEPTION_OLPN",
    "CAST(BUILD_LPN_BATCH_ID AS BIGINT) as BUILD_LPN_BATCH_ID",
    "CAST(ROUTING_LANE_ID AS INT) as ROUTING_LANE_ID",
    "CAST(ROUTING_LANE_DETAIL_ID AS BIGINT) as ROUTING_LANE_DETAIL_ID",
    "CAST(RATING_LANE_ID AS INT) as RATING_LANE_ID",
    "CAST(RATING_LANE_DETAIL_ID AS BIGINT) as RATING_LANE_DETAIL_ID",
    "CAST(PENDING_CANCELLATION AS STRING) as PENDING_CANCELLATION",
    "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )


    overwriteDeltaPartition(Shortcut_to_WM_LPN_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_LPN_PRE is written to the target table - " + target_table_name)
