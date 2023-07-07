#Code converted on 2023-06-26 17:59:05
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



def m_WM_Orders_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Orders_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_ORDERS_PRE"
    schemaName = raw
    source_schema = "WMSMIS"

    
    target_table_name = schemaName + "." + tableName
    refine_table_name = tableName[:-4]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + Prev_Run_Dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    query = f"""SELECT
                    ORDERS.ORDER_ID,
                    ORDERS.TC_ORDER_ID,
                    ORDERS.TC_ORDER_ID_U,
                    ORDERS.TC_COMPANY_ID,
                    ORDERS.CREATION_TYPE,
                    ORDERS.BUSINESS_PARTNER_ID,
                    ORDERS.PARENT_ORDER_ID,
                    ORDERS.EXT_PURCHASE_ORDER,
                    ORDERS.CONS_RUN_ID,
                    ORDERS.O_FACILITY_ALIAS_ID,
                    ORDERS.O_FACILITY_ID,
                    ORDERS.O_DOCK_ID,
                    ORDERS.O_ADDRESS_1,
                    ORDERS.O_ADDRESS_2,
                    ORDERS.O_ADDRESS_3,
                    ORDERS.O_CITY,
                    ORDERS.O_STATE_PROV,
                    ORDERS.O_POSTAL_CODE,
                    ORDERS.O_COUNTY,
                    ORDERS.O_COUNTRY_CODE,
                    ORDERS.D_FACILITY_ALIAS_ID,
                    ORDERS.D_FACILITY_ID,
                    ORDERS.D_DOCK_ID,
                    ORDERS.D_ADDRESS_1,
                    ORDERS.D_ADDRESS_2,
                    ORDERS.D_ADDRESS_3,
                    ORDERS.D_CITY,
                    ORDERS.D_STATE_PROV,
                    ORDERS.D_POSTAL_CODE,
                    ORDERS.D_COUNTY,
                    ORDERS.D_COUNTRY_CODE,
                    ORDERS.BILL_TO_NAME,
                    ORDERS.BILL_FACILITY_ALIAS_ID,
                    ORDERS.BILL_FACILITY_ID,
                    ORDERS.BILL_TO_ADDRESS_1,
                    ORDERS.BILL_TO_ADDRESS_2,
                    ORDERS.BILL_TO_ADDRESS_3,
                    ORDERS.BILL_TO_CITY,
                    ORDERS.BILL_TO_STATE_PROV,
                    ORDERS.BILL_TO_COUNTY,
                    ORDERS.BILL_TO_POSTAL_CODE,
                    ORDERS.BILL_TO_COUNTRY_CODE,
                    ORDERS.BILL_TO_PHONE_NUMBER,
                    ORDERS.BILL_TO_FAX_NUMBER,
                    ORDERS.BILL_TO_EMAIL,
                    ORDERS.PLAN_O_FACILITY_ID,
                    ORDERS.PLAN_D_FACILITY_ID,
                    ORDERS.PLAN_O_FACILITY_ALIAS_ID,
                    ORDERS.PLAN_D_FACILITY_ALIAS_ID,
                    ORDERS.PLAN_LEG_D_ALIAS_ID,
                    ORDERS.PLAN_LEG_O_ALIAS_ID,
                    ORDERS.INCOTERM_FACILITY_ID,
                    ORDERS.INCOTERM_FACILITY_ALIAS_ID,
                    ORDERS.INCOTERM_LOC_AVA_DTTM,
                    ORDERS.INCOTERM_LOC_AVA_TIME_ZONE_ID,
                    ORDERS.PICKUP_TZ,
                    ORDERS.DELIVERY_TZ,
                    ORDERS.PICKUP_START_DTTM,
                    ORDERS.PICKUP_END_DTTM,
                    ORDERS.DELIVERY_START_DTTM,
                    ORDERS.DELIVERY_END_DTTM,
                    ORDERS.ORDER_DATE_DTTM,
                    ORDERS.ORDER_RECON_DTTM,
                    ORDERS.INBOUND_REGION_ID,
                    ORDERS.OUTBOUND_REGION_ID,
                    ORDERS.DSG_SERVICE_LEVEL_ID,
                    ORDERS.DSG_CARRIER_ID,
                    ORDERS.DSG_EQUIPMENT_ID,
                    ORDERS.DSG_TRACTOR_EQUIPMENT_ID,
                    ORDERS.DSG_MOT_ID,
                    ORDERS.BASELINE_MOT_ID,
                    ORDERS.BASELINE_SERVICE_LEVEL_ID,
                    ORDERS.PRODUCT_CLASS_ID,
                    ORDERS.PROTECTION_LEVEL_ID,
                    ORDERS.MERCHANDIZING_DEPARTMENT_ID,
                    ORDERS.WAVE_ID,
                    ORDERS.WAVE_OPTION_ID,
                    ORDERS.PATH_ID,
                    ORDERS.PATH_SET_ID,
                    ORDERS.DRIVER_TYPE_ID,
                    ORDERS.UN_NUMBER_ID,
                    ORDERS.BLOCK_AUTO_CREATE,
                    ORDERS.BLOCK_AUTO_CONSOLIDATE,
                    ORDERS.HAS_ALERTS,
                    ORDERS.HAS_EM_NOTIFY_FLAG,
                    ORDERS.HAS_IMPORT_ERROR,
                    ORDERS.HAS_NOTES,
                    ORDERS.HAS_SOFT_CHECK_ERRORS,
                    ORDERS.HAS_SPLIT,
                    ORDERS.IS_BOOKING_REQUIRED,
                    ORDERS.IS_CANCELLED,
                    ORDERS.IS_HAZMAT,
                    ORDERS.IS_IMPORTED,
                    ORDERS.IS_PARTIALLY_PLANNED,
                    ORDERS.IS_PERISHABLE,
                    ORDERS.IS_SUSPENDED,
                    ORDERS.NORMALIZED_BASELINE_COST,
                    ORDERS.BASELINE_COST_CURRENCY_CODE,
                    ORDERS.ORIG_BUDG_COST,
                    ORDERS.BUDG_COST,
                    ORDERS.ACTUAL_COST,
                    ORDERS.BASELINE_COST,
                    ORDERS.TRANS_RESP_CODE,
                    ORDERS.BILLING_METHOD,
                    ORDERS.DROPOFF_PICKUP,
                    ORDERS.MOVEMENT_OPTION,
                    ORDERS.PRIORITY,
                    ORDERS.MOVE_TYPE,
                    ORDERS.SCHED_DOW,
                    ORDERS.EQUIPMENT_TYPE,
                    ORDERS.DELIVERY_REQ,
                    ORDERS.MV_CURRENCY_CODE,
                    ORDERS.MONETARY_VALUE,
                    ORDERS.COMPARTMENT_NO,
                    ORDERS.PACKAGING,
                    ORDERS.ORDER_LOADING_SEQ,
                    ORDERS.REF_FIELD_1,
                    ORDERS.REF_FIELD_2,
                    ORDERS.REF_FIELD_3,
                    ORDERS.CREATED_SOURCE_TYPE,
                    ORDERS.CREATED_SOURCE,
                    ORDERS.CREATED_DTTM,
                    ORDERS.LAST_UPDATED_SOURCE_TYPE,
                    ORDERS.LAST_UPDATED_SOURCE,
                    ORDERS.LAST_UPDATED_DTTM,
                    ORDERS.HIBERNATE_VERSION,
                    ORDERS.ACTUAL_COST_CURRENCY_CODE,
                    ORDERS.BUDG_COST_CURRENCY_CODE,
                    ORDERS.SHIPMENT_ID,
                    ORDERS.BILL_TO_TITLE,
                    ORDERS.ORDER_STATUS,
                    ORDERS.ADDR_CODE,
                    ORDERS.ADDR_VALID,
                    ORDERS.ADVT_DATE,
                    ORDERS.CHUTE_ID,
                    ORDERS.COD_FUNDS,
                    ORDERS.DC_CTR_NBR,
                    ORDERS.DO_STATUS,
                    ORDERS.DO_TYPE,
                    ORDERS.DOCS_ONLY_SHPMT,
                    ORDERS.DUTY_AND_TAX,
                    ORDERS.DUTY_TAX_ACCT_NBR,
                    ORDERS.DUTY_TAX_PAYMENT_TYPE,
                    ORDERS.EST_LPN,
                    ORDERS.EST_LPN_BRIDGED,
                    ORDERS.FTSR_NBR,
                    ORDERS.IS_BACK_ORDERED,
                    ORDERS.LPN_CUBING_INDIC,
                    ORDERS.O_PHONE_NUMBER,
                    ORDERS.ORDER_PRINT_DTTM,
                    ORDERS.PALLET_CUBING_INDIC,
                    ORDERS.PRE_PACK_FLAG,
                    ORDERS.RTE_TYPE_1,
                    ORDERS.RTE_TYPE_2,
                    ORDERS.SHIP_GROUP_SEQUENCE,
                    ORDERS.SHPNG_CHRG,
                    ORDERS.STAGE_INDIC,
                    ORDERS.STORE_NBR,
                    ORDERS.TOTAL_NBR_OF_LPN,
                    ORDERS.TOTAL_NBR_OF_PLT,
                    ORDERS.WM_ORDER_STATUS,
                    ORDERS.D_NAME,
                    ORDERS.O_CONTACT,
                    ORDERS.SECONDARY_MAXI_ADDR_NBR,
                    ORDERS.D_CONTACT,
                    ORDERS.AES_ITN,
                    ORDERS.BATCH_ID,
                    ORDERS.ORIGIN_SHIP_THRU_FACILITY_ID,
                    ORDERS.ORIGIN_SHIP_THRU_FAC_ALIAS_ID,
                    ORDERS.DEST_SHIP_THRU_FACILITY_ID,
                    ORDERS.DEST_SHIP_THRU_FAC_ALIAS_ID,
                    ORDERS.ACTUAL_SHIPPED_DTTM,
                    ORDERS.BILL_OF_LADING_NUMBER,
                    ORDERS.BOL_BREAK_ATTR,
                    ORDERS.D_EMAIL,
                    ORDERS.D_FAX_NUMBER,
                    ORDERS.D_PHONE_NUMBER,
                    ORDERS.DYNAMIC_ROUTING_REQD,
                    ORDERS.INTL_GOODS_DESC,
                    ORDERS.MAJOR_ORDER_GRP_ATTR,
                    ORDERS.ORDER_CONSOL_PROFILE,
                    ORDERS.PACK_SLIP_PRT_CNT,
                    ORDERS.PROD_SCHED_REF_NUMBER,
                    ORDERS.MUST_RELEASE_BY_DTTM,
                    ORDERS.CANCEL_DTTM,
                    ORDERS.ASSIGNED_STATIC_ROUTE_ID,
                    ORDERS.DSG_STATIC_ROUTE_ID,
                    ORDERS.O_FACILITY_NAME,
                    ORDERS.O_DOCK_DOOR_ID,
                    ORDERS.O_FAX_NUMBER,
                    ORDERS.O_EMAIL,
                    ORDERS.D_FACILITY_NAME,
                    ORDERS.D_DOCK_DOOR_ID,
                    ORDERS.PURCHASE_ORDER_NUMBER,
                    ORDERS.CUSTOMER_ID,
                    ORDERS.REPL_WAVE_NBR,
                    ORDERS.CUBING_STATUS,
                    ORDERS.SCHED_PICKUP_DTTM,
                    ORDERS.SCHED_DELIVERY_DTTM,
                    ORDERS.ZONE_SKIP_HUB_LOCATION_ID,
                    ORDERS.BILL_TO_FACILITY_NAME,
                    ORDERS.FREIGHT_CLASS,
                    ORDERS.NON_MACHINEABLE,
                    ORDERS.ASSIGNED_MOT_ID,
                    ORDERS.ASSIGNED_CARRIER_ID,
                    ORDERS.ASSIGNED_SERVICE_LEVEL_ID,
                    ORDERS.ASSIGNED_EQUIPMENT_ID,
                    ORDERS.DYNAMIC_REQUEST_SENT,
                    ORDERS.COMMODITY_CODE_ID,
                    ORDERS.TC_SHIPMENT_ID,
                    ORDERS.IN_TRANSIT_ALLOCATION,
                    ORDERS.RELEASE_DESTINATION,
                    ORDERS.TEMPLATE_ID,
                    ORDERS.COD_AMOUNT,
                    ORDERS.BILL_TO_CONTACT_NAME,
                    ORDERS.SHIP_GROUP_ID,
                    ORDERS.PURCHASE_ORDER_ID,
                    ORDERS.DSG_HUB_LOCATION_ID,
                    ORDERS.OVERRIDE_BILLING_METHOD,
                    ORDERS.PARENT_TYPE,
                    ORDERS.ACCT_RCVBL_CODE,
                    ORDERS.ADVT_CODE,
                    ORDERS.IS_CUSTOMER_PICKUP,
                    ORDERS.IS_DIRECT_ALLOWED,
                    ORDERS.RTE_ATTR,
                    ORDERS.LAST_RUN_ID,
                    ORDERS.PLAN_DUE_DTTM,
                    ORDERS.IS_ORDER_RECONCILED,
                    ORDERS.PRTL_SHIP_CONF_FLAG,
                    ORDERS.PRTL_SHIP_CONF_STATUS,
                    ORDERS.ALLOW_PRE_BILLING,
                    ORDERS.LANE_NAME,
                    ORDERS.DECLARED_VALUE,
                    ORDERS.DV_CURRENCY_CODE,
                    ORDERS.COD_CURRENCY_CODE,
                    ORDERS.WEIGHT_UOM_ID_BASE,
                    ORDERS.LINE_HAUL_SHIP_VIA,
                    ORDERS.ORIGINAL_ASSIGNED_SHIP_VIA,
                    ORDERS.DISTRIBUTION_SHIP_VIA,
                    ORDERS.DSG_SHIP_VIA,
                    ORDERS.TAX_ID,
                    ORDERS.ORDER_RECEIVED,
                    ORDERS.IS_ORIGINAL_ORDER,
                    ORDERS.FREIGHT_FORWARDER_ACCT_NBR,
                    ORDERS.GLOBAL_LOCN_NBR,
                    ORDERS.IMPORTER_DEFN,
                    ORDERS.LANG_ID,
                    ORDERS.MAJOR_MINOR_ORDER,
                    ORDERS.MAJOR_ORDER_CTRL_NBR,
                    ORDERS.MHE_FLAG,
                    ORDERS.MHE_ORD_STATE,
                    ORDERS.ORDER_CONSOL_LOCN_ID,
                    ORDERS.ORDER_TYPE,
                    ORDERS.PACK_WAVE_NBR,
                    ORDERS.PARTIAL_LPN_OPTION,
                    ORDERS.PARTIES_RELATED,
                    ORDERS.PNH_FLAG,
                    ORDERS.PRE_STICKER_CODE,
                    ORDERS.PRIMARY_MAXI_ADDR_NBR,
                    ORDERS.RTE_SWC_NBR,
                    ORDERS.RTE_TO,
                    ORDERS.RTE_WAVE_NBR,
                    ORDERS.BASELINE_CARRIER_ID,
                    ORDERS.COD_RETURN_COMPANY_NAME,
                    ORDERS.BILL_ACCT_NBR,
                    ORDERS.CUST_BROKER_ACCT_NBR,
                    ORDERS.MANIFEST_NBR,
                    ORDERS.DESTINATION_ACTION,
                    ORDERS.TRANS_PLAN_OWNER,
                    ORDERS.MARK_FOR,
                    ORDERS.DELIVERY_OPTIONS,
                    ORDERS.PRE_BILL_STATUS,
                    ORDERS.CONTNT_LABEL_TYPE,
                    ORDERS.NBR_OF_CONTNT_LABEL,
                    ORDERS.NBR_OF_LABEL,
                    ORDERS.NBR_OF_PAKNG_SLIPS,
                    ORDERS.PALLET_CONTENT_LABEL_TYPE,
                    ORDERS.LPN_LABEL_TYPE,
                    ORDERS.PACK_SLIP_TYPE,
                    ORDERS.BOL_TYPE,
                    ORDERS.MANIF_TYPE,
                    ORDERS.PRINT_CANADIAN_CUST_INVC_FLAG,
                    ORDERS.PRINT_COO,
                    ORDERS.PRINT_DOCK_RCPT_FLAG,
                    ORDERS.PRINT_INV,
                    ORDERS.PRINT_NAFTA_COO_FLAG,
                    ORDERS.PRINT_OCEAN_BOL_FLAG,
                    ORDERS.PRINT_SED,
                    ORDERS.PRINT_SHPR_LTR_OF_INSTR_FLAG,
                    ORDERS.PRINT_PKG_LIST_FLAG,
                    ORDERS.FREIGHT_REVENUE_CURRENCY_CODE,
                    ORDERS.FREIGHT_REVENUE,
                    ORDERS.TRANS_PLAN_DIRECTION,
                    ORDERS.DSG_VOYAGE_FLIGHT,
                    ORDERS.IS_D_POBOX,
                    ORDERS.REF_SHIPMENT_NBR,
                    ORDERS.REF_STOP_SEQ,
                    ORDERS.HAZ_OFFEROR_NAME,
                    ORDERS.DISTRO_NUMBER,
                    ORDERS.INCOTERM_ID,
                    ORDERS.DIRECTION,
                    ORDERS.ORDER_SHIPMENT_SEQ,
                    ORDERS.MOVEMENT_TYPE,
                    ORDERS.ACCT_RCVBL_ACCT_NBR,
                    ORDERS.REF_FIELD_4,
                    ORDERS.REF_FIELD_5,
                    ORDERS.REF_FIELD_6,
                    ORDERS.REF_FIELD_7,
                    ORDERS.REF_FIELD_8,
                    ORDERS.REF_FIELD_9,
                    ORDERS.REF_FIELD_10,
                    ORDERS.FIRST_ZONE,
                    ORDERS.LAST_ZONE,
                    ORDERS.NBR_OF_ZONES,
                    ORDERS.REF_NUM1,
                    ORDERS.REF_NUM2,
                    ORDERS.REF_NUM3,
                    ORDERS.REF_NUM4,
                    ORDERS.REF_NUM5,
                    ORDERS.SPL_INSTR_CODE_1,
                    ORDERS.SPL_INSTR_CODE_2,
                    ORDERS.SPL_INSTR_CODE_3,
                    ORDERS.SPL_INSTR_CODE_4,
                    ORDERS.SPL_INSTR_CODE_5,
                    ORDERS.SPL_INSTR_CODE_6,
                    ORDERS.SPL_INSTR_CODE_7,
                    ORDERS.SPL_INSTR_CODE_8,
                    ORDERS.SPL_INSTR_CODE_9,
                    ORDERS.SPL_INSTR_CODE_10,
                    ORDERS.SINGLE_UNIT_FLAG,
                    ORDERS.TOTAL_NBR_OF_UNITS,
                    ORDERS.IS_ROUTED,
                    ORDERS.DSG_TRAILER_NUMBER,
                    ORDERS.STAGING_LOCN_ID,
                    ORDERS.UPSMI_COST_CENTER,
                    ORDERS.EST_PALLET_BRIDGED,
                    ORDERS.EST_PALLET,
                    ORDERS.PICKLIST_ID,
                    ORDERS.EFFECTIVE_RANK,
                    ORDERS.CHASE_ELIGIBLE,
                    ORDERS.SHIPPING_CHANNEL,
                    ORDERS.IS_GUARANTEED_DELIVERY,
                    ORDERS.FAILED_GUARANTEED_DELIVERY,
                    ORDERS.APPLY_LPNTYPE_FOR_ORDER,
                    ORDERS.PO_TYPE_ATTR,
                    ORDERS.EPI_SERVICE_GROUP,
                    ORDERS.IMPORTER_OF_RECORD_NBR,
                    ORDERS.B13A_EXPORT_DECL_NBR,
                    ORDERS.RETURN_ADDR_CODE,
                    ORDERS.CHANNEL_TYPE,
                    ORDERS.STATIC_ROUTE_DELIVERY_TYPE,
                    ORDERS.ASSIGNED_CARRIER_CODE,
                    ORDERS.DSG_CARRIER_CODE,
                    ORDERS.SOURCE_ORDER,
                    ORDERS.AUTO_APPOINTMENT,
                    ORDERS.BASELINE_LINEHAUL_COST,
                    ORDERS.BASELINE_ACCESSORIAL_COST,
                    ORDERS.BASELINE_SHIP_VIA,
                    ORDERS.DELIVERY_CHANNEL_ID,
                    ORDERS.ORDER_STREAM,
                    ORDERS.LINEHAUL_REVENUE,
                    ORDERS.ACCESSORIAL_REVENUE,
                    ORDERS.STOP_OFF_REVENUE,
                    ORDERS.REVENUE_LANE_ID,
                    ORDERS.REVENUE_LANE_DETAIL_ID,
                    ORDERS.MARGIN,
                    ORDERS.MARGIN_CURRENCY_CODE,
                    ORDERS.CM_DISCOUNT_REVENUE,
                    ORDERS.C_TMS_PLAN_ID
               FROM {source_schema}.ORDERS"""

    SQ_Shortcut_to_ORDERS1 = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_ORDERS1 is executed and data is loaded using jdbc")

    
    
    # Processing node ExP_TRN, type EXPRESSION 
    # COLUMN COUNT: 371
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ORDERS1_temp = SQ_Shortcut_to_ORDERS1.toDF(*["SQ_Shortcut_to_ORDERS1___" + col for col in SQ_Shortcut_to_ORDERS1.columns])
    
    ExP_TRN = SQ_Shortcut_to_ORDERS1_temp.selectExpr( \
    	"SQ_Shortcut_to_ORDERS1___sys_row_id as sys_row_id", \
    	f"{dcnbr} as DC_NBR_EXP", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_ID as ORDER_ID", \
    	"SQ_Shortcut_to_ORDERS1___TC_ORDER_ID as TC_ORDER_ID", \
    	"SQ_Shortcut_to_ORDERS1___TC_ORDER_ID_U as TC_ORDER_ID_U", \
    	"SQ_Shortcut_to_ORDERS1___TC_COMPANY_ID as TC_COMPANY_ID", \
    	"SQ_Shortcut_to_ORDERS1___CREATION_TYPE as CREATION_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", \
    	"SQ_Shortcut_to_ORDERS1___PARENT_ORDER_ID as PARENT_ORDER_ID", \
    	"SQ_Shortcut_to_ORDERS1___EXT_PURCHASE_ORDER as EXT_PURCHASE_ORDER", \
    	"SQ_Shortcut_to_ORDERS1___CONS_RUN_ID as CONS_RUN_ID", \
    	"SQ_Shortcut_to_ORDERS1___O_FACILITY_ALIAS_ID as O_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_ORDERS1___O_FACILITY_ID as O_FACILITY_ID", \
    	"SQ_Shortcut_to_ORDERS1___O_DOCK_ID as O_DOCK_ID", \
    	"SQ_Shortcut_to_ORDERS1___O_ADDRESS_1 as O_ADDRESS_1", \
    	"SQ_Shortcut_to_ORDERS1___O_ADDRESS_2 as O_ADDRESS_2", \
    	"SQ_Shortcut_to_ORDERS1___O_ADDRESS_3 as O_ADDRESS_3", \
    	"SQ_Shortcut_to_ORDERS1___O_CITY as O_CITY", \
    	"SQ_Shortcut_to_ORDERS1___O_STATE_PROV as O_STATE_PROV", \
    	"SQ_Shortcut_to_ORDERS1___O_POSTAL_CODE as O_POSTAL_CODE", \
    	"SQ_Shortcut_to_ORDERS1___O_COUNTY as O_COUNTY", \
    	"SQ_Shortcut_to_ORDERS1___O_COUNTRY_CODE as O_COUNTRY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___D_FACILITY_ALIAS_ID as D_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_ORDERS1___D_FACILITY_ID as D_FACILITY_ID", \
    	"SQ_Shortcut_to_ORDERS1___D_DOCK_ID as D_DOCK_ID", \
    	"SQ_Shortcut_to_ORDERS1___D_ADDRESS_1 as D_ADDRESS_1", \
    	"SQ_Shortcut_to_ORDERS1___D_ADDRESS_2 as D_ADDRESS_2", \
    	"SQ_Shortcut_to_ORDERS1___D_ADDRESS_3 as D_ADDRESS_3", \
    	"SQ_Shortcut_to_ORDERS1___D_CITY as D_CITY", \
    	"SQ_Shortcut_to_ORDERS1___D_STATE_PROV as D_STATE_PROV", \
    	"SQ_Shortcut_to_ORDERS1___D_POSTAL_CODE as D_POSTAL_CODE", \
    	"SQ_Shortcut_to_ORDERS1___D_COUNTY as D_COUNTY", \
    	"SQ_Shortcut_to_ORDERS1___D_COUNTRY_CODE as D_COUNTRY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_NAME as BILL_TO_NAME", \
    	"SQ_Shortcut_to_ORDERS1___BILL_FACILITY_ALIAS_ID as BILL_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_ORDERS1___BILL_FACILITY_ID as BILL_FACILITY_ID", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_ADDRESS_1 as BILL_TO_ADDRESS_1", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_ADDRESS_2 as BILL_TO_ADDRESS_2", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_ADDRESS_3 as BILL_TO_ADDRESS_3", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_CITY as BILL_TO_CITY", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_STATE_PROV as BILL_TO_STATE_PROV", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_COUNTY as BILL_TO_COUNTY", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_POSTAL_CODE as BILL_TO_POSTAL_CODE", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_COUNTRY_CODE as BILL_TO_COUNTRY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_PHONE_NUMBER as BILL_TO_PHONE_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_FAX_NUMBER as BILL_TO_FAX_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_EMAIL as BILL_TO_EMAIL", \
    	"SQ_Shortcut_to_ORDERS1___PLAN_O_FACILITY_ID as PLAN_O_FACILITY_ID", \
    	"SQ_Shortcut_to_ORDERS1___PLAN_D_FACILITY_ID as PLAN_D_FACILITY_ID", \
    	"SQ_Shortcut_to_ORDERS1___PLAN_O_FACILITY_ALIAS_ID as PLAN_O_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_ORDERS1___PLAN_D_FACILITY_ALIAS_ID as PLAN_D_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_ORDERS1___PLAN_LEG_D_ALIAS_ID as PLAN_LEG_D_ALIAS_ID", \
    	"SQ_Shortcut_to_ORDERS1___PLAN_LEG_O_ALIAS_ID as PLAN_LEG_O_ALIAS_ID", \
    	"SQ_Shortcut_to_ORDERS1___INCOTERM_FACILITY_ID as INCOTERM_FACILITY_ID", \
    	"SQ_Shortcut_to_ORDERS1___INCOTERM_FACILITY_ALIAS_ID as INCOTERM_FACILITY_ALIAS_ID", \
    	"SQ_Shortcut_to_ORDERS1___INCOTERM_LOC_AVA_DTTM as INCOTERM_LOC_AVA_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___INCOTERM_LOC_AVA_TIME_ZONE_ID as INCOTERM_LOC_AVA_TIME_ZONE_ID", \
    	"SQ_Shortcut_to_ORDERS1___PICKUP_TZ as PICKUP_TZ", \
    	"SQ_Shortcut_to_ORDERS1___DELIVERY_TZ as DELIVERY_TZ", \
    	"SQ_Shortcut_to_ORDERS1___PICKUP_START_DTTM as PICKUP_START_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___PICKUP_END_DTTM as PICKUP_END_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___DELIVERY_START_DTTM as DELIVERY_START_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___DELIVERY_END_DTTM as DELIVERY_END_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_DATE_DTTM as ORDER_DATE_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_RECON_DTTM as ORDER_RECON_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___INBOUND_REGION_ID as INBOUND_REGION_ID", \
    	"SQ_Shortcut_to_ORDERS1___OUTBOUND_REGION_ID as OUTBOUND_REGION_ID", \
    	"SQ_Shortcut_to_ORDERS1___DSG_SERVICE_LEVEL_ID as DSG_SERVICE_LEVEL_ID", \
    	"SQ_Shortcut_to_ORDERS1___DSG_CARRIER_ID as DSG_CARRIER_ID", \
    	"SQ_Shortcut_to_ORDERS1___DSG_EQUIPMENT_ID as DSG_EQUIPMENT_ID", \
    	"SQ_Shortcut_to_ORDERS1___DSG_TRACTOR_EQUIPMENT_ID as DSG_TRACTOR_EQUIPMENT_ID", \
    	"SQ_Shortcut_to_ORDERS1___DSG_MOT_ID as DSG_MOT_ID", \
    	"SQ_Shortcut_to_ORDERS1___BASELINE_MOT_ID as BASELINE_MOT_ID", \
    	"SQ_Shortcut_to_ORDERS1___BASELINE_SERVICE_LEVEL_ID as BASELINE_SERVICE_LEVEL_ID", \
    	"SQ_Shortcut_to_ORDERS1___PRODUCT_CLASS_ID as PRODUCT_CLASS_ID", \
    	"SQ_Shortcut_to_ORDERS1___PROTECTION_LEVEL_ID as PROTECTION_LEVEL_ID", \
    	"SQ_Shortcut_to_ORDERS1___MERCHANDIZING_DEPARTMENT_ID as MERCHANDIZING_DEPARTMENT_ID", \
    	"SQ_Shortcut_to_ORDERS1___WAVE_ID as WAVE_ID", \
    	"SQ_Shortcut_to_ORDERS1___WAVE_OPTION_ID as WAVE_OPTION_ID", \
    	"SQ_Shortcut_to_ORDERS1___PATH_ID as PATH_ID", \
    	"SQ_Shortcut_to_ORDERS1___PATH_SET_ID as PATH_SET_ID", \
    	"SQ_Shortcut_to_ORDERS1___DRIVER_TYPE_ID as DRIVER_TYPE_ID", \
    	"SQ_Shortcut_to_ORDERS1___UN_NUMBER_ID as UN_NUMBER_ID", \
    	"SQ_Shortcut_to_ORDERS1___BLOCK_AUTO_CREATE as BLOCK_AUTO_CREATE", \
    	"SQ_Shortcut_to_ORDERS1___BLOCK_AUTO_CONSOLIDATE as BLOCK_AUTO_CONSOLIDATE", \
    	"SQ_Shortcut_to_ORDERS1___HAS_ALERTS as HAS_ALERTS", \
    	"SQ_Shortcut_to_ORDERS1___HAS_EM_NOTIFY_FLAG as HAS_EM_NOTIFY_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___HAS_IMPORT_ERROR as HAS_IMPORT_ERROR", \
    	"SQ_Shortcut_to_ORDERS1___HAS_NOTES as HAS_NOTES", \
    	"SQ_Shortcut_to_ORDERS1___HAS_SOFT_CHECK_ERRORS as HAS_SOFT_CHECK_ERRORS", \
    	"SQ_Shortcut_to_ORDERS1___HAS_SPLIT as HAS_SPLIT", \
    	"SQ_Shortcut_to_ORDERS1___IS_BOOKING_REQUIRED as IS_BOOKING_REQUIRED", \
    	"SQ_Shortcut_to_ORDERS1___IS_CANCELLED as IS_CANCELLED", \
    	"SQ_Shortcut_to_ORDERS1___IS_HAZMAT as IS_HAZMAT", \
    	"SQ_Shortcut_to_ORDERS1___IS_IMPORTED as IS_IMPORTED", \
    	"SQ_Shortcut_to_ORDERS1___IS_PARTIALLY_PLANNED as IS_PARTIALLY_PLANNED", \
    	"SQ_Shortcut_to_ORDERS1___IS_PERISHABLE as IS_PERISHABLE", \
    	"SQ_Shortcut_to_ORDERS1___IS_SUSPENDED as IS_SUSPENDED", \
    	"SQ_Shortcut_to_ORDERS1___NORMALIZED_BASELINE_COST as NORMALIZED_BASELINE_COST", \
    	"SQ_Shortcut_to_ORDERS1___BASELINE_COST_CURRENCY_CODE as BASELINE_COST_CURRENCY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___ORIG_BUDG_COST as ORIG_BUDG_COST", \
    	"SQ_Shortcut_to_ORDERS1___BUDG_COST as BUDG_COST", \
    	"SQ_Shortcut_to_ORDERS1___ACTUAL_COST as ACTUAL_COST", \
    	"SQ_Shortcut_to_ORDERS1___BASELINE_COST as BASELINE_COST", \
    	"SQ_Shortcut_to_ORDERS1___TRANS_RESP_CODE as TRANS_RESP_CODE", \
    	"SQ_Shortcut_to_ORDERS1___BILLING_METHOD as BILLING_METHOD", \
    	"SQ_Shortcut_to_ORDERS1___DROPOFF_PICKUP as DROPOFF_PICKUP", \
    	"SQ_Shortcut_to_ORDERS1___MOVEMENT_OPTION as MOVEMENT_OPTION", \
    	"SQ_Shortcut_to_ORDERS1___PRIORITY as PRIORITY", \
    	"SQ_Shortcut_to_ORDERS1___MOVE_TYPE as MOVE_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___SCHED_DOW as SCHED_DOW", \
    	"SQ_Shortcut_to_ORDERS1___EQUIPMENT_TYPE as EQUIPMENT_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___DELIVERY_REQ as DELIVERY_REQ", \
    	"SQ_Shortcut_to_ORDERS1___MV_CURRENCY_CODE as MV_CURRENCY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___MONETARY_VALUE as MONETARY_VALUE", \
    	"SQ_Shortcut_to_ORDERS1___COMPARTMENT_NO as COMPARTMENT_NO", \
    	"SQ_Shortcut_to_ORDERS1___PACKAGING as PACKAGING", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_LOADING_SEQ as ORDER_LOADING_SEQ", \
    	"SQ_Shortcut_to_ORDERS1___REF_FIELD_1 as REF_FIELD_1", \
    	"SQ_Shortcut_to_ORDERS1___REF_FIELD_2 as REF_FIELD_2", \
    	"SQ_Shortcut_to_ORDERS1___REF_FIELD_3 as REF_FIELD_3", \
    	"SQ_Shortcut_to_ORDERS1___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___CREATED_SOURCE as CREATED_SOURCE", \
    	"SQ_Shortcut_to_ORDERS1___CREATED_DTTM as CREATED_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
    	"SQ_Shortcut_to_ORDERS1___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___HIBERNATE_VERSION as HIBERNATE_VERSION", \
    	"SQ_Shortcut_to_ORDERS1___ACTUAL_COST_CURRENCY_CODE as ACTUAL_COST_CURRENCY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___BUDG_COST_CURRENCY_CODE as BUDG_COST_CURRENCY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___SHIPMENT_ID as SHIPMENT_ID", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_TITLE as BILL_TO_TITLE", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_STATUS as ORDER_STATUS", \
    	"SQ_Shortcut_to_ORDERS1___ADDR_CODE as ADDR_CODE", \
    	"SQ_Shortcut_to_ORDERS1___ADDR_VALID as ADDR_VALID", \
    	"SQ_Shortcut_to_ORDERS1___ADVT_DATE as ADVT_DATE", \
    	"SQ_Shortcut_to_ORDERS1___CHUTE_ID as CHUTE_ID", \
    	"SQ_Shortcut_to_ORDERS1___COD_FUNDS as COD_FUNDS", \
    	"SQ_Shortcut_to_ORDERS1___DC_CTR_NBR as DC_CTR_NBR", \
    	"SQ_Shortcut_to_ORDERS1___DO_STATUS as DO_STATUS", \
    	"SQ_Shortcut_to_ORDERS1___DO_TYPE as DO_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___DOCS_ONLY_SHPMT as DOCS_ONLY_SHPMT", \
    	"SQ_Shortcut_to_ORDERS1___DUTY_AND_TAX as DUTY_AND_TAX", \
    	"SQ_Shortcut_to_ORDERS1___DUTY_TAX_ACCT_NBR as DUTY_TAX_ACCT_NBR", \
    	"SQ_Shortcut_to_ORDERS1___DUTY_TAX_PAYMENT_TYPE as DUTY_TAX_PAYMENT_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___EST_LPN as EST_LPN", \
    	"SQ_Shortcut_to_ORDERS1___EST_LPN_BRIDGED as EST_LPN_BRIDGED", \
    	"SQ_Shortcut_to_ORDERS1___FTSR_NBR as FTSR_NBR", \
    	"SQ_Shortcut_to_ORDERS1___IS_BACK_ORDERED as IS_BACK_ORDERED", \
    	"SQ_Shortcut_to_ORDERS1___LPN_CUBING_INDIC as LPN_CUBING_INDIC", \
    	"SQ_Shortcut_to_ORDERS1___O_PHONE_NUMBER as O_PHONE_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_PRINT_DTTM as ORDER_PRINT_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___PALLET_CUBING_INDIC as PALLET_CUBING_INDIC", \
    	"SQ_Shortcut_to_ORDERS1___PRE_PACK_FLAG as PRE_PACK_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___RTE_TYPE_1 as RTE_TYPE_1", \
    	"SQ_Shortcut_to_ORDERS1___RTE_TYPE_2 as RTE_TYPE_2", \
    	"SQ_Shortcut_to_ORDERS1___SHIP_GROUP_SEQUENCE as SHIP_GROUP_SEQUENCE", \
    	"SQ_Shortcut_to_ORDERS1___SHPNG_CHRG as SHPNG_CHRG", \
    	"SQ_Shortcut_to_ORDERS1___STAGE_INDIC as STAGE_INDIC", \
    	"SQ_Shortcut_to_ORDERS1___STORE_NBR as STORE_NBR", \
    	"SQ_Shortcut_to_ORDERS1___TOTAL_NBR_OF_LPN as TOTAL_NBR_OF_LPN", \
    	"SQ_Shortcut_to_ORDERS1___TOTAL_NBR_OF_PLT as TOTAL_NBR_OF_PLT", \
    	"SQ_Shortcut_to_ORDERS1___WM_ORDER_STATUS as WM_ORDER_STATUS", \
    	"SQ_Shortcut_to_ORDERS1___D_NAME as D_NAME", \
    	"SQ_Shortcut_to_ORDERS1___O_CONTACT as O_CONTACT", \
    	"SQ_Shortcut_to_ORDERS1___SECONDARY_MAXI_ADDR_NBR as SECONDARY_MAXI_ADDR_NBR", \
    	"SQ_Shortcut_to_ORDERS1___D_CONTACT as D_CONTACT", \
    	"SQ_Shortcut_to_ORDERS1___AES_ITN as AES_ITN", \
    	"SQ_Shortcut_to_ORDERS1___BATCH_ID as BATCH_ID", \
    	"SQ_Shortcut_to_ORDERS1___ORIGIN_SHIP_THRU_FACILITY_ID as ORIGIN_SHIP_THRU_FACILITY_ID", \
    	"SQ_Shortcut_to_ORDERS1___ORIGIN_SHIP_THRU_FAC_ALIAS_ID as ORIGIN_SHIP_THRU_FAC_ALIAS_ID", \
    	"SQ_Shortcut_to_ORDERS1___DEST_SHIP_THRU_FACILITY_ID as DEST_SHIP_THRU_FACILITY_ID", \
    	"SQ_Shortcut_to_ORDERS1___DEST_SHIP_THRU_FAC_ALIAS_ID as DEST_SHIP_THRU_FAC_ALIAS_ID", \
    	"SQ_Shortcut_to_ORDERS1___ACTUAL_SHIPPED_DTTM as ACTUAL_SHIPPED_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___BILL_OF_LADING_NUMBER as BILL_OF_LADING_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___BOL_BREAK_ATTR as BOL_BREAK_ATTR", \
    	"SQ_Shortcut_to_ORDERS1___D_EMAIL as D_EMAIL", \
    	"SQ_Shortcut_to_ORDERS1___D_FAX_NUMBER as D_FAX_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___D_PHONE_NUMBER as D_PHONE_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___DYNAMIC_ROUTING_REQD as DYNAMIC_ROUTING_REQD", \
    	"SQ_Shortcut_to_ORDERS1___INTL_GOODS_DESC as INTL_GOODS_DESC", \
    	"SQ_Shortcut_to_ORDERS1___MAJOR_ORDER_GRP_ATTR as MAJOR_ORDER_GRP_ATTR", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_CONSOL_PROFILE as ORDER_CONSOL_PROFILE", \
    	"SQ_Shortcut_to_ORDERS1___PACK_SLIP_PRT_CNT as PACK_SLIP_PRT_CNT", \
    	"SQ_Shortcut_to_ORDERS1___PROD_SCHED_REF_NUMBER as PROD_SCHED_REF_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___MUST_RELEASE_BY_DTTM as MUST_RELEASE_BY_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___CANCEL_DTTM as CANCEL_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___ASSIGNED_STATIC_ROUTE_ID as ASSIGNED_STATIC_ROUTE_ID", \
    	"SQ_Shortcut_to_ORDERS1___DSG_STATIC_ROUTE_ID as DSG_STATIC_ROUTE_ID", \
    	"SQ_Shortcut_to_ORDERS1___O_FACILITY_NAME as O_FACILITY_NAME", \
    	"SQ_Shortcut_to_ORDERS1___O_DOCK_DOOR_ID as O_DOCK_DOOR_ID", \
    	"SQ_Shortcut_to_ORDERS1___O_FAX_NUMBER as O_FAX_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___O_EMAIL as O_EMAIL", \
    	"SQ_Shortcut_to_ORDERS1___D_FACILITY_NAME as D_FACILITY_NAME", \
    	"SQ_Shortcut_to_ORDERS1___D_DOCK_DOOR_ID as D_DOCK_DOOR_ID", \
    	"SQ_Shortcut_to_ORDERS1___PURCHASE_ORDER_NUMBER as PURCHASE_ORDER_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___CUSTOMER_ID as CUSTOMER_ID", \
    	"SQ_Shortcut_to_ORDERS1___REPL_WAVE_NBR as REPL_WAVE_NBR", \
    	"SQ_Shortcut_to_ORDERS1___CUBING_STATUS as CUBING_STATUS", \
    	"SQ_Shortcut_to_ORDERS1___SCHED_PICKUP_DTTM as SCHED_PICKUP_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___SCHED_DELIVERY_DTTM as SCHED_DELIVERY_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___ZONE_SKIP_HUB_LOCATION_ID as ZONE_SKIP_HUB_LOCATION_ID", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_FACILITY_NAME as BILL_TO_FACILITY_NAME", \
    	"SQ_Shortcut_to_ORDERS1___FREIGHT_CLASS as FREIGHT_CLASS", \
    	"SQ_Shortcut_to_ORDERS1___NON_MACHINEABLE as NON_MACHINEABLE", \
    	"SQ_Shortcut_to_ORDERS1___ASSIGNED_MOT_ID as ASSIGNED_MOT_ID", \
    	"SQ_Shortcut_to_ORDERS1___ASSIGNED_CARRIER_ID as ASSIGNED_CARRIER_ID", \
    	"SQ_Shortcut_to_ORDERS1___ASSIGNED_SERVICE_LEVEL_ID as ASSIGNED_SERVICE_LEVEL_ID", \
    	"SQ_Shortcut_to_ORDERS1___ASSIGNED_EQUIPMENT_ID as ASSIGNED_EQUIPMENT_ID", \
    	"SQ_Shortcut_to_ORDERS1___DYNAMIC_REQUEST_SENT as DYNAMIC_REQUEST_SENT", \
    	"SQ_Shortcut_to_ORDERS1___COMMODITY_CODE_ID as COMMODITY_CODE_ID", \
    	"SQ_Shortcut_to_ORDERS1___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
    	"SQ_Shortcut_to_ORDERS1___IN_TRANSIT_ALLOCATION as IN_TRANSIT_ALLOCATION", \
    	"SQ_Shortcut_to_ORDERS1___RELEASE_DESTINATION as RELEASE_DESTINATION", \
    	"SQ_Shortcut_to_ORDERS1___TEMPLATE_ID as TEMPLATE_ID", \
    	"SQ_Shortcut_to_ORDERS1___COD_AMOUNT as COD_AMOUNT", \
    	"SQ_Shortcut_to_ORDERS1___BILL_TO_CONTACT_NAME as BILL_TO_CONTACT_NAME", \
    	"SQ_Shortcut_to_ORDERS1___SHIP_GROUP_ID as SHIP_GROUP_ID", \
    	"SQ_Shortcut_to_ORDERS1___PURCHASE_ORDER_ID as PURCHASE_ORDER_ID", \
    	"SQ_Shortcut_to_ORDERS1___DSG_HUB_LOCATION_ID as DSG_HUB_LOCATION_ID", \
    	"SQ_Shortcut_to_ORDERS1___OVERRIDE_BILLING_METHOD as OVERRIDE_BILLING_METHOD", \
    	"SQ_Shortcut_to_ORDERS1___PARENT_TYPE as PARENT_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___ACCT_RCVBL_CODE as ACCT_RCVBL_CODE", \
    	"SQ_Shortcut_to_ORDERS1___ADVT_CODE as ADVT_CODE", \
    	"SQ_Shortcut_to_ORDERS1___IS_CUSTOMER_PICKUP as IS_CUSTOMER_PICKUP", \
    	"SQ_Shortcut_to_ORDERS1___IS_DIRECT_ALLOWED as IS_DIRECT_ALLOWED", \
    	"SQ_Shortcut_to_ORDERS1___RTE_ATTR as RTE_ATTR", \
    	"SQ_Shortcut_to_ORDERS1___LAST_RUN_ID as LAST_RUN_ID", \
    	"SQ_Shortcut_to_ORDERS1___PLAN_DUE_DTTM as PLAN_DUE_DTTM", \
    	"SQ_Shortcut_to_ORDERS1___IS_ORDER_RECONCILED as IS_ORDER_RECONCILED", \
    	"SQ_Shortcut_to_ORDERS1___PRTL_SHIP_CONF_FLAG as PRTL_SHIP_CONF_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___PRTL_SHIP_CONF_STATUS as PRTL_SHIP_CONF_STATUS", \
    	"SQ_Shortcut_to_ORDERS1___ALLOW_PRE_BILLING as ALLOW_PRE_BILLING", \
    	"SQ_Shortcut_to_ORDERS1___LANE_NAME as LANE_NAME", \
    	"SQ_Shortcut_to_ORDERS1___DECLARED_VALUE as DECLARED_VALUE", \
    	"SQ_Shortcut_to_ORDERS1___DV_CURRENCY_CODE as DV_CURRENCY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___COD_CURRENCY_CODE as COD_CURRENCY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___WEIGHT_UOM_ID_BASE as WEIGHT_UOM_ID_BASE", \
    	"SQ_Shortcut_to_ORDERS1___LINE_HAUL_SHIP_VIA as LINE_HAUL_SHIP_VIA", \
    	"SQ_Shortcut_to_ORDERS1___ORIGINAL_ASSIGNED_SHIP_VIA as ORIGINAL_ASSIGNED_SHIP_VIA", \
    	"SQ_Shortcut_to_ORDERS1___DISTRIBUTION_SHIP_VIA as DISTRIBUTION_SHIP_VIA", \
    	"SQ_Shortcut_to_ORDERS1___DSG_SHIP_VIA as DSG_SHIP_VIA", \
    	"SQ_Shortcut_to_ORDERS1___TAX_ID as TAX_ID", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_RECEIVED as ORDER_RECEIVED", \
    	"SQ_Shortcut_to_ORDERS1___IS_ORIGINAL_ORDER as IS_ORIGINAL_ORDER", \
    	"SQ_Shortcut_to_ORDERS1___FREIGHT_FORWARDER_ACCT_NBR as FREIGHT_FORWARDER_ACCT_NBR", \
    	"SQ_Shortcut_to_ORDERS1___GLOBAL_LOCN_NBR as GLOBAL_LOCN_NBR", \
    	"SQ_Shortcut_to_ORDERS1___IMPORTER_DEFN as IMPORTER_DEFN", \
    	"SQ_Shortcut_to_ORDERS1___LANG_ID as LANG_ID", \
    	"SQ_Shortcut_to_ORDERS1___MAJOR_MINOR_ORDER as MAJOR_MINOR_ORDER", \
    	"SQ_Shortcut_to_ORDERS1___MAJOR_ORDER_CTRL_NBR as MAJOR_ORDER_CTRL_NBR", \
    	"SQ_Shortcut_to_ORDERS1___MHE_FLAG as MHE_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___MHE_ORD_STATE as MHE_ORD_STATE", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_CONSOL_LOCN_ID as ORDER_CONSOL_LOCN_ID", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_TYPE as ORDER_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___PACK_WAVE_NBR as PACK_WAVE_NBR", \
    	"SQ_Shortcut_to_ORDERS1___PARTIAL_LPN_OPTION as PARTIAL_LPN_OPTION", \
    	"SQ_Shortcut_to_ORDERS1___PARTIES_RELATED as PARTIES_RELATED", \
    	"SQ_Shortcut_to_ORDERS1___PNH_FLAG as PNH_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___PRE_STICKER_CODE as PRE_STICKER_CODE", \
    	"SQ_Shortcut_to_ORDERS1___PRIMARY_MAXI_ADDR_NBR as PRIMARY_MAXI_ADDR_NBR", \
    	"SQ_Shortcut_to_ORDERS1___RTE_SWC_NBR as RTE_SWC_NBR", \
    	"SQ_Shortcut_to_ORDERS1___RTE_TO as RTE_TO", \
    	"SQ_Shortcut_to_ORDERS1___RTE_WAVE_NBR as RTE_WAVE_NBR", \
    	"SQ_Shortcut_to_ORDERS1___BASELINE_CARRIER_ID as BASELINE_CARRIER_ID", \
    	"SQ_Shortcut_to_ORDERS1___COD_RETURN_COMPANY_NAME as COD_RETURN_COMPANY_NAME", \
    	"SQ_Shortcut_to_ORDERS1___BILL_ACCT_NBR as BILL_ACCT_NBR", \
    	"SQ_Shortcut_to_ORDERS1___CUST_BROKER_ACCT_NBR as CUST_BROKER_ACCT_NBR", \
    	"SQ_Shortcut_to_ORDERS1___MANIFEST_NBR as MANIFEST_NBR", \
    	"SQ_Shortcut_to_ORDERS1___DESTINATION_ACTION as DESTINATION_ACTION", \
    	"SQ_Shortcut_to_ORDERS1___TRANS_PLAN_OWNER as TRANS_PLAN_OWNER", \
    	"SQ_Shortcut_to_ORDERS1___MARK_FOR as MARK_FOR", \
    	"SQ_Shortcut_to_ORDERS1___DELIVERY_OPTIONS as DELIVERY_OPTIONS", \
    	"SQ_Shortcut_to_ORDERS1___PRE_BILL_STATUS as PRE_BILL_STATUS", \
    	"SQ_Shortcut_to_ORDERS1___CONTNT_LABEL_TYPE as CONTNT_LABEL_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___NBR_OF_CONTNT_LABEL as NBR_OF_CONTNT_LABEL", \
    	"SQ_Shortcut_to_ORDERS1___NBR_OF_LABEL as NBR_OF_LABEL", \
    	"SQ_Shortcut_to_ORDERS1___NBR_OF_PAKNG_SLIPS as NBR_OF_PAKNG_SLIPS", \
    	"SQ_Shortcut_to_ORDERS1___PALLET_CONTENT_LABEL_TYPE as PALLET_CONTENT_LABEL_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___LPN_LABEL_TYPE as LPN_LABEL_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___PACK_SLIP_TYPE as PACK_SLIP_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___BOL_TYPE as BOL_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___MANIF_TYPE as MANIF_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___PRINT_CANADIAN_CUST_INVC_FLAG as PRINT_CANADIAN_CUST_INVC_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___PRINT_COO as PRINT_COO", \
    	"SQ_Shortcut_to_ORDERS1___PRINT_DOCK_RCPT_FLAG as PRINT_DOCK_RCPT_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___PRINT_INV as PRINT_INV", \
    	"SQ_Shortcut_to_ORDERS1___PRINT_NAFTA_COO_FLAG as PRINT_NAFTA_COO_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___PRINT_OCEAN_BOL_FLAG as PRINT_OCEAN_BOL_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___PRINT_SED as PRINT_SED", \
    	"SQ_Shortcut_to_ORDERS1___PRINT_SHPR_LTR_OF_INSTR_FLAG as PRINT_SHPR_LTR_OF_INSTR_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___PRINT_PKG_LIST_FLAG as PRINT_PKG_LIST_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___FREIGHT_REVENUE_CURRENCY_CODE as FREIGHT_REVENUE_CURRENCY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___FREIGHT_REVENUE as FREIGHT_REVENUE", \
    	"SQ_Shortcut_to_ORDERS1___TRANS_PLAN_DIRECTION as TRANS_PLAN_DIRECTION", \
    	"SQ_Shortcut_to_ORDERS1___DSG_VOYAGE_FLIGHT as DSG_VOYAGE_FLIGHT", \
    	"SQ_Shortcut_to_ORDERS1___IS_D_POBOX as IS_D_POBOX", \
    	"SQ_Shortcut_to_ORDERS1___REF_SHIPMENT_NBR as REF_SHIPMENT_NBR", \
    	"SQ_Shortcut_to_ORDERS1___REF_STOP_SEQ as REF_STOP_SEQ", \
    	"SQ_Shortcut_to_ORDERS1___HAZ_OFFEROR_NAME as HAZ_OFFEROR_NAME", \
    	"SQ_Shortcut_to_ORDERS1___DISTRO_NUMBER as DISTRO_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___INCOTERM_ID as INCOTERM_ID", \
    	"SQ_Shortcut_to_ORDERS1___DIRECTION as DIRECTION", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_SHIPMENT_SEQ as ORDER_SHIPMENT_SEQ", \
    	"SQ_Shortcut_to_ORDERS1___MOVEMENT_TYPE as MOVEMENT_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___ACCT_RCVBL_ACCT_NBR as ACCT_RCVBL_ACCT_NBR", \
    	"SQ_Shortcut_to_ORDERS1___REF_FIELD_4 as REF_FIELD_4", \
    	"SQ_Shortcut_to_ORDERS1___REF_FIELD_5 as REF_FIELD_5", \
    	"SQ_Shortcut_to_ORDERS1___REF_FIELD_6 as REF_FIELD_6", \
    	"SQ_Shortcut_to_ORDERS1___REF_FIELD_7 as REF_FIELD_7", \
    	"SQ_Shortcut_to_ORDERS1___REF_FIELD_8 as REF_FIELD_8", \
    	"SQ_Shortcut_to_ORDERS1___REF_FIELD_9 as REF_FIELD_9", \
    	"SQ_Shortcut_to_ORDERS1___REF_FIELD_10 as REF_FIELD_10", \
    	"SQ_Shortcut_to_ORDERS1___FIRST_ZONE as FIRST_ZONE", \
    	"SQ_Shortcut_to_ORDERS1___LAST_ZONE as LAST_ZONE", \
    	"SQ_Shortcut_to_ORDERS1___NBR_OF_ZONES as NBR_OF_ZONES", \
    	"SQ_Shortcut_to_ORDERS1___REF_NUM1 as REF_NUM1", \
    	"SQ_Shortcut_to_ORDERS1___REF_NUM2 as REF_NUM2", \
    	"SQ_Shortcut_to_ORDERS1___REF_NUM3 as REF_NUM3", \
    	"SQ_Shortcut_to_ORDERS1___REF_NUM4 as REF_NUM4", \
    	"SQ_Shortcut_to_ORDERS1___REF_NUM5 as REF_NUM5", \
    	"SQ_Shortcut_to_ORDERS1___SPL_INSTR_CODE_1 as SPL_INSTR_CODE_1", \
    	"SQ_Shortcut_to_ORDERS1___SPL_INSTR_CODE_2 as SPL_INSTR_CODE_2", \
    	"SQ_Shortcut_to_ORDERS1___SPL_INSTR_CODE_3 as SPL_INSTR_CODE_3", \
    	"SQ_Shortcut_to_ORDERS1___SPL_INSTR_CODE_4 as SPL_INSTR_CODE_4", \
    	"SQ_Shortcut_to_ORDERS1___SPL_INSTR_CODE_5 as SPL_INSTR_CODE_5", \
    	"SQ_Shortcut_to_ORDERS1___SPL_INSTR_CODE_6 as SPL_INSTR_CODE_6", \
    	"SQ_Shortcut_to_ORDERS1___SPL_INSTR_CODE_7 as SPL_INSTR_CODE_7", \
    	"SQ_Shortcut_to_ORDERS1___SPL_INSTR_CODE_8 as SPL_INSTR_CODE_8", \
    	"SQ_Shortcut_to_ORDERS1___SPL_INSTR_CODE_9 as SPL_INSTR_CODE_9", \
    	"SQ_Shortcut_to_ORDERS1___SPL_INSTR_CODE_10 as SPL_INSTR_CODE_10", \
    	"SQ_Shortcut_to_ORDERS1___SINGLE_UNIT_FLAG as SINGLE_UNIT_FLAG", \
    	"SQ_Shortcut_to_ORDERS1___TOTAL_NBR_OF_UNITS as TOTAL_NBR_OF_UNITS", \
    	"SQ_Shortcut_to_ORDERS1___IS_ROUTED as IS_ROUTED", \
    	"SQ_Shortcut_to_ORDERS1___DSG_TRAILER_NUMBER as DSG_TRAILER_NUMBER", \
    	"SQ_Shortcut_to_ORDERS1___STAGING_LOCN_ID as STAGING_LOCN_ID", \
    	"SQ_Shortcut_to_ORDERS1___UPSMI_COST_CENTER as UPSMI_COST_CENTER", \
    	"SQ_Shortcut_to_ORDERS1___EST_PALLET_BRIDGED as EST_PALLET_BRIDGED", \
    	"SQ_Shortcut_to_ORDERS1___EST_PALLET as EST_PALLET", \
    	"SQ_Shortcut_to_ORDERS1___PICKLIST_ID as PICKLIST_ID", \
    	"SQ_Shortcut_to_ORDERS1___EFFECTIVE_RANK as EFFECTIVE_RANK", \
    	"SQ_Shortcut_to_ORDERS1___CHASE_ELIGIBLE as CHASE_ELIGIBLE", \
    	"SQ_Shortcut_to_ORDERS1___SHIPPING_CHANNEL as SHIPPING_CHANNEL", \
    	"SQ_Shortcut_to_ORDERS1___IS_GUARANTEED_DELIVERY as IS_GUARANTEED_DELIVERY", \
    	"SQ_Shortcut_to_ORDERS1___FAILED_GUARANTEED_DELIVERY as FAILED_GUARANTEED_DELIVERY", \
    	"SQ_Shortcut_to_ORDERS1___APPLY_LPNTYPE_FOR_ORDER as APPLY_LPNTYPE_FOR_ORDER", \
    	"SQ_Shortcut_to_ORDERS1___PO_TYPE_ATTR as PO_TYPE_ATTR", \
    	"SQ_Shortcut_to_ORDERS1___EPI_SERVICE_GROUP as EPI_SERVICE_GROUP", \
    	"SQ_Shortcut_to_ORDERS1___IMPORTER_OF_RECORD_NBR as IMPORTER_OF_RECORD_NBR", \
    	"SQ_Shortcut_to_ORDERS1___B13A_EXPORT_DECL_NBR as B13A_EXPORT_DECL_NBR", \
    	"SQ_Shortcut_to_ORDERS1___RETURN_ADDR_CODE as RETURN_ADDR_CODE", \
    	"SQ_Shortcut_to_ORDERS1___CHANNEL_TYPE as CHANNEL_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___STATIC_ROUTE_DELIVERY_TYPE as STATIC_ROUTE_DELIVERY_TYPE", \
    	"SQ_Shortcut_to_ORDERS1___ASSIGNED_CARRIER_CODE as ASSIGNED_CARRIER_CODE", \
    	"SQ_Shortcut_to_ORDERS1___DSG_CARRIER_CODE as DSG_CARRIER_CODE", \
    	"SQ_Shortcut_to_ORDERS1___SOURCE_ORDER as SOURCE_ORDER", \
    	"SQ_Shortcut_to_ORDERS1___AUTO_APPOINTMENT as AUTO_APPOINTMENT", \
    	"SQ_Shortcut_to_ORDERS1___BASELINE_LINEHAUL_COST as BASELINE_LINEHAUL_COST", \
    	"SQ_Shortcut_to_ORDERS1___BASELINE_ACCESSORIAL_COST as BASELINE_ACCESSORIAL_COST", \
    	"SQ_Shortcut_to_ORDERS1___BASELINE_SHIP_VIA as BASELINE_SHIP_VIA", \
    	"SQ_Shortcut_to_ORDERS1___DELIVERY_CHANNEL_ID as DELIVERY_CHANNEL_ID", \
    	"SQ_Shortcut_to_ORDERS1___ORDER_STREAM as ORDER_STREAM", \
    	"SQ_Shortcut_to_ORDERS1___LINEHAUL_REVENUE as LINEHAUL_REVENUE", \
    	"SQ_Shortcut_to_ORDERS1___ACCESSORIAL_REVENUE as ACCESSORIAL_REVENUE", \
    	"SQ_Shortcut_to_ORDERS1___STOP_OFF_REVENUE as STOP_OFF_REVENUE", \
    	"SQ_Shortcut_to_ORDERS1___REVENUE_LANE_ID as REVENUE_LANE_ID", \
    	"SQ_Shortcut_to_ORDERS1___REVENUE_LANE_DETAIL_ID as REVENUE_LANE_DETAIL_ID", \
    	"SQ_Shortcut_to_ORDERS1___MARGIN as MARGIN", \
    	"SQ_Shortcut_to_ORDERS1___MARGIN_CURRENCY_CODE as MARGIN_CURRENCY_CODE", \
    	"SQ_Shortcut_to_ORDERS1___CM_DISCOUNT_REVENUE as CM_DISCOUNT_REVENUE", \
    	"SQ_Shortcut_to_ORDERS1___C_TMS_PLAN_ID as C_TMS_PLAN_ID", \
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_ORDERS_PRE, type TARGET 
    # COLUMN COUNT: 371
    
    
    Shortcut_to_WM_ORDERS_PRE = ExP_TRN.selectExpr( \
    	"CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR", \
    	"CAST(ORDER_ID AS BIGINT) as ORDER_ID", \
    	"CAST(TC_ORDER_ID AS STRING) as TC_ORDER_ID", \
    	"CAST(TC_ORDER_ID_U AS STRING) as TC_ORDER_ID_U", \
    	"CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID", \
    	"CAST(CREATION_TYPE AS TINYINT) as CREATION_TYPE", \
    	"CAST(BUSINESS_PARTNER_ID AS STRING) as BUSINESS_PARTNER_ID", \
    	"CAST(PARENT_ORDER_ID AS BIGINT) as PARENT_ORDER_ID", \
    	"CAST(EXT_PURCHASE_ORDER AS STRING) as EXT_PURCHASE_ORDER", \
    	"CAST(CONS_RUN_ID AS BIGINT) as CONS_RUN_ID", \
    	"CAST(O_FACILITY_ALIAS_ID AS STRING) as O_FACILITY_ALIAS_ID", \
    	"CAST(O_FACILITY_ID AS INT) as O_FACILITY_ID", \
    	"CAST(O_DOCK_ID AS STRING) as O_DOCK_ID", \
    	"CAST(O_ADDRESS_1 AS STRING) as O_ADDRESS_1", \
    	"CAST(O_ADDRESS_2 AS STRING) as O_ADDRESS_2", \
    	"CAST(O_ADDRESS_3 AS STRING) as O_ADDRESS_3", \
    	"CAST(O_CITY AS STRING) as O_CITY", \
    	"CAST(O_STATE_PROV AS STRING) as O_STATE_PROV", \
    	"CAST(O_POSTAL_CODE AS STRING) as O_POSTAL_CODE", \
    	"CAST(O_COUNTY AS STRING) as O_COUNTY", \
    	"CAST(O_COUNTRY_CODE AS STRING) as O_COUNTRY_CODE", \
    	"CAST(D_FACILITY_ALIAS_ID AS STRING) as D_FACILITY_ALIAS_ID", \
    	"CAST(D_FACILITY_ID AS INT) as D_FACILITY_ID", \
    	"CAST(D_DOCK_ID AS STRING) as D_DOCK_ID", \
    	"CAST(D_ADDRESS_1 AS STRING) as D_ADDRESS_1", \
    	"CAST(D_ADDRESS_2 AS STRING) as D_ADDRESS_2", \
    	"CAST(D_ADDRESS_3 AS STRING) as D_ADDRESS_3", \
    	"CAST(D_CITY AS STRING) as D_CITY", \
    	"CAST(D_STATE_PROV AS STRING) as D_STATE_PROV", \
    	"CAST(D_POSTAL_CODE AS STRING) as D_POSTAL_CODE", \
    	"CAST(D_COUNTY AS STRING) as D_COUNTY", \
    	"CAST(D_COUNTRY_CODE AS STRING) as D_COUNTRY_CODE", \
    	"CAST(BILL_TO_NAME AS STRING) as BILL_TO_NAME", \
    	"CAST(BILL_FACILITY_ALIAS_ID AS STRING) as BILL_FACILITY_ALIAS_ID", \
    	"CAST(BILL_FACILITY_ID AS BIGINT) as BILL_FACILITY_ID", \
    	"CAST(BILL_TO_ADDRESS_1 AS STRING) as BILL_TO_ADDRESS_1", \
    	"CAST(BILL_TO_ADDRESS_2 AS STRING) as BILL_TO_ADDRESS_2", \
    	"CAST(BILL_TO_ADDRESS_3 AS STRING) as BILL_TO_ADDRESS_3", \
    	"CAST(BILL_TO_CITY AS STRING) as BILL_TO_CITY", \
    	"CAST(BILL_TO_STATE_PROV AS STRING) as BILL_TO_STATE_PROV", \
    	"CAST(BILL_TO_COUNTY AS STRING) as BILL_TO_COUNTY", \
    	"CAST(BILL_TO_POSTAL_CODE AS STRING) as BILL_TO_POSTAL_CODE", \
    	"CAST(BILL_TO_COUNTRY_CODE AS STRING) as BILL_TO_COUNTRY_CODE", \
    	"CAST(BILL_TO_PHONE_NUMBER AS STRING) as BILL_TO_PHONE_NUMBER", \
    	"CAST(BILL_TO_FAX_NUMBER AS STRING) as BILL_TO_FAX_NUMBER", \
    	"CAST(BILL_TO_EMAIL AS STRING) as BILL_TO_EMAIL", \
    	"CAST(PLAN_O_FACILITY_ID AS BIGINT) as PLAN_O_FACILITY_ID", \
    	"CAST(PLAN_D_FACILITY_ID AS INT) as PLAN_D_FACILITY_ID", \
    	"CAST(PLAN_O_FACILITY_ALIAS_ID AS STRING) as PLAN_O_FACILITY_ALIAS_ID", \
    	"CAST(PLAN_D_FACILITY_ALIAS_ID AS STRING) as PLAN_D_FACILITY_ALIAS_ID", \
    	"CAST(PLAN_LEG_D_ALIAS_ID AS STRING) as PLAN_LEG_D_ALIAS_ID", \
    	"CAST(PLAN_LEG_O_ALIAS_ID AS STRING) as PLAN_LEG_O_ALIAS_ID", \
    	"CAST(INCOTERM_FACILITY_ID AS INT) as INCOTERM_FACILITY_ID", \
    	"CAST(INCOTERM_FACILITY_ALIAS_ID AS STRING) as INCOTERM_FACILITY_ALIAS_ID", \
    	"CAST(INCOTERM_LOC_AVA_DTTM AS TIMESTAMP) as INCOTERM_LOC_AVA_DTTM", \
    	"CAST(INCOTERM_LOC_AVA_TIME_ZONE_ID AS INT) as INCOTERM_LOC_AVA_TIME_ZONE_ID", \
    	"CAST(PICKUP_TZ AS SMALLINT) as PICKUP_TZ", \
    	"CAST(DELIVERY_TZ AS SMALLINT) as DELIVERY_TZ", \
    	"CAST(PICKUP_START_DTTM AS TIMESTAMP) as PICKUP_START_DTTM", \
    	"CAST(PICKUP_END_DTTM AS TIMESTAMP) as PICKUP_END_DTTM", \
    	"CAST(DELIVERY_START_DTTM AS TIMESTAMP) as DELIVERY_START_DTTM", \
    	"CAST(DELIVERY_END_DTTM AS TIMESTAMP) as DELIVERY_END_DTTM", \
    	"CAST(ORDER_DATE_DTTM AS TIMESTAMP) as ORDER_DATE_DTTM", \
    	"CAST(ORDER_RECON_DTTM AS TIMESTAMP) as ORDER_RECON_DTTM", \
    	"CAST(INBOUND_REGION_ID AS INT) as INBOUND_REGION_ID", \
    	"CAST(OUTBOUND_REGION_ID AS INT) as OUTBOUND_REGION_ID", \
    	"CAST(DSG_SERVICE_LEVEL_ID AS BIGINT) as DSG_SERVICE_LEVEL_ID", \
    	"CAST(DSG_CARRIER_ID AS BIGINT) as DSG_CARRIER_ID", \
    	"CAST(DSG_EQUIPMENT_ID AS BIGINT) as DSG_EQUIPMENT_ID", \
    	"CAST(DSG_TRACTOR_EQUIPMENT_ID AS BIGINT) as DSG_TRACTOR_EQUIPMENT_ID", \
    	"CAST(DSG_MOT_ID AS BIGINT) as DSG_MOT_ID", \
    	"CAST(BASELINE_MOT_ID AS BIGINT) as BASELINE_MOT_ID", \
    	"CAST(BASELINE_SERVICE_LEVEL_ID AS BIGINT) as BASELINE_SERVICE_LEVEL_ID", \
    	"CAST(PRODUCT_CLASS_ID AS INT) as PRODUCT_CLASS_ID", \
    	"CAST(PROTECTION_LEVEL_ID AS INT) as PROTECTION_LEVEL_ID", \
    	"CAST(MERCHANDIZING_DEPARTMENT_ID AS BIGINT) as MERCHANDIZING_DEPARTMENT_ID", \
    	"CAST(WAVE_ID AS BIGINT) as WAVE_ID", \
    	"CAST(WAVE_OPTION_ID AS BIGINT) as WAVE_OPTION_ID", \
    	"CAST(PATH_ID AS BIGINT) as PATH_ID", \
    	"CAST(PATH_SET_ID AS BIGINT) as PATH_SET_ID", \
    	"CAST(DRIVER_TYPE_ID AS BIGINT) as DRIVER_TYPE_ID", \
    	"CAST(UN_NUMBER_ID AS BIGINT) as UN_NUMBER_ID", \
    	"CAST(BLOCK_AUTO_CREATE AS TINYINT) as BLOCK_AUTO_CREATE", \
    	"CAST(BLOCK_AUTO_CONSOLIDATE AS TINYINT) as BLOCK_AUTO_CONSOLIDATE", \
    	"CAST(HAS_ALERTS AS TINYINT) as HAS_ALERTS", \
    	"CAST(HAS_EM_NOTIFY_FLAG AS TINYINT) as HAS_EM_NOTIFY_FLAG", \
    	"CAST(HAS_IMPORT_ERROR AS TINYINT) as HAS_IMPORT_ERROR", \
    	"CAST(HAS_NOTES AS TINYINT) as HAS_NOTES", \
    	"CAST(HAS_SOFT_CHECK_ERRORS AS TINYINT) as HAS_SOFT_CHECK_ERRORS", \
    	"CAST(HAS_SPLIT AS TINYINT) as HAS_SPLIT", \
    	"CAST(IS_BOOKING_REQUIRED AS TINYINT) as IS_BOOKING_REQUIRED", \
    	"CAST(IS_CANCELLED AS TINYINT) as IS_CANCELLED", \
    	"CAST(IS_HAZMAT AS TINYINT) as IS_HAZMAT", \
    	"CAST(IS_IMPORTED AS TINYINT) as IS_IMPORTED", \
    	"CAST(IS_PARTIALLY_PLANNED AS TINYINT) as IS_PARTIALLY_PLANNED", \
    	"CAST(IS_PERISHABLE AS TINYINT) as IS_PERISHABLE", \
    	"CAST(IS_SUSPENDED AS TINYINT) as IS_SUSPENDED", \
    	"CAST(NORMALIZED_BASELINE_COST AS DECIMAL(13,4)) as NORMALIZED_BASELINE_COST", \
    	"CAST(BASELINE_COST_CURRENCY_CODE AS STRING) as BASELINE_COST_CURRENCY_CODE", \
    	"CAST(ORIG_BUDG_COST AS DECIMAL(13,4)) as ORIG_BUDG_COST", \
    	"CAST(BUDG_COST AS DECIMAL(13,4)) as BUDG_COST", \
    	"CAST(ACTUAL_COST AS DECIMAL(13,4)) as ACTUAL_COST", \
    	"CAST(BASELINE_COST AS DECIMAL(13,4)) as BASELINE_COST", \
    	"CAST(TRANS_RESP_CODE AS STRING) as TRANS_RESP_CODE", \
    	"CAST(BILLING_METHOD AS TINYINT) as BILLING_METHOD", \
    	"CAST(DROPOFF_PICKUP AS STRING) as DROPOFF_PICKUP", \
    	"CAST(MOVEMENT_OPTION AS TINYINT) as MOVEMENT_OPTION", \
    	"CAST(PRIORITY AS SMALLINT) as PRIORITY", \
    	"CAST(MOVE_TYPE AS TINYINT) as MOVE_TYPE", \
    	"CAST(SCHED_DOW AS TINYINT) as SCHED_DOW", \
    	"CAST(EQUIPMENT_TYPE AS SMALLINT) as EQUIPMENT_TYPE", \
    	"CAST(DELIVERY_REQ AS STRING) as DELIVERY_REQ", \
    	"CAST(MV_CURRENCY_CODE AS STRING) as MV_CURRENCY_CODE", \
    	"CAST(MONETARY_VALUE AS DECIMAL(16,4)) as MONETARY_VALUE", \
    	"CAST(COMPARTMENT_NO AS TINYINT) as COMPARTMENT_NO", \
    	"CAST(PACKAGING AS STRING) as PACKAGING", \
    	"CAST(ORDER_LOADING_SEQ AS INT) as ORDER_LOADING_SEQ", \
    	"CAST(REF_FIELD_1 AS STRING) as REF_FIELD_1", \
    	"CAST(REF_FIELD_2 AS STRING) as REF_FIELD_2", \
    	"CAST(REF_FIELD_3 AS STRING) as REF_FIELD_3", \
    	"CAST(CREATED_SOURCE_TYPE AS TINYINT) as CREATED_SOURCE_TYPE", \
    	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
    	"CAST(LAST_UPDATED_SOURCE_TYPE AS TINYINT) as LAST_UPDATED_SOURCE_TYPE", \
    	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
    	"CAST(HIBERNATE_VERSION AS DECIMAL(20,7)) as HIBERNATE_VERSION", \
    	"CAST(ACTUAL_COST_CURRENCY_CODE AS STRING) as ACTUAL_COST_CURRENCY_CODE", \
    	"CAST(BUDG_COST_CURRENCY_CODE AS STRING) as BUDG_COST_CURRENCY_CODE", \
    	"CAST(SHIPMENT_ID AS BIGINT) as SHIPMENT_ID", \
    	"CAST(BILL_TO_TITLE AS STRING) as BILL_TO_TITLE", \
    	"CAST(ORDER_STATUS AS SMALLINT) as ORDER_STATUS", \
    	"CAST(ADDR_CODE AS STRING) as ADDR_CODE", \
    	"CAST(ADDR_VALID AS STRING) as ADDR_VALID", \
    	"CAST(ADVT_DATE AS TIMESTAMP) as ADVT_DATE", \
    	"CAST(CHUTE_ID AS STRING) as CHUTE_ID", \
    	"CAST(COD_FUNDS AS STRING) as COD_FUNDS", \
    	"CAST(DC_CTR_NBR AS STRING) as DC_CTR_NBR", \
    	"CAST(DO_STATUS AS SMALLINT) as DO_STATUS", \
    	"CAST(DO_TYPE AS TINYINT) as DO_TYPE", \
    	"CAST(DOCS_ONLY_SHPMT AS STRING) as DOCS_ONLY_SHPMT", \
    	"CAST(DUTY_AND_TAX AS DECIMAL(13,4)) as DUTY_AND_TAX", \
    	"CAST(DUTY_TAX_ACCT_NBR AS STRING) as DUTY_TAX_ACCT_NBR", \
    	"CAST(DUTY_TAX_PAYMENT_TYPE AS TINYINT) as DUTY_TAX_PAYMENT_TYPE", \
    	"CAST(EST_LPN AS BIGINT) as EST_LPN", \
    	"CAST(EST_LPN_BRIDGED AS BIGINT) as EST_LPN_BRIDGED", \
    	"CAST(FTSR_NBR AS STRING) as FTSR_NBR", \
    	"CAST(IS_BACK_ORDERED AS STRING) as IS_BACK_ORDERED", \
    	"CAST(LPN_CUBING_INDIC AS TINYINT) as LPN_CUBING_INDIC", \
    	"CAST(O_PHONE_NUMBER AS STRING) as O_PHONE_NUMBER", \
    	"CAST(ORDER_PRINT_DTTM AS TIMESTAMP) as ORDER_PRINT_DTTM", \
    	"CAST(PALLET_CUBING_INDIC AS TINYINT) as PALLET_CUBING_INDIC", \
    	"CAST(PRE_PACK_FLAG AS TINYINT) as PRE_PACK_FLAG", \
    	"CAST(RTE_TYPE_1 AS STRING) as RTE_TYPE_1", \
    	"CAST(RTE_TYPE_2 AS STRING) as RTE_TYPE_2", \
    	"CAST(SHIP_GROUP_SEQUENCE AS SMALLINT) as SHIP_GROUP_SEQUENCE", \
    	"CAST(SHPNG_CHRG AS DECIMAL(11,2)) as SHPNG_CHRG", \
    	"CAST(STAGE_INDIC AS TINYINT) as STAGE_INDIC", \
    	"CAST(STORE_NBR AS STRING) as STORE_NBR", \
    	"CAST(TOTAL_NBR_OF_LPN AS INT) as TOTAL_NBR_OF_LPN", \
    	"CAST(TOTAL_NBR_OF_PLT AS INT) as TOTAL_NBR_OF_PLT", \
    	"CAST(WM_ORDER_STATUS AS SMALLINT) as WM_ORDER_STATUS", \
    	"CAST(D_NAME AS STRING) as D_NAME", \
    	"CAST(O_CONTACT AS STRING) as O_CONTACT", \
    	"CAST(SECONDARY_MAXI_ADDR_NBR AS STRING) as SECONDARY_MAXI_ADDR_NBR", \
    	"CAST(D_CONTACT AS STRING) as D_CONTACT", \
    	"CAST(AES_ITN AS STRING) as AES_ITN", \
    	"CAST(BATCH_ID AS BIGINT) as BATCH_ID", \
    	"CAST(ORIGIN_SHIP_THRU_FACILITY_ID AS INT) as ORIGIN_SHIP_THRU_FACILITY_ID", \
    	"CAST(ORIGIN_SHIP_THRU_FAC_ALIAS_ID AS STRING) as ORIGIN_SHIP_THRU_FAC_ALIAS_ID", \
    	"CAST(DEST_SHIP_THRU_FACILITY_ID AS INT) as DEST_SHIP_THRU_FACILITY_ID", \
    	"CAST(DEST_SHIP_THRU_FAC_ALIAS_ID AS STRING) as DEST_SHIP_THRU_FAC_ALIAS_ID", \
    	"CAST(ACTUAL_SHIPPED_DTTM AS TIMESTAMP) as ACTUAL_SHIPPED_DTTM", \
    	"CAST(BILL_OF_LADING_NUMBER AS STRING) as BILL_OF_LADING_NUMBER", \
    	"CAST(BOL_BREAK_ATTR AS STRING) as BOL_BREAK_ATTR", \
    	"CAST(D_EMAIL AS STRING) as D_EMAIL", \
    	"CAST(D_FAX_NUMBER AS STRING) as D_FAX_NUMBER", \
    	"CAST(D_PHONE_NUMBER AS STRING) as D_PHONE_NUMBER", \
    	"CAST(DYNAMIC_ROUTING_REQD AS STRING) as DYNAMIC_ROUTING_REQD", \
    	"CAST(INTL_GOODS_DESC AS STRING) as INTL_GOODS_DESC", \
    	"CAST(MAJOR_ORDER_GRP_ATTR AS STRING) as MAJOR_ORDER_GRP_ATTR", \
    	"CAST(ORDER_CONSOL_PROFILE AS STRING) as ORDER_CONSOL_PROFILE", \
    	"CAST(PACK_SLIP_PRT_CNT AS TINYINT) as PACK_SLIP_PRT_CNT", \
    	"CAST(PROD_SCHED_REF_NUMBER AS STRING) as PROD_SCHED_REF_NUMBER", \
    	"CAST(MUST_RELEASE_BY_DTTM AS TIMESTAMP) as MUST_RELEASE_BY_DTTM", \
    	"CAST(CANCEL_DTTM AS TIMESTAMP) as CANCEL_DTTM", \
    	"CAST(ASSIGNED_STATIC_ROUTE_ID AS STRING) as ASSIGNED_STATIC_ROUTE_ID", \
    	"CAST(DSG_STATIC_ROUTE_ID AS STRING) as DSG_STATIC_ROUTE_ID", \
    	"CAST(O_FACILITY_NAME AS STRING) as O_FACILITY_NAME", \
    	"CAST(O_DOCK_DOOR_ID AS BIGINT) as O_DOCK_DOOR_ID", \
    	"CAST(O_FAX_NUMBER AS STRING) as O_FAX_NUMBER", \
    	"CAST(O_EMAIL AS STRING) as O_EMAIL", \
    	"CAST(D_FACILITY_NAME AS STRING) as D_FACILITY_NAME", \
    	"CAST(D_DOCK_DOOR_ID AS BIGINT) as D_DOCK_DOOR_ID", \
    	"CAST(PURCHASE_ORDER_NUMBER AS STRING) as PURCHASE_ORDER_NUMBER", \
    	"CAST(CUSTOMER_ID AS BIGINT) as CUSTOMER_ID", \
    	"CAST(REPL_WAVE_NBR AS STRING) as REPL_WAVE_NBR", \
    	"CAST(CUBING_STATUS AS SMALLINT) as CUBING_STATUS", \
    	"CAST(SCHED_PICKUP_DTTM AS TIMESTAMP) as SCHED_PICKUP_DTTM", \
    	"CAST(SCHED_DELIVERY_DTTM AS TIMESTAMP) as SCHED_DELIVERY_DTTM", \
    	"CAST(ZONE_SKIP_HUB_LOCATION_ID AS INT) as ZONE_SKIP_HUB_LOCATION_ID", \
    	"CAST(BILL_TO_FACILITY_NAME AS STRING) as BILL_TO_FACILITY_NAME", \
    	"CAST(FREIGHT_CLASS AS DECIMAL(5,1)) as FREIGHT_CLASS", \
    	"CAST(NON_MACHINEABLE AS SMALLINT) as NON_MACHINEABLE", \
    	"CAST(ASSIGNED_MOT_ID AS BIGINT) as ASSIGNED_MOT_ID", \
    	"CAST(ASSIGNED_CARRIER_ID AS BIGINT) as ASSIGNED_CARRIER_ID", \
    	"CAST(ASSIGNED_SERVICE_LEVEL_ID AS BIGINT) as ASSIGNED_SERVICE_LEVEL_ID", \
    	"CAST(ASSIGNED_EQUIPMENT_ID AS BIGINT) as ASSIGNED_EQUIPMENT_ID", \
    	"CAST(DYNAMIC_REQUEST_SENT AS BIGINT) as DYNAMIC_REQUEST_SENT", \
    	"CAST(COMMODITY_CODE_ID AS BIGINT) as COMMODITY_CODE_ID", \
    	"CAST(TC_SHIPMENT_ID AS STRING) as TC_SHIPMENT_ID", \
    	"CAST(IN_TRANSIT_ALLOCATION AS TINYINT) as IN_TRANSIT_ALLOCATION", \
    	"CAST(RELEASE_DESTINATION AS SMALLINT) as RELEASE_DESTINATION", \
    	"CAST(TEMPLATE_ID AS BIGINT) as TEMPLATE_ID", \
    	"CAST(COD_AMOUNT AS DECIMAL(13,4)) as COD_AMOUNT", \
    	"CAST(BILL_TO_CONTACT_NAME AS STRING) as BILL_TO_CONTACT_NAME", \
    	"CAST(SHIP_GROUP_ID AS STRING) as SHIP_GROUP_ID", \
    	"CAST(PURCHASE_ORDER_ID AS BIGINT) as PURCHASE_ORDER_ID", \
    	"CAST(DSG_HUB_LOCATION_ID AS INT) as DSG_HUB_LOCATION_ID", \
    	"CAST(OVERRIDE_BILLING_METHOD AS TINYINT) as OVERRIDE_BILLING_METHOD", \
    	"CAST(PARENT_TYPE AS TINYINT) as PARENT_TYPE", \
    	"CAST(ACCT_RCVBL_CODE AS STRING) as ACCT_RCVBL_CODE", \
    	"CAST(ADVT_CODE AS STRING) as ADVT_CODE", \
    	"CAST(IS_CUSTOMER_PICKUP AS STRING) as IS_CUSTOMER_PICKUP", \
    	"CAST(IS_DIRECT_ALLOWED AS STRING) as IS_DIRECT_ALLOWED", \
    	"CAST(RTE_ATTR AS STRING) as RTE_ATTR", \
    	"CAST(LAST_RUN_ID AS BIGINT) as LAST_RUN_ID", \
    	"CAST(PLAN_DUE_DTTM AS TIMESTAMP) as PLAN_DUE_DTTM", \
    	"CAST(IS_ORDER_RECONCILED AS TINYINT) as IS_ORDER_RECONCILED", \
    	"CAST(PRTL_SHIP_CONF_FLAG AS TINYINT) as PRTL_SHIP_CONF_FLAG", \
    	"CAST(PRTL_SHIP_CONF_STATUS AS TINYINT) as PRTL_SHIP_CONF_STATUS", \
    	"CAST(ALLOW_PRE_BILLING AS TINYINT) as ALLOW_PRE_BILLING", \
    	"CAST(LANE_NAME AS STRING) as LANE_NAME", \
    	"CAST(DECLARED_VALUE AS DECIMAL(16,4)) as DECLARED_VALUE", \
    	"CAST(DV_CURRENCY_CODE AS STRING) as DV_CURRENCY_CODE", \
    	"CAST(COD_CURRENCY_CODE AS STRING) as COD_CURRENCY_CODE", \
    	"CAST(WEIGHT_UOM_ID_BASE AS INT) as WEIGHT_UOM_ID_BASE", \
    	"CAST(LINE_HAUL_SHIP_VIA AS STRING) as LINE_HAUL_SHIP_VIA", \
    	"CAST(ORIGINAL_ASSIGNED_SHIP_VIA AS STRING) as ORIGINAL_ASSIGNED_SHIP_VIA", \
    	"CAST(DISTRIBUTION_SHIP_VIA AS STRING) as DISTRIBUTION_SHIP_VIA", \
    	"CAST(DSG_SHIP_VIA AS STRING) as DSG_SHIP_VIA", \
    	"CAST(TAX_ID AS STRING) as TAX_ID", \
    	"CAST(ORDER_RECEIVED AS TINYINT) as ORDER_RECEIVED", \
    	"CAST(IS_ORIGINAL_ORDER AS TINYINT) as IS_ORIGINAL_ORDER", \
    	"CAST(FREIGHT_FORWARDER_ACCT_NBR AS STRING) as FREIGHT_FORWARDER_ACCT_NBR", \
    	"CAST(GLOBAL_LOCN_NBR AS STRING) as GLOBAL_LOCN_NBR", \
    	"CAST(IMPORTER_DEFN AS STRING) as IMPORTER_DEFN", \
    	"CAST(LANG_ID AS STRING) as LANG_ID", \
    	"CAST(MAJOR_MINOR_ORDER AS STRING) as MAJOR_MINOR_ORDER", \
    	"CAST(MAJOR_ORDER_CTRL_NBR AS STRING) as MAJOR_ORDER_CTRL_NBR", \
    	"CAST(MHE_FLAG AS STRING) as MHE_FLAG", \
    	"CAST(MHE_ORD_STATE AS STRING) as MHE_ORD_STATE", \
    	"CAST(ORDER_CONSOL_LOCN_ID AS STRING) as ORDER_CONSOL_LOCN_ID", \
    	"CAST(ORDER_TYPE AS STRING) as ORDER_TYPE", \
    	"CAST(PACK_WAVE_NBR AS STRING) as PACK_WAVE_NBR", \
    	"CAST(PARTIAL_LPN_OPTION AS STRING) as PARTIAL_LPN_OPTION", \
    	"CAST(PARTIES_RELATED AS STRING) as PARTIES_RELATED", \
    	"CAST(PNH_FLAG AS STRING) as PNH_FLAG", \
    	"CAST(PRE_STICKER_CODE AS STRING) as PRE_STICKER_CODE", \
    	"CAST(PRIMARY_MAXI_ADDR_NBR AS STRING) as PRIMARY_MAXI_ADDR_NBR", \
    	"CAST(RTE_SWC_NBR AS STRING) as RTE_SWC_NBR", \
    	"CAST(RTE_TO AS STRING) as RTE_TO", \
    	"CAST(RTE_WAVE_NBR AS STRING) as RTE_WAVE_NBR", \
    	"CAST(BASELINE_CARRIER_ID AS BIGINT) as BASELINE_CARRIER_ID", \
    	"CAST(COD_RETURN_COMPANY_NAME AS STRING) as COD_RETURN_COMPANY_NAME", \
    	"CAST(BILL_ACCT_NBR AS STRING) as BILL_ACCT_NBR", \
    	"CAST(CUST_BROKER_ACCT_NBR AS STRING) as CUST_BROKER_ACCT_NBR", \
    	"CAST(MANIFEST_NBR AS STRING) as MANIFEST_NBR", \
    	"CAST(DESTINATION_ACTION AS STRING) as DESTINATION_ACTION", \
    	"CAST(TRANS_PLAN_OWNER AS SMALLINT) as TRANS_PLAN_OWNER", \
    	"CAST(MARK_FOR AS STRING) as MARK_FOR", \
    	"CAST(DELIVERY_OPTIONS AS STRING) as DELIVERY_OPTIONS", \
    	"CAST(PRE_BILL_STATUS AS STRING) as PRE_BILL_STATUS", \
    	"CAST(CONTNT_LABEL_TYPE AS STRING) as CONTNT_LABEL_TYPE", \
    	"CAST(NBR_OF_CONTNT_LABEL AS INT) as NBR_OF_CONTNT_LABEL", \
    	"CAST(NBR_OF_LABEL AS INT) as NBR_OF_LABEL", \
    	"CAST(NBR_OF_PAKNG_SLIPS AS INT) as NBR_OF_PAKNG_SLIPS", \
    	"CAST(PALLET_CONTENT_LABEL_TYPE AS STRING) as PALLET_CONTENT_LABEL_TYPE", \
    	"CAST(LPN_LABEL_TYPE AS STRING) as LPN_LABEL_TYPE", \
    	"CAST(PACK_SLIP_TYPE AS STRING) as PACK_SLIP_TYPE", \
    	"CAST(BOL_TYPE AS STRING) as BOL_TYPE", \
    	"CAST(MANIF_TYPE AS STRING) as MANIF_TYPE", \
    	"CAST(PRINT_CANADIAN_CUST_INVC_FLAG AS STRING) as PRINT_CANADIAN_CUST_INVC_FLAG", \
    	"CAST(PRINT_COO AS STRING) as PRINT_COO", \
    	"CAST(PRINT_DOCK_RCPT_FLAG AS STRING) as PRINT_DOCK_RCPT_FLAG", \
    	"CAST(PRINT_INV AS STRING) as PRINT_INV", \
    	"CAST(PRINT_NAFTA_COO_FLAG AS STRING) as PRINT_NAFTA_COO_FLAG", \
    	"CAST(PRINT_OCEAN_BOL_FLAG AS STRING) as PRINT_OCEAN_BOL_FLAG", \
    	"CAST(PRINT_SED AS STRING) as PRINT_SED", \
    	"CAST(PRINT_SHPR_LTR_OF_INSTR_FLAG AS STRING) as PRINT_SHPR_LTR_OF_INSTR_FLAG", \
    	"CAST(PRINT_PKG_LIST_FLAG AS STRING) as PRINT_PKG_LIST_FLAG", \
    	"CAST(FREIGHT_REVENUE_CURRENCY_CODE AS STRING) as FREIGHT_REVENUE_CURRENCY_CODE", \
    	"CAST(FREIGHT_REVENUE AS DECIMAL(13,4)) as FREIGHT_REVENUE", \
    	"CAST(TRANS_PLAN_DIRECTION AS STRING) as TRANS_PLAN_DIRECTION", \
    	"CAST(DSG_VOYAGE_FLIGHT AS STRING) as DSG_VOYAGE_FLIGHT", \
    	"CAST(IS_D_POBOX AS TINYINT) as IS_D_POBOX", \
    	"CAST(REF_SHIPMENT_NBR AS STRING) as REF_SHIPMENT_NBR", \
    	"CAST(REF_STOP_SEQ AS INT) as REF_STOP_SEQ", \
    	"CAST(HAZ_OFFEROR_NAME AS STRING) as HAZ_OFFEROR_NAME", \
    	"CAST(DISTRO_NUMBER AS STRING) as DISTRO_NUMBER", \
    	"CAST(INCOTERM_ID AS INT) as INCOTERM_ID", \
    	"CAST(DIRECTION AS STRING) as DIRECTION", \
    	"CAST(ORDER_SHIPMENT_SEQ AS BIGINT) as ORDER_SHIPMENT_SEQ", \
    	"CAST(MOVEMENT_TYPE AS STRING) as MOVEMENT_TYPE", \
    	"CAST(ACCT_RCVBL_ACCT_NBR AS STRING) as ACCT_RCVBL_ACCT_NBR", \
    	"CAST(REF_FIELD_4 AS STRING) as REF_FIELD_4", \
    	"CAST(REF_FIELD_5 AS STRING) as REF_FIELD_5", \
    	"CAST(REF_FIELD_6 AS STRING) as REF_FIELD_6", \
    	"CAST(REF_FIELD_7 AS STRING) as REF_FIELD_7", \
    	"CAST(REF_FIELD_8 AS STRING) as REF_FIELD_8", \
    	"CAST(REF_FIELD_9 AS STRING) as REF_FIELD_9", \
    	"CAST(REF_FIELD_10 AS STRING) as REF_FIELD_10", \
    	"CAST(FIRST_ZONE AS STRING) as FIRST_ZONE", \
    	"CAST(LAST_ZONE AS STRING) as LAST_ZONE", \
    	"CAST(NBR_OF_ZONES AS SMALLINT) as NBR_OF_ZONES", \
    	"CAST(REF_NUM1 AS DECIMAL(13,5)) as REF_NUM1", \
    	"CAST(REF_NUM2 AS DECIMAL(13,5)) as REF_NUM2", \
    	"CAST(REF_NUM3 AS DECIMAL(13,5)) as REF_NUM3", \
    	"CAST(REF_NUM4 AS DECIMAL(13,5)) as REF_NUM4", \
    	"CAST(REF_NUM5 AS DECIMAL(13,5)) as REF_NUM5", \
    	"CAST(SPL_INSTR_CODE_1 AS STRING) as SPL_INSTR_CODE_1", \
    	"CAST(SPL_INSTR_CODE_2 AS STRING) as SPL_INSTR_CODE_2", \
    	"CAST(SPL_INSTR_CODE_3 AS STRING) as SPL_INSTR_CODE_3", \
    	"CAST(SPL_INSTR_CODE_4 AS STRING) as SPL_INSTR_CODE_4", \
    	"CAST(SPL_INSTR_CODE_5 AS STRING) as SPL_INSTR_CODE_5", \
    	"CAST(SPL_INSTR_CODE_6 AS STRING) as SPL_INSTR_CODE_6", \
    	"CAST(SPL_INSTR_CODE_7 AS STRING) as SPL_INSTR_CODE_7", \
    	"CAST(SPL_INSTR_CODE_8 AS STRING) as SPL_INSTR_CODE_8", \
    	"CAST(SPL_INSTR_CODE_9 AS STRING) as SPL_INSTR_CODE_9", \
    	"CAST(SPL_INSTR_CODE_10 AS STRING) as SPL_INSTR_CODE_10", \
    	"CAST(SINGLE_UNIT_FLAG AS TINYINT) as SINGLE_UNIT_FLAG", \
    	"CAST(TOTAL_NBR_OF_UNITS AS DECIMAL(15,4)) as TOTAL_NBR_OF_UNITS", \
    	"CAST(IS_ROUTED AS TINYINT) as IS_ROUTED", \
    	"CAST(DSG_TRAILER_NUMBER AS STRING) as DSG_TRAILER_NUMBER", \
    	"CAST(STAGING_LOCN_ID AS STRING) as STAGING_LOCN_ID", \
    	"CAST(UPSMI_COST_CENTER AS STRING) as UPSMI_COST_CENTER", \
    	"CAST(EST_PALLET_BRIDGED AS BIGINT) as EST_PALLET_BRIDGED", \
    	"CAST(EST_PALLET AS BIGINT) as EST_PALLET", \
    	"CAST(PICKLIST_ID AS STRING) as PICKLIST_ID", \
    	"CAST(EFFECTIVE_RANK AS STRING) as EFFECTIVE_RANK", \
    	"CAST(CHASE_ELIGIBLE AS TINYINT) as CHASE_ELIGIBLE", \
    	"CAST(SHIPPING_CHANNEL AS STRING) as SHIPPING_CHANNEL", \
    	"CAST(IS_GUARANTEED_DELIVERY AS TINYINT) as IS_GUARANTEED_DELIVERY", \
    	"CAST(FAILED_GUARANTEED_DELIVERY AS STRING) as FAILED_GUARANTEED_DELIVERY", \
    	"CAST(APPLY_LPNTYPE_FOR_ORDER AS TINYINT) as APPLY_LPNTYPE_FOR_ORDER", \
    	"CAST(PO_TYPE_ATTR AS STRING) as PO_TYPE_ATTR", \
    	"CAST(EPI_SERVICE_GROUP AS STRING) as EPI_SERVICE_GROUP", \
    	"CAST(IMPORTER_OF_RECORD_NBR AS STRING) as IMPORTER_OF_RECORD_NBR", \
    	"CAST(B13A_EXPORT_DECL_NBR AS STRING) as B13A_EXPORT_DECL_NBR", \
    	"CAST(RETURN_ADDR_CODE AS STRING) as RETURN_ADDR_CODE", \
    	"CAST(CHANNEL_TYPE AS SMALLINT) as CHANNEL_TYPE", \
    	"CAST(STATIC_ROUTE_DELIVERY_TYPE AS STRING) as STATIC_ROUTE_DELIVERY_TYPE", \
    	"CAST(ASSIGNED_CARRIER_CODE AS STRING) as ASSIGNED_CARRIER_CODE", \
    	"CAST(DSG_CARRIER_CODE AS STRING) as DSG_CARRIER_CODE", \
    	"CAST(SOURCE_ORDER AS STRING) as SOURCE_ORDER", \
    	"CAST(AUTO_APPOINTMENT AS TINYINT) as AUTO_APPOINTMENT", \
    	"CAST(BASELINE_LINEHAUL_COST AS DECIMAL(13,4)) as BASELINE_LINEHAUL_COST", \
    	"CAST(BASELINE_ACCESSORIAL_COST AS DECIMAL(13,4)) as BASELINE_ACCESSORIAL_COST", \
    	"CAST(BASELINE_SHIP_VIA AS STRING) as BASELINE_SHIP_VIA", \
    	"CAST(DELIVERY_CHANNEL_ID AS SMALLINT) as DELIVERY_CHANNEL_ID", \
    	"CAST(ORDER_STREAM AS TINYINT) as ORDER_STREAM", \
    	"CAST(LINEHAUL_REVENUE AS DECIMAL(13,4)) as LINEHAUL_REVENUE", \
    	"CAST(ACCESSORIAL_REVENUE AS DECIMAL(13,4)) as ACCESSORIAL_REVENUE", \
    	"CAST(STOP_OFF_REVENUE AS DECIMAL(13,4)) as STOP_OFF_REVENUE", \
    	"CAST(REVENUE_LANE_ID AS DECIMAL(13,4)) as REVENUE_LANE_ID", \
    	"CAST(REVENUE_LANE_DETAIL_ID AS DECIMAL(13,4)) as REVENUE_LANE_DETAIL_ID", \
    	"CAST(MARGIN AS DECIMAL(13,4)) as MARGIN", \
    	"CAST(MARGIN_CURRENCY_CODE AS STRING) as MARGIN_CURRENCY_CODE", \
    	"CAST(CM_DISCOUNT_REVENUE AS DECIMAL(13,3)) as CM_DISCOUNT_REVENUE", \
    	"CAST(C_TMS_PLAN_ID AS INT) as C_TMS_PLAN_ID", \
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_ORDERS_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_ORDERS_PRE is written to the target table - " + target_table_name)
