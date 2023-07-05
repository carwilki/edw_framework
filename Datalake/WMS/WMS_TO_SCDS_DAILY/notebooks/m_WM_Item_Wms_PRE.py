#Code converted on 2023-06-22 10:47:10
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
from logging import getLogger, INFO



def m_WM_Item_Wms_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Item_Wms_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ITEM_WMS_PRE"

    schemaName = raw
    source_schema = "WMSMIS"


    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]


    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now() #start timestamp of the script


    # COMMAND ----------
    # Variable_declaration_comment
    dcnbr = dcnbr.strip()[2:]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)

    # COMMAND ----------
    # Processing node SQ_Shortcut_to_ITEM_WMS, type SOURCE 
    # COLUMN COUNT: 136

    SQ_Shortcut_to_ITEM_WMS = jdbcOracleConnection(  f"""SELECT
    ITEM_WMS.ITEM_ID,
    ITEM_WMS.SIZE_RANGE_CODE,
    ITEM_WMS.SIZE_REL_POSN_IN_TABLE,
    ITEM_WMS.VOLTY_CODE,
    ITEM_WMS.PKG_TYPE,
    ITEM_WMS.PROD_SUB_GRP,
    ITEM_WMS.PROD_TYPE,
    ITEM_WMS.PROD_LINE,
    ITEM_WMS.SALE_GRP,
    ITEM_WMS.COORD_1,
    ITEM_WMS.COORD_2,
    ITEM_WMS.CARTON_TYPE,
    ITEM_WMS.UNIT_PRICE,
    ITEM_WMS.RETAIL_PRICE,
    ITEM_WMS.OPER_CODE,
    ITEM_WMS.MAX_CASE_QTY,
    ITEM_WMS.CUBE_MULT_QTY,
    ITEM_WMS.NEST_VOL,
    ITEM_WMS.NEST_CNT,
    ITEM_WMS.UNITS_PER_PICK_ACTIVE,
    ITEM_WMS.HNDL_ATTR_ACTIVE,
    ITEM_WMS.UNITS_PER_PICK_CASE_PICK,
    ITEM_WMS.HNDL_ATTR_CASE_PICK,
    ITEM_WMS.UNITS_PER_PICK_RESV,
    ITEM_WMS.HNDL_ATTR_RESV,
    ITEM_WMS.PROD_LIFE_IN_DAY,
    ITEM_WMS.MAX_RECV_TO_XPIRE_DAYS,
    ITEM_WMS.AVG_DLY_DMND,
    ITEM_WMS.WT_TOL_PCNT,
    ITEM_WMS.CONS_PRTY_DATE_CODE,
    ITEM_WMS.CONS_PRTY_DATE_WINDOW,
    ITEM_WMS.CONS_PRTY_DATE_WINDOW_INCR,
    ITEM_WMS.ACTVTN_DATE,
    ITEM_WMS.ALLOW_RCPT_OLDER_ITEM,
    ITEM_WMS.CRITCL_DIM_1,
    ITEM_WMS.CRITCL_DIM_2,
    ITEM_WMS.CRITCL_DIM_3,
    ITEM_WMS.MFG_DATE_REQD,
    ITEM_WMS.XPIRE_DATE_REQD,
    ITEM_WMS.SHIP_BY_DATE_REQD,
    ITEM_WMS.ITEM_ATTR_REQD,
    ITEM_WMS.BATCH_REQD,
    ITEM_WMS.PROD_STAT_REQD,
    ITEM_WMS.CNTRY_OF_ORGN_REQD,
    ITEM_WMS.VENDOR_ITEM_NBR,
    ITEM_WMS.PICK_WT_TOL_TYPE,
    ITEM_WMS.PICK_WT_TOL_AMNT,
    ITEM_WMS.MHE_WT_TOL_TYPE,
    ITEM_WMS.MHE_WT_TOL_AMNT,
    ITEM_WMS.LOAD_ATTR,
    ITEM_WMS.TEMP_ZONE,
    ITEM_WMS.TRLR_TEMP_ZONE,
    ITEM_WMS.PKT_CONSOL_ATTR,
    ITEM_WMS.BUYER_DISP_CODE,
    ITEM_WMS.CRUSH_CODE,
    ITEM_WMS.CONVEY_FLAG,
    ITEM_WMS.STORE_DEPT,
    ITEM_WMS.MERCH_TYPE,
    ITEM_WMS.MERCH_GROUP,
    ITEM_WMS.SPL_INSTR_CODE_1,
    ITEM_WMS.SPL_INSTR_CODE_2,
    ITEM_WMS.SPL_INSTR_CODE_3,
    ITEM_WMS.SPL_INSTR_CODE_4,
    ITEM_WMS.SPL_INSTR_CODE_5,
    ITEM_WMS.SPL_INSTR_CODE_6,
    ITEM_WMS.SPL_INSTR_CODE_7,
    ITEM_WMS.SPL_INSTR_CODE_8,
    ITEM_WMS.SPL_INSTR_CODE_9,
    ITEM_WMS.SPL_INSTR_CODE_10,
    ITEM_WMS.SPL_INSTR_1,
    ITEM_WMS.SPL_INSTR_2,
    ITEM_WMS.PROMPT_FOR_VENDOR_ITEM_NBR,
    ITEM_WMS.PROMPT_PACK_QTY,
    ITEM_WMS.ECCN_NBR,
    ITEM_WMS.EXP_LICN_NBR,
    ITEM_WMS.EXP_LICN_XP_DATE,
    ITEM_WMS.EXP_LICN_SYMBOL,
    ITEM_WMS.ORGN_CERT_CODE,
    ITEM_WMS.ITAR_EXEMPT_NBR,
    ITEM_WMS.NMFC_CODE,
    ITEM_WMS.FRT_CLASS,
    ITEM_WMS.DFLT_BATCH_STAT,
    ITEM_WMS.DFLT_INCUB_LOCK,
    ITEM_WMS.BASE_INCUB_FLAG,
    ITEM_WMS.INCUB_DAYS,
    ITEM_WMS.INCUB_HOURS,
    ITEM_WMS.SRL_NBR_BRCD_TYPE,
    ITEM_WMS.MINOR_SRL_NBR_REQ,
    ITEM_WMS.DUP_SRL_NBR_FLAG,
    ITEM_WMS.MAX_RCPT_QTY,
    ITEM_WMS.VOCOLLECT_BASE_WT,
    ITEM_WMS.VOCOLLECT_BASE_QTY,
    ITEM_WMS.VOCOLLECT_BASE_ITEM,
    ITEM_WMS.PICK_WT_TOL_AMNT_ERROR,
    ITEM_WMS.PRICE_TKT_TYPE,
    ITEM_WMS.MONETARY_VALUE,
    ITEM_WMS.MV_CURRENCY_CODE,
    ITEM_WMS.CODE_DATE_PROMPT_METHOD_FLAG,
    ITEM_WMS.MIN_RECV_TO_XPIRE_DAYS,
    ITEM_WMS.MIN_PCNT_FOR_LPN_SPLIT,
    ITEM_WMS.MIN_LPN_QTY_FOR_SPLIT,
    ITEM_WMS.PROD_CATGRY,
    ITEM_WMS.AUDIT_CREATED_SOURCE,
    ITEM_WMS.AUDIT_CREATED_SOURCE_TYPE,
    ITEM_WMS.AUDIT_CREATED_DTTM,
    ITEM_WMS.AUDIT_LAST_UPDATED_SOURCE,
    ITEM_WMS.AUDIT_LAST_UPDATED_SOURCE_TYPE,
    ITEM_WMS.AUDIT_LAST_UPDATED_DTTM,
    ITEM_WMS.MARK_FOR_DELETION,
    ITEM_WMS.TOP_SHELF_ELIGIBLE,
    ITEM_WMS.VOCO_ABS_PICK_TOL_AMT,
    ITEM_WMS.CARTON_CNT_DATE_TIME,
    ITEM_WMS.TRANS_INVN_CNT_DATE_TIME,
    ITEM_WMS.WORK_ORD_CNT_DATE_TIME,
    ITEM_WMS.VENDOR_MASTER_ID,
    ITEM_WMS.NBR_OF_DYN_ACTV_PICK_PER_SKU,
    ITEM_WMS.NBR_OF_DYN_CASE_PICK_PER_SKU,
    ITEM_WMS.LET_UP_PRTY,
    ITEM_WMS.PREF_CRITERIA_FLAG,
    ITEM_WMS.PRODUCER_FLAG,
    ITEM_WMS.NET_COST_FLAG,
    ITEM_WMS.MARKS_NBRS,
    ITEM_WMS.SLOTTING_OPT_STAT_CODE,
    ITEM_WMS.PRICE_TIX_AVAIL,
    ITEM_WMS.MV_SIZE_UOM,
    ITEM_WMS.UNITS_PER_GRAB_PLT,
    ITEM_WMS.HNDL_ATTR_PLT,
    ITEM_WMS.HNDL_ATTR_ACT_UOM_ID,
    ITEM_WMS.HNDL_ATTR_CASE_PICK_UOM_ID,
    ITEM_WMS.SRL_NBR_REQD,
    ITEM_WMS.CC_UNIT_TOLER_VALUE,
    ITEM_WMS.CC_WGT_TOLER_VALUE,
    ITEM_WMS.CC_DLR_TOLER_VALUE,
    ITEM_WMS.CC_PCNT_TOLER_VALUE,
    ITEM_WMS.DISPOSITION_TYPE,
    ITEM_WMS.EXP_LICN_DESCRIPTION
    FROM {source_schema}.ITEM_WMS
    WHERE (trunc(ITEM_WMS.AUDIT_CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (trunc(ITEM_WMS.AUDIT_LAST_UPDATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) AND 
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 138

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ITEM_WMS_temp = SQ_Shortcut_to_ITEM_WMS.toDF(*["SQ_Shortcut_to_ITEM_WMS___" + col for col in SQ_Shortcut_to_ITEM_WMS.columns])

    EXPTRANS = SQ_Shortcut_to_ITEM_WMS_temp.selectExpr( \
        "SQ_Shortcut_to_ITEM_WMS___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_ITEM_WMS___ITEM_ID as ITEM_ID", \
        "SQ_Shortcut_to_ITEM_WMS___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
        "SQ_Shortcut_to_ITEM_WMS___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
        "SQ_Shortcut_to_ITEM_WMS___VOLTY_CODE as VOLTY_CODE", \
        "SQ_Shortcut_to_ITEM_WMS___PKG_TYPE as PKG_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___PROD_SUB_GRP as PROD_SUB_GRP", \
        "SQ_Shortcut_to_ITEM_WMS___PROD_TYPE as PROD_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___PROD_LINE as PROD_LINE", \
        "SQ_Shortcut_to_ITEM_WMS___SALE_GRP as SALE_GRP", \
        "SQ_Shortcut_to_ITEM_WMS___COORD_1 as COORD_1", \
        "SQ_Shortcut_to_ITEM_WMS___COORD_2 as COORD_2", \
        "SQ_Shortcut_to_ITEM_WMS___CARTON_TYPE as CARTON_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___UNIT_PRICE as UNIT_PRICE", \
        "SQ_Shortcut_to_ITEM_WMS___RETAIL_PRICE as RETAIL_PRICE", \
        "SQ_Shortcut_to_ITEM_WMS___OPER_CODE as OPER_CODE", \
        "SQ_Shortcut_to_ITEM_WMS___MAX_CASE_QTY as MAX_CASE_QTY", \
        "SQ_Shortcut_to_ITEM_WMS___CUBE_MULT_QTY as CUBE_MULT_QTY", \
        "SQ_Shortcut_to_ITEM_WMS___NEST_VOL as NEST_VOL", \
        "SQ_Shortcut_to_ITEM_WMS___NEST_CNT as NEST_CNT", \
        "SQ_Shortcut_to_ITEM_WMS___UNITS_PER_PICK_ACTIVE as UNITS_PER_PICK_ACTIVE", \
        "SQ_Shortcut_to_ITEM_WMS___HNDL_ATTR_ACTIVE as HNDL_ATTR_ACTIVE", \
        "SQ_Shortcut_to_ITEM_WMS___UNITS_PER_PICK_CASE_PICK as UNITS_PER_PICK_CASE_PICK", \
        "SQ_Shortcut_to_ITEM_WMS___HNDL_ATTR_CASE_PICK as HNDL_ATTR_CASE_PICK", \
        "SQ_Shortcut_to_ITEM_WMS___UNITS_PER_PICK_RESV as UNITS_PER_PICK_RESV", \
        "SQ_Shortcut_to_ITEM_WMS___HNDL_ATTR_RESV as HNDL_ATTR_RESV", \
        "SQ_Shortcut_to_ITEM_WMS___PROD_LIFE_IN_DAY as PROD_LIFE_IN_DAY", \
        "SQ_Shortcut_to_ITEM_WMS___MAX_RECV_TO_XPIRE_DAYS as MAX_RECV_TO_XPIRE_DAYS", \
        "SQ_Shortcut_to_ITEM_WMS___AVG_DLY_DMND as AVG_DLY_DMND", \
        "SQ_Shortcut_to_ITEM_WMS___WT_TOL_PCNT as WT_TOL_PCNT", \
        "SQ_Shortcut_to_ITEM_WMS___CONS_PRTY_DATE_CODE as CONS_PRTY_DATE_CODE", \
        "SQ_Shortcut_to_ITEM_WMS___CONS_PRTY_DATE_WINDOW as CONS_PRTY_DATE_WINDOW", \
        "SQ_Shortcut_to_ITEM_WMS___CONS_PRTY_DATE_WINDOW_INCR as CONS_PRTY_DATE_WINDOW_INCR", \
        "SQ_Shortcut_to_ITEM_WMS___ACTVTN_DATE as ACTVTN_DATE", \
        "SQ_Shortcut_to_ITEM_WMS___ALLOW_RCPT_OLDER_ITEM as ALLOW_RCPT_OLDER_ITEM", \
        "SQ_Shortcut_to_ITEM_WMS___CRITCL_DIM_1 as CRITCL_DIM_1", \
        "SQ_Shortcut_to_ITEM_WMS___CRITCL_DIM_2 as CRITCL_DIM_2", \
        "SQ_Shortcut_to_ITEM_WMS___CRITCL_DIM_3 as CRITCL_DIM_3", \
        "SQ_Shortcut_to_ITEM_WMS___MFG_DATE_REQD as MFG_DATE_REQD", \
        "SQ_Shortcut_to_ITEM_WMS___XPIRE_DATE_REQD as XPIRE_DATE_REQD", \
        "SQ_Shortcut_to_ITEM_WMS___SHIP_BY_DATE_REQD as SHIP_BY_DATE_REQD", \
        "SQ_Shortcut_to_ITEM_WMS___ITEM_ATTR_REQD as ITEM_ATTR_REQD", \
        "SQ_Shortcut_to_ITEM_WMS___BATCH_REQD as BATCH_REQD", \
        "SQ_Shortcut_to_ITEM_WMS___PROD_STAT_REQD as PROD_STAT_REQD", \
        "SQ_Shortcut_to_ITEM_WMS___CNTRY_OF_ORGN_REQD as CNTRY_OF_ORGN_REQD", \
        "SQ_Shortcut_to_ITEM_WMS___VENDOR_ITEM_NBR as VENDOR_ITEM_NBR", \
        "SQ_Shortcut_to_ITEM_WMS___PICK_WT_TOL_TYPE as PICK_WT_TOL_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___PICK_WT_TOL_AMNT as PICK_WT_TOL_AMNT", \
        "SQ_Shortcut_to_ITEM_WMS___MHE_WT_TOL_TYPE as MHE_WT_TOL_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___MHE_WT_TOL_AMNT as MHE_WT_TOL_AMNT", \
        "SQ_Shortcut_to_ITEM_WMS___LOAD_ATTR as LOAD_ATTR", \
        "SQ_Shortcut_to_ITEM_WMS___TEMP_ZONE as TEMP_ZONE", \
        "SQ_Shortcut_to_ITEM_WMS___TRLR_TEMP_ZONE as TRLR_TEMP_ZONE", \
        "SQ_Shortcut_to_ITEM_WMS___PKT_CONSOL_ATTR as PKT_CONSOL_ATTR", \
        "SQ_Shortcut_to_ITEM_WMS___BUYER_DISP_CODE as BUYER_DISP_CODE", \
        "SQ_Shortcut_to_ITEM_WMS___CRUSH_CODE as CRUSH_CODE", \
        "SQ_Shortcut_to_ITEM_WMS___CONVEY_FLAG as CONVEY_FLAG", \
        "SQ_Shortcut_to_ITEM_WMS___STORE_DEPT as STORE_DEPT", \
        "SQ_Shortcut_to_ITEM_WMS___MERCH_TYPE as MERCH_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___MERCH_GROUP as MERCH_GROUP", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_CODE_1 as SPL_INSTR_CODE_1", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_CODE_2 as SPL_INSTR_CODE_2", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_CODE_3 as SPL_INSTR_CODE_3", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_CODE_4 as SPL_INSTR_CODE_4", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_CODE_5 as SPL_INSTR_CODE_5", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_CODE_6 as SPL_INSTR_CODE_6", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_CODE_7 as SPL_INSTR_CODE_7", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_CODE_8 as SPL_INSTR_CODE_8", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_CODE_9 as SPL_INSTR_CODE_9", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_CODE_10 as SPL_INSTR_CODE_10", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_1 as SPL_INSTR_1", \
        "SQ_Shortcut_to_ITEM_WMS___SPL_INSTR_2 as SPL_INSTR_2", \
        "SQ_Shortcut_to_ITEM_WMS___PROMPT_FOR_VENDOR_ITEM_NBR as PROMPT_FOR_VENDOR_ITEM_NBR", \
        "SQ_Shortcut_to_ITEM_WMS___PROMPT_PACK_QTY as PROMPT_PACK_QTY", \
        "SQ_Shortcut_to_ITEM_WMS___ECCN_NBR as ECCN_NBR", \
        "SQ_Shortcut_to_ITEM_WMS___EXP_LICN_NBR as EXP_LICN_NBR", \
        "SQ_Shortcut_to_ITEM_WMS___EXP_LICN_XP_DATE as EXP_LICN_XP_DATE", \
        "SQ_Shortcut_to_ITEM_WMS___EXP_LICN_SYMBOL as EXP_LICN_SYMBOL", \
        "SQ_Shortcut_to_ITEM_WMS___ORGN_CERT_CODE as ORGN_CERT_CODE", \
        "SQ_Shortcut_to_ITEM_WMS___ITAR_EXEMPT_NBR as ITAR_EXEMPT_NBR", \
        "SQ_Shortcut_to_ITEM_WMS___NMFC_CODE as NMFC_CODE", \
        "SQ_Shortcut_to_ITEM_WMS___FRT_CLASS as FRT_CLASS", \
        "SQ_Shortcut_to_ITEM_WMS___DFLT_BATCH_STAT as DFLT_BATCH_STAT", \
        "SQ_Shortcut_to_ITEM_WMS___DFLT_INCUB_LOCK as DFLT_INCUB_LOCK", \
        "SQ_Shortcut_to_ITEM_WMS___BASE_INCUB_FLAG as BASE_INCUB_FLAG", \
        "SQ_Shortcut_to_ITEM_WMS___INCUB_DAYS as INCUB_DAYS", \
        "SQ_Shortcut_to_ITEM_WMS___INCUB_HOURS as INCUB_HOURS", \
        "SQ_Shortcut_to_ITEM_WMS___SRL_NBR_BRCD_TYPE as SRL_NBR_BRCD_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___MINOR_SRL_NBR_REQ as MINOR_SRL_NBR_REQ", \
        "SQ_Shortcut_to_ITEM_WMS___DUP_SRL_NBR_FLAG as DUP_SRL_NBR_FLAG", \
        "SQ_Shortcut_to_ITEM_WMS___MAX_RCPT_QTY as MAX_RCPT_QTY", \
        "SQ_Shortcut_to_ITEM_WMS___VOCOLLECT_BASE_WT as VOCOLLECT_BASE_WT", \
        "SQ_Shortcut_to_ITEM_WMS___VOCOLLECT_BASE_QTY as VOCOLLECT_BASE_QTY", \
        "SQ_Shortcut_to_ITEM_WMS___VOCOLLECT_BASE_ITEM as VOCOLLECT_BASE_ITEM", \
        "SQ_Shortcut_to_ITEM_WMS___PICK_WT_TOL_AMNT_ERROR as PICK_WT_TOL_AMNT_ERROR", \
        "SQ_Shortcut_to_ITEM_WMS___PRICE_TKT_TYPE as PRICE_TKT_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___MONETARY_VALUE as MONETARY_VALUE", \
        "SQ_Shortcut_to_ITEM_WMS___MV_CURRENCY_CODE as MV_CURRENCY_CODE", \
        "SQ_Shortcut_to_ITEM_WMS___CODE_DATE_PROMPT_METHOD_FLAG as CODE_DATE_PROMPT_METHOD_FLAG", \
        "SQ_Shortcut_to_ITEM_WMS___MIN_RECV_TO_XPIRE_DAYS as MIN_RECV_TO_XPIRE_DAYS", \
        "SQ_Shortcut_to_ITEM_WMS___MIN_PCNT_FOR_LPN_SPLIT as MIN_PCNT_FOR_LPN_SPLIT", \
        "SQ_Shortcut_to_ITEM_WMS___MIN_LPN_QTY_FOR_SPLIT as MIN_LPN_QTY_FOR_SPLIT", \
        "SQ_Shortcut_to_ITEM_WMS___PROD_CATGRY as PROD_CATGRY", \
        "SQ_Shortcut_to_ITEM_WMS___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
        "SQ_Shortcut_to_ITEM_WMS___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
        "SQ_Shortcut_to_ITEM_WMS___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_ITEM_WMS___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_ITEM_WMS___MARK_FOR_DELETION as MARK_FOR_DELETION", \
        "SQ_Shortcut_to_ITEM_WMS___TOP_SHELF_ELIGIBLE as TOP_SHELF_ELIGIBLE", \
        "SQ_Shortcut_to_ITEM_WMS___VOCO_ABS_PICK_TOL_AMT as VOCO_ABS_PICK_TOL_AMT", \
        "SQ_Shortcut_to_ITEM_WMS___CARTON_CNT_DATE_TIME as CARTON_CNT_DATE_TIME", \
        "SQ_Shortcut_to_ITEM_WMS___TRANS_INVN_CNT_DATE_TIME as TRANS_INVN_CNT_DATE_TIME", \
        "SQ_Shortcut_to_ITEM_WMS___WORK_ORD_CNT_DATE_TIME as WORK_ORD_CNT_DATE_TIME", \
        "SQ_Shortcut_to_ITEM_WMS___VENDOR_MASTER_ID as VENDOR_MASTER_ID", \
        "SQ_Shortcut_to_ITEM_WMS___NBR_OF_DYN_ACTV_PICK_PER_SKU as NBR_OF_DYN_ACTV_PICK_PER_SKU", \
        "SQ_Shortcut_to_ITEM_WMS___NBR_OF_DYN_CASE_PICK_PER_SKU as NBR_OF_DYN_CASE_PICK_PER_SKU", \
        "SQ_Shortcut_to_ITEM_WMS___LET_UP_PRTY as LET_UP_PRTY", \
        "SQ_Shortcut_to_ITEM_WMS___PREF_CRITERIA_FLAG as PREF_CRITERIA_FLAG", \
        "SQ_Shortcut_to_ITEM_WMS___PRODUCER_FLAG as PRODUCER_FLAG", \
        "SQ_Shortcut_to_ITEM_WMS___NET_COST_FLAG as NET_COST_FLAG", \
        "SQ_Shortcut_to_ITEM_WMS___MARKS_NBRS as MARKS_NBRS", \
        "SQ_Shortcut_to_ITEM_WMS___SLOTTING_OPT_STAT_CODE as SLOTTING_OPT_STAT_CODE", \
        "SQ_Shortcut_to_ITEM_WMS___PRICE_TIX_AVAIL as PRICE_TIX_AVAIL", \
        "SQ_Shortcut_to_ITEM_WMS___MV_SIZE_UOM as MV_SIZE_UOM", \
        "SQ_Shortcut_to_ITEM_WMS___UNITS_PER_GRAB_PLT as UNITS_PER_GRAB_PLT", \
        "SQ_Shortcut_to_ITEM_WMS___HNDL_ATTR_PLT as HNDL_ATTR_PLT", \
        "SQ_Shortcut_to_ITEM_WMS___HNDL_ATTR_ACT_UOM_ID as HNDL_ATTR_ACT_UOM_ID", \
        "SQ_Shortcut_to_ITEM_WMS___HNDL_ATTR_CASE_PICK_UOM_ID as HNDL_ATTR_CASE_PICK_UOM_ID", \
        "SQ_Shortcut_to_ITEM_WMS___SRL_NBR_REQD as SRL_NBR_REQD", \
        "SQ_Shortcut_to_ITEM_WMS___CC_UNIT_TOLER_VALUE as CC_UNIT_TOLER_VALUE", \
        "SQ_Shortcut_to_ITEM_WMS___CC_WGT_TOLER_VALUE as CC_WGT_TOLER_VALUE", \
        "SQ_Shortcut_to_ITEM_WMS___CC_DLR_TOLER_VALUE as CC_DLR_TOLER_VALUE", \
        "SQ_Shortcut_to_ITEM_WMS___CC_PCNT_TOLER_VALUE as CC_PCNT_TOLER_VALUE", \
        "SQ_Shortcut_to_ITEM_WMS___DISPOSITION_TYPE as DISPOSITION_TYPE", \
        "SQ_Shortcut_to_ITEM_WMS___EXP_LICN_DESCRIPTION as EXP_LICN_DESCRIPTION", \
        "CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ITEM_WMS_PRE, type TARGET 
    # COLUMN COUNT: 139


    Shortcut_to_WM_ITEM_WMS_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
        "CAST(ITEM_ID AS BIGINT) as ITEM_ID", \
        "CAST(SIZE_RANGE_CODE AS STRING) as SIZE_RANGE_CODE", \
        "CAST(SIZE_REL_POSN_IN_TABLE AS STRING) as SIZE_REL_POSN_IN_TABLE", \
        "CAST(VOLTY_CODE AS STRING) as VOLTY_CODE", \
        "CAST(PKG_TYPE AS STRING) as PKG_TYPE", \
        "CAST(PROD_SUB_GRP AS STRING) as PROD_SUB_GRP", \
        "CAST(PROD_TYPE AS STRING) as PROD_TYPE", \
        "CAST(PROD_LINE AS STRING) as PROD_LINE", \
        "CAST(SALE_GRP AS STRING) as SALE_GRP", \
        "CAST(COORD_1 AS BIGINT) as COORD_1", \
        "CAST(COORD_2 AS BIGINT) as COORD_2", \
        "CAST(CARTON_TYPE AS STRING) as CARTON_TYPE", \
        "CAST(UNIT_PRICE AS BIGINT) as UNIT_PRICE", \
        "CAST(RETAIL_PRICE AS BIGINT) as RETAIL_PRICE", \
        "CAST(OPER_CODE AS STRING) as OPER_CODE", \
        "CAST(MAX_CASE_QTY AS BIGINT) as MAX_CASE_QTY", \
        "CAST(CUBE_MULT_QTY AS BIGINT) as CUBE_MULT_QTY", \
        "CAST(NEST_VOL AS BIGINT) as NEST_VOL", \
        "CAST(NEST_CNT AS BIGINT) as NEST_CNT", \
        "CAST(UNITS_PER_PICK_ACTIVE AS BIGINT) as UNITS_PER_PICK_ACTIVE", \
        "CAST(HNDL_ATTR_ACTIVE AS STRING) as HNDL_ATTR_ACTIVE", \
        "CAST(UNITS_PER_PICK_CASE_PICK AS BIGINT) as UNITS_PER_PICK_CASE_PICK", \
        "CAST(HNDL_ATTR_CASE_PICK AS STRING) as HNDL_ATTR_CASE_PICK", \
        "CAST(UNITS_PER_PICK_RESV AS BIGINT) as UNITS_PER_PICK_RESV", \
        "CAST(HNDL_ATTR_RESV AS STRING) as HNDL_ATTR_RESV", \
        "CAST(PROD_LIFE_IN_DAY AS BIGINT) as PROD_LIFE_IN_DAY", \
        "CAST(MAX_RECV_TO_XPIRE_DAYS AS BIGINT) as MAX_RECV_TO_XPIRE_DAYS", \
        "CAST(AVG_DLY_DMND AS BIGINT) as AVG_DLY_DMND", \
        "CAST(WT_TOL_PCNT AS BIGINT) as WT_TOL_PCNT", \
        "CAST(CONS_PRTY_DATE_CODE AS STRING) as CONS_PRTY_DATE_CODE", \
        "CAST(CONS_PRTY_DATE_WINDOW AS STRING) as CONS_PRTY_DATE_WINDOW", \
        "CAST(CONS_PRTY_DATE_WINDOW_INCR AS BIGINT) as CONS_PRTY_DATE_WINDOW_INCR", \
        "CAST(ACTVTN_DATE AS TIMESTAMP) as ACTVTN_DATE", \
        "CAST(ALLOW_RCPT_OLDER_ITEM AS STRING) as ALLOW_RCPT_OLDER_ITEM", \
        "CAST(CRITCL_DIM_1 AS BIGINT) as CRITCL_DIM_1", \
        "CAST(CRITCL_DIM_2 AS BIGINT) as CRITCL_DIM_2", \
        "CAST(CRITCL_DIM_3 AS BIGINT) as CRITCL_DIM_3", \
        "CAST(MFG_DATE_REQD AS STRING) as MFG_DATE_REQD", \
        "CAST(XPIRE_DATE_REQD AS STRING) as XPIRE_DATE_REQD", \
        "CAST(SHIP_BY_DATE_REQD AS STRING) as SHIP_BY_DATE_REQD", \
        "CAST(ITEM_ATTR_REQD AS STRING) as ITEM_ATTR_REQD", \
        "CAST(BATCH_REQD AS STRING) as BATCH_REQD", \
        "CAST(PROD_STAT_REQD AS STRING) as PROD_STAT_REQD", \
        "CAST(CNTRY_OF_ORGN_REQD AS STRING) as CNTRY_OF_ORGN_REQD", \
        "CAST(VENDOR_ITEM_NBR AS STRING) as VENDOR_ITEM_NBR", \
        "CAST(PICK_WT_TOL_TYPE AS STRING) as PICK_WT_TOL_TYPE", \
        "CAST(PICK_WT_TOL_AMNT AS BIGINT) as PICK_WT_TOL_AMNT", \
        "CAST(MHE_WT_TOL_TYPE AS STRING) as MHE_WT_TOL_TYPE", \
        "CAST(MHE_WT_TOL_AMNT AS BIGINT) as MHE_WT_TOL_AMNT", \
        "CAST(LOAD_ATTR AS STRING) as LOAD_ATTR", \
        "CAST(TEMP_ZONE AS STRING) as TEMP_ZONE", \
        "CAST(TRLR_TEMP_ZONE AS STRING) as TRLR_TEMP_ZONE", \
        "CAST(PKT_CONSOL_ATTR AS STRING) as PKT_CONSOL_ATTR", \
        "CAST(BUYER_DISP_CODE AS STRING) as BUYER_DISP_CODE", \
        "CAST(CRUSH_CODE AS STRING) as CRUSH_CODE", \
        "CAST(CONVEY_FLAG AS STRING) as CONVEY_FLAG", \
        "CAST(STORE_DEPT AS STRING) as STORE_DEPT", \
        "CAST(MERCH_TYPE AS STRING) as MERCH_TYPE", \
        "CAST(MERCH_GROUP AS STRING) as MERCH_GROUP", \
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
        "CAST(SPL_INSTR_1 AS STRING) as SPL_INSTR_1", \
        "CAST(SPL_INSTR_2 AS STRING) as SPL_INSTR_2", \
        "CAST(PROMPT_FOR_VENDOR_ITEM_NBR AS STRING) as PROMPT_FOR_VENDOR_ITEM_NBR", \
        "CAST(PROMPT_PACK_QTY AS STRING) as PROMPT_PACK_QTY", \
        "CAST(ECCN_NBR AS STRING) as ECCN_NBR", \
        "CAST(EXP_LICN_NBR AS STRING) as EXP_LICN_NBR", \
        "CAST(EXP_LICN_XP_DATE AS TIMESTAMP) as EXP_LICN_XP_DATE", \
        "CAST(EXP_LICN_SYMBOL AS STRING) as EXP_LICN_SYMBOL", \
        "CAST(ORGN_CERT_CODE AS STRING) as ORGN_CERT_CODE", \
        "CAST(ITAR_EXEMPT_NBR AS STRING) as ITAR_EXEMPT_NBR", \
        "CAST(NMFC_CODE AS STRING) as NMFC_CODE", \
        "CAST(FRT_CLASS AS STRING) as FRT_CLASS", \
        "CAST(DFLT_BATCH_STAT AS STRING) as DFLT_BATCH_STAT", \
        "CAST(DFLT_INCUB_LOCK AS STRING) as DFLT_INCUB_LOCK", \
        "CAST(BASE_INCUB_FLAG AS STRING) as BASE_INCUB_FLAG", \
        "CAST(INCUB_DAYS AS BIGINT) as INCUB_DAYS", \
        "CAST(INCUB_HOURS AS BIGINT) as INCUB_HOURS", \
        "CAST(SRL_NBR_BRCD_TYPE AS STRING) as SRL_NBR_BRCD_TYPE", \
        "CAST(MINOR_SRL_NBR_REQ AS BIGINT) as MINOR_SRL_NBR_REQ", \
        "CAST(DUP_SRL_NBR_FLAG AS BIGINT) as DUP_SRL_NBR_FLAG", \
        "CAST(MAX_RCPT_QTY AS BIGINT) as MAX_RCPT_QTY", \
        "CAST(VOCOLLECT_BASE_WT AS BIGINT) as VOCOLLECT_BASE_WT", \
        "CAST(VOCOLLECT_BASE_QTY AS BIGINT) as VOCOLLECT_BASE_QTY", \
        "CAST(VOCOLLECT_BASE_ITEM AS STRING) as VOCOLLECT_BASE_ITEM", \
        "CAST(PICK_WT_TOL_AMNT_ERROR AS BIGINT) as PICK_WT_TOL_AMNT_ERROR", \
        "CAST(PRICE_TKT_TYPE AS STRING) as PRICE_TKT_TYPE", \
        "CAST(MONETARY_VALUE AS BIGINT) as MONETARY_VALUE", \
        "CAST(MV_CURRENCY_CODE AS STRING) as MV_CURRENCY_CODE", \
        "CAST(CODE_DATE_PROMPT_METHOD_FLAG AS STRING) as CODE_DATE_PROMPT_METHOD_FLAG", \
        "CAST(MIN_RECV_TO_XPIRE_DAYS AS BIGINT) as MIN_RECV_TO_XPIRE_DAYS", \
        "CAST(MIN_PCNT_FOR_LPN_SPLIT AS BIGINT) as MIN_PCNT_FOR_LPN_SPLIT", \
        "CAST(MIN_LPN_QTY_FOR_SPLIT AS BIGINT) as MIN_LPN_QTY_FOR_SPLIT", \
        "CAST(PROD_CATGRY AS STRING) as PROD_CATGRY", \
        "CAST(AUDIT_CREATED_SOURCE AS STRING) as AUDIT_CREATED_SOURCE", \
        "CAST(AUDIT_CREATED_SOURCE_TYPE AS BIGINT) as AUDIT_CREATED_SOURCE_TYPE", \
        "CAST(AUDIT_CREATED_DTTM AS TIMESTAMP) as AUDIT_CREATED_DTTM", \
        "CAST(AUDIT_LAST_UPDATED_SOURCE AS STRING) as AUDIT_LAST_UPDATED_SOURCE", \
        "CAST(AUDIT_LAST_UPDATED_SOURCE_TYPE AS BIGINT) as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
        "CAST(AUDIT_LAST_UPDATED_DTTM AS TIMESTAMP) as AUDIT_LAST_UPDATED_DTTM", \
        "CAST(MARK_FOR_DELETION AS BIGINT) as MARK_FOR_DELETION", \
        "CAST(TOP_SHELF_ELIGIBLE AS BIGINT) as TOP_SHELF_ELIGIBLE", \
        "CAST(VOCO_ABS_PICK_TOL_AMT AS BIGINT) as VOCO_ABS_PICK_TOL_AMT", \
        "CAST(CARTON_CNT_DATE_TIME AS TIMESTAMP) as CARTON_CNT_DATE_TIME", \
        "CAST(TRANS_INVN_CNT_DATE_TIME AS TIMESTAMP) as TRANS_INVN_CNT_DATE_TIME", \
        "CAST(WORK_ORD_CNT_DATE_TIME AS TIMESTAMP) as WORK_ORD_CNT_DATE_TIME", \
        "CAST(lit(None) AS BIGINT) as SHELF_DAYS", \
        "CAST(VENDOR_MASTER_ID AS BIGINT) as VENDOR_MASTER_ID", \
        "CAST(NBR_OF_DYN_ACTV_PICK_PER_SKU AS BIGINT) as NBR_OF_DYN_ACTV_PICK_PER_SKU", \
        "CAST(NBR_OF_DYN_CASE_PICK_PER_SKU AS BIGINT) as NBR_OF_DYN_CASE_PICK_PER_SKU", \
        "CAST(LET_UP_PRTY AS BIGINT) as LET_UP_PRTY", \
        "CAST(PREF_CRITERIA_FLAG AS STRING) as PREF_CRITERIA_FLAG", \
        "CAST(PRODUCER_FLAG AS STRING) as PRODUCER_FLAG", \
        "CAST(NET_COST_FLAG AS STRING) as NET_COST_FLAG", \
        "CAST(MARKS_NBRS AS STRING) as MARKS_NBRS", \
        "CAST(SLOTTING_OPT_STAT_CODE AS BIGINT) as SLOTTING_OPT_STAT_CODE", \
        "CAST(PRICE_TIX_AVAIL AS STRING) as PRICE_TIX_AVAIL", \
        "CAST(MV_SIZE_UOM AS BIGINT) as MV_SIZE_UOM", \
        "CAST(UNITS_PER_GRAB_PLT AS BIGINT) as UNITS_PER_GRAB_PLT", \
        "CAST(HNDL_ATTR_PLT AS STRING) as HNDL_ATTR_PLT", \
        "CAST(HNDL_ATTR_ACT_UOM_ID AS BIGINT) as HNDL_ATTR_ACT_UOM_ID", \
        "CAST(HNDL_ATTR_CASE_PICK_UOM_ID AS BIGINT) as HNDL_ATTR_CASE_PICK_UOM_ID", \
        "CAST(SRL_NBR_REQD AS BIGINT) as SRL_NBR_REQD", \
        "CAST(CC_UNIT_TOLER_VALUE AS BIGINT) as CC_UNIT_TOLER_VALUE", \
        "CAST(CC_WGT_TOLER_VALUE AS BIGINT) as CC_WGT_TOLER_VALUE", \
        "CAST(CC_DLR_TOLER_VALUE AS BIGINT) as CC_DLR_TOLER_VALUE", \
        "CAST(CC_PCNT_TOLER_VALUE AS BIGINT) as CC_PCNT_TOLER_VALUE", \
        "CAST(DISPOSITION_TYPE AS STRING) as DISPOSITION_TYPE", \
        "CAST(EXP_LICN_DESCRIPTION AS STRING) as EXP_LICN_DESCRIPTION", \
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )

    overwriteDeltaPartition(Shortcut_to_WM_ITEM_WMS_PRE,"DC_NBR",dcnbr,target_table_name)
    
    logger.info(
        "Shortcut_to_WM_ITEM_WMS_PRE is written to the target table - "
        + target_table_name
    )