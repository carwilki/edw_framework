#Code converted on 2023-06-21 18:43:25
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



def m_WM_Item_Facility_Mapping_Wms_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Item_Facility_Mapping_Wms_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ITEM_FACILITY_MAPPING_WMS_PRE"

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
    
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)
    dcnbr = dcnbr.strip()[2:]
    # COMMAND ----------
    # Processing node SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS, type SOURCE 
    # COLUMN COUNT: 81

    SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS = jdbcOracleConnection(  f"""SELECT
    ITEM_FACILITY_MAPPING_WMS.ITEM_FACILITY_MAPPING_ID,
    ITEM_FACILITY_MAPPING_WMS.LPN_PER_TIER,
    ITEM_FACILITY_MAPPING_WMS.TIER_PER_PLT,
    ITEM_FACILITY_MAPPING_WMS.CASE_SIZE_TYPE,
    ITEM_FACILITY_MAPPING_WMS.PICK_RATE,
    ITEM_FACILITY_MAPPING_WMS.WAGE_VALUE,
    ITEM_FACILITY_MAPPING_WMS.PACK_RATE,
    ITEM_FACILITY_MAPPING_WMS.SPL_PROC_RATE,
    ITEM_FACILITY_MAPPING_WMS.AUTO_SUB_CASE,
    ITEM_FACILITY_MAPPING_WMS.ASSIGN_DYNAMIC_ACTV_PICK_SITE,
    ITEM_FACILITY_MAPPING_WMS.ASSIGN_DYNAMIC_CASE_PICK_SITE,
    ITEM_FACILITY_MAPPING_WMS.PICK_LOCN_ASSIGN_TYPE,
    ITEM_FACILITY_MAPPING_WMS.PUTWY_TYPE,
    ITEM_FACILITY_MAPPING_WMS.DFLT_WAVE_PROC_TYPE,
    ITEM_FACILITY_MAPPING_WMS.XCESS_WAVE_NEED_PROC_TYPE,
    ITEM_FACILITY_MAPPING_WMS.ALLOC_TYPE,
    ITEM_FACILITY_MAPPING_WMS.VIOLATE_FIFO_ALLOC_QTY_MATCH,
    ITEM_FACILITY_MAPPING_WMS.QV_ITEM_GRP,
    ITEM_FACILITY_MAPPING_WMS.QUAL_INSPCT_ITEM_GRP,
    ITEM_FACILITY_MAPPING_WMS.MAX_UNITS_IN_DYNAMIC_ACTV,
    ITEM_FACILITY_MAPPING_WMS.MAX_CASES_IN_DYNAMIC_ACTV,
    ITEM_FACILITY_MAPPING_WMS.MAX_UNITS_IN_DYNAMIC_CASE_PICK,
    ITEM_FACILITY_MAPPING_WMS.MAX_CASES_IN_DYNAMIC_CASE_PICK,
    ITEM_FACILITY_MAPPING_WMS.CASE_CNT_DATE_TIME,
    ITEM_FACILITY_MAPPING_WMS.ACTV_CNT_DATE_TIME,
    ITEM_FACILITY_MAPPING_WMS.CASE_PICK_CNT_DATE_TIME,
    ITEM_FACILITY_MAPPING_WMS.VENDOR_CARTON_PER_TIER,
    ITEM_FACILITY_MAPPING_WMS.VENDOR_TIER_PER_PLT,
    ITEM_FACILITY_MAPPING_WMS.ORD_CARTON_PER_TIER,
    ITEM_FACILITY_MAPPING_WMS.ORD_TIER_PER_PLT,
    ITEM_FACILITY_MAPPING_WMS.DFLT_CATCH_WT_METHOD,
    ITEM_FACILITY_MAPPING_WMS.DFLT_DATE_MASK,
    ITEM_FACILITY_MAPPING_WMS.CARTON_BREAK_ATTR,
    ITEM_FACILITY_MAPPING_WMS.MISC_SHORT_ALPHA_1,
    ITEM_FACILITY_MAPPING_WMS.MISC_SHORT_ALPHA_2,
    ITEM_FACILITY_MAPPING_WMS.MISC_ALPHA_1,
    ITEM_FACILITY_MAPPING_WMS.MISC_ALPHA_2,
    ITEM_FACILITY_MAPPING_WMS.MISC_ALPHA_3,
    ITEM_FACILITY_MAPPING_WMS.MISC_NUMERIC_1,
    ITEM_FACILITY_MAPPING_WMS.MISC_NUMERIC_2,
    ITEM_FACILITY_MAPPING_WMS.MISC_NUMERIC_3,
    ITEM_FACILITY_MAPPING_WMS.MISC_DATE_1,
    ITEM_FACILITY_MAPPING_WMS.MISC_DATE_2,
    ITEM_FACILITY_MAPPING_WMS.CHUTE_ASSIGN_TYPE,
    ITEM_FACILITY_MAPPING_WMS.ACTV_REPL_ORGN,
    ITEM_FACILITY_MAPPING_WMS.UIN_NBR,
    ITEM_FACILITY_MAPPING_WMS.FIFO_RANGE,
    ITEM_FACILITY_MAPPING_WMS.PRTL_CASE_ALLOC_THRESH_UNITS,
    ITEM_FACILITY_MAPPING_WMS.PRTL_CASE_PUTWY_THRESH_UNITS,
    ITEM_FACILITY_MAPPING_WMS.VENDOR_TAGGED_EPC_FLAG,
    ITEM_FACILITY_MAPPING_WMS.ITEM_AVG_WT,
    ITEM_FACILITY_MAPPING_WMS.DFLT_MIN_FROM_PREV_LOCN_FLAG,
    ITEM_FACILITY_MAPPING_WMS.SLOT_MISC_1,
    ITEM_FACILITY_MAPPING_WMS.SLOT_MISC_2,
    ITEM_FACILITY_MAPPING_WMS.SLOT_MISC_3,
    ITEM_FACILITY_MAPPING_WMS.SLOT_MISC_4,
    ITEM_FACILITY_MAPPING_WMS.SLOT_MISC_5,
    ITEM_FACILITY_MAPPING_WMS.SLOT_MISC_6,
    ITEM_FACILITY_MAPPING_WMS.SLOT_ROTATE_EACHES_FLAG,
    ITEM_FACILITY_MAPPING_WMS.SLOT_ROTATE_INNERS_FLAG,
    ITEM_FACILITY_MAPPING_WMS.SLOT_ROTATE_BINS_FLAG,
    ITEM_FACILITY_MAPPING_WMS.SLOT_ROTATE_CASES_FLAG,
    ITEM_FACILITY_MAPPING_WMS.SLOT_3D_SLOTTING_FLAG,
    ITEM_FACILITY_MAPPING_WMS.SLOT_NEST_EACHES_FLAG,
    ITEM_FACILITY_MAPPING_WMS.SLOT_INCR_HT,
    ITEM_FACILITY_MAPPING_WMS.SLOT_INCR_LEN,
    ITEM_FACILITY_MAPPING_WMS.SLOT_INCR_WIDTH,
    ITEM_FACILITY_MAPPING_WMS.NBR_OF_DYN_ACTV_PICK_PER_ITEM,
    ITEM_FACILITY_MAPPING_WMS.NBR_OF_DYN_CASE_PICK_PER_ITEM,
    ITEM_FACILITY_MAPPING_WMS.AUDIT_CREATED_SOURCE,
    ITEM_FACILITY_MAPPING_WMS.AUDIT_CREATED_SOURCE_TYPE,
    ITEM_FACILITY_MAPPING_WMS.AUDIT_CREATED_DTTM,
    ITEM_FACILITY_MAPPING_WMS.AUDIT_LAST_UPDATED_SOURCE,
    ITEM_FACILITY_MAPPING_WMS.AUDIT_LAST_UPDATED_SOURCE_TYPE,
    ITEM_FACILITY_MAPPING_WMS.AUDIT_LAST_UPDATED_DTTM,
    ITEM_FACILITY_MAPPING_WMS.MARK_FOR_DELETION,
    ITEM_FACILITY_MAPPING_WMS.ITEM_ID,
    ITEM_FACILITY_MAPPING_WMS.FACILITY_ID,
    ITEM_FACILITY_MAPPING_WMS.BUSINESS_PARTNER_ID,
    ITEM_FACILITY_MAPPING_WMS.AVERAGE_MOVEMENT,
    ITEM_FACILITY_MAPPING_WMS.SHELF_DAYS
    FROM {source_schema}.ITEM_FACILITY_MAPPING_WMS
    WHERE (trunc(ITEM_FACILITY_MAPPING_WMS.AUDIT_CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (trunc(ITEM_FACILITY_MAPPING_WMS.AUDIT_LAST_UPDATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) AND 
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 83

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS_temp = SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS.toDF(*["SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___" + col for col in SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS.columns])

    EXPTRANS = SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS_temp.selectExpr( \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___ITEM_FACILITY_MAPPING_ID as ITEM_FACILITY_MAPPING_ID", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___LPN_PER_TIER as LPN_PER_TIER", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___TIER_PER_PLT as TIER_PER_PLT", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___CASE_SIZE_TYPE as CASE_SIZE_TYPE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___PICK_RATE as PICK_RATE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___WAGE_VALUE as WAGE_VALUE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___PACK_RATE as PACK_RATE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SPL_PROC_RATE as SPL_PROC_RATE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___AUTO_SUB_CASE as AUTO_SUB_CASE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___ASSIGN_DYNAMIC_ACTV_PICK_SITE as ASSIGN_DYNAMIC_ACTV_PICK_SITE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___ASSIGN_DYNAMIC_CASE_PICK_SITE as ASSIGN_DYNAMIC_CASE_PICK_SITE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___PICK_LOCN_ASSIGN_TYPE as PICK_LOCN_ASSIGN_TYPE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___PUTWY_TYPE as PUTWY_TYPE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___DFLT_WAVE_PROC_TYPE as DFLT_WAVE_PROC_TYPE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___XCESS_WAVE_NEED_PROC_TYPE as XCESS_WAVE_NEED_PROC_TYPE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___ALLOC_TYPE as ALLOC_TYPE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___VIOLATE_FIFO_ALLOC_QTY_MATCH as VIOLATE_FIFO_ALLOC_QTY_MATCH", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___QV_ITEM_GRP as QV_ITEM_GRP", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___QUAL_INSPCT_ITEM_GRP as QUAL_INSPCT_ITEM_GRP", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MAX_UNITS_IN_DYNAMIC_ACTV as MAX_UNITS_IN_DYNAMIC_ACTV", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MAX_CASES_IN_DYNAMIC_ACTV as MAX_CASES_IN_DYNAMIC_ACTV", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MAX_UNITS_IN_DYNAMIC_CASE_PICK as MAX_UNITS_IN_DYNAMIC_CASE_PICK", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MAX_CASES_IN_DYNAMIC_CASE_PICK as MAX_CASES_IN_DYNAMIC_CASE_PICK", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___CASE_CNT_DATE_TIME as CASE_CNT_DATE_TIME", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___ACTV_CNT_DATE_TIME as ACTV_CNT_DATE_TIME", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___CASE_PICK_CNT_DATE_TIME as CASE_PICK_CNT_DATE_TIME", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___VENDOR_CARTON_PER_TIER as VENDOR_CARTON_PER_TIER", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___VENDOR_TIER_PER_PLT as VENDOR_TIER_PER_PLT", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___ORD_CARTON_PER_TIER as ORD_CARTON_PER_TIER", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___ORD_TIER_PER_PLT as ORD_TIER_PER_PLT", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___DFLT_CATCH_WT_METHOD as DFLT_CATCH_WT_METHOD", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___DFLT_DATE_MASK as DFLT_DATE_MASK", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___CARTON_BREAK_ATTR as CARTON_BREAK_ATTR", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MISC_SHORT_ALPHA_1 as MISC_SHORT_ALPHA_1", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MISC_SHORT_ALPHA_2 as MISC_SHORT_ALPHA_2", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MISC_ALPHA_1 as MISC_ALPHA_1", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MISC_ALPHA_2 as MISC_ALPHA_2", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MISC_ALPHA_3 as MISC_ALPHA_3", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MISC_NUMERIC_1 as MISC_NUMERIC_1", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MISC_NUMERIC_2 as MISC_NUMERIC_2", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MISC_NUMERIC_3 as MISC_NUMERIC_3", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MISC_DATE_1 as MISC_DATE_1", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MISC_DATE_2 as MISC_DATE_2", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___CHUTE_ASSIGN_TYPE as CHUTE_ASSIGN_TYPE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___ACTV_REPL_ORGN as ACTV_REPL_ORGN", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___UIN_NBR as UIN_NBR", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___FIFO_RANGE as FIFO_RANGE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___PRTL_CASE_ALLOC_THRESH_UNITS as PRTL_CASE_ALLOC_THRESH_UNITS", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___PRTL_CASE_PUTWY_THRESH_UNITS as PRTL_CASE_PUTWY_THRESH_UNITS", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___VENDOR_TAGGED_EPC_FLAG as VENDOR_TAGGED_EPC_FLAG", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___ITEM_AVG_WT as ITEM_AVG_WT", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___DFLT_MIN_FROM_PREV_LOCN_FLAG as DFLT_MIN_FROM_PREV_LOCN_FLAG", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_MISC_1 as SLOT_MISC_1", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_MISC_2 as SLOT_MISC_2", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_MISC_3 as SLOT_MISC_3", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_MISC_4 as SLOT_MISC_4", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_MISC_5 as SLOT_MISC_5", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_MISC_6 as SLOT_MISC_6", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_ROTATE_EACHES_FLAG as SLOT_ROTATE_EACHES_FLAG", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_ROTATE_INNERS_FLAG as SLOT_ROTATE_INNERS_FLAG", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_ROTATE_BINS_FLAG as SLOT_ROTATE_BINS_FLAG", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_ROTATE_CASES_FLAG as SLOT_ROTATE_CASES_FLAG", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_3D_SLOTTING_FLAG as SLOT_3D_SLOTTING_FLAG", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_NEST_EACHES_FLAG as SLOT_NEST_EACHES_FLAG", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_INCR_HT as SLOT_INCR_HT", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_INCR_LEN as SLOT_INCR_LEN", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SLOT_INCR_WIDTH as SLOT_INCR_WIDTH", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___NBR_OF_DYN_ACTV_PICK_PER_ITEM as NBR_OF_DYN_ACTV_PICK_PER_ITEM", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___NBR_OF_DYN_CASE_PICK_PER_ITEM as NBR_OF_DYN_CASE_PICK_PER_ITEM", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___MARK_FOR_DELETION as MARK_FOR_DELETION", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___ITEM_ID as ITEM_ID", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___FACILITY_ID as FACILITY_ID", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___AVERAGE_MOVEMENT as AVERAGE_MOVEMENT", \
        "SQ_Shortcut_to_ITEM_FACILITY_MAPPING_WMS___SHELF_DAYS as SHELF_DAYS", \
        "CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ITEM_FACILITY_MAPPING_WMS_PRE, type TARGET 
    # COLUMN COUNT: 83


    Shortcut_to_WM_ITEM_FACILITY_MAPPING_WMS_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(ITEM_FACILITY_MAPPING_ID AS DECIMAL(19,0)) as ITEM_FACILITY_MAPPING_ID",
        "CAST(LPN_PER_TIER AS INT) as LPN_PER_TIER",
        "CAST(TIER_PER_PLT AS INT) as TIER_PER_PLT",
        "CAST(CASE_SIZE_TYPE AS STRING) as CASE_SIZE_TYPE",
        "CAST(PICK_RATE AS DECIMAL(9,4)) as PICK_RATE",
        "CAST(WAGE_VALUE AS DECIMAL(9,4)) as WAGE_VALUE",
        "CAST(PACK_RATE AS DECIMAL(13,4)) as PACK_RATE",
        "CAST(SPL_PROC_RATE AS DECIMAL(9,4)) as SPL_PROC_RATE",
        "CAST(AUTO_SUB_CASE AS STRING) as AUTO_SUB_CASE",
        "CAST(ASSIGN_DYNAMIC_ACTV_PICK_SITE AS STRING) as ASSIGN_DYNAMIC_ACTV_PICK_SITE",
        "CAST(ASSIGN_DYNAMIC_CASE_PICK_SITE AS STRING) as ASSIGN_DYNAMIC_CASE_PICK_SITE",
        "CAST(PICK_LOCN_ASSIGN_TYPE AS STRING) as PICK_LOCN_ASSIGN_TYPE",
        "CAST(PUTWY_TYPE AS STRING) as PUTWY_TYPE",
        "CAST(DFLT_WAVE_PROC_TYPE AS SMALLINT) as DFLT_WAVE_PROC_TYPE",
        "CAST(XCESS_WAVE_NEED_PROC_TYPE AS TINYINT) as XCESS_WAVE_NEED_PROC_TYPE",
        "CAST(ALLOC_TYPE AS STRING) as ALLOC_TYPE",
        "CAST(VIOLATE_FIFO_ALLOC_QTY_MATCH AS STRING) as VIOLATE_FIFO_ALLOC_QTY_MATCH",
        "CAST(QV_ITEM_GRP AS STRING) as QV_ITEM_GRP",
        "CAST(QUAL_INSPCT_ITEM_GRP AS STRING) as QUAL_INSPCT_ITEM_GRP",
        "CAST(MAX_UNITS_IN_DYNAMIC_ACTV AS DECIMAL(13,5)) as MAX_UNITS_IN_DYNAMIC_ACTV",
        "CAST(MAX_CASES_IN_DYNAMIC_ACTV AS INT) as MAX_CASES_IN_DYNAMIC_ACTV",
        "CAST(MAX_UNITS_IN_DYNAMIC_CASE_PICK AS DECIMAL(13,5)) as MAX_UNITS_IN_DYNAMIC_CASE_PICK",
        "CAST(MAX_CASES_IN_DYNAMIC_CASE_PICK AS INT) as MAX_CASES_IN_DYNAMIC_CASE_PICK",
        "CAST(CASE_CNT_DATE_TIME AS TIMESTAMP) as CASE_CNT_DATE_TIME",
        "CAST(ACTV_CNT_DATE_TIME AS TIMESTAMP) as ACTV_CNT_DATE_TIME",
        "CAST(CASE_PICK_CNT_DATE_TIME AS TIMESTAMP) as CASE_PICK_CNT_DATE_TIME",
        "CAST(VENDOR_CARTON_PER_TIER AS INT) as VENDOR_CARTON_PER_TIER",
        "CAST(VENDOR_TIER_PER_PLT AS INT) as VENDOR_TIER_PER_PLT",
        "CAST(ORD_CARTON_PER_TIER AS INT) as ORD_CARTON_PER_TIER",
        "CAST(ORD_TIER_PER_PLT AS INT) as ORD_TIER_PER_PLT",
        "CAST(DFLT_CATCH_WT_METHOD AS STRING) as DFLT_CATCH_WT_METHOD",
        "CAST(DFLT_DATE_MASK AS STRING) as DFLT_DATE_MASK",
        "CAST(CARTON_BREAK_ATTR AS STRING) as CARTON_BREAK_ATTR",
        "CAST(MISC_SHORT_ALPHA_1 AS STRING) as MISC_SHORT_ALPHA_1",
        "CAST(MISC_SHORT_ALPHA_2 AS STRING) as MISC_SHORT_ALPHA_2",
        "CAST(MISC_ALPHA_1 AS STRING) as MISC_ALPHA_1",
        "CAST(MISC_ALPHA_2 AS STRING) as MISC_ALPHA_2",
        "CAST(MISC_ALPHA_3 AS STRING) as MISC_ALPHA_3",
        "CAST(MISC_NUMERIC_1 AS DECIMAL(9,3)) as MISC_NUMERIC_1",
        "CAST(MISC_NUMERIC_2 AS DECIMAL(9,3)) as MISC_NUMERIC_2",
        "CAST(MISC_NUMERIC_3 AS DECIMAL(9,3)) as MISC_NUMERIC_3",
        "CAST(MISC_DATE_1 AS TIMESTAMP) as MISC_DATE_1",
        "CAST(MISC_DATE_2 AS TIMESTAMP) as MISC_DATE_2",
        "CAST(CHUTE_ASSIGN_TYPE AS STRING) as CHUTE_ASSIGN_TYPE",
        "CAST(ACTV_REPL_ORGN AS STRING) as ACTV_REPL_ORGN",
        "CAST(UIN_NBR AS STRING) as UIN_NBR",
        "CAST(FIFO_RANGE AS SMALLINT) as FIFO_RANGE",
        "CAST(PRTL_CASE_ALLOC_THRESH_UNITS AS DECIMAL(13,4)) as PRTL_CASE_ALLOC_THRESH_UNITS",
        "CAST(PRTL_CASE_PUTWY_THRESH_UNITS AS DECIMAL(13,4)) as PRTL_CASE_PUTWY_THRESH_UNITS",
        "CAST(VENDOR_TAGGED_EPC_FLAG AS STRING) as VENDOR_TAGGED_EPC_FLAG",
        "CAST(ITEM_AVG_WT AS DECIMAL(13,4)) as ITEM_AVG_WT",
        "CAST(DFLT_MIN_FROM_PREV_LOCN_FLAG AS STRING) as DFLT_MIN_FROM_PREV_LOCN_FLAG",
        "CAST(SLOT_MISC_1 AS STRING) as SLOT_MISC_1",
        "CAST(SLOT_MISC_2 AS STRING) as SLOT_MISC_2",
        "CAST(SLOT_MISC_3 AS STRING) as SLOT_MISC_3",
        "CAST(SLOT_MISC_4 AS STRING) as SLOT_MISC_4",
        "CAST(SLOT_MISC_5 AS STRING) as SLOT_MISC_5",
        "CAST(SLOT_MISC_6 AS STRING) as SLOT_MISC_6",
        "CAST(SLOT_ROTATE_EACHES_FLAG AS STRING) as SLOT_ROTATE_EACHES_FLAG",
        "CAST(SLOT_ROTATE_INNERS_FLAG AS STRING) as SLOT_ROTATE_INNERS_FLAG",
        "CAST(SLOT_ROTATE_BINS_FLAG AS STRING) as SLOT_ROTATE_BINS_FLAG",
        "CAST(SLOT_ROTATE_CASES_FLAG AS STRING) as SLOT_ROTATE_CASES_FLAG",
        "CAST(SLOT_3D_SLOTTING_FLAG AS STRING) as SLOT_3D_SLOTTING_FLAG",
        "CAST(SLOT_NEST_EACHES_FLAG AS STRING) as SLOT_NEST_EACHES_FLAG",
        "CAST(SLOT_INCR_HT AS DECIMAL(7,2)) as SLOT_INCR_HT",
        "CAST(SLOT_INCR_LEN AS DECIMAL(7,2)) as SLOT_INCR_LEN",
        "CAST(SLOT_INCR_WIDTH AS DECIMAL(7,2)) as SLOT_INCR_WIDTH",
        "CAST(NBR_OF_DYN_ACTV_PICK_PER_ITEM AS SMALLINT) as NBR_OF_DYN_ACTV_PICK_PER_ITEM",
        "CAST(NBR_OF_DYN_CASE_PICK_PER_ITEM AS SMALLINT) as NBR_OF_DYN_CASE_PICK_PER_ITEM",
        "CAST(AUDIT_CREATED_SOURCE AS STRING) as AUDIT_CREATED_SOURCE",
        "CAST(AUDIT_CREATED_SOURCE_TYPE AS TINYINT) as AUDIT_CREATED_SOURCE_TYPE",
        "CAST(AUDIT_CREATED_DTTM AS TIMESTAMP) as AUDIT_CREATED_DTTM",
        "CAST(AUDIT_LAST_UPDATED_SOURCE AS STRING) as AUDIT_LAST_UPDATED_SOURCE",
        "CAST(AUDIT_LAST_UPDATED_SOURCE_TYPE AS TINYINT) as AUDIT_LAST_UPDATED_SOURCE_TYPE",
        "CAST(AUDIT_LAST_UPDATED_DTTM AS TIMESTAMP) as AUDIT_LAST_UPDATED_DTTM",
        "CAST(MARK_FOR_DELETION AS TINYINT) as MARK_FOR_DELETION",
        "CAST(ITEM_ID AS INT) as ITEM_ID",
        "CAST(FACILITY_ID AS INT) as FACILITY_ID",
        "CAST(BUSINESS_PARTNER_ID AS STRING) as BUSINESS_PARTNER_ID",
        "CAST(AVERAGE_MOVEMENT AS DECIMAL(9,2)) as AVERAGE_MOVEMENT",
        "CAST(SHELF_DAYS AS INT) as SHELF_DAYS",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )

    overwriteDeltaPartition(Shortcut_to_WM_ITEM_FACILITY_MAPPING_WMS_PRE,"DC_NBR",dcnbr,target_table_name)
    
    logger.info(
        "Shortcut_to_WM_ITEM_FACILITY_MAPPING_WMS_PRE is written to the target table - "
        + target_table_name
    )    