#Code converted on 2023-06-22 21:01:40
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



def m_WM_Task_Dtl_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Task_Dtl_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_TASK_DTL_PRE"
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
                    TASK_DTL.TASK_ID,
                    TASK_DTL.TASK_SEQ_NBR,
                    TASK_DTL.CNTR_NBR,
                    TASK_DTL.INVN_TYPE,
                    TASK_DTL.PROD_STAT,
                    TASK_DTL.BATCH_NBR,
                    TASK_DTL.SKU_ATTR_1,
                    TASK_DTL.SKU_ATTR_2,
                    TASK_DTL.SKU_ATTR_3,
                    TASK_DTL.SKU_ATTR_4,
                    TASK_DTL.SKU_ATTR_5,
                    TASK_DTL.CNTRY_OF_ORGN,
                    TASK_DTL.ALLOC_INVN_CODE,
                    TASK_DTL.PULL_LOCN_ID,
                    TASK_DTL.ALLOC_UOM_QTY,
                    TASK_DTL.FULL_CNTR_ALLOCD,
                    TASK_DTL.INVN_NEED_TYPE,
                    TASK_DTL.TASK_PRTY,
                    TASK_DTL.TASK_GENRTN_REF_CODE,
                    TASK_DTL.TASK_GENRTN_REF_NBR,
                    TASK_DTL.TASK_CMPL_REF_CODE,
                    TASK_DTL.TASK_CMPL_REF_NBR,
                    TASK_DTL.ERLST_START_DATE_TIME,
                    TASK_DTL.LTST_START_DATE_TIME,
                    TASK_DTL.LTST_CMPL_DATE_TIME,
                    TASK_DTL.ALLOC_UOM,
                    TASK_DTL.ORIG_REQMT,
                    TASK_DTL.QTY_ALLOC,
                    TASK_DTL.QTY_PULLD,
                    TASK_DTL.DEST_LOCN_ID,
                    TASK_DTL.DEST_LOCN_SEQ,
                    TASK_DTL.STAT_CODE,
                    TASK_DTL.TASK_TYPE,
                    TASK_DTL.CURR_WORK_GRP,
                    TASK_DTL.CURR_WORK_AREA,
                    TASK_DTL.REPL_DIVRT_LOCN,
                    TASK_DTL.PICK_SEQ_CODE,
                    TASK_DTL.TASK_LOCN_SEQ,
                    TASK_DTL.CREATE_DATE_TIME,
                    TASK_DTL.MOD_DATE_TIME,
                    TASK_DTL.USER_ID,
                    TASK_DTL.PKT_CTRL_NBR,
                    TASK_DTL.REQD_INVN_TYPE,
                    TASK_DTL.REQD_PROD_STAT,
                    TASK_DTL.REQD_BATCH_NBR,
                    TASK_DTL.REQD_SKU_ATTR_1,
                    TASK_DTL.REQD_SKU_ATTR_2,
                    TASK_DTL.REQD_SKU_ATTR_3,
                    TASK_DTL.REQD_SKU_ATTR_4,
                    TASK_DTL.REQD_SKU_ATTR_5,
                    TASK_DTL.REQD_CNTRY_OF_ORGN,
                    TASK_DTL.PKT_SEQ_NBR,
                    TASK_DTL.CARTON_NBR,
                    TASK_DTL.CARTON_SEQ_NBR,
                    TASK_DTL.PIKR_NBR,
                    TASK_DTL.TASK_CMPL_REF_NBR_SEQ,
                    TASK_DTL.ALLOC_INVN_DTL_ID,
                    TASK_DTL.SUBSTITUTION_FLAG,
                    TASK_DTL.NEXT_TASK_ID,
                    TASK_DTL.NEXT_TASK_SEQ_NBR,
                    TASK_DTL.NEXT_TASK_DESC,
                    TASK_DTL.NEXT_TASK_TYPE,
                    TASK_DTL.MISC_ALPHA_FIELD_1,
                    TASK_DTL.MISC_ALPHA_FIELD_2,
                    TASK_DTL.MISC_ALPHA_FIELD_3,
                    TASK_DTL.VOCOLLECT_POSN,
                    TASK_DTL.VOCOLLECT_SHORT_FLAG,
                    TASK_DTL.PAGE_NBR,
                    TASK_DTL.RSN_CODE,
                    TASK_DTL.SLOT_NBR,
                    TASK_DTL.SUBSLOT_NBR,
                    TASK_DTL.CD_MASTER_ID,
                    TASK_DTL.TASK_DTL_ID,
                    TASK_DTL.TASK_HDR_ID,
                    TASK_DTL.WM_VERSION_ID,
                    TASK_DTL.ITEM_ID,
                    TASK_DTL.TC_ORDER_ID,
                    TASK_DTL.LINE_ITEM_ID,
                    TASK_DTL.TRANS_INVN_TYPE,
                    TASK_DTL.TOTE_NBR,
                    TASK_DTL.IS_CHASE_CREATED,
                    TASK_DTL.RESOURCE_GROUP_ID,
                    TASK_DTL.WORK_RESOURCE_ID,
                    TASK_DTL.WORK_RELEASE_BATCH_NUMBER,
                    TASK_DTL.ALLOCATION_KEY,
                    TASK_DTL.DISTRIBUTION_KEY
                FROM {source_schema}.TASK_DTL
                WHERE  (TRUNC(TASK_DTL.CREATE_DATE_TIME) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR ( TRUNC(TASK_DTL.MOD_DATE_TIME)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1)"""
    

    SQ_Shortcut_to_TASK_DTL = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_TASK_DTL is executed and data is loaded using jdbc")
    
    
    # Processing node EXP_TRN, type EXPRESSION 
    # COLUMN COUNT: 88
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_TASK_DTL_temp = SQ_Shortcut_to_TASK_DTL.toDF(*["SQ_Shortcut_to_TASK_DTL___" + col for col in SQ_Shortcut_to_TASK_DTL.columns])
    
    EXP_TRN = SQ_Shortcut_to_TASK_DTL_temp.selectExpr( 
        "SQ_Shortcut_to_TASK_DTL___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_EXP", 
        "SQ_Shortcut_to_TASK_DTL___TASK_ID as TASK_ID", 
        "SQ_Shortcut_to_TASK_DTL___TASK_SEQ_NBR as TASK_SEQ_NBR", 
        "SQ_Shortcut_to_TASK_DTL___CNTR_NBR as CNTR_NBR", 
        "SQ_Shortcut_to_TASK_DTL___INVN_TYPE as INVN_TYPE", 
        "SQ_Shortcut_to_TASK_DTL___PROD_STAT as PROD_STAT", 
        "SQ_Shortcut_to_TASK_DTL___BATCH_NBR as BATCH_NBR", 
        "SQ_Shortcut_to_TASK_DTL___SKU_ATTR_1 as SKU_ATTR_1", 
        "SQ_Shortcut_to_TASK_DTL___SKU_ATTR_2 as SKU_ATTR_2", 
        "SQ_Shortcut_to_TASK_DTL___SKU_ATTR_3 as SKU_ATTR_3", 
        "SQ_Shortcut_to_TASK_DTL___SKU_ATTR_4 as SKU_ATTR_4", 
        "SQ_Shortcut_to_TASK_DTL___SKU_ATTR_5 as SKU_ATTR_5", 
        "SQ_Shortcut_to_TASK_DTL___CNTRY_OF_ORGN as CNTRY_OF_ORGN", 
        "SQ_Shortcut_to_TASK_DTL___ALLOC_INVN_CODE as ALLOC_INVN_CODE", 
        "SQ_Shortcut_to_TASK_DTL___PULL_LOCN_ID as PULL_LOCN_ID", 
        "SQ_Shortcut_to_TASK_DTL___ALLOC_UOM_QTY as ALLOC_UOM_QTY", 
        "SQ_Shortcut_to_TASK_DTL___FULL_CNTR_ALLOCD as FULL_CNTR_ALLOCD", 
        "SQ_Shortcut_to_TASK_DTL___INVN_NEED_TYPE as INVN_NEED_TYPE", 
        "SQ_Shortcut_to_TASK_DTL___TASK_PRTY as TASK_PRTY", 
        "SQ_Shortcut_to_TASK_DTL___TASK_GENRTN_REF_CODE as TASK_GENRTN_REF_CODE", 
        "SQ_Shortcut_to_TASK_DTL___TASK_GENRTN_REF_NBR as TASK_GENRTN_REF_NBR", 
        "SQ_Shortcut_to_TASK_DTL___TASK_CMPL_REF_CODE as TASK_CMPL_REF_CODE", 
        "SQ_Shortcut_to_TASK_DTL___TASK_CMPL_REF_NBR as TASK_CMPL_REF_NBR", 
        "SQ_Shortcut_to_TASK_DTL___ERLST_START_DATE_TIME as ERLST_START_DATE_TIME", 
        "SQ_Shortcut_to_TASK_DTL___LTST_START_DATE_TIME as LTST_START_DATE_TIME", 
        "SQ_Shortcut_to_TASK_DTL___LTST_CMPL_DATE_TIME as LTST_CMPL_DATE_TIME", 
        "SQ_Shortcut_to_TASK_DTL___ALLOC_UOM as ALLOC_UOM", 
        "SQ_Shortcut_to_TASK_DTL___ORIG_REQMT as ORIG_REQMT", 
        "SQ_Shortcut_to_TASK_DTL___QTY_ALLOC as QTY_ALLOC", 
        "SQ_Shortcut_to_TASK_DTL___QTY_PULLD as QTY_PULLD", 
        "SQ_Shortcut_to_TASK_DTL___DEST_LOCN_ID as DEST_LOCN_ID", 
        "SQ_Shortcut_to_TASK_DTL___DEST_LOCN_SEQ as DEST_LOCN_SEQ", 
        "SQ_Shortcut_to_TASK_DTL___STAT_CODE as STAT_CODE", 
        "SQ_Shortcut_to_TASK_DTL___TASK_TYPE as TASK_TYPE", 
        "SQ_Shortcut_to_TASK_DTL___CURR_WORK_GRP as CURR_WORK_GRP", 
        "SQ_Shortcut_to_TASK_DTL___CURR_WORK_AREA as CURR_WORK_AREA", 
        "SQ_Shortcut_to_TASK_DTL___REPL_DIVRT_LOCN as REPL_DIVRT_LOCN", 
        "SQ_Shortcut_to_TASK_DTL___PICK_SEQ_CODE as PICK_SEQ_CODE", 
        "SQ_Shortcut_to_TASK_DTL___TASK_LOCN_SEQ as TASK_LOCN_SEQ", 
        "SQ_Shortcut_to_TASK_DTL___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_TASK_DTL___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_TASK_DTL___USER_ID as USER_ID", 
        "SQ_Shortcut_to_TASK_DTL___PKT_CTRL_NBR as PKT_CTRL_NBR", 
        "SQ_Shortcut_to_TASK_DTL___REQD_INVN_TYPE as REQD_INVN_TYPE", 
        "SQ_Shortcut_to_TASK_DTL___REQD_PROD_STAT as REQD_PROD_STAT", 
        "SQ_Shortcut_to_TASK_DTL___REQD_BATCH_NBR as REQD_BATCH_NBR", 
        "SQ_Shortcut_to_TASK_DTL___REQD_SKU_ATTR_1 as REQD_SKU_ATTR_1", 
        "SQ_Shortcut_to_TASK_DTL___REQD_SKU_ATTR_2 as REQD_SKU_ATTR_2", 
        "SQ_Shortcut_to_TASK_DTL___REQD_SKU_ATTR_3 as REQD_SKU_ATTR_3", 
        "SQ_Shortcut_to_TASK_DTL___REQD_SKU_ATTR_4 as REQD_SKU_ATTR_4", 
        "SQ_Shortcut_to_TASK_DTL___REQD_SKU_ATTR_5 as REQD_SKU_ATTR_5", 
        "SQ_Shortcut_to_TASK_DTL___REQD_CNTRY_OF_ORGN as REQD_CNTRY_OF_ORGN", 
        "SQ_Shortcut_to_TASK_DTL___PKT_SEQ_NBR as PKT_SEQ_NBR", 
        "SQ_Shortcut_to_TASK_DTL___CARTON_NBR as CARTON_NBR", 
        "SQ_Shortcut_to_TASK_DTL___CARTON_SEQ_NBR as CARTON_SEQ_NBR", 
        "SQ_Shortcut_to_TASK_DTL___PIKR_NBR as PIKR_NBR", 
        "SQ_Shortcut_to_TASK_DTL___TASK_CMPL_REF_NBR_SEQ as TASK_CMPL_REF_NBR_SEQ", 
        "SQ_Shortcut_to_TASK_DTL___ALLOC_INVN_DTL_ID as ALLOC_INVN_DTL_ID", 
        "SQ_Shortcut_to_TASK_DTL___SUBSTITUTION_FLAG as SUBSTITUTION_FLAG", 
        "SQ_Shortcut_to_TASK_DTL___NEXT_TASK_ID as NEXT_TASK_ID", 
        "SQ_Shortcut_to_TASK_DTL___NEXT_TASK_SEQ_NBR as NEXT_TASK_SEQ_NBR", 
        "SQ_Shortcut_to_TASK_DTL___NEXT_TASK_DESC as NEXT_TASK_DESC", 
        "SQ_Shortcut_to_TASK_DTL___NEXT_TASK_TYPE as NEXT_TASK_TYPE", 
        "SQ_Shortcut_to_TASK_DTL___MISC_ALPHA_FIELD_1 as MISC_ALPHA_FIELD_1", 
        "SQ_Shortcut_to_TASK_DTL___MISC_ALPHA_FIELD_2 as MISC_ALPHA_FIELD_2", 
        "SQ_Shortcut_to_TASK_DTL___MISC_ALPHA_FIELD_3 as MISC_ALPHA_FIELD_3", 
        "SQ_Shortcut_to_TASK_DTL___VOCOLLECT_POSN as VOCOLLECT_POSN", 
        "SQ_Shortcut_to_TASK_DTL___VOCOLLECT_SHORT_FLAG as VOCOLLECT_SHORT_FLAG", 
        "SQ_Shortcut_to_TASK_DTL___PAGE_NBR as PAGE_NBR", 
        "SQ_Shortcut_to_TASK_DTL___RSN_CODE as RSN_CODE", 
        "SQ_Shortcut_to_TASK_DTL___SLOT_NBR as SLOT_NBR", 
        "SQ_Shortcut_to_TASK_DTL___SUBSLOT_NBR as SUBSLOT_NBR", 
        "SQ_Shortcut_to_TASK_DTL___CD_MASTER_ID as CD_MASTER_ID", 
        "SQ_Shortcut_to_TASK_DTL___TASK_DTL_ID as TASK_DTL_ID", 
        "SQ_Shortcut_to_TASK_DTL___TASK_HDR_ID as TASK_HDR_ID", 
        "SQ_Shortcut_to_TASK_DTL___WM_VERSION_ID as WM_VERSION_ID", 
        "SQ_Shortcut_to_TASK_DTL___ITEM_ID as ITEM_ID", 
        "SQ_Shortcut_to_TASK_DTL___TC_ORDER_ID as TC_ORDER_ID", 
        "SQ_Shortcut_to_TASK_DTL___LINE_ITEM_ID as LINE_ITEM_ID", 
        "SQ_Shortcut_to_TASK_DTL___TRANS_INVN_TYPE as TRANS_INVN_TYPE", 
        "SQ_Shortcut_to_TASK_DTL___TOTE_NBR as TOTE_NBR", 
        "SQ_Shortcut_to_TASK_DTL___IS_CHASE_CREATED as IS_CHASE_CREATED", 
        "SQ_Shortcut_to_TASK_DTL___RESOURCE_GROUP_ID as RESOURCE_GROUP_ID", 
        "SQ_Shortcut_to_TASK_DTL___WORK_RESOURCE_ID as WORK_RESOURCE_ID", 
        "SQ_Shortcut_to_TASK_DTL___WORK_RELEASE_BATCH_NUMBER as WORK_RELEASE_BATCH_NUMBER", 
        "SQ_Shortcut_to_TASK_DTL___ALLOCATION_KEY as ALLOCATION_KEY", 
        "SQ_Shortcut_to_TASK_DTL___DISTRIBUTION_KEY as DISTRIBUTION_KEY", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_TASK_DTL_PRE, type TARGET 
    # COLUMN COUNT: 88
    
    
    Shortcut_to_WM_TASK_DTL_PRE = EXP_TRN.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(TASK_DTL_ID AS INT) as TASK_DTL_ID",
        "CAST(TASK_ID AS INT) as TASK_ID",
        "CAST(TASK_SEQ_NBR AS SMALLINT) as TASK_SEQ_NBR",
        "CAST(CNTR_NBR AS STRING) as CNTR_NBR",
        "CAST(INVN_TYPE AS STRING) as INVN_TYPE",
        "CAST(PROD_STAT AS STRING) as PROD_STAT",
        "CAST(BATCH_NBR AS STRING) as BATCH_NBR",
        "CAST(SKU_ATTR_1 AS STRING) as SKU_ATTR_1",
        "CAST(SKU_ATTR_2 AS STRING) as SKU_ATTR_2",
        "CAST(SKU_ATTR_3 AS STRING) as SKU_ATTR_3",
        "CAST(SKU_ATTR_4 AS STRING) as SKU_ATTR_4",
        "CAST(SKU_ATTR_5 AS STRING) as SKU_ATTR_5",
        "CAST(CNTRY_OF_ORGN AS STRING) as CNTRY_OF_ORGN",
        "CAST(ALLOC_INVN_CODE AS SMALLINT) as ALLOC_INVN_CODE",
        "CAST(PULL_LOCN_ID AS STRING) as PULL_LOCN_ID",
        "CAST(ALLOC_UOM_QTY AS DECIMAL(9,2)) as ALLOC_UOM_QTY",
        "CAST(FULL_CNTR_ALLOCD AS STRING) as FULL_CNTR_ALLOCD",
        "CAST(INVN_NEED_TYPE AS SMALLINT) as INVN_NEED_TYPE",
        "CAST(TASK_PRTY AS INT) as TASK_PRTY",
        "CAST(TASK_GENRTN_REF_CODE AS STRING) as TASK_GENRTN_REF_CODE",
        "CAST(TASK_GENRTN_REF_NBR AS STRING) as TASK_GENRTN_REF_NBR",
        "CAST(TASK_CMPL_REF_CODE AS STRING) as TASK_CMPL_REF_CODE",
        "CAST(TASK_CMPL_REF_NBR AS STRING) as TASK_CMPL_REF_NBR",
        "CAST(ERLST_START_DATE_TIME AS TIMESTAMP) as ERLST_START_DATE_TIME",
        "CAST(LTST_START_DATE_TIME AS TIMESTAMP) as LTST_START_DATE_TIME",
        "CAST(LTST_CMPL_DATE_TIME AS TIMESTAMP) as LTST_CMPL_DATE_TIME",
        "CAST(ALLOC_UOM AS STRING) as ALLOC_UOM",
        "CAST(ORIG_REQMT AS DECIMAL(13,5)) as ORIG_REQMT",
        "CAST(QTY_ALLOC AS DECIMAL(13,5)) as QTY_ALLOC",
        "CAST(QTY_PULLD AS DECIMAL(13,5)) as QTY_PULLD",
        "CAST(DEST_LOCN_ID AS STRING) as DEST_LOCN_ID",
        "CAST(DEST_LOCN_SEQ AS INT) as DEST_LOCN_SEQ",
        "CAST(STAT_CODE AS SMALLINT) as STAT_CODE",
        "CAST(TASK_TYPE AS STRING) as TASK_TYPE",
        "CAST(CURR_WORK_GRP AS STRING) as CURR_WORK_GRP",
        "CAST(CURR_WORK_AREA AS STRING) as CURR_WORK_AREA",
        "CAST(REPL_DIVRT_LOCN AS STRING) as REPL_DIVRT_LOCN",
        "CAST(PICK_SEQ_CODE AS STRING) as PICK_SEQ_CODE",
        "CAST(TASK_LOCN_SEQ AS SMALLINT) as TASK_LOCN_SEQ",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(PKT_CTRL_NBR AS STRING) as PKT_CTRL_NBR",
        "CAST(REQD_INVN_TYPE AS STRING) as REQD_INVN_TYPE",
        "CAST(REQD_PROD_STAT AS STRING) as REQD_PROD_STAT",
        "CAST(REQD_BATCH_NBR AS STRING) as REQD_BATCH_NBR",
        "CAST(REQD_SKU_ATTR_1 AS STRING) as REQD_SKU_ATTR_1",
        "CAST(REQD_SKU_ATTR_2 AS STRING) as REQD_SKU_ATTR_2",
        "CAST(REQD_SKU_ATTR_3 AS STRING) as REQD_SKU_ATTR_3",
        "CAST(REQD_SKU_ATTR_4 AS STRING) as REQD_SKU_ATTR_4",
        "CAST(REQD_SKU_ATTR_5 AS STRING) as REQD_SKU_ATTR_5",
        "CAST(REQD_CNTRY_OF_ORGN AS STRING) as REQD_CNTRY_OF_ORGN",
        "CAST(PKT_SEQ_NBR AS INT) as PKT_SEQ_NBR",
        "CAST(CARTON_NBR AS STRING) as CARTON_NBR",
        "CAST(CARTON_SEQ_NBR AS INT) as CARTON_SEQ_NBR",
        "CAST(PIKR_NBR AS SMALLINT) as PIKR_NBR",
        "CAST(TASK_CMPL_REF_NBR_SEQ AS INT) as TASK_CMPL_REF_NBR_SEQ",
        "CAST(ALLOC_INVN_DTL_ID AS INT) as ALLOC_INVN_DTL_ID",
        "CAST(SUBSTITUTION_FLAG AS SMALLINT) as SUBSTITUTION_FLAG",
        "CAST(NEXT_TASK_ID AS INT) as NEXT_TASK_ID",
        "CAST(NEXT_TASK_SEQ_NBR AS SMALLINT) as NEXT_TASK_SEQ_NBR",
        "CAST(NEXT_TASK_DESC AS STRING) as NEXT_TASK_DESC",
        "CAST(NEXT_TASK_TYPE AS STRING) as NEXT_TASK_TYPE",
        "CAST(MISC_ALPHA_FIELD_1 AS STRING) as MISC_ALPHA_FIELD_1",
        "CAST(MISC_ALPHA_FIELD_2 AS STRING) as MISC_ALPHA_FIELD_2",
        "CAST(MISC_ALPHA_FIELD_3 AS STRING) as MISC_ALPHA_FIELD_3",
        "CAST(VOCOLLECT_POSN AS SMALLINT) as VOCOLLECT_POSN",
        "CAST(VOCOLLECT_SHORT_FLAG AS STRING) as VOCOLLECT_SHORT_FLAG",
        "CAST(PAGE_NBR AS INT) as PAGE_NBR",
        "CAST(RSN_CODE AS STRING) as RSN_CODE",
        "CAST(SLOT_NBR AS INT) as SLOT_NBR",
        "CAST(SUBSLOT_NBR AS INT) as SUBSLOT_NBR",
        "CAST(CD_MASTER_ID AS INT) as CD_MASTER_ID",
        "CAST(TASK_HDR_ID AS INT) as TASK_HDR_ID",
        "CAST(WM_VERSION_ID AS INT) as WM_VERSION_ID",
        "CAST(ITEM_ID AS BIGINT) as ITEM_ID",
        "CAST(TC_ORDER_ID AS STRING) as TC_ORDER_ID",
        "CAST(LINE_ITEM_ID AS BIGINT) as LINE_ITEM_ID",
        "CAST(TRANS_INVN_TYPE AS SMALLINT) as TRANS_INVN_TYPE",
        "CAST(TOTE_NBR AS STRING) as TOTE_NBR",
        "CAST(IS_CHASE_CREATED AS TINYINT) as IS_CHASE_CREATED",
        "CAST(RESOURCE_GROUP_ID AS STRING) as RESOURCE_GROUP_ID",
        "CAST(WORK_RESOURCE_ID AS STRING) as WORK_RESOURCE_ID",
        "CAST(WORK_RELEASE_BATCH_NUMBER AS STRING) as WORK_RELEASE_BATCH_NUMBER",
        "CAST(ALLOCATION_KEY AS STRING) as ALLOCATION_KEY",
        "CAST(DISTRIBUTION_KEY AS STRING) as DISTRIBUTION_KEY",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )    
    overwriteDeltaPartition(Shortcut_to_WM_TASK_DTL_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_TASK_DTL_PRE is written to the target table - " + target_table_name)
