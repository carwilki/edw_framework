#Code converted on 2023-06-22 21:01:35
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
from utils.logger import *



def m_WM_Task_Hdr_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Task_Hdr_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_TASK_HDR_PRE"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_TASK_HDR"
    prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + prev_run_dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
                    TASK_HDR.TASK_ID,
                    TASK_HDR.WHSE,
                    TASK_HDR.TASK_DESC,
                    TASK_HDR.INVN_TYPE,
                    TASK_HDR.INVN_NEED_TYPE,
                    TASK_HDR.DFLT_TASK_PRTY,
                    TASK_HDR.CURR_TASK_PRTY,
                    TASK_HDR.XPECTD_DURTN,
                    TASK_HDR.ACTL_DURTN,
                    TASK_HDR.ERLST_START_DATE_TIME,
                    TASK_HDR.LTST_START_DATE_TIME,
                    TASK_HDR.LTST_CMPL_DATE_TIME,
                    TASK_HDR.BEGIN_AREA,
                    TASK_HDR.BEGIN_ZONE,
                    TASK_HDR.BEGIN_AISLE,
                    TASK_HDR.END_AREA,
                    TASK_HDR.END_ZONE,
                    TASK_HDR.END_AISLE,
                    TASK_HDR.START_CURR_WORK_GRP,
                    TASK_HDR.START_CURR_WORK_AREA,
                    TASK_HDR.END_CURR_WORK_GRP,
                    TASK_HDR.END_CURR_WORK_AREA,
                    TASK_HDR.START_DEST_WORK_GRP,
                    TASK_HDR.START_DEST_WORK_AREA,
                    TASK_HDR.END_DEST_WORK_GRP,
                    TASK_HDR.END_DEST_WORK_AREA,
                    TASK_HDR.TASK_TYPE,
                    TASK_HDR.TASK_GENRTN_REF_CODE,
                    TASK_HDR.TASK_GENRTN_REF_NBR,
                    TASK_HDR.NEED_ID,
                    TASK_HDR.TASK_BATCH,
                    TASK_HDR.STAT_CODE,
                    TASK_HDR.CREATE_DATE_TIME,
                    TASK_HDR.MOD_DATE_TIME,
                    TASK_HDR.USER_ID,
                    TASK_HDR.RLS_DATE_TIME,
                    TASK_HDR.TASK_CMPL_REF_CODE,
                    TASK_HDR.TASK_CMPL_REF_NBR,
                    TASK_HDR.OWNER_USER_ID,
                    TASK_HDR.ONE_USER_PER_GRP,
                    TASK_HDR.NEXT_TASK_ID,
                    TASK_HDR.EXCEPTION_CODE,
                    TASK_HDR.CURR_LOCN_ID,
                    TASK_HDR.TASK_PARM_ID,
                    TASK_HDR.RULE_ID,
                    TASK_HDR.VOCOLLECT_ASSIGN_ID,
                    TASK_HDR.CURR_USER_ID,
                    TASK_HDR.MHE_FLAG,
                    TASK_HDR.PICK_TO_TOTE_FLAG,
                    TASK_HDR.MHE_ORD_STATE,
                    TASK_HDR.PRT_TASK_LIST_FLAG,
                    TASK_HDR.RPT_PRTR_REQSTR,
                    TASK_HDR.ORIG_TASK_ID,
                    TASK_HDR.DOC_ID,
                    TASK_HDR.VOCO_INTRNL_REVERSE_ID,
                    TASK_HDR.TASK_HDR_ID,
                    TASK_HDR.WM_VERSION_ID,
                    TASK_HDR.ITEM_ID,
                    TASK_HDR.LOCN_HDR_ID,
                    TASK_HDR.TASK_RULE_PARM_ID,
                    TASK_HDR.PICK_CART_TYPE,
                    TASK_HDR.REPRINT_COUNT,
                    TASK_HDR.ESTIMATED_TIME,
                    TASK_HDR.ESTIMATED_DISTANCE
                FROM TASK_HDR
                WHERE {Initial_Load} (TRUNC(TASK_HDR.CREATE_DATE_TIME)>= TRUNC(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC(TASK_HDR.MOD_DATE_TIME)>= TRUNC(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)"""
            

    SQ_Shortcut_to_TASK_HDR = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_TASK_HDR is executed and data is loaded using jdbc")
    
    
    # Processing node EXP_TRN, type EXPRESSION 
    # COLUMN COUNT: 66
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_TASK_HDR_temp = SQ_Shortcut_to_TASK_HDR.toDF(*["SQ_Shortcut_to_TASK_HDR___" + col for col in SQ_Shortcut_to_TASK_HDR.columns])
    
    EXP_TRN = SQ_Shortcut_to_TASK_HDR_temp.selectExpr( 
    	"SQ_Shortcut_to_TASK_HDR___sys_row_id as sys_row_id", 
    	f"{DC_NBR} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_ID as TASK_ID", 
    	"SQ_Shortcut_to_TASK_HDR___WHSE as WHSE", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_DESC as TASK_DESC", 
    	"SQ_Shortcut_to_TASK_HDR___INVN_TYPE as INVN_TYPE", 
    	"SQ_Shortcut_to_TASK_HDR___INVN_NEED_TYPE as INVN_NEED_TYPE", 
    	"SQ_Shortcut_to_TASK_HDR___DFLT_TASK_PRTY as DFLT_TASK_PRTY", 
    	"SQ_Shortcut_to_TASK_HDR___CURR_TASK_PRTY as CURR_TASK_PRTY", 
    	"SQ_Shortcut_to_TASK_HDR___XPECTD_DURTN as XPECTD_DURTN", 
    	"SQ_Shortcut_to_TASK_HDR___ACTL_DURTN as ACTL_DURTN", 
    	"SQ_Shortcut_to_TASK_HDR___ERLST_START_DATE_TIME as ERLST_START_DATE_TIME", 
    	"SQ_Shortcut_to_TASK_HDR___LTST_START_DATE_TIME as LTST_START_DATE_TIME", 
    	"SQ_Shortcut_to_TASK_HDR___LTST_CMPL_DATE_TIME as LTST_CMPL_DATE_TIME", 
    	"SQ_Shortcut_to_TASK_HDR___BEGIN_AREA as BEGIN_AREA", 
    	"SQ_Shortcut_to_TASK_HDR___BEGIN_ZONE as BEGIN_ZONE", 
    	"SQ_Shortcut_to_TASK_HDR___BEGIN_AISLE as BEGIN_AISLE", 
    	"SQ_Shortcut_to_TASK_HDR___END_AREA as END_AREA", 
    	"SQ_Shortcut_to_TASK_HDR___END_ZONE as END_ZONE", 
    	"SQ_Shortcut_to_TASK_HDR___END_AISLE as END_AISLE", 
    	"SQ_Shortcut_to_TASK_HDR___START_CURR_WORK_GRP as START_CURR_WORK_GRP", 
    	"SQ_Shortcut_to_TASK_HDR___START_CURR_WORK_AREA as START_CURR_WORK_AREA", 
    	"SQ_Shortcut_to_TASK_HDR___END_CURR_WORK_GRP as END_CURR_WORK_GRP", 
    	"SQ_Shortcut_to_TASK_HDR___END_CURR_WORK_AREA as END_CURR_WORK_AREA", 
    	"SQ_Shortcut_to_TASK_HDR___START_DEST_WORK_GRP as START_DEST_WORK_GRP", 
    	"SQ_Shortcut_to_TASK_HDR___START_DEST_WORK_AREA as START_DEST_WORK_AREA", 
    	"SQ_Shortcut_to_TASK_HDR___END_DEST_WORK_GRP as END_DEST_WORK_GRP", 
    	"SQ_Shortcut_to_TASK_HDR___END_DEST_WORK_AREA as END_DEST_WORK_AREA", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_TYPE as TASK_TYPE", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_GENRTN_REF_CODE as TASK_GENRTN_REF_CODE", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_GENRTN_REF_NBR as TASK_GENRTN_REF_NBR", 
    	"SQ_Shortcut_to_TASK_HDR___NEED_ID as NEED_ID", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_BATCH as TASK_BATCH", 
    	"SQ_Shortcut_to_TASK_HDR___STAT_CODE as STAT_CODE", 
    	"SQ_Shortcut_to_TASK_HDR___CREATE_DATE_TIME as CREATE_DATE_TIME", 
    	"SQ_Shortcut_to_TASK_HDR___MOD_DATE_TIME as MOD_DATE_TIME", 
    	"SQ_Shortcut_to_TASK_HDR___USER_ID as USER_ID", 
    	"SQ_Shortcut_to_TASK_HDR___RLS_DATE_TIME as RLS_DATE_TIME", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_CMPL_REF_CODE as TASK_CMPL_REF_CODE", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_CMPL_REF_NBR as TASK_CMPL_REF_NBR", 
    	"SQ_Shortcut_to_TASK_HDR___OWNER_USER_ID as OWNER_USER_ID", 
    	"SQ_Shortcut_to_TASK_HDR___ONE_USER_PER_GRP as ONE_USER_PER_GRP", 
    	"SQ_Shortcut_to_TASK_HDR___NEXT_TASK_ID as NEXT_TASK_ID", 
    	"SQ_Shortcut_to_TASK_HDR___EXCEPTION_CODE as EXCEPTION_CODE", 
    	"SQ_Shortcut_to_TASK_HDR___CURR_LOCN_ID as CURR_LOCN_ID", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_PARM_ID as TASK_PARM_ID", 
    	"SQ_Shortcut_to_TASK_HDR___RULE_ID as RULE_ID", 
    	"SQ_Shortcut_to_TASK_HDR___VOCOLLECT_ASSIGN_ID as VOCOLLECT_ASSIGN_ID", 
    	"SQ_Shortcut_to_TASK_HDR___CURR_USER_ID as CURR_USER_ID", 
    	"SQ_Shortcut_to_TASK_HDR___MHE_FLAG as MHE_FLAG", 
    	"SQ_Shortcut_to_TASK_HDR___PICK_TO_TOTE_FLAG as PICK_TO_TOTE_FLAG", 
    	"SQ_Shortcut_to_TASK_HDR___MHE_ORD_STATE as MHE_ORD_STATE", 
    	"SQ_Shortcut_to_TASK_HDR___PRT_TASK_LIST_FLAG as PRT_TASK_LIST_FLAG", 
    	"SQ_Shortcut_to_TASK_HDR___RPT_PRTR_REQSTR as RPT_PRTR_REQSTR", 
    	"SQ_Shortcut_to_TASK_HDR___ORIG_TASK_ID as ORIG_TASK_ID", 
    	"SQ_Shortcut_to_TASK_HDR___DOC_ID as DOC_ID", 
    	"SQ_Shortcut_to_TASK_HDR___VOCO_INTRNL_REVERSE_ID as VOCO_INTRNL_REVERSE_ID", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_HDR_ID as TASK_HDR_ID", 
    	"SQ_Shortcut_to_TASK_HDR___WM_VERSION_ID as WM_VERSION_ID", 
    	"SQ_Shortcut_to_TASK_HDR___ITEM_ID as ITEM_ID", 
    	"SQ_Shortcut_to_TASK_HDR___LOCN_HDR_ID as LOCN_HDR_ID", 
    	"SQ_Shortcut_to_TASK_HDR___TASK_RULE_PARM_ID as TASK_RULE_PARM_ID", 
    	"SQ_Shortcut_to_TASK_HDR___PICK_CART_TYPE as PICK_CART_TYPE", 
    	"SQ_Shortcut_to_TASK_HDR___REPRINT_COUNT as REPRINT_COUNT", 
    	"SQ_Shortcut_to_TASK_HDR___ESTIMATED_TIME as ESTIMATED_TIME", 
    	"SQ_Shortcut_to_TASK_HDR___ESTIMATED_DISTANCE as ESTIMATED_DISTANCE", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_TASK_HDR_PRE, type TARGET 
    # COLUMN COUNT: 66
    
    
    Shortcut_to_WM_TASK_HDR_PRE = EXP_TRN.selectExpr( 
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
    	"CAST(TASK_HDR_ID AS BIGINT) as TASK_HDR_ID", 
    	"CAST(TASK_ID AS BIGINT) as TASK_ID", 
    	"CAST(WHSE AS STRING) as WHSE", 
    	"CAST(TASK_DESC AS STRING) as TASK_DESC", 
    	"CAST(INVN_TYPE AS STRING) as INVN_TYPE", 
    	"CAST(INVN_NEED_TYPE AS BIGINT) as INVN_NEED_TYPE", 
    	"CAST(DFLT_TASK_PRTY AS BIGINT) as DFLT_TASK_PRTY", 
    	"CAST(CURR_TASK_PRTY AS BIGINT) as CURR_TASK_PRTY", 
    	"CAST(XPECTD_DURTN AS BIGINT) as XPECTD_DURTN", 
    	"CAST(ACTL_DURTN AS BIGINT) as ACTL_DURTN", 
    	"CAST(ERLST_START_DATE_TIME AS TIMESTAMP) as ERLST_START_DATE_TIME", 
    	"CAST(LTST_START_DATE_TIME AS TIMESTAMP) as LTST_START_DATE_TIME", 
    	"CAST(LTST_CMPL_DATE_TIME AS TIMESTAMP) as LTST_CMPL_DATE_TIME", 
    	"CAST(BEGIN_AREA AS STRING) as BEGIN_AREA", 
    	"CAST(BEGIN_ZONE AS STRING) as BEGIN_ZONE", 
    	"CAST(BEGIN_AISLE AS STRING) as BEGIN_AISLE", 
    	"CAST(END_AREA AS STRING) as END_AREA", 
    	"CAST(END_ZONE AS STRING) as END_ZONE", 
    	"CAST(END_AISLE AS STRING) as END_AISLE", 
    	"CAST(START_CURR_WORK_GRP AS STRING) as START_CURR_WORK_GRP", 
    	"CAST(START_CURR_WORK_AREA AS STRING) as START_CURR_WORK_AREA", 
    	"CAST(END_CURR_WORK_GRP AS STRING) as END_CURR_WORK_GRP", 
    	"CAST(END_CURR_WORK_AREA AS STRING) as END_CURR_WORK_AREA", 
    	"CAST(START_DEST_WORK_GRP AS STRING) as START_DEST_WORK_GRP", 
    	"CAST(START_DEST_WORK_AREA AS STRING) as START_DEST_WORK_AREA", 
    	"CAST(END_DEST_WORK_GRP AS STRING) as END_DEST_WORK_GRP", 
    	"CAST(END_DEST_WORK_AREA AS STRING) as END_DEST_WORK_AREA", 
    	"CAST(TASK_TYPE AS STRING) as TASK_TYPE", 
    	"CAST(TASK_GENRTN_REF_CODE AS STRING) as TASK_GENRTN_REF_CODE", 
    	"CAST(TASK_GENRTN_REF_NBR AS STRING) as TASK_GENRTN_REF_NBR", 
    	"CAST(NEED_ID AS STRING) as NEED_ID", 
    	"CAST(TASK_BATCH AS STRING) as TASK_BATCH", 
    	"CAST(STAT_CODE AS BIGINT) as STAT_CODE", 
    	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
    	"CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
    	"CAST(USER_ID AS STRING) as USER_ID", 
    	"CAST(RLS_DATE_TIME AS TIMESTAMP) as RLS_DATE_TIME", 
    	"CAST(TASK_CMPL_REF_CODE AS STRING) as TASK_CMPL_REF_CODE", 
    	"CAST(TASK_CMPL_REF_NBR AS STRING) as TASK_CMPL_REF_NBR", 
    	"CAST(OWNER_USER_ID AS STRING) as OWNER_USER_ID", 
    	"CAST(ONE_USER_PER_GRP AS STRING) as ONE_USER_PER_GRP", 
    	"CAST(NEXT_TASK_ID AS BIGINT) as NEXT_TASK_ID", 
    	"CAST(EXCEPTION_CODE AS STRING) as EXCEPTION_CODE", 
    	"CAST(CURR_LOCN_ID AS STRING) as CURR_LOCN_ID", 
    	"CAST(TASK_PARM_ID AS BIGINT) as TASK_PARM_ID", 
    	"CAST(RULE_ID AS BIGINT) as RULE_ID", 
    	"CAST(VOCOLLECT_ASSIGN_ID AS BIGINT) as VOCOLLECT_ASSIGN_ID", 
    	"CAST(CURR_USER_ID AS STRING) as CURR_USER_ID", 
    	"CAST(MHE_FLAG AS STRING) as MHE_FLAG", 
    	"CAST(PICK_TO_TOTE_FLAG AS STRING) as PICK_TO_TOTE_FLAG", 
    	"CAST(MHE_ORD_STATE AS STRING) as MHE_ORD_STATE", 
    	"CAST(PRT_TASK_LIST_FLAG AS STRING) as PRT_TASK_LIST_FLAG", 
    	"CAST(RPT_PRTR_REQSTR AS STRING) as RPT_PRTR_REQSTR", 
    	"CAST(ORIG_TASK_ID AS BIGINT) as ORIG_TASK_ID", 
    	"CAST(DOC_ID AS STRING) as DOC_ID", 
    	"CAST(VOCO_INTRNL_REVERSE_ID AS STRING) as VOCO_INTRNL_REVERSE_ID", 
    	"CAST(WM_VERSION_ID AS BIGINT) as WM_VERSION_ID", 
    	"CAST(ITEM_ID AS BIGINT) as ITEM_ID", 
    	"CAST(LOCN_HDR_ID AS BIGINT) as LOCN_HDR_ID", 
    	"CAST(TASK_RULE_PARM_ID AS BIGINT) as TASK_RULE_PARM_ID", 
    	"CAST(PICK_CART_TYPE AS STRING) as PICK_CART_TYPE", 
    	"CAST(REPRINT_COUNT AS BIGINT) as REPRINT_COUNT", 
    	"CAST(ESTIMATED_TIME AS BIGINT) as ESTIMATED_TIME", 
    	"CAST(ESTIMATED_DISTANCE AS BIGINT) as ESTIMATED_DISTANCE", 
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    gu.overwriteDeltaPartition(Shortcut_to_WM_TASK_HDR_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_TASK_HDR_PRE is written to the target table - " + target_table_name)
