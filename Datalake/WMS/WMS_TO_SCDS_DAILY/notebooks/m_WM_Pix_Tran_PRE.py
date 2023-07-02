#Code converted on 2023-06-26 17:05:58
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



def m_WM_Pix_Tran_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Pix_Tran_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_PIX_TRAN_PRE"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_PIX_TRAN"
    prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + prev_run_dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    query = f"""SELECT
                    PIX_TRAN.TRAN_TYPE,
                    PIX_TRAN.TRAN_CODE,
                    PIX_TRAN.TRAN_NBR,
                    PIX_TRAN.PIX_SEQ_NBR,
                    PIX_TRAN.PROC_STAT_CODE,
                    PIX_TRAN.WHSE,
                    PIX_TRAN.CASE_NBR,
                    PIX_TRAN.SEASON,
                    PIX_TRAN.SEASON_YR,
                    PIX_TRAN.STYLE,
                    PIX_TRAN.STYLE_SFX,
                    PIX_TRAN.COLOR,
                    PIX_TRAN.COLOR_SFX,
                    PIX_TRAN.SEC_DIM,
                    PIX_TRAN.QUAL,
                    PIX_TRAN.SIZE_DESC,
                    PIX_TRAN.SIZE_RANGE_CODE,
                    PIX_TRAN.SIZE_REL_POSN_IN_TABLE,
                    PIX_TRAN.INVN_TYPE,
                    PIX_TRAN.PROD_STAT,
                    PIX_TRAN.BATCH_NBR,
                    PIX_TRAN.SKU_ATTR_1,
                    PIX_TRAN.SKU_ATTR_2,
                    PIX_TRAN.SKU_ATTR_3,
                    PIX_TRAN.SKU_ATTR_4,
                    PIX_TRAN.SKU_ATTR_5,
                    PIX_TRAN.CNTRY_OF_ORGN,
                    PIX_TRAN.INVN_ADJMT_QTY,
                    PIX_TRAN.INVN_ADJMT_TYPE,
                    PIX_TRAN.WT_ADJMT_QTY,
                    PIX_TRAN.WT_ADJMT_TYPE,
                    PIX_TRAN.UOM,
                    PIX_TRAN.REF_WHSE,
                    PIX_TRAN.RSN_CODE,
                    PIX_TRAN.RCPT_VARI,
                    PIX_TRAN.RCPT_CMPL,
                    PIX_TRAN.CASES_SHPD,
                    PIX_TRAN.UNITS_SHPD,
                    PIX_TRAN.CASES_RCVD,
                    PIX_TRAN.UNITS_RCVD,
                    PIX_TRAN.ACTN_CODE,
                    PIX_TRAN.CUSTOM_REF,
                    PIX_TRAN.DATE_PROC,
                    PIX_TRAN.SYS_USER_ID,
                    PIX_TRAN.ERROR_CMNT,
                    PIX_TRAN.REF_CODE_ID_1,
                    PIX_TRAN.REF_FIELD_1,
                    PIX_TRAN.REF_CODE_ID_2,
                    PIX_TRAN.REF_FIELD_2,
                    PIX_TRAN.REF_CODE_ID_3,
                    PIX_TRAN.REF_FIELD_3,
                    PIX_TRAN.REF_CODE_ID_4,
                    PIX_TRAN.REF_FIELD_4,
                    PIX_TRAN.REF_CODE_ID_5,
                    PIX_TRAN.REF_FIELD_5,
                    PIX_TRAN.REF_CODE_ID_6,
                    PIX_TRAN.REF_FIELD_6,
                    PIX_TRAN.REF_CODE_ID_7,
                    PIX_TRAN.REF_FIELD_7,
                    PIX_TRAN.REF_CODE_ID_8,
                    PIX_TRAN.REF_FIELD_8,
                    PIX_TRAN.REF_CODE_ID_9,
                    PIX_TRAN.REF_FIELD_9,
                    PIX_TRAN.REF_CODE_ID_10,
                    PIX_TRAN.REF_FIELD_10,
                    PIX_TRAN.XML_GROUP_ID,
                    PIX_TRAN.CREATE_DATE_TIME,
                    PIX_TRAN.MOD_DATE_TIME,
                    PIX_TRAN.USER_ID,
                    PIX_TRAN.ITEM_ID,
                    PIX_TRAN.TC_COMPANY_ID,
                    PIX_TRAN.PIX_TRAN_ID,
                    PIX_TRAN.WM_VERSION_ID,
                    PIX_TRAN.FACILITY_ID,
                    PIX_TRAN.COMPANY_CODE,
                    PIX_TRAN.ITEM_NAME,
                    PIX_TRAN.ESIGN_USER_NAME
                FROM PIX_TRAN
                WHERE {Initial_Load} (trunc(PIX_TRAN.CREATE_DATE_TIME) >= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (trunc(PIX_TRAN.MOD_DATE_TIME) >=  trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS'))-1)"""

    SQ_Shortcut_to_PIX_TRAN = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_PIX_TRAN is executed and data is loaded using jdbc")


    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 79
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_PIX_TRAN_temp = SQ_Shortcut_to_PIX_TRAN.toDF(*["SQ_Shortcut_to_PIX_TRAN___" + col for col in SQ_Shortcut_to_PIX_TRAN.columns])
    
    EXPTRANS = SQ_Shortcut_to_PIX_TRAN_temp.selectExpr( \
    	"SQ_Shortcut_to_PIX_TRAN___sys_row_id as sys_row_id", \
    	f"{DC_NBR} as DC_NBR_EXP", \
    	"SQ_Shortcut_to_PIX_TRAN___TRAN_TYPE as TRAN_TYPE", \
    	"SQ_Shortcut_to_PIX_TRAN___TRAN_CODE as TRAN_CODE", \
    	"SQ_Shortcut_to_PIX_TRAN___TRAN_NBR as TRAN_NBR", \
    	"SQ_Shortcut_to_PIX_TRAN___PIX_SEQ_NBR as PIX_SEQ_NBR", \
    	"SQ_Shortcut_to_PIX_TRAN___PROC_STAT_CODE as PROC_STAT_CODE", \
    	"SQ_Shortcut_to_PIX_TRAN___WHSE as WHSE", \
    	"SQ_Shortcut_to_PIX_TRAN___CASE_NBR as CASE_NBR", \
    	"SQ_Shortcut_to_PIX_TRAN___SEASON as SEASON", \
    	"SQ_Shortcut_to_PIX_TRAN___SEASON_YR as SEASON_YR", \
    	"SQ_Shortcut_to_PIX_TRAN___STYLE as STYLE", \
    	"SQ_Shortcut_to_PIX_TRAN___STYLE_SFX as STYLE_SFX", \
    	"SQ_Shortcut_to_PIX_TRAN___COLOR as COLOR", \
    	"SQ_Shortcut_to_PIX_TRAN___COLOR_SFX as COLOR_SFX", \
    	"SQ_Shortcut_to_PIX_TRAN___SEC_DIM as SEC_DIM", \
    	"SQ_Shortcut_to_PIX_TRAN___QUAL as QUAL", \
    	"SQ_Shortcut_to_PIX_TRAN___SIZE_DESC as SIZE_DESC", \
    	"SQ_Shortcut_to_PIX_TRAN___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
    	"SQ_Shortcut_to_PIX_TRAN___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
    	"SQ_Shortcut_to_PIX_TRAN___INVN_TYPE as INVN_TYPE", \
    	"SQ_Shortcut_to_PIX_TRAN___PROD_STAT as PROD_STAT", \
    	"SQ_Shortcut_to_PIX_TRAN___BATCH_NBR as BATCH_NBR", \
    	"SQ_Shortcut_to_PIX_TRAN___SKU_ATTR_1 as SKU_ATTR_1", \
    	"SQ_Shortcut_to_PIX_TRAN___SKU_ATTR_2 as SKU_ATTR_2", \
    	"SQ_Shortcut_to_PIX_TRAN___SKU_ATTR_3 as SKU_ATTR_3", \
    	"SQ_Shortcut_to_PIX_TRAN___SKU_ATTR_4 as SKU_ATTR_4", \
    	"SQ_Shortcut_to_PIX_TRAN___SKU_ATTR_5 as SKU_ATTR_5", \
    	"SQ_Shortcut_to_PIX_TRAN___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
    	"SQ_Shortcut_to_PIX_TRAN___INVN_ADJMT_QTY as INVN_ADJMT_QTY", \
    	"SQ_Shortcut_to_PIX_TRAN___INVN_ADJMT_TYPE as INVN_ADJMT_TYPE", \
    	"SQ_Shortcut_to_PIX_TRAN___WT_ADJMT_QTY as WT_ADJMT_QTY", \
    	"SQ_Shortcut_to_PIX_TRAN___WT_ADJMT_TYPE as WT_ADJMT_TYPE", \
    	"SQ_Shortcut_to_PIX_TRAN___UOM as UOM", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_WHSE as REF_WHSE", \
    	"SQ_Shortcut_to_PIX_TRAN___RSN_CODE as RSN_CODE", \
    	"SQ_Shortcut_to_PIX_TRAN___RCPT_VARI as RCPT_VARI", \
    	"SQ_Shortcut_to_PIX_TRAN___RCPT_CMPL as RCPT_CMPL", \
    	"SQ_Shortcut_to_PIX_TRAN___CASES_SHPD as CASES_SHPD", \
    	"SQ_Shortcut_to_PIX_TRAN___UNITS_SHPD as UNITS_SHPD", \
    	"SQ_Shortcut_to_PIX_TRAN___CASES_RCVD as CASES_RCVD", \
    	"SQ_Shortcut_to_PIX_TRAN___UNITS_RCVD as UNITS_RCVD", \
    	"SQ_Shortcut_to_PIX_TRAN___ACTN_CODE as ACTN_CODE", \
    	"SQ_Shortcut_to_PIX_TRAN___CUSTOM_REF as CUSTOM_REF", \
    	"SQ_Shortcut_to_PIX_TRAN___DATE_PROC as DATE_PROC", \
    	"SQ_Shortcut_to_PIX_TRAN___SYS_USER_ID as SYS_USER_ID", \
    	"SQ_Shortcut_to_PIX_TRAN___ERROR_CMNT as ERROR_CMNT", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_CODE_ID_1 as REF_CODE_ID_1", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_FIELD_1 as REF_FIELD_1", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_CODE_ID_2 as REF_CODE_ID_2", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_FIELD_2 as REF_FIELD_2", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_CODE_ID_3 as REF_CODE_ID_3", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_FIELD_3 as REF_FIELD_3", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_CODE_ID_4 as REF_CODE_ID_4", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_FIELD_4 as REF_FIELD_4", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_CODE_ID_5 as REF_CODE_ID_5", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_FIELD_5 as REF_FIELD_5", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_CODE_ID_6 as REF_CODE_ID_6", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_FIELD_6 as REF_FIELD_6", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_CODE_ID_7 as REF_CODE_ID_7", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_FIELD_7 as REF_FIELD_7", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_CODE_ID_8 as REF_CODE_ID_8", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_FIELD_8 as REF_FIELD_8", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_CODE_ID_9 as REF_CODE_ID_9", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_FIELD_9 as REF_FIELD_9", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_CODE_ID_10 as REF_CODE_ID_10", \
    	"SQ_Shortcut_to_PIX_TRAN___REF_FIELD_10 as REF_FIELD_10", \
    	"SQ_Shortcut_to_PIX_TRAN___XML_GROUP_ID as XML_GROUP_ID", \
    	"SQ_Shortcut_to_PIX_TRAN___CREATE_DATE_TIME as CREATE_DATE_TIME", \
    	"SQ_Shortcut_to_PIX_TRAN___MOD_DATE_TIME as MOD_DATE_TIME", \
    	"SQ_Shortcut_to_PIX_TRAN___USER_ID as USER_ID", \
    	"SQ_Shortcut_to_PIX_TRAN___ITEM_ID as ITEM_ID", \
    	"SQ_Shortcut_to_PIX_TRAN___TC_COMPANY_ID as TC_COMPANY_ID", \
    	"SQ_Shortcut_to_PIX_TRAN___PIX_TRAN_ID as PIX_TRAN_ID", \
    	"SQ_Shortcut_to_PIX_TRAN___WM_VERSION_ID as WM_VERSION_ID", \
    	"SQ_Shortcut_to_PIX_TRAN___FACILITY_ID as FACILITY_ID", \
    	"SQ_Shortcut_to_PIX_TRAN___COMPANY_CODE as COMPANY_CODE", \
    	"SQ_Shortcut_to_PIX_TRAN___ITEM_NAME as ITEM_NAME", \
    	"SQ_Shortcut_to_PIX_TRAN___ESIGN_USER_NAME as ESIGN_USER_NAME", \
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_PIX_TRAN_PRE, type TARGET 
    # COLUMN COUNT: 79
    
    
    Shortcut_to_WM_PIX_TRAN_PRE = EXPTRANS.selectExpr( \
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
    	"CAST(PIX_TRAN_ID AS BIGINT) as PIX_TRAN_ID", \
    	"CAST(TRAN_TYPE AS STRING) as TRAN_TYPE", \
    	"CAST(TRAN_CODE AS STRING) as TRAN_CODE", \
    	"CAST(TRAN_NBR AS BIGINT) as TRAN_NBR", \
    	"CAST(PIX_SEQ_NBR AS BIGINT) as PIX_SEQ_NBR", \
    	"CAST(PROC_STAT_CODE AS BIGINT) as PROC_STAT_CODE", \
    	"CAST(WHSE AS STRING) as WHSE", \
    	"CAST(CASE_NBR AS STRING) as CASE_NBR", \
    	"CAST(SEASON AS STRING) as SEASON", \
    	"CAST(SEASON_YR AS STRING) as SEASON_YR", \
    	"CAST(STYLE AS STRING) as STYLE", \
    	"CAST(STYLE_SFX AS STRING) as STYLE_SFX", \
    	"CAST(COLOR AS STRING) as COLOR", \
    	"CAST(COLOR_SFX AS STRING) as COLOR_SFX", \
    	"CAST(SEC_DIM AS STRING) as SEC_DIM", \
    	"CAST(QUAL AS STRING) as QUAL", \
    	"CAST(SIZE_DESC AS STRING) as SIZE_DESC", \
    	"CAST(SIZE_RANGE_CODE AS STRING) as SIZE_RANGE_CODE", \
    	"CAST(SIZE_REL_POSN_IN_TABLE AS STRING) as SIZE_REL_POSN_IN_TABLE", \
    	"CAST(INVN_TYPE AS STRING) as INVN_TYPE", \
    	"CAST(PROD_STAT AS STRING) as PROD_STAT", \
    	"CAST(BATCH_NBR AS STRING) as BATCH_NBR", \
    	"CAST(SKU_ATTR_1 AS STRING) as SKU_ATTR_1", \
    	"CAST(SKU_ATTR_2 AS STRING) as SKU_ATTR_2", \
    	"CAST(SKU_ATTR_3 AS STRING) as SKU_ATTR_3", \
    	"CAST(SKU_ATTR_4 AS STRING) as SKU_ATTR_4", \
    	"CAST(SKU_ATTR_5 AS STRING) as SKU_ATTR_5", \
    	"CAST(CNTRY_OF_ORGN AS STRING) as CNTRY_OF_ORGN", \
    	"CAST(INVN_ADJMT_QTY AS BIGINT) as INVN_ADJMT_QTY", \
    	"CAST(INVN_ADJMT_TYPE AS STRING) as INVN_ADJMT_TYPE", \
    	"CAST(WT_ADJMT_QTY AS BIGINT) as WT_ADJMT_QTY", \
    	"CAST(WT_ADJMT_TYPE AS STRING) as WT_ADJMT_TYPE", \
    	"CAST(UOM AS STRING) as UOM", \
    	"CAST(REF_WHSE AS STRING) as REF_WHSE", \
    	"CAST(RSN_CODE AS STRING) as RSN_CODE", \
    	"CAST(RCPT_VARI AS STRING) as RCPT_VARI", \
    	"CAST(RCPT_CMPL AS STRING) as RCPT_CMPL", \
    	"CAST(CASES_SHPD AS BIGINT) as CASES_SHPD", \
    	"CAST(UNITS_SHPD AS BIGINT) as UNITS_SHPD", \
    	"CAST(CASES_RCVD AS BIGINT) as CASES_RCVD", \
    	"CAST(UNITS_RCVD AS BIGINT) as UNITS_RCVD", \
    	"CAST(ACTN_CODE AS STRING) as ACTN_CODE", \
    	"CAST(CUSTOM_REF AS STRING) as CUSTOM_REF", \
    	"CAST(DATE_PROC AS TIMESTAMP) as DATE_PROC", \
    	"CAST(SYS_USER_ID AS STRING) as SYS_USER_ID", \
    	"CAST(ERROR_CMNT AS STRING) as ERROR_CMNT", \
    	"CAST(REF_CODE_ID_1 AS STRING) as REF_CODE_ID_1", \
    	"CAST(REF_FIELD_1 AS STRING) as REF_FIELD_1", \
    	"CAST(REF_CODE_ID_2 AS STRING) as REF_CODE_ID_2", \
    	"CAST(REF_FIELD_2 AS STRING) as REF_FIELD_2", \
    	"CAST(REF_CODE_ID_3 AS STRING) as REF_CODE_ID_3", \
    	"CAST(REF_FIELD_3 AS STRING) as REF_FIELD_3", \
    	"CAST(REF_CODE_ID_4 AS STRING) as REF_CODE_ID_4", \
    	"CAST(REF_FIELD_4 AS STRING) as REF_FIELD_4", \
    	"CAST(REF_CODE_ID_5 AS STRING) as REF_CODE_ID_5", \
    	"CAST(REF_FIELD_5 AS STRING) as REF_FIELD_5", \
    	"CAST(REF_CODE_ID_6 AS STRING) as REF_CODE_ID_6", \
    	"CAST(REF_FIELD_6 AS STRING) as REF_FIELD_6", \
    	"CAST(REF_CODE_ID_7 AS STRING) as REF_CODE_ID_7", \
    	"CAST(REF_FIELD_7 AS STRING) as REF_FIELD_7", \
    	"CAST(REF_CODE_ID_8 AS STRING) as REF_CODE_ID_8", \
    	"CAST(REF_FIELD_8 AS STRING) as REF_FIELD_8", \
    	"CAST(REF_CODE_ID_9 AS STRING) as REF_CODE_ID_9", \
    	"CAST(REF_FIELD_9 AS STRING) as REF_FIELD_9", \
    	"CAST(REF_CODE_ID_10 AS STRING) as REF_CODE_ID_10", \
    	"CAST(REF_FIELD_10 AS STRING) as REF_FIELD_10", \
    	"CAST(XML_GROUP_ID AS STRING) as XML_GROUP_ID", \
    	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", \
    	"CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", \
    	"CAST(USER_ID AS STRING) as USER_ID", \
    	"CAST(ITEM_ID AS BIGINT) as ITEM_ID", \
    	"CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", \
    	"CAST(WM_VERSION_ID AS BIGINT) as WM_VERSION_ID", \
    	"CAST(FACILITY_ID AS BIGINT) as FACILITY_ID", \
    	"CAST(COMPANY_CODE AS STRING) as COMPANY_CODE", \
    	"CAST(ITEM_NAME AS STRING) as ITEM_NAME", \
    	"CAST(ESIGN_USER_NAME AS STRING) as ESIGN_USER_NAME", \
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    gu.overwriteDeltaPartition(Shortcut_to_WM_PIX_TRAN_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_PIX_TRAN_PRE is written to the target table - " + target_table_name)
