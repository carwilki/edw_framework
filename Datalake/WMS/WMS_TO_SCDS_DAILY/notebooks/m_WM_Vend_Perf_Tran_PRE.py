#Code converted on 2023-06-22 21:04:13
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



def m_WM_Vend_Perf_Tran_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Vend_Perf_Tran_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_VEND_PERF_TRAN_PRE"
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
                    VEND_PERF_TRAN.VEND_PERF_TRAN_ID,
                    VEND_PERF_TRAN.PERF_CODE,
                    VEND_PERF_TRAN.WHSE,
                    VEND_PERF_TRAN.SHPMT_NBR,
                    VEND_PERF_TRAN.PO_NBR,
                    VEND_PERF_TRAN.CASE_NBR,
                    VEND_PERF_TRAN.UOM,
                    VEND_PERF_TRAN.QTY,
                    VEND_PERF_TRAN.SAMS,
                    VEND_PERF_TRAN.STAT_CODE,
                    VEND_PERF_TRAN.CREATE_DATE_TIME,
                    VEND_PERF_TRAN.MOD_DATE_TIME,
                    VEND_PERF_TRAN.USER_ID,
                    VEND_PERF_TRAN.CHRG_AMT,
                    VEND_PERF_TRAN.BILL_FLAG,
                    VEND_PERF_TRAN.LOAD_NBR,
                    VEND_PERF_TRAN.ILM_APPT_NBR,
                    VEND_PERF_TRAN.VENDOR_MASTER_ID,
                    VEND_PERF_TRAN.CD_MASTER_ID,
                    VEND_PERF_TRAN.CMNT,
                    VEND_PERF_TRAN.CREATED_BY_USER_ID,
                    VEND_PERF_TRAN.WM_VERSION_ID,
                    VEND_PERF_TRAN.PO_HDR_ID,
                    VEND_PERF_TRAN.ASN_HDR_ID,
                    VEND_PERF_TRAN.CASE_HDR_ID,
                    VEND_PERF_TRAN.ITEM_ID
                FROM {source_schema}.VEND_PERF_TRAN
                WHERE  (TRUNC(VEND_PERF_TRAN.CREATE_DATE_TIME)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (TRUNC(VEND_PERF_TRAN.MOD_DATE_TIME)>= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1)"""
    

    SQ_Shortcut_to_VEND_PERF_TRAN = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_VEND_PERF_TRAN is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 28
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_VEND_PERF_TRAN_temp = SQ_Shortcut_to_VEND_PERF_TRAN.toDF(*["SQ_Shortcut_to_VEND_PERF_TRAN___" + col for col in SQ_Shortcut_to_VEND_PERF_TRAN.columns])
    
    EXPTRANS = SQ_Shortcut_to_VEND_PERF_TRAN_temp.selectExpr( 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___VEND_PERF_TRAN_ID as VEND_PERF_TRAN_ID", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___PERF_CODE as PERF_CODE", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___WHSE as WHSE", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___SHPMT_NBR as SHPMT_NBR", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___PO_NBR as PO_NBR", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___CASE_NBR as CASE_NBR", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___UOM as UOM", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___QTY as QTY", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___SAMS as SAMS", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___STAT_CODE as STAT_CODE", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___CREATE_DATE_TIME as CREATE_DATE_TIME", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___MOD_DATE_TIME as MOD_DATE_TIME", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___USER_ID as USER_ID", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___CHRG_AMT as CHRG_AMT", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___BILL_FLAG as BILL_FLAG", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___LOAD_NBR as LOAD_NBR", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___ILM_APPT_NBR as ILM_APPT_NBR", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___VENDOR_MASTER_ID as VENDOR_MASTER_ID", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___CD_MASTER_ID as CD_MASTER_ID", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___CMNT as CMNT", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___CREATED_BY_USER_ID as CREATED_BY_USER_ID", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___WM_VERSION_ID as WM_VERSION_ID", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___PO_HDR_ID as PO_HDR_ID", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___ASN_HDR_ID as ASN_HDR_ID", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___CASE_HDR_ID as CASE_HDR_ID", 
    	"SQ_Shortcut_to_VEND_PERF_TRAN___ITEM_ID as ITEM_ID", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_VEND_PERF_TRAN_PRE, type TARGET 
    # COLUMN COUNT: 28
    
    
    Shortcut_to_WM_VEND_PERF_TRAN_PRE = EXPTRANS.selectExpr( 
    	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
    	"CAST(VEND_PERF_TRAN_ID AS BIGINT) as VEND_PERF_TRAN_ID", 
    	"CAST(PERF_CODE AS STRING) as PERF_CODE", 
    	"CAST(WHSE AS STRING) as WHSE", 
    	"CAST(SHPMT_NBR AS STRING) as SHPMT_NBR", 
    	"CAST(PO_NBR AS STRING) as PO_NBR", 
    	"CAST(CASE_NBR AS STRING) as CASE_NBR", 
    	"CAST(UOM AS STRING) as UOM", 
    	"CAST(QTY AS BIGINT) as QTY", 
    	"CAST(SAMS AS BIGINT) as SAMS", 
    	"CAST(STAT_CODE AS BIGINT) as STAT_CODE", 
    	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
    	"CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
    	"CAST(USER_ID AS STRING) as USER_ID", 
    	"CAST(CHRG_AMT AS BIGINT) as CHRG_AMT", 
    	"CAST(BILL_FLAG AS STRING) as BILL_FLAG", 
    	"CAST(LOAD_NBR AS STRING) as LOAD_NBR", 
    	"CAST(ILM_APPT_NBR AS STRING) as ILM_APPT_NBR", 
    	"CAST(VENDOR_MASTER_ID AS BIGINT) as VENDOR_MASTER_ID", 
    	"CAST(CD_MASTER_ID AS BIGINT) as CD_MASTER_ID", 
    	"CAST(CMNT AS STRING) as CMNT", 
    	"CAST(CREATED_BY_USER_ID AS STRING) as CREATED_BY_USER_ID", 
    	"CAST(WM_VERSION_ID AS BIGINT) as WM_VERSION_ID", 
    	"CAST(PO_HDR_ID AS BIGINT) as PO_HDR_ID", 
    	"CAST(ASN_HDR_ID AS BIGINT) as ASN_HDR_ID", 
    	"CAST(CASE_HDR_ID AS BIGINT) as CASE_HDR_ID", 
    	"CAST(ITEM_ID AS BIGINT) as ITEM_ID", 
    	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_VEND_PERF_TRAN_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_VEND_PERF_TRAN_PRE is written to the target table - " + target_table_name)
