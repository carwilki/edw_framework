#Code converted on 2023-06-22 20:25:04
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



def m_WM_Rack_Type_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Rack_Type_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_RACK_TYPE_PRE"
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
                    RACK_TYPE.RACK_TYPE,
                    RACK_TYPE.WHSE_CODE,
                    RACK_TYPE.RT_NAME,
                    RACK_TYPE.RACK_TYPE_DESC,
                    RACK_TYPE.SEQUENCE,
                    RACK_TYPE.WT_LIMIT,
                    RACK_TYPE.HT,
                    RACK_TYPE.WIDTH,
                    RACK_TYPE.DEPTH,
                    RACK_TYPE.RACK_CLASS,
                    RACK_TYPE.FIX_LABELS,
                    RACK_TYPE.LABEL_WID,
                    RACK_TYPE.OVERLAP_DIST,
                    RACK_TYPE.MOVABLE_LABEL,
                    RACK_TYPE.HN_CONSIDER,
                    RACK_TYPE.CASE_HT_MAX,
                    RACK_TYPE.CASE_LEN_MAX,
                    RACK_TYPE.CASE_WID_MAX,
                    RACK_TYPE.CASE_WT_MAX,
                    RACK_TYPE.CASE_CUBE_MAX,
                    RACK_TYPE.INNER_HT_MAX,
                    RACK_TYPE.INNER_LEN_MAX,
                    RACK_TYPE.INNER_WID_MAX,
                    RACK_TYPE.INNER_WT_MAX,
                    RACK_TYPE.INNER_CUBE_MAX,
                    RACK_TYPE.EACH_HT_MAX,
                    RACK_TYPE.EACH_LEN_MAX,
                    RACK_TYPE.EACH_WID_MAX,
                    RACK_TYPE.EACH_WT_MAX,
                    RACK_TYPE.EACH_CUBE_MAX,
                    RACK_TYPE.PALLET_INVEN_MAX,
                    RACK_TYPE.PALLET_INVEN_MIN,
                    RACK_TYPE.CASE_INVEN_MAX,
                    RACK_TYPE.CASE_INVEN_MIN,
                    RACK_TYPE.CASE_MOVE_MAX,
                    RACK_TYPE.CASE_MOVE_MIN,
                    RACK_TYPE.CUBE_MOVE_MAX,
                    RACK_TYPE.CUBE_MOVE_MIN,
                    RACK_TYPE.CASE_HITS_MAX,
                    RACK_TYPE.CASE_HITS_MIN,
                    RACK_TYPE.CUBE_HITS_MAX,
                    RACK_TYPE.CUBE_HITS_MIN,
                    RACK_TYPE.CASE_HT_MIN,
                    RACK_TYPE.CASE_LEN_MIN,
                    RACK_TYPE.CASE_WID_MIN,
                    RACK_TYPE.CASE_WT_MIN,
                    RACK_TYPE.CASE_CUBE_MIN,
                    RACK_TYPE.INNER_HT_MIN,
                    RACK_TYPE.INNER_LEN_MIN,
                    RACK_TYPE.INNER_WID_MIN,
                    RACK_TYPE.INNER_WT_MIN,
                    RACK_TYPE.INNER_CUBE_MIN,
                    RACK_TYPE.EACH_HT_MIN,
                    RACK_TYPE.EACH_LEN_MIN,
                    RACK_TYPE.EACH_WID_MIN,
                    RACK_TYPE.EACH_WT_MIN,
                    RACK_TYPE.EACH_CUBE_MIN,
                    RACK_TYPE.CUBE_INVEN_MIN,
                    RACK_TYPE.CUBE_INVEN_MAX,
                    RACK_TYPE.WEEK_IN_SLOT,
                    RACK_TYPE.REF_LEVEL_ID,
                    RACK_TYPE.VISC_MIN,
                    RACK_TYPE.VISC_MAX,
                    RACK_TYPE.FILL_PERCENT_MIN,
                    RACK_TYPE.FILL_PERCENT_MAX,
                    RACK_TYPE.UPRIGHT_THICKNESS,
                    RACK_TYPE.RESERVED_1,
                    RACK_TYPE.RESERVED_2,
                    RACK_TYPE.RESERVED_3,
                    RACK_TYPE.RESERVED_4,
                    RACK_TYPE.CREATE_DATE_TIME,
                    RACK_TYPE.MOD_DATE_TIME,
                    RACK_TYPE.MOD_USER,
                    RACK_TYPE.FUNCTION_TYPE,
                    RACK_TYPE.OVERSTOCK_CONSIDER,
                    RACK_TYPE.USE_3D_SLOT,
                    RACK_TYPE.WEEKS_IN_SLOT_CONSTRAINT,
                    RACK_TYPE.NUM_OF_BAYS_AVAIL,
                    RACK_TYPE.PARENT_BT
                FROM {source_schema}.RACK_TYPE
                WHERE  (TRUNC(CREATE_DATE_TIME) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (TRUNC(MOD_DATE_TIME) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14)"""
    

    SQ_Shortcut_to_RACK_TYPE = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for spark.read.format('jdbc').option('url', connection_string).option( is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 81
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_RACK_TYPE_temp = SQ_Shortcut_to_RACK_TYPE.toDF(*["SQ_Shortcut_to_RACK_TYPE___" + col for col in SQ_Shortcut_to_RACK_TYPE.columns])
    
    EXPTRANS = SQ_Shortcut_to_RACK_TYPE_temp.selectExpr( 
    	"SQ_Shortcut_to_RACK_TYPE___sys_row_id as sys_row_id", 
    	"{DC_NBR} as DC_NBR_exp", 
    	"SQ_Shortcut_to_RACK_TYPE___RACK_TYPE as RACK_TYPE", 
    	"SQ_Shortcut_to_RACK_TYPE___WHSE_CODE as WHSE_CODE", 
    	"SQ_Shortcut_to_RACK_TYPE___RT_NAME as RT_NAME", 
    	"SQ_Shortcut_to_RACK_TYPE___RACK_TYPE_DESC as RACK_TYPE_DESC", 
    	"SQ_Shortcut_to_RACK_TYPE___SEQUENCE as SEQUENCE", 
    	"SQ_Shortcut_to_RACK_TYPE___WT_LIMIT as WT_LIMIT", 
    	"SQ_Shortcut_to_RACK_TYPE___HT as HT", 
    	"SQ_Shortcut_to_RACK_TYPE___WIDTH as WIDTH", 
    	"SQ_Shortcut_to_RACK_TYPE___DEPTH as DEPTH", 
    	"SQ_Shortcut_to_RACK_TYPE___RACK_CLASS as RACK_CLASS", 
    	"SQ_Shortcut_to_RACK_TYPE___FIX_LABELS as FIX_LABELS", 
    	"SQ_Shortcut_to_RACK_TYPE___LABEL_WID as LABEL_WID", 
    	"SQ_Shortcut_to_RACK_TYPE___OVERLAP_DIST as OVERLAP_DIST", 
    	"SQ_Shortcut_to_RACK_TYPE___MOVABLE_LABEL as MOVABLE_LABEL", 
    	"SQ_Shortcut_to_RACK_TYPE___HN_CONSIDER as HN_CONSIDER", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_HT_MAX as CASE_HT_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_LEN_MAX as CASE_LEN_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_WID_MAX as CASE_WID_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_WT_MAX as CASE_WT_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_CUBE_MAX as CASE_CUBE_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___INNER_HT_MAX as INNER_HT_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___INNER_LEN_MAX as INNER_LEN_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___INNER_WID_MAX as INNER_WID_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___INNER_WT_MAX as INNER_WT_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___INNER_CUBE_MAX as INNER_CUBE_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___EACH_HT_MAX as EACH_HT_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___EACH_LEN_MAX as EACH_LEN_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___EACH_WID_MAX as EACH_WID_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___EACH_WT_MAX as EACH_WT_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___EACH_CUBE_MAX as EACH_CUBE_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___PALLET_INVEN_MAX as PALLET_INVEN_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___PALLET_INVEN_MIN as PALLET_INVEN_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_INVEN_MAX as CASE_INVEN_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_INVEN_MIN as CASE_INVEN_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_MOVE_MAX as CASE_MOVE_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_MOVE_MIN as CASE_MOVE_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CUBE_MOVE_MAX as CUBE_MOVE_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___CUBE_MOVE_MIN as CUBE_MOVE_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_HITS_MAX as CASE_HITS_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_HITS_MIN as CASE_HITS_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CUBE_HITS_MAX as CUBE_HITS_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___CUBE_HITS_MIN as CUBE_HITS_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_HT_MIN as CASE_HT_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_LEN_MIN as CASE_LEN_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_WID_MIN as CASE_WID_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_WT_MIN as CASE_WT_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CASE_CUBE_MIN as CASE_CUBE_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___INNER_HT_MIN as INNER_HT_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___INNER_LEN_MIN as INNER_LEN_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___INNER_WID_MIN as INNER_WID_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___INNER_WT_MIN as INNER_WT_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___INNER_CUBE_MIN as INNER_CUBE_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___EACH_HT_MIN as EACH_HT_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___EACH_LEN_MIN as EACH_LEN_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___EACH_WID_MIN as EACH_WID_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___EACH_WT_MIN as EACH_WT_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___EACH_CUBE_MIN as EACH_CUBE_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CUBE_INVEN_MIN as CUBE_INVEN_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___CUBE_INVEN_MAX as CUBE_INVEN_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___WEEK_IN_SLOT as WEEK_IN_SLOT", 
    	"SQ_Shortcut_to_RACK_TYPE___REF_LEVEL_ID as REF_LEVEL_ID", 
    	"SQ_Shortcut_to_RACK_TYPE___VISC_MIN as VISC_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___VISC_MAX as VISC_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___FILL_PERCENT_MIN as FILL_PERCENT_MIN", 
    	"SQ_Shortcut_to_RACK_TYPE___FILL_PERCENT_MAX as FILL_PERCENT_MAX", 
    	"SQ_Shortcut_to_RACK_TYPE___UPRIGHT_THICKNESS as UPRIGHT_THICKNESS", 
    	"SQ_Shortcut_to_RACK_TYPE___RESERVED_1 as RESERVED_1", 
    	"SQ_Shortcut_to_RACK_TYPE___RESERVED_2 as RESERVED_2", 
    	"SQ_Shortcut_to_RACK_TYPE___RESERVED_3 as RESERVED_3", 
    	"SQ_Shortcut_to_RACK_TYPE___RESERVED_4 as RESERVED_4", 
    	"SQ_Shortcut_to_RACK_TYPE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
    	"SQ_Shortcut_to_RACK_TYPE___MOD_DATE_TIME as MOD_DATE_TIME", 
    	"SQ_Shortcut_to_RACK_TYPE___MOD_USER as MOD_USER", 
    	"SQ_Shortcut_to_RACK_TYPE___FUNCTION_TYPE as FUNCTION_TYPE", 
    	"SQ_Shortcut_to_RACK_TYPE___OVERSTOCK_CONSIDER as OVERSTOCK_CONSIDER", 
    	"SQ_Shortcut_to_RACK_TYPE___USE_3D_SLOT as USE_3D_SLOT", 
    	"SQ_Shortcut_to_RACK_TYPE___WEEKS_IN_SLOT_CONSTRAINT as WEEKS_IN_SLOT_CONSTRAINT", 
    	"SQ_Shortcut_to_RACK_TYPE___NUM_OF_BAYS_AVAIL as NUM_OF_BAYS_AVAIL", 
    	"SQ_Shortcut_to_RACK_TYPE___PARENT_BT as PARENT_BT", 
    	"CURRENT_TIMESTAMP() as LOADTSTMP" 
    )
    
    
    # Processing node Shortcut_to_WM_RACK_TYPE_PRE, type TARGET 
    # COLUMN COUNT: 81
    
    
    Shortcut_to_WM_RACK_TYPE_PRE = EXPTRANS.selectExpr( 
    	"CAST(DC_NBR_exp AS BIGINT) as DC_NBR", 
    	"CAST(RACK_TYPE AS BIGINT) as RACK_TYPE", 
    	"CAST(WHSE_CODE AS STRING) as WHSE_CODE", 
    	"CAST(RT_NAME AS STRING) as RT_NAME", 
    	"CAST(RACK_TYPE_DESC AS STRING) as RACK_TYPE_DESC", 
    	"CAST(SEQUENCE AS BIGINT) as SEQUENCE", 
    	"CAST(WT_LIMIT AS BIGINT) as WT_LIMIT", 
    	"CAST(HT AS BIGINT) as HT", 
    	"CAST(WIDTH AS BIGINT) as WIDTH", 
    	"CAST(DEPTH AS BIGINT) as DEPTH", 
    	"CAST(RACK_CLASS AS BIGINT) as RACK_CLASS", 
    	"CAST(FIX_LABELS AS BIGINT) as FIX_LABELS", 
    	"CAST(LABEL_WID AS BIGINT) as LABEL_WID", 
    	"CAST(OVERLAP_DIST AS BIGINT) as OVERLAP_DIST", 
    	"CAST(MOVABLE_LABEL AS BIGINT) as MOVABLE_LABEL", 
    	"CAST(HN_CONSIDER AS BIGINT) as HN_CONSIDER", 
    	"CAST(CASE_HT_MAX AS BIGINT) as CASE_HT_MAX", 
    	"CAST(CASE_LEN_MAX AS BIGINT) as CASE_LEN_MAX", 
    	"CAST(CASE_WID_MAX AS BIGINT) as CASE_WID_MAX", 
    	"CAST(CASE_WT_MAX AS BIGINT) as CASE_WT_MAX", 
    	"CAST(CASE_CUBE_MAX AS BIGINT) as CASE_CUBE_MAX", 
    	"CAST(INNER_HT_MAX AS BIGINT) as INNER_HT_MAX", 
    	"CAST(INNER_LEN_MAX AS BIGINT) as INNER_LEN_MAX", 
    	"CAST(INNER_WID_MAX AS BIGINT) as INNER_WID_MAX", 
    	"CAST(INNER_WT_MAX AS BIGINT) as INNER_WT_MAX", 
    	"CAST(INNER_CUBE_MAX AS BIGINT) as INNER_CUBE_MAX", 
    	"CAST(EACH_HT_MAX AS BIGINT) as EACH_HT_MAX", 
    	"CAST(EACH_LEN_MAX AS BIGINT) as EACH_LEN_MAX", 
    	"CAST(EACH_WID_MAX AS BIGINT) as EACH_WID_MAX", 
    	"CAST(EACH_WT_MAX AS BIGINT) as EACH_WT_MAX", 
    	"CAST(EACH_CUBE_MAX AS BIGINT) as EACH_CUBE_MAX", 
    	"CAST(PALLET_INVEN_MAX AS BIGINT) as PALLET_INVEN_MAX", 
    	"CAST(PALLET_INVEN_MIN AS BIGINT) as PALLET_INVEN_MIN", 
    	"CAST(CASE_INVEN_MAX AS BIGINT) as CASE_INVEN_MAX", 
    	"CAST(CASE_INVEN_MIN AS BIGINT) as CASE_INVEN_MIN", 
    	"CAST(CASE_MOVE_MAX AS BIGINT) as CASE_MOVE_MAX", 
    	"CAST(CASE_MOVE_MIN AS BIGINT) as CASE_MOVE_MIN", 
    	"CAST(CUBE_MOVE_MAX AS BIGINT) as CUBE_MOVE_MAX", 
    	"CAST(CUBE_MOVE_MIN AS BIGINT) as CUBE_MOVE_MIN", 
    	"CAST(CASE_HITS_MAX AS BIGINT) as CASE_HITS_MAX", 
    	"CAST(CASE_HITS_MIN AS BIGINT) as CASE_HITS_MIN", 
    	"CAST(CUBE_HITS_MAX AS BIGINT) as CUBE_HITS_MAX", 
    	"CAST(CUBE_HITS_MIN AS BIGINT) as CUBE_HITS_MIN", 
    	"CAST(CASE_HT_MIN AS BIGINT) as CASE_HT_MIN", 
    	"CAST(CASE_LEN_MIN AS BIGINT) as CASE_LEN_MIN", 
    	"CAST(CASE_WID_MIN AS BIGINT) as CASE_WID_MIN", 
    	"CAST(CASE_WT_MIN AS BIGINT) as CASE_WT_MIN", 
    	"CAST(CASE_CUBE_MIN AS BIGINT) as CASE_CUBE_MIN", 
    	"CAST(INNER_HT_MIN AS BIGINT) as INNER_HT_MIN", 
    	"CAST(INNER_LEN_MIN AS BIGINT) as INNER_LEN_MIN", 
    	"CAST(INNER_WID_MIN AS BIGINT) as INNER_WID_MIN", 
    	"CAST(INNER_WT_MIN AS BIGINT) as INNER_WT_MIN", 
    	"CAST(INNER_CUBE_MIN AS BIGINT) as INNER_CUBE_MIN", 
    	"CAST(EACH_HT_MIN AS BIGINT) as EACH_HT_MIN", 
    	"CAST(EACH_LEN_MIN AS BIGINT) as EACH_LEN_MIN", 
    	"CAST(EACH_WID_MIN AS BIGINT) as EACH_WID_MIN", 
    	"CAST(EACH_WT_MIN AS BIGINT) as EACH_WT_MIN", 
    	"CAST(EACH_CUBE_MIN AS BIGINT) as EACH_CUBE_MIN", 
    	"CAST(CUBE_INVEN_MIN AS BIGINT) as CUBE_INVEN_MIN", 
    	"CAST(CUBE_INVEN_MAX AS BIGINT) as CUBE_INVEN_MAX", 
    	"CAST(WEEK_IN_SLOT AS BIGINT) as WEEK_IN_SLOT", 
    	"CAST(REF_LEVEL_ID AS BIGINT) as REF_LEVEL_ID", 
    	"CAST(VISC_MIN AS BIGINT) as VISC_MIN", 
    	"CAST(VISC_MAX AS BIGINT) as VISC_MAX", 
    	"CAST(FILL_PERCENT_MIN AS BIGINT) as FILL_PERCENT_MIN", 
    	"CAST(FILL_PERCENT_MAX AS BIGINT) as FILL_PERCENT_MAX", 
    	"CAST(UPRIGHT_THICKNESS AS BIGINT) as UPRIGHT_THICKNESS", 
    	"CAST(RESERVED_1 AS STRING) as RESERVED_1", 
    	"CAST(RESERVED_2 AS STRING) as RESERVED_2", 
    	"CAST(RESERVED_3 AS BIGINT) as RESERVED_3", 
    	"CAST(RESERVED_4 AS BIGINT) as RESERVED_4", 
    	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
    	"CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
    	"CAST(MOD_USER AS STRING) as MOD_USER", 
    	"CAST(FUNCTION_TYPE AS BIGINT) as FUNCTION_TYPE", 
    	"CAST(OVERSTOCK_CONSIDER AS BIGINT) as OVERSTOCK_CONSIDER", 
    	"CAST(USE_3D_SLOT AS BIGINT) as USE_3D_SLOT", 
    	"CAST(WEEKS_IN_SLOT_CONSTRAINT AS BIGINT) as WEEKS_IN_SLOT_CONSTRAINT", 
    	"CAST(NUM_OF_BAYS_AVAIL AS BIGINT) as NUM_OF_BAYS_AVAIL", 
    	"CAST(PARENT_BT AS BIGINT) as PARENT_BT", 
    	"CAST(LOADTSTMP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_RACK_TYPE_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_RACK_TYPE_PRE is written to the target table - " + target_table_name)
