#Code converted on 2023-06-22 20:59:54
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



def m_WM_Slot_Item_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Slot_Item_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_SLOT_ITEM_PRE"
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
                    SLOT_ITEM.SLOTITEM_ID,
                    SLOT_ITEM.SLOT_ID,
                    SLOT_ITEM.SKU_ID,
                    SLOT_ITEM.SLOT_UNIT,
                    SLOT_ITEM.SHIP_UNIT,
                    SLOT_ITEM.MAX_LANES,
                    SLOT_ITEM.DEEPS,
                    SLOT_ITEM.SLOT_LOCKED,
                    SLOT_ITEM.PRIMARY_MOVE,
                    SLOT_ITEM.SLOT_WIDTH,
                    SLOT_ITEM.EST_HITS,
                    SLOT_ITEM.EST_INVENTORY,
                    SLOT_ITEM.EST_MOVEMENT,
                    SLOT_ITEM.HIST_MATCH,
                    SLOT_ITEM.LAST_CHANGE,
                    SLOT_ITEM.BIN_UNIT,
                    SLOT_ITEM.PALLETE_PATTERN,
                    SLOT_ITEM.SCORE,
                    SLOT_ITEM.CUR_ORIENTATION,
                    SLOT_ITEM.IGN_FOR_RESLOT,
                    SLOT_ITEM.REC_LANES,
                    SLOT_ITEM.REC_STACKING,
                    SLOT_ITEM.PALLET_MOV,
                    SLOT_ITEM.CASE_MOV,
                    SLOT_ITEM.INNER_MOV,
                    SLOT_ITEM.EACH_MOV,
                    SLOT_ITEM.BIN_MOV,
                    SLOT_ITEM.PALLET_INVEN,
                    SLOT_ITEM.CASE_INVEN,
                    SLOT_ITEM.INNER_INVEN,
                    SLOT_ITEM.EACH_INVEN,
                    SLOT_ITEM.BIN_INVEN,
                    SLOT_ITEM.CALC_HITS,
                    SLOT_ITEM.CURRENT_BIN,
                    SLOT_ITEM.CURRENT_PALLET,
                    SLOT_ITEM.OPT_PALLET_PATTERN,
                    SLOT_ITEM.ALLOW_EXPAND,
                    SLOT_ITEM.PALLET_HI,
                    SLOT_ITEM.NEEDED_RACK_TYPE,
                    SLOT_ITEM.LEGAL_FIT_REASON,
                    SLOT_ITEM.ITEM_COST,
                    SLOT_ITEM.USER_DEFINED,
                    SLOT_ITEM.LEGAL_FIT,
                    SLOT_ITEM.SCORE_DIRTY,
                    SLOT_ITEM.USE_ESTIMATED_HIST,
                    SLOT_ITEM.OPT_FLUID_VOL,
                    SLOT_ITEM.CALC_VISC,
                    SLOT_ITEM.EST_VISC,
                    SLOT_ITEM.SESSION_ID,
                    SLOT_ITEM.RANK,
                    SLOT_ITEM.INFO1,
                    SLOT_ITEM.INFO2,
                    SLOT_ITEM.INFO3,
                    SLOT_ITEM.INFO4,
                    SLOT_ITEM.INFO5,
                    SLOT_ITEM.INFO6,
                    SLOT_ITEM.RESERVED_1,
                    SLOT_ITEM.RESERVED_2,
                    SLOT_ITEM.RESERVED_3,
                    SLOT_ITEM.RESERVED_4,
                    SLOT_ITEM.CREATE_DATE_TIME,
                    SLOT_ITEM.MOD_DATE_TIME,
                    SLOT_ITEM.MOD_USER,
                    SLOT_ITEM.SI_NUM_1,
                    SLOT_ITEM.SI_NUM_2,
                    SLOT_ITEM.SI_NUM_3,
                    SLOT_ITEM.SI_NUM_4,
                    SLOT_ITEM.SI_NUM_5,
                    SLOT_ITEM.SI_NUM_6,
                    SLOT_ITEM.SLOT_UNIT_WEIGHT,
                    SLOT_ITEM.TOTAL_ITEM_WT,
                    SLOT_ITEM.BORROWING_OBJECT,
                    SLOT_ITEM.BORROWING_SPECIFIC,
                    SLOT_ITEM.EST_MVMT_CAN_BORROW,
                    SLOT_ITEM.EST_HITS_CAN_BORROW,
                    SLOT_ITEM.PLB_SCORE,
                    SLOT_ITEM.MULT_LOC_GRP,
                    SLOT_ITEM.DELETE_MULT,
                    SLOT_ITEM.GAVE_HIST_TO,
                    SLOT_ITEM.ABW_TEMP_TAG,
                    SLOT_ITEM.FORECAST_BORROWED,
                    SLOT_ITEM.NUM_VERT_DIV,
                    SLOT_ITEM.REPLEN_GROUP,
                    SLOT_ITEM.ADDED_FOR_MLM,
                    SLOT_ITEM.RESERVED_5,
                    SLOT_ITEM.ADJ_GRP_ID,
                    SLOT_ITEM.IGA_SCORE
                FROM {source_schema}.SLOT_ITEM
                WHERE  (TRUNC(CREATE_DATE_TIME) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC(MOD_DATE_TIME) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""
            

    SQ_Shortcut_to_SLOT_ITEM = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_SLOT_ITEM is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 89
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_SLOT_ITEM_temp = SQ_Shortcut_to_SLOT_ITEM.toDF(*["SQ_Shortcut_to_SLOT_ITEM___" + col for col in SQ_Shortcut_to_SLOT_ITEM.columns])
    
    EXPTRANS = SQ_Shortcut_to_SLOT_ITEM_temp.selectExpr( 
        "SQ_Shortcut_to_SLOT_ITEM___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR", 
        "SQ_Shortcut_to_SLOT_ITEM___SLOTITEM_ID as SLOTITEM_ID", 
        "SQ_Shortcut_to_SLOT_ITEM___SLOT_ID as SLOT_ID", 
        "SQ_Shortcut_to_SLOT_ITEM___SKU_ID as SKU_ID", 
        "SQ_Shortcut_to_SLOT_ITEM___SLOT_UNIT as SLOT_UNIT", 
        "SQ_Shortcut_to_SLOT_ITEM___SHIP_UNIT as SHIP_UNIT", 
        "SQ_Shortcut_to_SLOT_ITEM___MAX_LANES as MAX_LANES", 
        "SQ_Shortcut_to_SLOT_ITEM___DEEPS as DEEPS", 
        "SQ_Shortcut_to_SLOT_ITEM___SLOT_LOCKED as SLOT_LOCKED", 
        "SQ_Shortcut_to_SLOT_ITEM___PRIMARY_MOVE as PRIMARY_MOVE", 
        "SQ_Shortcut_to_SLOT_ITEM___SLOT_WIDTH as SLOT_WIDTH", 
        "SQ_Shortcut_to_SLOT_ITEM___EST_HITS as EST_HITS", 
        "SQ_Shortcut_to_SLOT_ITEM___EST_INVENTORY as EST_INVENTORY", 
        "SQ_Shortcut_to_SLOT_ITEM___EST_MOVEMENT as EST_MOVEMENT", 
        "SQ_Shortcut_to_SLOT_ITEM___HIST_MATCH as HIST_MATCH", 
        "SQ_Shortcut_to_SLOT_ITEM___LAST_CHANGE as LAST_CHANGE", 
        "SQ_Shortcut_to_SLOT_ITEM___BIN_UNIT as BIN_UNIT", 
        "SQ_Shortcut_to_SLOT_ITEM___PALLETE_PATTERN as PALLETE_PATTERN", 
        "SQ_Shortcut_to_SLOT_ITEM___SCORE as SCORE", 
        "SQ_Shortcut_to_SLOT_ITEM___CUR_ORIENTATION as CUR_ORIENTATION", 
        "SQ_Shortcut_to_SLOT_ITEM___IGN_FOR_RESLOT as IGN_FOR_RESLOT", 
        "SQ_Shortcut_to_SLOT_ITEM___REC_LANES as REC_LANES", 
        "SQ_Shortcut_to_SLOT_ITEM___REC_STACKING as REC_STACKING", 
        "SQ_Shortcut_to_SLOT_ITEM___PALLET_MOV as PALLET_MOV", 
        "SQ_Shortcut_to_SLOT_ITEM___CASE_MOV as CASE_MOV", 
        "SQ_Shortcut_to_SLOT_ITEM___INNER_MOV as INNER_MOV", 
        "SQ_Shortcut_to_SLOT_ITEM___EACH_MOV as EACH_MOV", 
        "SQ_Shortcut_to_SLOT_ITEM___BIN_MOV as BIN_MOV", 
        "SQ_Shortcut_to_SLOT_ITEM___PALLET_INVEN as PALLET_INVEN", 
        "SQ_Shortcut_to_SLOT_ITEM___CASE_INVEN as CASE_INVEN", 
        "SQ_Shortcut_to_SLOT_ITEM___INNER_INVEN as INNER_INVEN", 
        "SQ_Shortcut_to_SLOT_ITEM___EACH_INVEN as EACH_INVEN", 
        "SQ_Shortcut_to_SLOT_ITEM___BIN_INVEN as BIN_INVEN", 
        "SQ_Shortcut_to_SLOT_ITEM___CALC_HITS as CALC_HITS", 
        "SQ_Shortcut_to_SLOT_ITEM___CURRENT_BIN as CURRENT_BIN", 
        "SQ_Shortcut_to_SLOT_ITEM___CURRENT_PALLET as CURRENT_PALLET", 
        "SQ_Shortcut_to_SLOT_ITEM___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", 
        "SQ_Shortcut_to_SLOT_ITEM___ALLOW_EXPAND as ALLOW_EXPAND", 
        "SQ_Shortcut_to_SLOT_ITEM___PALLET_HI as PALLET_HI", 
        "SQ_Shortcut_to_SLOT_ITEM___NEEDED_RACK_TYPE as NEEDED_RACK_TYPE", 
        "SQ_Shortcut_to_SLOT_ITEM___LEGAL_FIT_REASON as LEGAL_FIT_REASON", 
        "SQ_Shortcut_to_SLOT_ITEM___ITEM_COST as ITEM_COST", 
        "SQ_Shortcut_to_SLOT_ITEM___USER_DEFINED as USER_DEFINED", 
        "SQ_Shortcut_to_SLOT_ITEM___LEGAL_FIT as LEGAL_FIT", 
        "SQ_Shortcut_to_SLOT_ITEM___SCORE_DIRTY as SCORE_DIRTY", 
        "SQ_Shortcut_to_SLOT_ITEM___USE_ESTIMATED_HIST as USE_ESTIMATED_HIST", 
        "SQ_Shortcut_to_SLOT_ITEM___OPT_FLUID_VOL as OPT_FLUID_VOL", 
        "SQ_Shortcut_to_SLOT_ITEM___CALC_VISC as CALC_VISC", 
        "SQ_Shortcut_to_SLOT_ITEM___EST_VISC as EST_VISC", 
        "SQ_Shortcut_to_SLOT_ITEM___SESSION_ID as SESSION_ID", 
        "SQ_Shortcut_to_SLOT_ITEM___RANK as RANK", 
        "SQ_Shortcut_to_SLOT_ITEM___INFO1 as INFO1", 
        "SQ_Shortcut_to_SLOT_ITEM___INFO2 as INFO2", 
        "SQ_Shortcut_to_SLOT_ITEM___INFO3 as INFO3", 
        "SQ_Shortcut_to_SLOT_ITEM___INFO4 as INFO4", 
        "SQ_Shortcut_to_SLOT_ITEM___INFO5 as INFO5", 
        "SQ_Shortcut_to_SLOT_ITEM___INFO6 as INFO6", 
        "SQ_Shortcut_to_SLOT_ITEM___RESERVED_1 as RESERVED_1", 
        "SQ_Shortcut_to_SLOT_ITEM___RESERVED_2 as RESERVED_2", 
        "SQ_Shortcut_to_SLOT_ITEM___RESERVED_3 as RESERVED_3", 
        "SQ_Shortcut_to_SLOT_ITEM___RESERVED_4 as RESERVED_4", 
        "SQ_Shortcut_to_SLOT_ITEM___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_SLOT_ITEM___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_SLOT_ITEM___MOD_USER as MOD_USER", 
        "SQ_Shortcut_to_SLOT_ITEM___SI_NUM_1 as SI_NUM_1", 
        "SQ_Shortcut_to_SLOT_ITEM___SI_NUM_2 as SI_NUM_2", 
        "SQ_Shortcut_to_SLOT_ITEM___SI_NUM_3 as SI_NUM_3", 
        "SQ_Shortcut_to_SLOT_ITEM___SI_NUM_4 as SI_NUM_4", 
        "SQ_Shortcut_to_SLOT_ITEM___SI_NUM_5 as SI_NUM_5", 
        "SQ_Shortcut_to_SLOT_ITEM___SI_NUM_6 as SI_NUM_6", 
        "SQ_Shortcut_to_SLOT_ITEM___SLOT_UNIT_WEIGHT as SLOT_UNIT_WEIGHT", 
        "SQ_Shortcut_to_SLOT_ITEM___TOTAL_ITEM_WT as TOTAL_ITEM_WT", 
        "SQ_Shortcut_to_SLOT_ITEM___BORROWING_OBJECT as BORROWING_OBJECT", 
        "SQ_Shortcut_to_SLOT_ITEM___BORROWING_SPECIFIC as BORROWING_SPECIFIC", 
        "SQ_Shortcut_to_SLOT_ITEM___EST_MVMT_CAN_BORROW as EST_MVMT_CAN_BORROW", 
        "SQ_Shortcut_to_SLOT_ITEM___EST_HITS_CAN_BORROW as EST_HITS_CAN_BORROW", 
        "SQ_Shortcut_to_SLOT_ITEM___PLB_SCORE as PLB_SCORE", 
        "SQ_Shortcut_to_SLOT_ITEM___MULT_LOC_GRP as MULT_LOC_GRP", 
        "SQ_Shortcut_to_SLOT_ITEM___DELETE_MULT as DELETE_MULT", 
        "SQ_Shortcut_to_SLOT_ITEM___GAVE_HIST_TO as GAVE_HIST_TO", 
        "SQ_Shortcut_to_SLOT_ITEM___ABW_TEMP_TAG as ABW_TEMP_TAG", 
        "SQ_Shortcut_to_SLOT_ITEM___FORECAST_BORROWED as FORECAST_BORROWED", 
        "SQ_Shortcut_to_SLOT_ITEM___NUM_VERT_DIV as NUM_VERT_DIV", 
        "SQ_Shortcut_to_SLOT_ITEM___REPLEN_GROUP as REPLEN_GROUP", 
        "SQ_Shortcut_to_SLOT_ITEM___ADDED_FOR_MLM as ADDED_FOR_MLM", 
        "SQ_Shortcut_to_SLOT_ITEM___RESERVED_5 as RESERVED_5", 
        "SQ_Shortcut_to_SLOT_ITEM___ADJ_GRP_ID as ADJ_GRP_ID", 
        "SQ_Shortcut_to_SLOT_ITEM___IGA_SCORE as IGA_SCORE", 
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_SLOT_ITEM_PRE, type TARGET 
    # COLUMN COUNT: 89
    
    
    Shortcut_to_WM_SLOT_ITEM_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR AS SMALLINT) as DC_NBR",
        "CAST(SLOTITEM_ID AS DECIMAL(19,0)) as SLOT_ITEM_ID",
        "CAST(SLOT_ID AS DECIMAL(19,0)) as SLOT_ID",
        "CAST(SKU_ID AS DECIMAL(19,0)) as SKU_ID",
        "CAST(SLOT_UNIT AS BIGINT) as SLOT_UNIT",
        "CAST(SHIP_UNIT AS BIGINT) as SHIP_UNIT",
        "CAST(MAX_LANES AS BIGINT) as MAX_LANES",
        "CAST(DEEPS AS BIGINT) as DEEPS",
        "CAST(SLOT_LOCKED AS TINYINT) as SLOT_LOCKED",
        "CAST(PRIMARY_MOVE AS TINYINT) as PRIMARY_MOVE",
        "CAST(SLOT_WIDTH AS DECIMAL(13,4)) as SLOT_WIDTH",
        "CAST(EST_HITS AS DECIMAL(13,4)) as EST_HITS",
        "CAST(EST_INVENTORY AS DECIMAL(13,4)) as EST_INVENTORY",
        "CAST(EST_MOVEMENT AS DECIMAL(13,4)) as EST_MOVEMENT",
        "CAST(HIST_MATCH AS STRING) as HIST_MATCH",
        "CAST(LAST_CHANGE AS TIMESTAMP) as LAST_CHANGE",
        "CAST(BIN_UNIT AS BIGINT) as BIN_UNIT",
        "CAST(PALLETE_PATTERN AS BIGINT) as PALLETE_PATTERN",
        "CAST(SCORE AS DECIMAL(9,4)) as SCORE",
        "CAST(CUR_ORIENTATION AS BIGINT) as CUR_ORIENTATION",
        "CAST(IGN_FOR_RESLOT AS TINYINT) as IGN_FOR_RESLOT",
        "CAST(REC_LANES AS BIGINT) as REC_LANES",
        "CAST(REC_STACKING AS BIGINT) as REC_STACKING",
        "CAST(PALLET_MOV AS DECIMAL(13,4)) as PALLET_MOV",
        "CAST(CASE_MOV AS DECIMAL(13,4)) as CASE_MOV",
        "CAST(INNER_MOV AS DECIMAL(13,4)) as INNER_MOV",
        "CAST(EACH_MOV AS DECIMAL(13,4)) as EACH_MOV",
        "CAST(BIN_MOV AS DECIMAL(13,4)) as BIN_MOV",
        "CAST(PALLET_INVEN AS DECIMAL(13,4)) as PALLET_INVEN",
        "CAST(CASE_INVEN AS DECIMAL(13,4)) as CASE_INVEN",
        "CAST(INNER_INVEN AS DECIMAL(13,4)) as INNER_INVEN",
        "CAST(EACH_INVEN AS DECIMAL(13,4)) as EACH_INVEN",
        "CAST(BIN_INVEN AS DECIMAL(13,4)) as BIN_INVEN",
        "CAST(CALC_HITS AS DECIMAL(13,4)) as CALC_HITS",
        "CAST(CURRENT_BIN AS BIGINT) as CURRENT_BIN",
        "CAST(CURRENT_PALLET AS BIGINT) as CURRENT_PALLET",
        "CAST(OPT_PALLET_PATTERN AS BIGINT) as OPT_PALLET_PATTERN",
        "CAST(ALLOW_EXPAND AS TINYINT) as ALLOW_EXPAND",
        "CAST(PALLET_HI AS BIGINT) as PALLET_HI",
        "CAST(NEEDED_RACK_TYPE AS STRING) as NEEDED_RACK_TYPE",
        "CAST(LEGAL_FIT_REASON AS BIGINT) as LEGAL_FIT_REASON",
        "CAST(ITEM_COST AS DECIMAL(9,4)) as ITEM_COST",
        "CAST(USER_DEFINED AS DECIMAL(9,2)) as USER_DEFINED",
        "CAST(LEGAL_FIT AS TINYINT) as LEGAL_FIT",
        "CAST(SCORE_DIRTY AS TINYINT) as SCORE_DIRTY",
        "CAST(USE_ESTIMATED_HIST AS TINYINT) as USE_ESTIMATED_HIST",
        "CAST(OPT_FLUID_VOL AS DECIMAL(19,7)) as OPT_FLUID_VOL",
        "CAST(CALC_VISC AS DECIMAL(9,2)) as CALC_VISC",
        "CAST(EST_VISC AS DECIMAL(9,2)) as EST_VISC",
        "CAST(SESSION_ID AS BIGINT) as SESSION_ID",
        "CAST(RANK AS BIGINT) as RANK",
        "CAST(INFO1 AS STRING) as INFO1",
        "CAST(INFO2 AS STRING) as INFO2",
        "CAST(INFO3 AS STRING) as INFO3",
        "CAST(INFO4 AS STRING) as INFO4",
        "CAST(INFO5 AS STRING) as INFO5",
        "CAST(INFO6 AS STRING) as INFO6",
        "CAST(RESERVED_1 AS STRING) as RESERVED_1",
        "CAST(RESERVED_2 AS STRING) as RESERVED_2",
        "CAST(RESERVED_3 AS BIGINT) as RESERVED_3",
        "CAST(RESERVED_4 AS BIGINT) as RESERVED_4",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(MOD_USER AS STRING) as MOD_USER",
        "CAST(SI_NUM_1 AS DECIMAL(13,4)) as SI_NUM_1",
        "CAST(SI_NUM_2 AS DECIMAL(13,4)) as SI_NUM_2",
        "CAST(SI_NUM_3 AS DECIMAL(13,4)) as SI_NUM_3",
        "CAST(SI_NUM_4 AS DECIMAL(13,4)) as SI_NUM_4",
        "CAST(SI_NUM_5 AS DECIMAL(13,4)) as SI_NUM_5",
        "CAST(SI_NUM_6 AS DECIMAL(13,4)) as SI_NUM_6",
        "CAST(SLOT_UNIT_WEIGHT AS DECIMAL(9,4)) as SLOT_UNIT_WEIGHT",
        "CAST(TOTAL_ITEM_WT AS DECIMAL(9,4)) as TOTAL_ITEM_WT",
        "CAST(BORROWING_OBJECT AS BIGINT) as BORROWING_OBJECT",
        "CAST(BORROWING_SPECIFIC AS BIGINT) as BORROWING_SPECIFIC",
        "CAST(EST_MVMT_CAN_BORROW AS TINYINT) as EST_MVMT_CAN_BORROW",
        "CAST(EST_HITS_CAN_BORROW AS TINYINT) as EST_HITS_CAN_BORROW",
        "CAST(PLB_SCORE AS DECIMAL(9,4)) as PLB_SCORE",
        "CAST(MULT_LOC_GRP AS STRING) as MULT_LOC_GRP",
        "CAST(DELETE_MULT AS TINYINT) as DELETE_MULT",
        "CAST(GAVE_HIST_TO AS BIGINT) as GAVE_HIST_TO",
        "CAST(ABW_TEMP_TAG AS STRING) as ABW_TEMP_TAG",
        "CAST(FORECAST_BORROWED AS TINYINT) as FORECAST_BORROWED",
        "CAST(NUM_VERT_DIV AS BIGINT) as NUM_VERT_DIV",
        "CAST(REPLEN_GROUP AS STRING) as REPLEN_GROUP",
        "CAST(ADDED_FOR_MLM AS TINYINT) as ADDED_FOR_MLM",
        "CAST(RESERVED_5 AS STRING) as RESERVED_5",
        "CAST(ADJ_GRP_ID AS BIGINT) as ADJ_GRP_ID",
        "CAST(IGA_SCORE AS DECIMAL(9,4)) as IGA_SCORE",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )

    overwriteDeltaPartition(Shortcut_to_WM_SLOT_ITEM_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_SLOT_ITEM_PRE is written to the target table - " + target_table_name)
