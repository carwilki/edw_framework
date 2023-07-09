# Code converted on 2023-06-22 20:25:06
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


def m_WM_Rack_Type_Level_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Rack_Type_Level_PRE")

    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")

    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"

    tableName = "WM_RACK_TYPE_LEVEL_PRE"
    schemaName = raw
    source_schema = "WMSMIS"

    target_table_name = schemaName + "." + tableName
    refine_table_name = tableName[:-4]
    Prev_Run_Dt = genPrevRunDt(refine_table_name, refine, raw)
    print("The prev run date is " + Prev_Run_Dt)

    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")

    dcnbr = dcnbr.strip()[2:]

    query = f"""SELECT
                    RACK_TYPE_LEVEL.RACK_LEVEL_ID,
                    RACK_TYPE_LEVEL.RACK_TYPE,
                    RACK_TYPE_LEVEL.LEVEL_NUMBER,
                    RACK_TYPE_LEVEL.LEVEL_WT_LIMIT,
                    RACK_TYPE_LEVEL.LEVEL_HT,
                    RACK_TYPE_LEVEL.LEVEL_WIDTH,
                    RACK_TYPE_LEVEL.LEVEL_DEPTH,
                    RACK_TYPE_LEVEL.NBR_OF_SLOTS,
                    RACK_TYPE_LEVEL.SLOT_WT_LIMIT,
                    RACK_TYPE_LEVEL.MAX_LANE_WT,
                    RACK_TYPE_LEVEL.HT_CLEARANCE,
                    RACK_TYPE_LEVEL.SIDE_CLEAR,
                    RACK_TYPE_LEVEL.MAX_STACKING,
                    RACK_TYPE_LEVEL.MAX_LANES,
                    RACK_TYPE_LEVEL.GUIDE_WIDTH,
                    RACK_TYPE_LEVEL.LANE_GUIDES,
                    RACK_TYPE_LEVEL.SLOT_GUIDES,
                    RACK_TYPE_LEVEL.MOVE_GUIDES,
                    RACK_TYPE_LEVEL.USE_FULL_WID,
                    RACK_TYPE_LEVEL.SHELF_THICKNESS,
                    RACK_TYPE_LEVEL.LABEL_POS,
                    RACK_TYPE_LEVEL.MAX_PICK_HT,
                    RACK_TYPE_LEVEL.SLOT_UNITS,
                    RACK_TYPE_LEVEL.SHIP_UNITS,
                    RACK_TYPE_LEVEL.MAX_SL_WID,
                    RACK_TYPE_LEVEL.ONE_FACING_PER_DSW,
                    RACK_TYPE_LEVEL.LEVEL_PRIORITY,
                    RACK_TYPE_LEVEL.COST_TO_PICK,
                    RACK_TYPE_LEVEL.RESERVED_1,
                    RACK_TYPE_LEVEL.RESERVED_2,
                    RACK_TYPE_LEVEL.RESERVED_3,
                    RACK_TYPE_LEVEL.RESERVED_4,
                    RACK_TYPE_LEVEL.CREATE_DATE_TIME,
                    RACK_TYPE_LEVEL.MOD_DATE_TIME,
                    RACK_TYPE_LEVEL.MOD_USER,
                    RACK_TYPE_LEVEL.USE_INCREMENT,
                    RACK_TYPE_LEVEL.WIDTH_INCREMENT,
                    RACK_TYPE_LEVEL.MIN_SL_WIDTH,
                    RACK_TYPE_LEVEL.MIN_ITEM_WIDTH,
                    RACK_TYPE_LEVEL.MAX_OVERHANG_PCT,
                    RACK_TYPE_LEVEL.LEV_MAP_STR,
                    RACK_TYPE_LEVEL.POS_MAP_STR,
                    RACK_TYPE_LEVEL.USE_LEV_MAP_STR,
                    RACK_TYPE_LEVEL.USE_POS_MAP_STR,
                    RACK_TYPE_LEVEL.USE_VERT_DIV,
                    RACK_TYPE_LEVEL.VERT_DIV_HT,
                    RACK_TYPE_LEVEL.MIN_CHANNEL_WID,
                    RACK_TYPE_LEVEL.MAX_CHANNEL_WID,
                    RACK_TYPE_LEVEL.MAX_CHANNEL_WT,
                    RACK_TYPE_LEVEL.MAX_ADJ_SLOTS,
                    RACK_TYPE_LEVEL.MAX_EJECTOR_WT,
                    RACK_TYPE_LEVEL.AFRAME_ROTATE,
                    RACK_TYPE_LEVEL.AFRAME_MIN_HT,
                    RACK_TYPE_LEVEL.AFRAME_MAX_HT,
                    RACK_TYPE_LEVEL.AFRAME_MIN_LEN,
                    RACK_TYPE_LEVEL.AFRAME_MAX_LEN,
                    RACK_TYPE_LEVEL.AFRAME_MIN_WID,
                    RACK_TYPE_LEVEL.AFRAME_MAX_WID,
                    RACK_TYPE_LEVEL.AFRAME_MIN_WT,
                    RACK_TYPE_LEVEL.AFRAME_MAX_WT,
                    RACK_TYPE_LEVEL.REACH_DIST,
                    RACK_TYPE_LEVEL.MOVE_GUIDES_RGT,
                    RACK_TYPE_LEVEL.MOVE_GUIDES_LFT
                FROM {source_schema}.RACK_TYPE_LEVEL
                WHERE  (TRUNC(CREATE_DATE_TIME) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (TRUNC(MOD_DATE_TIME) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14)"""

    SQ_Shortcut_to_RACK_TYPE_LEVEL = jdbcOracleConnection(
        query, username, password, connection_string
    ).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info(
        "SQL query for spark.read.format('jdbc').option('url', connection_string).option( is executed and data is loaded using jdbc"
    )

    # Processing node EXPTRANS, type EXPRESSION
    # COLUMN COUNT: 65

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_RACK_TYPE_LEVEL_temp = SQ_Shortcut_to_RACK_TYPE_LEVEL.toDF(
        *[
            "SQ_Shortcut_to_RACK_TYPE_LEVEL___" + col
            for col in SQ_Shortcut_to_RACK_TYPE_LEVEL.columns
        ]
    )

    EXPTRANS = SQ_Shortcut_to_RACK_TYPE_LEVEL_temp.selectExpr(
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___sys_row_id as sys_row_id",
        f"{dcnbr} as DC_NBR_exp",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___RACK_LEVEL_ID as RACK_LEVEL_ID",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___RACK_TYPE as RACK_TYPE",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___LEVEL_NUMBER as LEVEL_NUMBER",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___LEVEL_WT_LIMIT as LEVEL_WT_LIMIT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___LEVEL_HT as LEVEL_HT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___LEVEL_WIDTH as LEVEL_WIDTH",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___LEVEL_DEPTH as LEVEL_DEPTH",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___NBR_OF_SLOTS as NBR_OF_SLOTS",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___SLOT_WT_LIMIT as SLOT_WT_LIMIT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MAX_LANE_WT as MAX_LANE_WT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___HT_CLEARANCE as HT_CLEARANCE",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___SIDE_CLEAR as SIDE_CLEAR",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MAX_STACKING as MAX_STACKING",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MAX_LANES as MAX_LANES",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___GUIDE_WIDTH as GUIDE_WIDTH",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___LANE_GUIDES as LANE_GUIDES",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___SLOT_GUIDES as SLOT_GUIDES",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MOVE_GUIDES as MOVE_GUIDES",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___USE_FULL_WID as USE_FULL_WID",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___SHELF_THICKNESS as SHELF_THICKNESS",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___LABEL_POS as LABEL_POS",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MAX_PICK_HT as MAX_PICK_HT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___SLOT_UNITS as SLOT_UNITS",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___SHIP_UNITS as SHIP_UNITS",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MAX_SL_WID as MAX_SL_WID",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___ONE_FACING_PER_DSW as ONE_FACING_PER_DSW",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___LEVEL_PRIORITY as LEVEL_PRIORITY",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___COST_TO_PICK as COST_TO_PICK",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___RESERVED_1 as RESERVED_1",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___RESERVED_2 as RESERVED_2",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___RESERVED_3 as RESERVED_3",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___RESERVED_4 as RESERVED_4",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___CREATE_DATE_TIME as CREATE_DATE_TIME",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MOD_DATE_TIME as MOD_DATE_TIME",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MOD_USER as MOD_USER",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___USE_INCREMENT as USE_INCREMENT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___WIDTH_INCREMENT as WIDTH_INCREMENT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MIN_SL_WIDTH as MIN_SL_WIDTH",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MIN_ITEM_WIDTH as MIN_ITEM_WIDTH",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MAX_OVERHANG_PCT as MAX_OVERHANG_PCT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___LEV_MAP_STR as LEV_MAP_STR",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___POS_MAP_STR as POS_MAP_STR",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___USE_LEV_MAP_STR as USE_LEV_MAP_STR",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___USE_POS_MAP_STR as USE_POS_MAP_STR",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___USE_VERT_DIV as USE_VERT_DIV",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___VERT_DIV_HT as VERT_DIV_HT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MIN_CHANNEL_WID as MIN_CHANNEL_WID",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MAX_CHANNEL_WID as MAX_CHANNEL_WID",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MAX_CHANNEL_WT as MAX_CHANNEL_WT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MAX_ADJ_SLOTS as MAX_ADJ_SLOTS",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MAX_EJECTOR_WT as MAX_EJECTOR_WT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___AFRAME_ROTATE as AFRAME_ROTATE",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___AFRAME_MIN_HT as AFRAME_MIN_HT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___AFRAME_MAX_HT as AFRAME_MAX_HT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___AFRAME_MIN_LEN as AFRAME_MIN_LEN",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___AFRAME_MAX_LEN as AFRAME_MAX_LEN",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___AFRAME_MIN_WID as AFRAME_MIN_WID",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___AFRAME_MAX_WID as AFRAME_MAX_WID",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___AFRAME_MIN_WT as AFRAME_MIN_WT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___AFRAME_MAX_WT as AFRAME_MAX_WT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___REACH_DIST as REACH_DIST",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MOVE_GUIDES_RGT as MOVE_GUIDES_RGT",
        "SQ_Shortcut_to_RACK_TYPE_LEVEL___MOVE_GUIDES_LFT as MOVE_GUIDES_LFT",
        "CURRENT_TIMESTAMP() as LOAD_TSTMP",
    )

    # Processing node Shortcut_to_WM_RACK_TYPE_LEVEL_PRE, type TARGET
    # COLUMN COUNT: 65

    Shortcut_to_WM_RACK_TYPE_LEVEL_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_exp AS BIGINT) as DC_NBR",
        "CAST(RACK_LEVEL_ID AS BIGINT) as RACK_LEVEL_ID",
        "CAST(RACK_TYPE AS BIGINT) as RACK_TYPE",
        "CAST(LEVEL_NUMBER AS BIGINT) as LEVEL_NUMBER",
        "CAST(LEVEL_WT_LIMIT AS BIGINT) as LEVEL_WT_LIMIT",
        "CAST(LEVEL_HT AS BIGINT) as LEVEL_HT",
        "CAST(LEVEL_WIDTH AS BIGINT) as LEVEL_WIDTH",
        "CAST(LEVEL_DEPTH AS BIGINT) as LEVEL_DEPTH",
        "CAST(NBR_OF_SLOTS AS BIGINT) as NBR_OF_SLOTS",
        "CAST(SLOT_WT_LIMIT AS BIGINT) as SLOT_WT_LIMIT",
        "CAST(MAX_LANE_WT AS BIGINT) as MAX_LANE_WT",
        "CAST(HT_CLEARANCE AS BIGINT) as HT_CLEARANCE",
        "CAST(SIDE_CLEAR AS BIGINT) as SIDE_CLEAR",
        "CAST(MAX_STACKING AS BIGINT) as MAX_STACKING",
        "CAST(MAX_LANES AS BIGINT) as MAX_LANES",
        "CAST(GUIDE_WIDTH AS BIGINT) as GUIDE_WIDTH",
        "CAST(LANE_GUIDES AS BIGINT) as LANE_GUIDES",
        "CAST(SLOT_GUIDES AS BIGINT) as SLOT_GUIDES",
        "CAST(MOVE_GUIDES AS BIGINT) as MOVE_GUIDES",
        "CAST(USE_FULL_WID AS BIGINT) as USE_FULL_WID",
        "CAST(SHELF_THICKNESS AS BIGINT) as SHELF_THICKNESS",
        "CAST(LABEL_POS AS BIGINT) as LABEL_POS",
        "CAST(MAX_PICK_HT AS BIGINT) as MAX_PICK_HT",
        "CAST(SLOT_UNITS AS BIGINT) as SLOT_UNITS",
        "CAST(SHIP_UNITS AS BIGINT) as SHIP_UNITS",
        "CAST(MAX_SL_WID AS BIGINT) as MAX_SL_WID",
        "CAST(ONE_FACING_PER_DSW AS BIGINT) as ONE_FACING_PER_DSW",
        "CAST(LEVEL_PRIORITY AS BIGINT) as LEVEL_PRIORITY",
        "CAST(COST_TO_PICK AS BIGINT) as COST_TO_PICK",
        "CAST(RESERVED_1 AS STRING) as RESERVED_1",
        "CAST(RESERVED_2 AS STRING) as RESERVED_2",
        "CAST(RESERVED_3 AS BIGINT) as RESERVED_3",
        "CAST(RESERVED_4 AS BIGINT) as RESERVED_4",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(MOD_USER AS STRING) as MOD_USER",
        "CAST(USE_INCREMENT AS BIGINT) as USE_INCREMENT",
        "CAST(WIDTH_INCREMENT AS BIGINT) as WIDTH_INCREMENT",
        "CAST(MIN_SL_WIDTH AS BIGINT) as MIN_SL_WIDTH",
        "CAST(MIN_ITEM_WIDTH AS BIGINT) as MIN_ITEM_WIDTH",
        "CAST(MAX_OVERHANG_PCT AS BIGINT) as MAX_OVERHANG_PCT",
        "CAST(LEV_MAP_STR AS STRING) as LEV_MAP_STR",
        "CAST(POS_MAP_STR AS STRING) as POS_MAP_STR",
        "CAST(USE_LEV_MAP_STR AS BIGINT) as USE_LEV_MAP_STR",
        "CAST(USE_POS_MAP_STR AS BIGINT) as USE_POS_MAP_STR",
        "CAST(USE_VERT_DIV AS BIGINT) as USE_VERT_DIV",
        "CAST(VERT_DIV_HT AS BIGINT) as VERT_DIV_HT",
        "CAST(MIN_CHANNEL_WID AS BIGINT) as MIN_CHANNEL_WID",
        "CAST(MAX_CHANNEL_WID AS BIGINT) as MAX_CHANNEL_WID",
        "CAST(MAX_CHANNEL_WT AS BIGINT) as MAX_CHANNEL_WT",
        "CAST(MAX_ADJ_SLOTS AS BIGINT) as MAX_ADJ_SLOTS",
        "CAST(MAX_EJECTOR_WT AS BIGINT) as MAX_EJECTOR_WT",
        "CAST(AFRAME_ROTATE AS BIGINT) as AFRAME_ROTATE",
        "CAST(AFRAME_MIN_HT AS BIGINT) as AFRAME_MIN_HT",
        "CAST(AFRAME_MAX_HT AS BIGINT) as AFRAME_MAX_HT",
        "CAST(AFRAME_MIN_LEN AS BIGINT) as AFRAME_MIN_LEN",
        "CAST(AFRAME_MAX_LEN AS BIGINT) as AFRAME_MAX_LEN",
        "CAST(AFRAME_MIN_WID AS BIGINT) as AFRAME_MIN_WID",
        "CAST(AFRAME_MAX_WID AS BIGINT) as AFRAME_MAX_WID",
        "CAST(AFRAME_MIN_WT AS BIGINT) as AFRAME_MIN_WT",
        "CAST(AFRAME_MAX_WT AS BIGINT) as AFRAME_MAX_WT",
        "CAST(REACH_DIST AS BIGINT) as REACH_DIST",
        "CAST(MOVE_GUIDES_RGT AS BIGINT) as MOVE_GUIDES_RGT",
        "CAST(MOVE_GUIDES_LFT AS BIGINT) as MOVE_GUIDES_LFT",
        "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
    )

    overwriteDeltaPartition(
        Shortcut_to_WM_RACK_TYPE_LEVEL_PRE, "DC_NBR", dcnbr, target_table_name
    )
    logger.info(
        "Shortcut_to_WM_RACK_TYPE_LEVEL_PRE is written to the target table - "
        + target_table_name
    )
