#Code converted on 2023-06-26 17:06:05
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



def m_WM_Pick_Locn_Hdr_Slotting_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Pick_Locn_Hdr_Slotting_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_PICK_LOCN_HDR_SLOTTING_PRE"
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
                    PICK_LOCN_HDR_SLOTTING.ALLOW_EXPAND,
                    PICK_LOCN_HDR_SLOTTING.ALLOW_EXPAND_LFT,
                    PICK_LOCN_HDR_SLOTTING.ALLOW_EXPAND_LFT_OVR,
                    PICK_LOCN_HDR_SLOTTING.ALLOW_EXPAND_OVR,
                    PICK_LOCN_HDR_SLOTTING.ALLOW_EXPAND_RGT,
                    PICK_LOCN_HDR_SLOTTING.ALLOW_EXPAND_RGT_OVR,
                    PICK_LOCN_HDR_SLOTTING.DEPTH_OVERRIDE,
                    PICK_LOCN_HDR_SLOTTING.HT_OVERRIDE,
                    PICK_LOCN_HDR_SLOTTING.LABEL_POS,
                    PICK_LOCN_HDR_SLOTTING.LABEL_POS_OVR,
                    PICK_LOCN_HDR_SLOTTING.LEFT_SLOT,
                    PICK_LOCN_HDR_SLOTTING.LOCKED,
                    PICK_LOCN_HDR_SLOTTING.MAX_HC_OVR,
                    PICK_LOCN_HDR_SLOTTING.MAX_HT_CLEAR,
                    PICK_LOCN_HDR_SLOTTING.MAX_LANE_WT,
                    PICK_LOCN_HDR_SLOTTING.MAX_LANES,
                    PICK_LOCN_HDR_SLOTTING.MAX_LN_OVR,
                    PICK_LOCN_HDR_SLOTTING.MAX_LW_OVR,
                    PICK_LOCN_HDR_SLOTTING.MAX_SC_OVR,
                    PICK_LOCN_HDR_SLOTTING.MAX_SIDE_CLEAR,
                    PICK_LOCN_HDR_SLOTTING.MAX_ST_OVR,
                    PICK_LOCN_HDR_SLOTTING.MAX_STACK,
                    PICK_LOCN_HDR_SLOTTING.MY_RANGE,
                    PICK_LOCN_HDR_SLOTTING.MY_SNS,
                    PICK_LOCN_HDR_SLOTTING.OLD_REC_SLOT_WIDTH,
                    PICK_LOCN_HDR_SLOTTING.PICK_LOCN_HDR_ID,
                    PICK_LOCN_HDR_SLOTTING.PROCESSED,
                    PICK_LOCN_HDR_SLOTTING.RACK_LEVEL_ID,
                    PICK_LOCN_HDR_SLOTTING.RACK_TYPE,
                    PICK_LOCN_HDR_SLOTTING.REACH_DIST,
                    PICK_LOCN_HDR_SLOTTING.REACH_DIST_OVERRIDE,
                    PICK_LOCN_HDR_SLOTTING.RESERVED_1,
                    PICK_LOCN_HDR_SLOTTING.RESERVED_2,
                    PICK_LOCN_HDR_SLOTTING.RESERVED_3,
                    PICK_LOCN_HDR_SLOTTING.RESERVED_4,
                    PICK_LOCN_HDR_SLOTTING.RIGHT_SLOT,
                    PICK_LOCN_HDR_SLOTTING.SIDE_OF_AISLE,
                    PICK_LOCN_HDR_SLOTTING.SLOT_PRIORITY,
                    PICK_LOCN_HDR_SLOTTING.WIDTH_OVERRIDE,
                    PICK_LOCN_HDR_SLOTTING.WT_LIMIT_OVERRIDE,
                    PICK_LOCN_HDR_SLOTTING.CREATED_SOURCE_TYPE,
                    PICK_LOCN_HDR_SLOTTING.CREATED_SOURCE,
                    PICK_LOCN_HDR_SLOTTING.CREATED_DTTM,
                    PICK_LOCN_HDR_SLOTTING.LAST_UPDATED_SOURCE_TYPE,
                    PICK_LOCN_HDR_SLOTTING.LAST_UPDATED_SOURCE,
                    PICK_LOCN_HDR_SLOTTING.LAST_UPDATED_DTTM,
                    PICK_LOCN_HDR_SLOTTING.SLOTTING_GROUP
                FROM {source_schema}.PICK_LOCN_HDR_SLOTTING
                WHERE  (trunc(CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""


    SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING is executed and data is loaded using jdbc")

    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 49
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING_temp = SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING.toDF(*["SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___" + col for col in SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING.columns])
    
    EXPTRANS = SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING_temp.selectExpr( \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___PICK_LOCN_HDR_ID as PICK_LOCN_HDR_ID", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND as ALLOW_EXPAND", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_LFT as ALLOW_EXPAND_LFT", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_LFT_OVR as ALLOW_EXPAND_LFT_OVR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_OVR as ALLOW_EXPAND_OVR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_RGT as ALLOW_EXPAND_RGT", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_RGT_OVR as ALLOW_EXPAND_RGT_OVR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___DEPTH_OVERRIDE as DEPTH_OVERRIDE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___HT_OVERRIDE as HT_OVERRIDE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___LABEL_POS as LABEL_POS", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___LABEL_POS_OVR as LABEL_POS_OVR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___LEFT_SLOT as LEFT_SLOT", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___LOCKED as LOCKED", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MAX_HC_OVR as MAX_HC_OVR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MAX_HT_CLEAR as MAX_HT_CLEAR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MAX_LANE_WT as MAX_LANE_WT", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MAX_LANES as MAX_LANES", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MAX_LN_OVR as MAX_LN_OVR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MAX_LW_OVR as MAX_LW_OVR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MAX_SC_OVR as MAX_SC_OVR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MAX_SIDE_CLEAR as MAX_SIDE_CLEAR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MAX_ST_OVR as MAX_ST_OVR", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MAX_STACK as MAX_STACK", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MY_RANGE as MY_RANGE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___MY_SNS as MY_SNS", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___OLD_REC_SLOT_WIDTH as OLD_REC_SLOT_WIDTH", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___PROCESSED as PROCESSED", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___RACK_LEVEL_ID as RACK_LEVEL_ID", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___RACK_TYPE as RACK_TYPE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___REACH_DIST as REACH_DIST", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___REACH_DIST_OVERRIDE as REACH_DIST_OVERRIDE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___RESERVED_1 as RESERVED_1", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___RESERVED_2 as RESERVED_2", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___RESERVED_3 as RESERVED_3", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___RESERVED_4 as RESERVED_4", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___RIGHT_SLOT as RIGHT_SLOT", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___SIDE_OF_AISLE as SIDE_OF_AISLE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___SLOT_PRIORITY as SLOT_PRIORITY", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___WIDTH_OVERRIDE as WIDTH_OVERRIDE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___WT_LIMIT_OVERRIDE as WT_LIMIT_OVERRIDE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_PICK_LOCN_HDR_SLOTTING___SLOTTING_GROUP as SLOTTING_GROUP", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE, type TARGET 
    # COLUMN COUNT: 49
    
    
    Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR AS SMALLINT) as DC_NBR",
        "CAST(PICK_LOCN_HDR_ID AS DECIMAL(19,0)) as PICK_LOCN_HDR_ID",
        "CAST(ALLOW_EXPAND AS TINYINT) as ALLOW_EXPAND",
        "CAST(ALLOW_EXPAND_LFT AS TINYINT) as ALLOW_EXPAND_LFT",
        "CAST(ALLOW_EXPAND_LFT_OVR AS TINYINT) as ALLOW_EXPAND_LFT_OVR",
        "CAST(ALLOW_EXPAND_OVR AS TINYINT) as ALLOW_EXPAND_OVR",
        "CAST(ALLOW_EXPAND_RGT AS TINYINT) as ALLOW_EXPAND_RGT",
        "CAST(ALLOW_EXPAND_RGT_OVR AS TINYINT) as ALLOW_EXPAND_RGT_OVR",
        "CAST(DEPTH_OVERRIDE AS TINYINT) as DEPTH_OVERRIDE",
        "CAST(HT_OVERRIDE AS TINYINT) as HT_OVERRIDE",
        "CAST(LABEL_POS AS DECIMAL(9,4)) as LABEL_POS",
        "CAST(LABEL_POS_OVR AS TINYINT) as LABEL_POS_OVR",
        "CAST(LEFT_SLOT AS DECIMAL(19,0)) as LEFT_SLOT",
        "CAST(LOCKED AS TINYINT) as LOCKED",
        "CAST(MAX_HC_OVR AS TINYINT) as MAX_HC_OVR",
        "CAST(MAX_HT_CLEAR AS DECIMAL(9,4)) as MAX_HT_CLEAR",
        "CAST(MAX_LANE_WT AS DECIMAL(13,4)) as MAX_LANE_WT",
        "CAST(MAX_LANES AS BIGINT) as MAX_LANES",
        "CAST(MAX_LN_OVR AS TINYINT) as MAX_LN_OVR",
        "CAST(MAX_LW_OVR AS DECIMAL(9,4)) as MAX_LW_OVR",
        "CAST(MAX_SC_OVR AS TINYINT) as MAX_SC_OVR",
        "CAST(MAX_SIDE_CLEAR AS DECIMAL(9,4)) as MAX_SIDE_CLEAR",
        "CAST(MAX_ST_OVR AS TINYINT) as MAX_ST_OVR",
        "CAST(MAX_STACK AS BIGINT) as MAX_STACK",
        "CAST(MY_RANGE AS BIGINT) as MY_RANGE",
        "CAST(MY_SNS AS BIGINT) as MY_SNS",
        "CAST(OLD_REC_SLOT_WIDTH AS DECIMAL(13,4)) as OLD_REC_SLOT_WIDTH",
        "CAST(PROCESSED AS TINYINT) as PROCESSED",
        "CAST(RACK_LEVEL_ID AS BIGINT) as RACK_LEVEL_ID",
        "CAST(RACK_TYPE AS BIGINT) as RACK_TYPE",
        "CAST(REACH_DIST AS DECIMAL(13,4)) as REACH_DIST",
        "CAST(REACH_DIST_OVERRIDE AS TINYINT) as REACH_DIST_OVERRIDE",
        "CAST(RESERVED_1 AS STRING) as RESERVED_1",
        "CAST(RESERVED_2 AS STRING) as RESERVED_2",
        "CAST(RESERVED_3 AS BIGINT) as RESERVED_3",
        "CAST(RESERVED_4 AS BIGINT) as RESERVED_4",
        "CAST(RIGHT_SLOT AS DECIMAL(19,0)) as RIGHT_SLOT",
        "CAST(SIDE_OF_AISLE AS SMALLINT) as SIDE_OF_AISLE",
        "CAST(SLOT_PRIORITY AS STRING) as SLOT_PRIORITY",
        "CAST(WIDTH_OVERRIDE AS TINYINT) as WIDTH_OVERRIDE",
        "CAST(WT_LIMIT_OVERRIDE AS TINYINT) as WT_LIMIT_OVERRIDE",
        "CAST(CREATED_SOURCE_TYPE AS TINYINT) as CREATED_SOURCE_TYPE",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_SOURCE_TYPE AS TINYINT) as LAST_UPDATED_SOURCE_TYPE",
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(SLOTTING_GROUP AS STRING) as SLOTTING_GROUP",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )    
    
    overwriteDeltaPartition(Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE is written to the target table - " + target_table_name)
