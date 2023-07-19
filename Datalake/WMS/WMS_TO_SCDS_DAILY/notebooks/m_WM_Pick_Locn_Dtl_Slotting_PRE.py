#Code converted on 2023-06-26 17:03:39
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


def m_WM_Pick_Locn_Dtl_Slotting_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Pick_Locn_Dtl_Slotting_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_PICK_LOCN_DTL_SLOTTING_PRE"
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
                    PICK_LOCN_DTL_SLOTTING.PICK_LOCN_DTL_ID,
                    PICK_LOCN_DTL_SLOTTING.HIST_MATCH,
                    PICK_LOCN_DTL_SLOTTING.CUR_ORIENTATION,
                    PICK_LOCN_DTL_SLOTTING.IGN_FOR_RESLOT,
                    PICK_LOCN_DTL_SLOTTING.REC_LANES,
                    PICK_LOCN_DTL_SLOTTING.REC_STACKING,
                    PICK_LOCN_DTL_SLOTTING.HI_RESIDUAL_1,
                    PICK_LOCN_DTL_SLOTTING.OPT_PALLET_PATTERN,
                    PICK_LOCN_DTL_SLOTTING.SI_NUM_1,
                    PICK_LOCN_DTL_SLOTTING.SI_NUM_2,
                    PICK_LOCN_DTL_SLOTTING.SI_NUM_3,
                    PICK_LOCN_DTL_SLOTTING.SI_NUM_4,
                    PICK_LOCN_DTL_SLOTTING.SI_NUM_5,
                    PICK_LOCN_DTL_SLOTTING.SI_NUM_6,
                    PICK_LOCN_DTL_SLOTTING.MULT_LOC_GRP,
                    PICK_LOCN_DTL_SLOTTING.REPLEN_GROUP,
                    PICK_LOCN_DTL_SLOTTING.CREATED_SOURCE_TYPE,
                    PICK_LOCN_DTL_SLOTTING.CREATED_SOURCE,
                    PICK_LOCN_DTL_SLOTTING.CREATED_DTTM,
                    PICK_LOCN_DTL_SLOTTING.LAST_UPDATED_SOURCE_TYPE,
                    PICK_LOCN_DTL_SLOTTING.LAST_UPDATED_SOURCE,
                    PICK_LOCN_DTL_SLOTTING.LAST_UPDATED_DTTM
                FROM {source_schema}.PICK_LOCN_DTL_SLOTTING
                WHERE  (trunc(CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""

    SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 24
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING_temp = SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING.toDF(*["SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___" + col for col in SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING.columns])
    
    EXPTRANS = SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING_temp.selectExpr( \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___PICK_LOCN_DTL_ID as PICK_LOCN_DTL_ID", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___HIST_MATCH as HIST_MATCH", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___CUR_ORIENTATION as CUR_ORIENTATION", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___IGN_FOR_RESLOT as IGN_FOR_RESLOT", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___REC_LANES as REC_LANES", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___REC_STACKING as REC_STACKING", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___HI_RESIDUAL_1 as HI_RESIDUAL_1", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___SI_NUM_1 as SI_NUM_1", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___SI_NUM_2 as SI_NUM_2", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___SI_NUM_3 as SI_NUM_3", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___SI_NUM_4 as SI_NUM_4", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___SI_NUM_5 as SI_NUM_5", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___SI_NUM_6 as SI_NUM_6", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___MULT_LOC_GRP as MULT_LOC_GRP", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___REPLEN_GROUP as REPLEN_GROUP", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_PICK_LOCN_DTL_SLOTTING___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE, type TARGET 
    # COLUMN COUNT: 24
    
    
    Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE = EXPTRANS.selectExpr(
    "CAST(DC_NBR AS SMALLINT) as DC_NBR",
    "CAST(PICK_LOCN_DTL_ID AS INT) as PICK_LOCN_DTL_ID",
    "CAST(HIST_MATCH AS STRING) as HIST_MATCH",
    "CAST(CUR_ORIENTATION AS STRING) as CUR_ORIENTATION",
    "CAST(IGN_FOR_RESLOT AS TINYINT) as IGN_FOR_RESLOT",
    "CAST(REC_LANES AS BIGINT) as REC_LANES",
    "CAST(REC_STACKING AS BIGINT) as REC_STACKING",
    "CAST(HI_RESIDUAL_1 AS TINYINT) as HI_RESIDUAL_1",
    "CAST(OPT_PALLET_PATTERN AS STRING) as OPT_PALLET_PATTERN",
    "CAST(SI_NUM_1 AS DECIMAL(13,4)) as SI_NUM_1",
    "CAST(SI_NUM_2 AS DECIMAL(13,4)) as SI_NUM_2",
    "CAST(SI_NUM_3 AS DECIMAL(13,4)) as SI_NUM_3",
    "CAST(SI_NUM_4 AS DECIMAL(13,4)) as SI_NUM_4",
    "CAST(SI_NUM_5 AS DECIMAL(13,4)) as SI_NUM_5",
    "CAST(SI_NUM_6 AS DECIMAL(13,4)) as SI_NUM_6",
    "CAST(MULT_LOC_GRP AS STRING) as MULT_LOC_GRP",
    "CAST(REPLEN_GROUP AS STRING) as REPLEN_GROUP",
    "CAST(CREATED_SOURCE_TYPE AS TINYINT) as CREATED_SOURCE_TYPE",
    "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
    "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
    "CAST(LAST_UPDATED_SOURCE_TYPE AS TINYINT) as LAST_UPDATED_SOURCE_TYPE",
    "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
    "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
    "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )

    
    overwriteDeltaPartition(Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE is written to the target table - " + target_table_name)
