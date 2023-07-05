#Code converted on 2023-06-26 10:18:20
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
from logging import getLogger, INFO



def m_WM_Labor_Msg_Dtl_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Labor_Msg_Dtl_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_LABOR_MSG_DTL_PRE"

    schemaName = raw
    source_schema = "WMSMIS"


    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]


    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now() #start timestamp of the script

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)

    # COMMAND ----------
    # Variable_declaration_comment
    dcnbr = dcnbr.strip()[2:]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)

    # COMMAND ----------
    # Processing node SQ_Shortcut_to_LABOR_MSG_DTL, type SOURCE 
    # COLUMN COUNT: 67

    SQ_Shortcut_to_LABOR_MSG_DTL = jdbcOracleConnection(  f"""SELECT
    LABOR_MSG_DTL.LABOR_MSG_DTL_ID,
    LABOR_MSG_DTL.LABOR_MSG_ID,
    LABOR_MSG_DTL.TRAN_NBR,
    LABOR_MSG_DTL.TRAN_SEQ_NBR,
    LABOR_MSG_DTL.MSG_STAT_CODE,
    LABOR_MSG_DTL.SEQ_NBR,
    LABOR_MSG_DTL.HNDL_ATTR,
    LABOR_MSG_DTL.QTY_PER_GRAB,
    LABOR_MSG_DTL.LOCN_CLASS,
    LABOR_MSG_DTL.QTY,
    LABOR_MSG_DTL.TC_ILPN_ID,
    LABOR_MSG_DTL.TC_OLPN_ID,
    LABOR_MSG_DTL.PALLET_ID,
    LABOR_MSG_DTL.LOCN_SLOT_TYPE,
    LABOR_MSG_DTL.LOCN_X_COORD,
    LABOR_MSG_DTL.LOCN_Y_COORD,
    LABOR_MSG_DTL.LOCN_Z_COORD,
    LABOR_MSG_DTL.LOCN_TRAV_AISLE,
    LABOR_MSG_DTL.LOCN_TRAV_ZONE,
    LABOR_MSG_DTL.BOX_QTY,
    LABOR_MSG_DTL.INNERPACK_QTY,
    LABOR_MSG_DTL.PACK_QTY,
    LABOR_MSG_DTL.CASE_QTY,
    LABOR_MSG_DTL.TIER_QTY,
    LABOR_MSG_DTL.PALLET_QTY,
    LABOR_MSG_DTL.MISC,
    LABOR_MSG_DTL.CO,
    LABOR_MSG_DTL.DIV,
    LABOR_MSG_DTL.DSP_LOCN,
    LABOR_MSG_DTL.ITEM_BAR_CODE,
    LABOR_MSG_DTL.WEIGHT,
    LABOR_MSG_DTL.VOLUME,
    LABOR_MSG_DTL.CRIT_DIM1,
    LABOR_MSG_DTL.CRIT_DIM2,
    LABOR_MSG_DTL.CRIT_DIM3,
    LABOR_MSG_DTL.START_DATE_TIME,
    LABOR_MSG_DTL.HANDLING_UOM,
    LABOR_MSG_DTL.SEASON,
    LABOR_MSG_DTL.SEASON_YR,
    LABOR_MSG_DTL.STYLE,
    LABOR_MSG_DTL.STYLE_SFX,
    LABOR_MSG_DTL.COLOR,
    LABOR_MSG_DTL.COLOR_SFX,
    LABOR_MSG_DTL.SEC_DIM,
    LABOR_MSG_DTL.QUAL,
    LABOR_MSG_DTL.SIZE_RNGE_CODE,
    LABOR_MSG_DTL.SIZE_REL_POSN_IN_TABLE,
    LABOR_MSG_DTL.MISC_NUM_1,
    LABOR_MSG_DTL.MISC_NUM_2,
    LABOR_MSG_DTL.MISC_2,
    LABOR_MSG_DTL.LOADED,
    LABOR_MSG_DTL.PUTAWAY_ZONE,
    LABOR_MSG_DTL.PICK_DETERMINATION_ZONE,
    LABOR_MSG_DTL.WORK_GROUP,
    LABOR_MSG_DTL.WORK_AREA,
    LABOR_MSG_DTL.PULL_ZONE,
    LABOR_MSG_DTL.ASSIGNMENT_ZONE,
    LABOR_MSG_DTL.CREATED_SOURCE_TYPE,
    LABOR_MSG_DTL.CREATED_SOURCE,
    LABOR_MSG_DTL.CREATED_DTTM,
    LABOR_MSG_DTL.LAST_UPDATED_SOURCE_TYPE,
    LABOR_MSG_DTL.LAST_UPDATED_SOURCE,
    LABOR_MSG_DTL.LAST_UPDATED_DTTM,
    LABOR_MSG_DTL.HIBERNATE_VERSION,
    LABOR_MSG_DTL.ITEM_NAME,
    LABOR_MSG_DTL.LOCN_GRP_ATTR,
    LABOR_MSG_DTL.RESOURCE_GROUP_ID
    FROM {source_schema}.LABOR_MSG_DTL
    WHERE (trunc(LABOR_MSG_DTL.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(LABOR_MSG_DTL.LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)  AND
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 69

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LABOR_MSG_DTL_temp = SQ_Shortcut_to_LABOR_MSG_DTL.toDF(*["SQ_Shortcut_to_LABOR_MSG_DTL___" + col for col in SQ_Shortcut_to_LABOR_MSG_DTL.columns])

    EXPTRANS = SQ_Shortcut_to_LABOR_MSG_DTL_temp.selectExpr( \
        "SQ_Shortcut_to_LABOR_MSG_DTL___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LABOR_MSG_DTL_ID as LABOR_MSG_DTL_ID", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LABOR_MSG_ID as LABOR_MSG_ID", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___TRAN_NBR as TRAN_NBR", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___TRAN_SEQ_NBR as TRAN_SEQ_NBR", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___MSG_STAT_CODE as MSG_STAT_CODE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___SEQ_NBR as SEQ_NBR", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___HNDL_ATTR as HNDL_ATTR", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___QTY_PER_GRAB as QTY_PER_GRAB", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LOCN_CLASS as LOCN_CLASS", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___QTY as QTY", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___TC_ILPN_ID as TC_ILPN_ID", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___TC_OLPN_ID as TC_OLPN_ID", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___PALLET_ID as PALLET_ID", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LOCN_SLOT_TYPE as LOCN_SLOT_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LOCN_X_COORD as LOCN_X_COORD", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LOCN_Y_COORD as LOCN_Y_COORD", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LOCN_Z_COORD as LOCN_Z_COORD", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LOCN_TRAV_AISLE as LOCN_TRAV_AISLE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LOCN_TRAV_ZONE as LOCN_TRAV_ZONE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___BOX_QTY as BOX_QTY", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___INNERPACK_QTY as INNERPACK_QTY", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___PACK_QTY as PACK_QTY", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___CASE_QTY as CASE_QTY", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___TIER_QTY as TIER_QTY", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___PALLET_QTY as PALLET_QTY", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___MISC as MISC", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___CO as CO", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___DIV as DIV", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___DSP_LOCN as DSP_LOCN", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___ITEM_BAR_CODE as ITEM_BAR_CODE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___WEIGHT as WEIGHT", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___VOLUME as VOLUME", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___CRIT_DIM1 as CRIT_DIM1", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___CRIT_DIM2 as CRIT_DIM2", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___CRIT_DIM3 as CRIT_DIM3", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___START_DATE_TIME as START_DATE_TIME", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___HANDLING_UOM as HANDLING_UOM", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___SEASON as SEASON", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___SEASON_YR as SEASON_YR", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___STYLE as STYLE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___STYLE_SFX as STYLE_SFX", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___COLOR as COLOR", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___COLOR_SFX as COLOR_SFX", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___SEC_DIM as SEC_DIM", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___QUAL as QUAL", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___SIZE_RNGE_CODE as SIZE_RNGE_CODE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___MISC_NUM_1 as MISC_NUM_1", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___MISC_NUM_2 as MISC_NUM_2", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___MISC_2 as MISC_2", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LOADED as LOADED", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___PUTAWAY_ZONE as PUTAWAY_ZONE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___PICK_DETERMINATION_ZONE as PICK_DETERMINATION_ZONE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___WORK_GROUP as WORK_GROUP", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___WORK_AREA as WORK_AREA", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___PULL_ZONE as PULL_ZONE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___ASSIGNMENT_ZONE as ASSIGNMENT_ZONE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___CREATED_SOURCE as CREATED_SOURCE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___HIBERNATE_VERSION as HIBERNATE_VERSION", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___ITEM_NAME as ITEM_NAME", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___LOCN_GRP_ATTR as LOCN_GRP_ATTR", \
        "SQ_Shortcut_to_LABOR_MSG_DTL___RESOURCE_GROUP_ID as RESOURCE_GROUP_ID", \
        "CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_LABOR_MSG_DTL_PRE, type TARGET 
    # COLUMN COUNT: 69


    Shortcut_to_WM_LABOR_MSG_DTL_PRE = EXPTRANS.selectExpr( \
        "CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", \
        "CAST(LABOR_MSG_DTL_ID AS BIGINT) as LABOR_MSG_DTL_ID", \
        "CAST(LABOR_MSG_ID AS BIGINT) as LABOR_MSG_ID", \
        "CAST(TRAN_NBR AS BIGINT) as TRAN_NBR", \
        "CAST(TRAN_SEQ_NBR AS BIGINT) as TRAN_SEQ_NBR", \
        "CAST(MSG_STAT_CODE AS STRING) as MSG_STAT_CODE", \
        "CAST(SEQ_NBR AS STRING) as SEQ_NBR", \
        "CAST(HNDL_ATTR AS STRING) as HNDL_ATTR", \
        "CAST(QTY_PER_GRAB AS BIGINT) as QTY_PER_GRAB", \
        "CAST(LOCN_CLASS AS STRING) as LOCN_CLASS", \
        "CAST(QTY AS BIGINT) as QTY", \
        "CAST(TC_ILPN_ID AS STRING) as TC_ILPN_ID", \
        "CAST(TC_OLPN_ID AS STRING) as TC_OLPN_ID", \
        "CAST(PALLET_ID AS STRING) as PALLET_ID", \
        "CAST(LOCN_SLOT_TYPE AS STRING) as LOCN_SLOT_TYPE", \
        "CAST(LOCN_X_COORD AS BIGINT) as LOCN_X_COORD", \
        "CAST(LOCN_Y_COORD AS BIGINT) as LOCN_Y_COORD", \
        "CAST(LOCN_Z_COORD AS BIGINT) as LOCN_Z_COORD", \
        "CAST(LOCN_TRAV_AISLE AS STRING) as LOCN_TRAV_AISLE", \
        "CAST(LOCN_TRAV_ZONE AS STRING) as LOCN_TRAV_ZONE", \
        "CAST(BOX_QTY AS BIGINT) as BOX_QTY", \
        "CAST(INNERPACK_QTY AS BIGINT) as INNERPACK_QTY", \
        "CAST(PACK_QTY AS BIGINT) as PACK_QTY", \
        "CAST(CASE_QTY AS BIGINT) as CASE_QTY", \
        "CAST(TIER_QTY AS BIGINT) as TIER_QTY", \
        "CAST(PALLET_QTY AS BIGINT) as PALLET_QTY", \
        "CAST(MISC AS STRING) as MISC", \
        "CAST(CO AS STRING) as CO", \
        "CAST(DIV AS STRING) as DIV", \
        "CAST(DSP_LOCN AS STRING) as DSP_LOCN", \
        "CAST(ITEM_BAR_CODE AS STRING) as ITEM_BAR_CODE", \
        "CAST(WEIGHT AS BIGINT) as WEIGHT", \
        "CAST(VOLUME AS BIGINT) as VOLUME", \
        "CAST(CRIT_DIM1 AS BIGINT) as CRIT_DIM1", \
        "CAST(CRIT_DIM2 AS BIGINT) as CRIT_DIM2", \
        "CAST(CRIT_DIM3 AS BIGINT) as CRIT_DIM3", \
        "CAST(START_DATE_TIME AS TIMESTAMP) as START_DATE_TIME", \
        "CAST(HANDLING_UOM AS STRING) as HANDLING_UOM", \
        "CAST(SEASON AS STRING) as SEASON", \
        "CAST(SEASON_YR AS STRING) as SEASON_YR", \
        "CAST(STYLE AS STRING) as STYLE", \
        "CAST(STYLE_SFX AS STRING) as STYLE_SFX", \
        "CAST(COLOR AS STRING) as COLOR", \
        "CAST(COLOR_SFX AS STRING) as COLOR_SFX", \
        "CAST(SEC_DIM AS STRING) as SEC_DIM", \
        "CAST(QUAL AS STRING) as QUAL", \
        "CAST(SIZE_RNGE_CODE AS STRING) as SIZE_RNGE_CODE", \
        "CAST(SIZE_REL_POSN_IN_TABLE AS STRING) as SIZE_REL_POSN_IN_TABLE", \
        "CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1", \
        "CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2", \
        "CAST(MISC_2 AS STRING) as MISC_2", \
        "CAST(LOADED AS STRING) as LOADED", \
        "CAST(PUTAWAY_ZONE AS STRING) as PUTAWAY_ZONE", \
        "CAST(PICK_DETERMINATION_ZONE AS STRING) as PICK_DETERMINATION_ZONE", \
        "CAST(WORK_GROUP AS STRING) as WORK_GROUP", \
        "CAST(WORK_AREA AS STRING) as WORK_AREA", \
        "CAST(PULL_ZONE AS STRING) as PULL_ZONE", \
        "CAST(ASSIGNMENT_ZONE AS STRING) as ASSIGNMENT_ZONE", \
        "CAST(CREATED_SOURCE_TYPE AS BIGINT) as CREATED_SOURCE_TYPE", \
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", \
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
        "CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as LAST_UPDATED_SOURCE_TYPE", \
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", \
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION", \
        "CAST(ITEM_NAME AS STRING) as ITEM_NAME", \
        "CAST(LOCN_GRP_ATTR AS STRING) as LOCN_GRP_ATTR", \
        "CAST(RESOURCE_GROUP_ID AS STRING) as RESOURCE_GROUP_ID", \
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_LABOR_MSG_DTL_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE is written to the target table - "
        + target_table_name
    )