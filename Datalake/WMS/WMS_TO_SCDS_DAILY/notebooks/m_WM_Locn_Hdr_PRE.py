#Code converted on 2023-06-26 10:16:27
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



def m_WM_Locn_Hdr_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Locn_Hdr_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_LOCN_HDR_PRE"

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
    # Processing node SQ_Shortcut_to_LOCN_HDR, type SOURCE 
    # COLUMN COUNT: 46

    SQ_Shortcut_to_LOCN_HDR = jdbcOracleConnection(  f"""SELECT
    LOCN_HDR.LOCN_ID,
    LOCN_HDR.WHSE,
    LOCN_HDR.LOCN_CLASS,
    LOCN_HDR.LOCN_BRCD,
    LOCN_HDR.AREA,
    LOCN_HDR.ZONE,
    LOCN_HDR.AISLE,
    LOCN_HDR.BAY,
    LOCN_HDR.LVL,
    LOCN_HDR.POSN,
    LOCN_HDR.DSP_LOCN,
    LOCN_HDR.LOCN_PICK_SEQ,
    LOCN_HDR.SKU_DEDCTN_TYPE,
    LOCN_HDR.SLOT_TYPE,
    LOCN_HDR.PUTWY_ZONE,
    LOCN_HDR.PULL_ZONE,
    LOCN_HDR.PICK_DETRM_ZONE,
    LOCN_HDR.LEN,
    LOCN_HDR.WIDTH,
    LOCN_HDR.HT,
    LOCN_HDR.X_COORD,
    LOCN_HDR.Y_COORD,
    LOCN_HDR.Z_COORD,
    LOCN_HDR.WORK_GRP,
    LOCN_HDR.WORK_AREA,
    LOCN_HDR.LAST_FROZN_DATE_TIME,
    LOCN_HDR.LAST_CNT_DATE_TIME,
    LOCN_HDR.CYCLE_CNT_PENDING,
    LOCN_HDR.PRT_LABEL_FLAG,
    LOCN_HDR.TRAVEL_AISLE,
    LOCN_HDR.TRAVEL_ZONE,
    LOCN_HDR.STORAGE_UOM,
    LOCN_HDR.PICK_UOM,
    LOCN_HDR.CREATE_DATE_TIME,
    LOCN_HDR.MOD_DATE_TIME,
    LOCN_HDR.USER_ID,
    LOCN_HDR.SLOT_UNUSABLE,
    LOCN_HDR.CHECK_DIGIT,
    LOCN_HDR.VOCO_INTRNL_REVERSE_BRCD,
    LOCN_HDR.LOCN_HDR_ID,
    LOCN_HDR.WM_VERSION_ID,
    LOCN_HDR.LOCN_PUTWY_SEQ,
    LOCN_HDR.LOCN_DYN_ASSGN_SEQ,
    LOCN_HDR.CREATED_DTTM,
    LOCN_HDR.LAST_UPDATED_DTTM,
    LOCN_HDR.FACILITY_ID
    FROM {source_schema}.LOCN_HDR
    WHERE (trunc(LOCN_HDR.CREATE_DATE_TIME) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (trunc(LOCN_HDR.MOD_DATE_TIME) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (trunc(LOCN_HDR.CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) OR (trunc(LOCN_HDR.LAST_UPDATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 1) AND 
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 48

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LOCN_HDR_temp = SQ_Shortcut_to_LOCN_HDR.toDF(*["SQ_Shortcut_to_LOCN_HDR___" + col for col in SQ_Shortcut_to_LOCN_HDR.columns])

    EXPTRANS = SQ_Shortcut_to_LOCN_HDR_temp.selectExpr( \
        "SQ_Shortcut_to_LOCN_HDR___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_LOCN_HDR___LOCN_ID as LOCN_ID", \
        "SQ_Shortcut_to_LOCN_HDR___WHSE as WHSE", \
        "SQ_Shortcut_to_LOCN_HDR___LOCN_CLASS as LOCN_CLASS", \
        "SQ_Shortcut_to_LOCN_HDR___LOCN_BRCD as LOCN_BRCD", \
        "SQ_Shortcut_to_LOCN_HDR___AREA as AREA", \
        "SQ_Shortcut_to_LOCN_HDR___ZONE as ZONE", \
        "SQ_Shortcut_to_LOCN_HDR___AISLE as AISLE", \
        "SQ_Shortcut_to_LOCN_HDR___BAY as BAY", \
        "SQ_Shortcut_to_LOCN_HDR___LVL as LVL", \
        "SQ_Shortcut_to_LOCN_HDR___POSN as POSN", \
        "SQ_Shortcut_to_LOCN_HDR___DSP_LOCN as DSP_LOCN", \
        "SQ_Shortcut_to_LOCN_HDR___LOCN_PICK_SEQ as LOCN_PICK_SEQ", \
        "SQ_Shortcut_to_LOCN_HDR___SKU_DEDCTN_TYPE as SKU_DEDCTN_TYPE", \
        "SQ_Shortcut_to_LOCN_HDR___SLOT_TYPE as SLOT_TYPE", \
        "SQ_Shortcut_to_LOCN_HDR___PUTWY_ZONE as PUTWY_ZONE", \
        "SQ_Shortcut_to_LOCN_HDR___PULL_ZONE as PULL_ZONE", \
        "SQ_Shortcut_to_LOCN_HDR___PICK_DETRM_ZONE as PICK_DETRM_ZONE", \
        "SQ_Shortcut_to_LOCN_HDR___LEN as LEN", \
        "SQ_Shortcut_to_LOCN_HDR___WIDTH as WIDTH", \
        "SQ_Shortcut_to_LOCN_HDR___HT as HT", \
        "SQ_Shortcut_to_LOCN_HDR___X_COORD as X_COORD", \
        "SQ_Shortcut_to_LOCN_HDR___Y_COORD as Y_COORD", \
        "SQ_Shortcut_to_LOCN_HDR___Z_COORD as Z_COORD", \
        "SQ_Shortcut_to_LOCN_HDR___WORK_GRP as WORK_GRP", \
        "SQ_Shortcut_to_LOCN_HDR___WORK_AREA as WORK_AREA", \
        "SQ_Shortcut_to_LOCN_HDR___LAST_FROZN_DATE_TIME as LAST_FROZN_DATE_TIME", \
        "SQ_Shortcut_to_LOCN_HDR___LAST_CNT_DATE_TIME as LAST_CNT_DATE_TIME", \
        "SQ_Shortcut_to_LOCN_HDR___CYCLE_CNT_PENDING as CYCLE_CNT_PENDING", \
        "SQ_Shortcut_to_LOCN_HDR___PRT_LABEL_FLAG as PRT_LABEL_FLAG", \
        "SQ_Shortcut_to_LOCN_HDR___TRAVEL_AISLE as TRAVEL_AISLE", \
        "SQ_Shortcut_to_LOCN_HDR___TRAVEL_ZONE as TRAVEL_ZONE", \
        "SQ_Shortcut_to_LOCN_HDR___STORAGE_UOM as STORAGE_UOM", \
        "SQ_Shortcut_to_LOCN_HDR___PICK_UOM as PICK_UOM", \
        "SQ_Shortcut_to_LOCN_HDR___CREATE_DATE_TIME as CREATE_DATE_TIME", \
        "SQ_Shortcut_to_LOCN_HDR___MOD_DATE_TIME as MOD_DATE_TIME", \
        "SQ_Shortcut_to_LOCN_HDR___USER_ID as USER_ID", \
        "SQ_Shortcut_to_LOCN_HDR___SLOT_UNUSABLE as SLOT_UNUSABLE", \
        "SQ_Shortcut_to_LOCN_HDR___CHECK_DIGIT as CHECK_DIGIT", \
        "SQ_Shortcut_to_LOCN_HDR___VOCO_INTRNL_REVERSE_BRCD as VOCO_INTRNL_REVERSE_BRCD", \
        "SQ_Shortcut_to_LOCN_HDR___LOCN_HDR_ID as LOCN_HDR_ID", \
        "SQ_Shortcut_to_LOCN_HDR___WM_VERSION_ID as WM_VERSION_ID", \
        "SQ_Shortcut_to_LOCN_HDR___LOCN_PUTWY_SEQ as LOCN_PUTWY_SEQ", \
        "SQ_Shortcut_to_LOCN_HDR___LOCN_DYN_ASSGN_SEQ as LOCN_DYN_ASSGN_SEQ", \
        "SQ_Shortcut_to_LOCN_HDR___CREATED_DTTM as CREATED_DTTM", \
        "SQ_Shortcut_to_LOCN_HDR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
        "SQ_Shortcut_to_LOCN_HDR___FACILITY_ID as FACILITY_ID", \
        "CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_LOCN_HDR_PRE, type TARGET 
    # COLUMN COUNT: 48


    Shortcut_to_WM_LOCN_HDR_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(LOCN_HDR_ID AS INT) as LOCN_HDR_ID",
        "CAST(LOCN_ID AS STRING) as LOCN_ID",
        "CAST(WHSE AS STRING) as WHSE",
        "CAST(LOCN_CLASS AS STRING) as LOCN_CLASS",
        "CAST(LOCN_BRCD AS STRING) as LOCN_BRCD",
        "CAST(AREA AS STRING) as AREA",
        "CAST(ZONE AS STRING) as ZONE",
        "CAST(AISLE AS STRING) as AISLE",
        "CAST(BAY AS STRING) as BAY",
        "CAST(LVL AS STRING) as LVL",
        "CAST(POSN AS STRING) as POSN",
        "CAST(DSP_LOCN AS STRING) as DSP_LOCN",
        "CAST(LOCN_PICK_SEQ AS STRING) as LOCN_PICK_SEQ",
        "CAST(SKU_DEDCTN_TYPE AS STRING) as SKU_DEDCTN_TYPE",
        "CAST(SLOT_TYPE AS STRING) as SLOT_TYPE",
        "CAST(PUTWY_ZONE AS STRING) as PUTWY_ZONE",
        "CAST(PULL_ZONE AS STRING) as PULL_ZONE",
        "CAST(PICK_DETRM_ZONE AS STRING) as PICK_DETRM_ZONE",
        "CAST(LEN AS DECIMAL(16,4)) as LEN",
        "CAST(WIDTH AS DECIMAL(16,4)) as WIDTH",
        "CAST(HT AS DECIMAL(16,4)) as HT",
        "CAST(X_COORD AS DECIMAL(13,5)) as X_COORD",
        "CAST(Y_COORD AS DECIMAL(13,5)) as Y_COORD",
        "CAST(Z_COORD AS DECIMAL(13,5)) as Z_COORD",
        "CAST(WORK_GRP AS STRING) as WORK_GRP",
        "CAST(WORK_AREA AS STRING) as WORK_AREA",
        "CAST(LAST_FROZN_DATE_TIME AS TIMESTAMP) as LAST_FROZN_DATE_TIME",
        "CAST(LAST_CNT_DATE_TIME AS TIMESTAMP) as LAST_CNT_DATE_TIME",
        "CAST(CYCLE_CNT_PENDING AS STRING) as CYCLE_CNT_PENDIN",
        "CAST(PRT_LABEL_FLAG AS STRING) as PRT_LABEL_FLAG",
        "CAST(TRAVEL_AISLE AS STRING) as TRAVEL_AISLE",
        "CAST(TRAVEL_ZONE AS STRING) as TRAVEL_ZONE",
        "CAST(STORAGE_UOM AS STRING) as STORAGE_UOM",
        "CAST(PICK_UOM AS STRING) as PICK_UOM",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(SLOT_UNUSABLE AS STRING) as SLOT_UNUSABLE",
        "CAST(CHECK_DIGIT AS STRING) as CHECK_DIGIT",
        "CAST(VOCO_INTRNL_REVERSE_BRCD AS STRING) as VOCO_INTRNL_REVERSE_BRCD",
        "CAST(WM_VERSION_ID AS INT) as WM_VERSION_ID",
        "CAST(LOCN_PUTWY_SEQ AS STRING) as LOCN_PUTWY_SEQ",
        "CAST(LOCN_DYN_ASSGN_SEQ AS STRING) as LOCN_DYN_ASSGN_SEQ",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(FACILITY_ID AS INT) as FACILITY_ID",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )


    overwriteDeltaPartition(Shortcut_to_WM_LOCN_HDR_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_LOCN_HDR_PRE is written to the target table - "
        + target_table_name
    )    