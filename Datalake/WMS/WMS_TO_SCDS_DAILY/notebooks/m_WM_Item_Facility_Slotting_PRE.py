#Code converted on 2023-06-21 18:42:31
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from logging import getLogger, INFO



def m_WM_Item_Facility_Slotting_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_Item_Facility_Slotting_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_ITEM_FACILITY_SLOTTING_PRE"

    schemaName = raw
    source_schema = "WMSMIS"


    target_table_name = schemaName + "." + tableName

    refine_table_name = tableName[:-4]


    # Set global variables
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")

    starttime = datetime.now() #start timestamp of the script


    # COMMAND ----------
    # Variable_declaration_comment
    dcnbr = dcnbr.strip()[2:]
    Prev_Run_Dt=genPrevRunDt(refine_table_name, refine,raw)

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)
    # COMMAND ----------
    # Processing node SQ_Shortcut_to_ITEM_FACILITY_SLOTTING, type SOURCE 
    # COLUMN COUNT: 91

    SQ_Shortcut_to_ITEM_FACILITY_SLOTTING = jdbcOracleConnection(  f"""SELECT
    ITEM_FACILITY_SLOTTING.ITEM_FACILITY_MAPPING_ID,
    ITEM_FACILITY_SLOTTING.ITEM_ID,
    ITEM_FACILITY_SLOTTING.ITEM_NAME,
    ITEM_FACILITY_SLOTTING.WHSE_CODE,
    ITEM_FACILITY_SLOTTING.UNIT_MEAS,
    ITEM_FACILITY_SLOTTING.MANUAL,
    ITEM_FACILITY_SLOTTING.IS_NEW,
    ITEM_FACILITY_SLOTTING.ALLOW_SLU,
    ITEM_FACILITY_SLOTTING.ALLOW_SU,
    ITEM_FACILITY_SLOTTING.DISC_TRANS,
    ITEM_FACILITY_SLOTTING.EACH_PER_BIN,
    ITEM_FACILITY_SLOTTING.EX_RECPT_DATE,
    ITEM_FACILITY_SLOTTING.DISCONT,
    ITEM_FACILITY_SLOTTING.PROCESSED,
    ITEM_FACILITY_SLOTTING.INN_PER_CS,
    ITEM_FACILITY_SLOTTING.MAX_STACKING,
    ITEM_FACILITY_SLOTTING.MAX_LANES,
    ITEM_FACILITY_SLOTTING.NUM_UNITS_PER_BIN,
    ITEM_FACILITY_SLOTTING.RESERVED_1,
    ITEM_FACILITY_SLOTTING.RESERVED_2,
    ITEM_FACILITY_SLOTTING.RESERVED_3,
    ITEM_FACILITY_SLOTTING.RESERVED_4,
    ITEM_FACILITY_SLOTTING.CREATE_DATE_TIME,
    ITEM_FACILITY_SLOTTING.MOD_DATE_TIME,
    ITEM_FACILITY_SLOTTING.MOD_USER,
    ITEM_FACILITY_SLOTTING.RESERVE_RACK_TYPE_ID,
    ITEM_FACILITY_SLOTTING.ALLOW_RESERVE,
    ITEM_FACILITY_SLOTTING.PALPAT_RESERVE,
    ITEM_FACILITY_SLOTTING.THREE_D_CALC_DONE,
    ITEM_FACILITY_SLOTTING.MAX_SLOTS,
    ITEM_FACILITY_SLOTTING.MAX_PALLET_STACKING,
    ITEM_FACILITY_SLOTTING.PROP_BORROWING_OBJECT,
    ITEM_FACILITY_SLOTTING.PROP_BORROWING_SPECIFIC,
    ITEM_FACILITY_SLOTTING.HEIGHT_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.LENGTH_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.WIDTH_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.WEIGHT_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.VEND_PACK_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.INNER_PACK_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.TI_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.HI_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.QTY_PER_GRAB_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.HAND_ATTR_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.FAM_GRP_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.COMMODITY_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.CRUSHABILITY_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.HAZARD_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.VEND_CODE_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.MISC1_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.MISC2_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.MISC3_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.MISC4_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.MISC5_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.MISC6_CAN_BORROW,
    ITEM_FACILITY_SLOTTING.HEIGHT_BORROWED,
    ITEM_FACILITY_SLOTTING.LENGTH_BORROWED,
    ITEM_FACILITY_SLOTTING.WIDTH_BORROWED,
    ITEM_FACILITY_SLOTTING.WEIGHT_BORROWED,
    ITEM_FACILITY_SLOTTING.VEND_PACK_BORROWED,
    ITEM_FACILITY_SLOTTING.INNER_PACK_BORROWED,
    ITEM_FACILITY_SLOTTING.TI_BORROWED,
    ITEM_FACILITY_SLOTTING.HI_BORROWED,
    ITEM_FACILITY_SLOTTING.QTY_PER_GRAB_BORROWED,
    ITEM_FACILITY_SLOTTING.HAND_ATTR_BORROWED,
    ITEM_FACILITY_SLOTTING.FAM_GRP_BORROWED,
    ITEM_FACILITY_SLOTTING.COMMODITY_BORROWED,
    ITEM_FACILITY_SLOTTING.CRUSHABILITY_BORROWED,
    ITEM_FACILITY_SLOTTING.HAZARD_BORROWED,
    ITEM_FACILITY_SLOTTING.VEND_CODE_BORROWED,
    ITEM_FACILITY_SLOTTING.MISC1_BORROWED,
    ITEM_FACILITY_SLOTTING.MISC2_BORROWED,
    ITEM_FACILITY_SLOTTING.MISC3_BORROWED,
    ITEM_FACILITY_SLOTTING.MISC4_BORROWED,
    ITEM_FACILITY_SLOTTING.MISC5_BORROWED,
    ITEM_FACILITY_SLOTTING.MISC6_BORROWED,
    ITEM_FACILITY_SLOTTING.AFRAME_HT,
    ITEM_FACILITY_SLOTTING.AFRAME_LEN,
    ITEM_FACILITY_SLOTTING.AFRAME_WID,
    ITEM_FACILITY_SLOTTING.AFRAME_WT,
    ITEM_FACILITY_SLOTTING.AFRAME_ALLOW,
    ITEM_FACILITY_SLOTTING.ITEM_NUM_1,
    ITEM_FACILITY_SLOTTING.ITEM_NUM_2,
    ITEM_FACILITY_SLOTTING.ITEM_NUM_3,
    ITEM_FACILITY_SLOTTING.ITEM_NUM_4,
    ITEM_FACILITY_SLOTTING.ITEM_NUM_5,
    ITEM_FACILITY_SLOTTING.ITEM_CHAR_1,
    ITEM_FACILITY_SLOTTING.ITEM_CHAR_2,
    ITEM_FACILITY_SLOTTING.ITEM_CHAR_3,
    ITEM_FACILITY_SLOTTING.ITEM_CHAR_4,
    ITEM_FACILITY_SLOTTING.ITEM_CHAR_5,
    ITEM_FACILITY_SLOTTING.SLOTTING_GROUP
    FROM {source_schema}.ITEM_FACILITY_SLOTTING
    WHERE (trunc(CREATE_DATE_TIME)>= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (trunc(MOD_DATE_TIME)>= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) AND 
    1=1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 93

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_ITEM_FACILITY_SLOTTING_temp = SQ_Shortcut_to_ITEM_FACILITY_SLOTTING.toDF(*["SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___" + col for col in SQ_Shortcut_to_ITEM_FACILITY_SLOTTING.columns])

    EXPTRANS = SQ_Shortcut_to_ITEM_FACILITY_SLOTTING_temp.selectExpr( \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___sys_row_id as sys_row_id", \
        f"{dcnbr} as DC_NBR_EXP", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_FACILITY_MAPPING_ID as ITEM_FACILITY_MAPPING_ID", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_ID as ITEM_ID", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_NAME as ITEM_NAME", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___WHSE_CODE as WHSE_CODE", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___UNIT_MEAS as UNIT_MEAS", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MANUAL as MANUAL", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___IS_NEW as IS_NEW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ALLOW_SLU as ALLOW_SLU", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ALLOW_SU as ALLOW_SU", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___DISC_TRANS as DISC_TRANS", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___EACH_PER_BIN as EACH_PER_BIN", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___EX_RECPT_DATE as EX_RECPT_DATE", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___DISCONT as DISCONT", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___PROCESSED as PROCESSED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___INN_PER_CS as INN_PER_CS", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MAX_STACKING as MAX_STACKING", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MAX_LANES as MAX_LANES", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___NUM_UNITS_PER_BIN as NUM_UNITS_PER_BIN", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___RESERVED_1 as RESERVED_1", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___RESERVED_2 as RESERVED_2", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___RESERVED_3 as RESERVED_3", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___RESERVED_4 as RESERVED_4", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___CREATE_DATE_TIME as CREATE_DATE_TIME", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MOD_DATE_TIME as MOD_DATE_TIME", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MOD_USER as MOD_USER", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___RESERVE_RACK_TYPE_ID as RESERVE_RACK_TYPE_ID", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ALLOW_RESERVE as ALLOW_RESERVE", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___PALPAT_RESERVE as PALPAT_RESERVE", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___THREE_D_CALC_DONE as THREE_D_CALC_DONE", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MAX_SLOTS as MAX_SLOTS", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MAX_PALLET_STACKING as MAX_PALLET_STACKING", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___PROP_BORROWING_OBJECT as PROP_BORROWING_OBJECT", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___PROP_BORROWING_SPECIFIC as PROP_BORROWING_SPECIFIC", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___HEIGHT_CAN_BORROW as HEIGHT_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___LENGTH_CAN_BORROW as LENGTH_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___WIDTH_CAN_BORROW as WIDTH_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___WEIGHT_CAN_BORROW as WEIGHT_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___VEND_PACK_CAN_BORROW as VEND_PACK_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___INNER_PACK_CAN_BORROW as INNER_PACK_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___TI_CAN_BORROW as TI_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___HI_CAN_BORROW as HI_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___QTY_PER_GRAB_CAN_BORROW as QTY_PER_GRAB_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___HAND_ATTR_CAN_BORROW as HAND_ATTR_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___FAM_GRP_CAN_BORROW as FAM_GRP_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___COMMODITY_CAN_BORROW as COMMODITY_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___CRUSHABILITY_CAN_BORROW as CRUSHABILITY_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___HAZARD_CAN_BORROW as HAZARD_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___VEND_CODE_CAN_BORROW as VEND_CODE_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC1_CAN_BORROW as MISC1_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC2_CAN_BORROW as MISC2_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC3_CAN_BORROW as MISC3_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC4_CAN_BORROW as MISC4_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC5_CAN_BORROW as MISC5_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC6_CAN_BORROW as MISC6_CAN_BORROW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___HEIGHT_BORROWED as HEIGHT_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___LENGTH_BORROWED as LENGTH_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___WIDTH_BORROWED as WIDTH_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___WEIGHT_BORROWED as WEIGHT_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___VEND_PACK_BORROWED as VEND_PACK_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___INNER_PACK_BORROWED as INNER_PACK_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___TI_BORROWED as TI_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___HI_BORROWED as HI_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___QTY_PER_GRAB_BORROWED as QTY_PER_GRAB_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___HAND_ATTR_BORROWED as HAND_ATTR_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___FAM_GRP_BORROWED as FAM_GRP_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___COMMODITY_BORROWED as COMMODITY_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___CRUSHABILITY_BORROWED as CRUSHABILITY_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___HAZARD_BORROWED as HAZARD_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___VEND_CODE_BORROWED as VEND_CODE_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC1_BORROWED as MISC1_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC2_BORROWED as MISC2_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC3_BORROWED as MISC3_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC4_BORROWED as MISC4_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC5_BORROWED as MISC5_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___MISC6_BORROWED as MISC6_BORROWED", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___AFRAME_HT as AFRAME_HT", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___AFRAME_LEN as AFRAME_LEN", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___AFRAME_WID as AFRAME_WID", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___AFRAME_WT as AFRAME_WT", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___AFRAME_ALLOW as AFRAME_ALLOW", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_NUM_1 as ITEM_NUM_1", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_NUM_2 as ITEM_NUM_2", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_NUM_3 as ITEM_NUM_3", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_NUM_4 as ITEM_NUM_4", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_NUM_5 as ITEM_NUM_5", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_CHAR_1 as ITEM_CHAR_1", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_CHAR_2 as ITEM_CHAR_2", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_CHAR_3 as ITEM_CHAR_3", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_CHAR_4 as ITEM_CHAR_4", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___ITEM_CHAR_5 as ITEM_CHAR_5", \
        "SQ_Shortcut_to_ITEM_FACILITY_SLOTTING___SLOTTING_GROUP as SLOTTING_GROUP", \
        "CURRENT_TIMESTAMP () as LOAD_TSTMP_EXP" \
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_ITEM_FACILITY_SLOTTING_PRE, type TARGET 
    # COLUMN COUNT: 93


    Shortcut_to_WM_ITEM_FACILITY_SLOTTING_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(ITEM_FACILITY_MAPPING_ID AS INT) as ITEM_FACILITY_MAPPING_ID",
        "CAST(ITEM_ID AS INT) as ITEM_ID",
        "CAST(ITEM_NAME AS STRING) as ITEM_NAME",
        "CAST(WHSE_CODE AS STRING) as WHSE_CODE",
        "CAST(UNIT_MEAS AS BIGINT) as UNIT_MEAS",
        "CAST(MANUAL AS TINYINT) as MANUAL",
        "CAST(IS_NEW AS TINYINT) as IS_NEW",
        "CAST(ALLOW_SLU AS BIGINT) as ALLOW_SLU",
        "CAST(ALLOW_SU AS BIGINT) as ALLOW_SU",
        "CAST(DISC_TRANS AS TIMESTAMP) as DISC_TRANS",
        "CAST(EACH_PER_BIN AS BIGINT) as EACH_PER_BIN",
        "CAST(EX_RECPT_DATE AS TIMESTAMP) as EX_RECPT_DATE",
        "CAST(DISCONT AS TINYINT) as DISCONT",
        "CAST(PROCESSED AS TINYINT) as PROCESSED",
        "CAST(INN_PER_CS AS BIGINT) as INN_PER_CS",
        "CAST(MAX_STACKING AS BIGINT) as MAX_STACKING",
        "CAST(MAX_LANES AS BIGINT) as MAX_LANES",
        "CAST(NUM_UNITS_PER_BIN AS BIGINT) as NUM_UNITS_PER_BIN",
        "CAST(RESERVED_1 AS STRING) as RESERVED_1",
        "CAST(RESERVED_2 AS STRING) as RESERVED_2",
        "CAST(RESERVED_3 AS BIGINT) as RESERVED_3",
        "CAST(RESERVED_4 AS BIGINT) as RESERVED_4",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(MOD_USER AS STRING) as MOD_USER",
        "CAST(RESERVE_RACK_TYPE_ID AS BIGINT) as RESERVE_RACK_TYPE_ID",
        "CAST(ALLOW_RESERVE AS BIGINT) as ALLOW_RESERVE",
        "CAST(PALPAT_RESERVE AS SMALLINT) as PALPAT_RESERVE",
        "CAST(THREE_D_CALC_DONE AS TINYINT) as THREE_D_CALC_DONE",
        "CAST(MAX_SLOTS AS BIGINT) as MAX_SLOTS",
        "CAST(MAX_PALLET_STACKING AS BIGINT) as MAX_PALLET_STACKING",
        "CAST(PROP_BORROWING_OBJECT AS BIGINT) as PROP_BORROWING_OBJECT",
        "CAST(PROP_BORROWING_SPECIFIC AS BIGINT) as PROP_BORROWING_SPECIFIC",
        "CAST(HEIGHT_CAN_BORROW AS TINYINT) as HEIGHT_CAN_BORROW",
        "CAST(LENGTH_CAN_BORROW AS TINYINT) as LENGTH_CAN_BORROW",
        "CAST(WIDTH_CAN_BORROW AS TINYINT) as WIDTH_CAN_BORROW",
        "CAST(WEIGHT_CAN_BORROW AS TINYINT) as WEIGHT_CAN_BORROW",
        "CAST(VEND_PACK_CAN_BORROW AS TINYINT) as VEND_PACK_CAN_BORROW",
        "CAST(INNER_PACK_CAN_BORROW AS TINYINT) as INNER_PACK_CAN_BORROW",
        "CAST(TI_CAN_BORROW AS TINYINT) as TI_CAN_BORROW",
        "CAST(HI_CAN_BORROW AS TINYINT) as HI_CAN_BORROW",
        "CAST(QTY_PER_GRAB_CAN_BORROW AS TINYINT) as QTY_PER_GRAB_CAN_BORROW",
        "CAST(HAND_ATTR_CAN_BORROW AS TINYINT) as HAND_ATTR_CAN_BORROW",
        "CAST(FAM_GRP_CAN_BORROW AS TINYINT) as FAM_GRP_CAN_BORROW",
        "CAST(COMMODITY_CAN_BORROW AS TINYINT) as COMMODITY_CAN_BORROW",
        "CAST(CRUSHABILITY_CAN_BORROW AS TINYINT) as CRUSHABILITY_CAN_BORROW",
        "CAST(HAZARD_CAN_BORROW AS TINYINT) as HAZARD_CAN_BORROW",
        "CAST(VEND_CODE_CAN_BORROW AS TINYINT) as VEND_CODE_CAN_BORROW",
        "CAST(MISC1_CAN_BORROW AS TINYINT) as MISC1_CAN_BORROW",
        "CAST(MISC2_CAN_BORROW AS TINYINT) as MISC2_CAN_BORROW",
        "CAST(MISC3_CAN_BORROW AS TINYINT) as MISC3_CAN_BORROW",
        "CAST(MISC4_CAN_BORROW AS TINYINT) as MISC4_CAN_BORROW",
        "CAST(MISC5_CAN_BORROW AS TINYINT) as MISC5_CAN_BORROW",
        "CAST(MISC6_CAN_BORROW AS TINYINT) as MISC6_CAN_BORROW",
        "CAST(HEIGHT_BORROWED AS TINYINT) as HEIGHT_BORROWED",
        "CAST(LENGTH_BORROWED AS TINYINT) as LENGTH_BORROWED",
        "CAST(WIDTH_BORROWED AS TINYINT) as WIDTH_BORROWED",
        "CAST(WEIGHT_BORROWED AS TINYINT) as WEIGHT_BORROWED",
        "CAST(VEND_PACK_BORROWED AS TINYINT) as VEND_PACK_BORROWED",
        "CAST(INNER_PACK_BORROWED AS TINYINT) as INNER_PACK_BORROWED",
        "CAST(TI_BORROWED AS TINYINT) as TI_BORROWED",
        "CAST(HI_BORROWED AS TINYINT) as HI_BORROWED",
        "CAST(QTY_PER_GRAB_BORROWED AS TINYINT) as QTY_PER_GRAB_BORROWED",
        "CAST(HAND_ATTR_BORROWED AS TINYINT) as HAND_ATTR_BORROWED",
        "CAST(FAM_GRP_BORROWED AS TINYINT) as FAM_GRP_BORROWED",
        "CAST(COMMODITY_BORROWED AS TINYINT) as COMMODITY_BORROWED",
        "CAST(CRUSHABILITY_BORROWED AS TINYINT) as CRUSHABILITY_BORROWED",
        "CAST(HAZARD_BORROWED AS TINYINT) as HAZARD_BORROWED",
        "CAST(VEND_CODE_BORROWED AS TINYINT) as VEND_CODE_BORROWED",
        "CAST(MISC1_BORROWED AS TINYINT) as MISC1_BORROWED",
        "CAST(MISC2_BORROWED AS TINYINT) as MISC2_BORROWED",
        "CAST(MISC3_BORROWED AS TINYINT) as MISC3_BORROWED",
        "CAST(MISC4_BORROWED AS TINYINT) as MISC4_BORROWED",
        "CAST(MISC5_BORROWED AS TINYINT) as MISC5_BORROWED",
        "CAST(MISC6_BORROWED AS TINYINT) as MISC6_BORROWED",
        "CAST(AFRAME_HT AS DECIMAL(9,4)) as AFRAME_HT",
        "CAST(AFRAME_LEN AS DECIMAL(9,4)) as AFRAME_LEN",
        "CAST(AFRAME_WID AS DECIMAL(9,4)) as AFRAME_WID",
        "CAST(AFRAME_WT AS DECIMAL(9,4)) as AFRAME_WT",
        "CAST(AFRAME_ALLOW AS TINYINT) as AFRAME_ALLOW",
        "CAST(ITEM_NUM_1 AS DECIMAL(13,4)) as ITEM_NUM_1",
        "CAST(ITEM_NUM_2 AS DECIMAL(13,4)) as ITEM_NUM_2",
        "CAST(ITEM_NUM_3 AS DECIMAL(13,4)) as ITEM_NUM_3",
        "CAST(ITEM_NUM_4 AS DECIMAL(13,4)) as ITEM_NUM_4",
        "CAST(ITEM_NUM_5 AS DECIMAL(13,4)) as ITEM_NUM_5",
        "CAST(ITEM_CHAR_1 AS STRING) as ITEM_CHAR_1",
        "CAST(ITEM_CHAR_2 AS STRING) as ITEM_CHAR_2",
        "CAST(ITEM_CHAR_3 AS STRING) as ITEM_CHAR_3",
        "CAST(ITEM_CHAR_4 AS STRING) as ITEM_CHAR_4",
        "CAST(ITEM_CHAR_5 AS STRING) as ITEM_CHAR_5",
        "CAST(SLOTTING_GROUP AS STRING) as SLOTTING_GROUP",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )

    overwriteDeltaPartition(Shortcut_to_WM_ITEM_FACILITY_SLOTTING_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_ITEM_FACILITY_SLOTTING_PRE is written to the target table - "
        + target_table_name
    )    