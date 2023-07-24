#Code converted on 2023-06-26 17:06:13
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



def m_WM_Pick_Locn_Hdr_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Pick_Locn_Hdr_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_PICK_LOCN_HDR_PRE"
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
                    PICK_LOCN_HDR.LOCN_ID,
                    PICK_LOCN_HDR.REPL_LOCN_BRCD,
                    PICK_LOCN_HDR.PUTWY_TYPE,
                    PICK_LOCN_HDR.MAX_NBR_OF_SKU,
                    PICK_LOCN_HDR.REPL_FLAG,
                    PICK_LOCN_HDR.PICK_LOCN_ASSIGN_ZONE,
                    PICK_LOCN_HDR.CREATE_DATE_TIME,
                    PICK_LOCN_HDR.MOD_DATE_TIME,
                    PICK_LOCN_HDR.USER_ID,
                    PICK_LOCN_HDR.REPL_CHECK_DIGIT,
                    PICK_LOCN_HDR.MAX_VOL,
                    PICK_LOCN_HDR.MAX_WT,
                    PICK_LOCN_HDR.REPL_X_COORD,
                    PICK_LOCN_HDR.REPL_Y_COORD,
                    PICK_LOCN_HDR.REPL_Z_COORD,
                    PICK_LOCN_HDR.REPL_TRAVEL_AISLE,
                    PICK_LOCN_HDR.REPL_TRAVEL_ZONE,
                    PICK_LOCN_HDR.XCESS_WAVE_NEED_PROC_TYPE,
                    PICK_LOCN_HDR.PICK_LOCN_ASSIGN_TYPE,
                    PICK_LOCN_HDR.SUPPR_PR40_REPL,
                    PICK_LOCN_HDR.COMB_4050_REPL,
                    PICK_LOCN_HDR.PICK_TO_LIGHT_FLAG,
                    PICK_LOCN_HDR.PICK_TO_LIGHT_REPL_FLAG,
                    PICK_LOCN_HDR.PICK_LOCN_HDR_ID,
                    PICK_LOCN_HDR.WM_VERSION_ID,
                    PICK_LOCN_HDR.LOCN_HDR_ID,
                    PICK_LOCN_HDR.LOCN_PUTAWAY_LOCK,
                    PICK_LOCN_HDR.INVN_LOCK_CODE,
                    PICK_LOCN_HDR.CREATED_DTTM,
                    PICK_LOCN_HDR.LAST_UPDATED_DTTM
                FROM {source_schema}.PICK_LOCN_HDR
                WHERE  (trunc(CREATE_DATE_TIME) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (trunc(MOD_DATE_TIME) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (trunc(CREATED_DTTM) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14) OR (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-14)"""

    SQ_Shortcut_to_PICK_LOCN_HDR = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_PICK_LOCN_HDR is executed and data is loaded using jdbc")

    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 32
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_PICK_LOCN_HDR_temp = SQ_Shortcut_to_PICK_LOCN_HDR.toDF(*["SQ_Shortcut_to_PICK_LOCN_HDR___" + col for col in SQ_Shortcut_to_PICK_LOCN_HDR.columns])
    
    EXPTRANS = SQ_Shortcut_to_PICK_LOCN_HDR_temp.selectExpr( \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___sys_row_id as sys_row_id", \
    	f"{dcnbr} as DC_NBR_EXP", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___PICK_LOCN_HDR_ID as PICK_LOCN_HDR_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___LOCN_ID as LOCN_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___REPL_LOCN_BRCD as REPL_LOCN_BRCD", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___PUTWY_TYPE as PUTWY_TYPE", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___MAX_NBR_OF_SKU as MAX_NBR_OF_SKU", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___REPL_FLAG as REPL_FLAG", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___PICK_LOCN_ASSIGN_ZONE as PICK_LOCN_ASSIGN_ZONE", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___CREATE_DATE_TIME as CREATE_DATE_TIME", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___MOD_DATE_TIME as MOD_DATE_TIME", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___USER_ID as USER_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___REPL_CHECK_DIGIT as REPL_CHECK_DIGIT", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___MAX_VOL as MAX_VOL", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___MAX_WT as MAX_WT", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___REPL_X_COORD as REPL_X_COORD", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___REPL_Y_COORD as REPL_Y_COORD", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___REPL_Z_COORD as REPL_Z_COORD", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___REPL_TRAVEL_AISLE as REPL_TRAVEL_AISLE", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___REPL_TRAVEL_ZONE as REPL_TRAVEL_ZONE", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___XCESS_WAVE_NEED_PROC_TYPE as XCESS_WAVE_NEED_PROC_TYPE", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___PICK_LOCN_ASSIGN_TYPE as PICK_LOCN_ASSIGN_TYPE", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___SUPPR_PR40_REPL as SUPPR_PR40_REPL", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___COMB_4050_REPL as COMB_4050_REPL", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___PICK_TO_LIGHT_FLAG as PICK_TO_LIGHT_FLAG", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___PICK_TO_LIGHT_REPL_FLAG as PICK_TO_LIGHT_REPL_FLAG", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___WM_VERSION_ID as WM_VERSION_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___LOCN_HDR_ID as LOCN_HDR_ID", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___LOCN_PUTAWAY_LOCK as LOCN_PUTAWAY_LOCK", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___INVN_LOCK_CODE as INVN_LOCK_CODE", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___CREATED_DTTM as CREATED_DTTM", \
    	"SQ_Shortcut_to_PICK_LOCN_HDR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" \
    )
    
    
    # Processing node Shortcut_to_WM_PICK_LOCN_HDR_PRE, type TARGET 
    # COLUMN COUNT: 32
    
    
    Shortcut_to_WM_PICK_LOCN_HDR_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(PICK_LOCN_HDR_ID AS INT) as PICK_LOCN_HDR_ID",
        "CAST(LOCN_ID AS STRING) as LOCN_ID",
        "CAST(REPL_LOCN_BRCD AS STRING) as REPL_LOCN_BRCD",
        "CAST(PUTWY_TYPE AS STRING) as PUTWY_TYPE",
        "CAST(MAX_NBR_OF_SKU AS SMALLINT) as MAX_NBR_OF_SKU",
        "CAST(REPL_FLAG AS STRING) as REPL_FLAG",
        "CAST(PICK_LOCN_ASSIGN_ZONE AS STRING) as PICK_LOCN_ASSIGN_ZONE",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(USER_ID AS STRING) as USER_ID",
        "CAST(REPL_CHECK_DIGIT AS STRING) as REPL_CHECK_DIGIT",
        "CAST(MAX_VOL AS DECIMAL(13,4)) as MAX_VOL",
        "CAST(MAX_WT AS DECIMAL(13,4)) as MAX_WT",
        "CAST(REPL_X_COORD AS DECIMAL(13,5)) as REPL_X_COORD",
        "CAST(REPL_Y_COORD AS DECIMAL(13,5)) as REPL_Y_COORD",
        "CAST(REPL_Z_COORD AS DECIMAL(13,5)) as REPL_Z_COORD",
        "CAST(REPL_TRAVEL_AISLE AS STRING) as REPL_TRAVEL_AISLE",
        "CAST(REPL_TRAVEL_ZONE AS STRING) as REPL_TRAVEL_ZONE",
        "CAST(XCESS_WAVE_NEED_PROC_TYPE AS TINYINT) as XCESS_WAVE_NEED_PROC_TYPE",
        "CAST(PICK_LOCN_ASSIGN_TYPE AS STRING) as PICK_LOCN_ASSIGN_TYPE",
        "CAST(SUPPR_PR40_REPL AS SMALLINT) as SUPPR_PR40_REPL",
        "CAST(COMB_4050_REPL AS SMALLINT) as COMB_4050_REPL",
        "CAST(PICK_TO_LIGHT_FLAG AS STRING) as PICK_TO_LIGHT_FLAG",
        "CAST(PICK_TO_LIGHT_REPL_FLAG AS STRING) as PICK_TO_LIGHT_REPL_FLAG",
        "CAST(WM_VERSION_ID AS INT) as WM_VERSION_ID",
        "CAST(LOCN_HDR_ID AS INT) as LOCN_HDR_ID",
        "CAST(LOCN_PUTAWAY_LOCK AS STRING) as LOCN_PUTAWAY_LOCK",
        "CAST(INVN_LOCK_CODE AS STRING) as INVN_LOCK_CODE",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_PICK_LOCN_HDR_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_PICK_LOCN_HDR_PRE is written to the target table - " + target_table_name)
