#Code converted on 2023-06-22 20:59:52
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



def m_WM_Slot_Item_Score_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Slot_Item_Score_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_SLOT_ITEM_SCORE_PRE"
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
                    SLOT_ITEM_SCORE.SLOT_ITEM_SCORE_ID,
                    SLOT_ITEM_SCORE.SLOTITEM_ID,
                    SLOT_ITEM_SCORE.CNSTR_ID,
                    SLOT_ITEM_SCORE.SCORE,
                    SLOT_ITEM_SCORE.CREATE_DATE_TIME,
                    SLOT_ITEM_SCORE.MOD_DATE_TIME,
                    SLOT_ITEM_SCORE.MOD_USER,
                    SLOT_ITEM_SCORE.SEQ_CNSTR_VIOLATION
                FROM {source_schema}.SLOT_ITEM_SCORE
                WHERE  (trunc(CREATE_DATE_TIME) >= trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (trunc(MOD_DATE_TIME) >=  trunc(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""
    

    SQ_Shortcut_to_SLOT_ITEM_SCORE = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_SLOT_ITEM_SCORE is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 10
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_SLOT_ITEM_SCORE_temp = SQ_Shortcut_to_SLOT_ITEM_SCORE.toDF(*["SQ_Shortcut_to_SLOT_ITEM_SCORE___" + col for col in SQ_Shortcut_to_SLOT_ITEM_SCORE.columns])
    
    EXPTRANS = SQ_Shortcut_to_SLOT_ITEM_SCORE_temp.selectExpr( 
    	"SQ_Shortcut_to_SLOT_ITEM_SCORE___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR", 
    	"SQ_Shortcut_to_SLOT_ITEM_SCORE___SLOT_ITEM_SCORE_ID as SLOT_ITEM_SCORE_ID", 
    	"SQ_Shortcut_to_SLOT_ITEM_SCORE___SLOTITEM_ID as SLOTITEM_ID", 
    	"SQ_Shortcut_to_SLOT_ITEM_SCORE___CNSTR_ID as CNSTR_ID", 
    	"SQ_Shortcut_to_SLOT_ITEM_SCORE___SCORE as SCORE", 
    	"SQ_Shortcut_to_SLOT_ITEM_SCORE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
    	"SQ_Shortcut_to_SLOT_ITEM_SCORE___MOD_DATE_TIME as MOD_DATE_TIME", 
    	"SQ_Shortcut_to_SLOT_ITEM_SCORE___MOD_USER as MOD_USER", 
    	"SQ_Shortcut_to_SLOT_ITEM_SCORE___SEQ_CNSTR_VIOLATION as SEQ_CNSTR_VIOLATION", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_SLOT_ITEM_SCORE_PRE, type TARGET 
    # COLUMN COUNT: 10
    
    
    Shortcut_to_WM_SLOT_ITEM_SCORE_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR AS SMALLINT) as DC_NBR",
        "CAST(SLOT_ITEM_SCORE_ID AS BIGINT) as SLOT_ITEM_SCORE_ID",
        "CAST(SLOTITEM_ID AS BIGINT) as SLOTITEM_ID",
        "CAST(CNSTR_ID AS BIGINT) as CNSTR_ID",
        "CAST(SCORE AS DECIMAL(9,4)) as SCORE",
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
        "CAST(MOD_USER AS STRING) as MOD_USER",
        "CAST(SEQ_CNSTR_VIOLATION AS TINYINT) as SEQ_CNSTR_VIOLATION",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_SLOT_ITEM_SCORE_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_SLOT_ITEM_SCORE_PRE is written to the target table - " + target_table_name)
