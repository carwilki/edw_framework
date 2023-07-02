#Code converted on 2023-06-20 18:04:15
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from utils.genericUtilities import *
from utils.configs import *
from utils.mergeUtils import *
from utils.logger import *


def m_WM_Lpn_Size_Type_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Lpn_Size_Type_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_LPN_SIZE_TYPE_PRE', mode = 'append"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "WM_LPN_SIZE_TYPE', mode = 'append"
    prev_run_dt=gu.genPrevRunDt(refine_table_name, refine,raw)
    print("The prev run date is " + prev_run_dt)
    
    (username, password, connection_string) = getConfig(dcnbr, env)
    logger.info("username, password, connection_string is obtained from getConfig fun")
    
    dcnbr = dcnbr.strip()[2:]
    
    query = f"""SELECT
                    LPN_SIZE_TYPE.LPN_DESC,
                    LPN_SIZE_TYPE.LPN_SIZE_DESC,
                    LPN_SIZE_TYPE.LPN_SIZE_TYPE,
                    LPN_SIZE_TYPE.LPN_SIZE_TYPE_ID,
                    LPN_SIZE_TYPE.LPN_TYPE_ID,
                    LPN_SIZE_TYPE.PKG_DESC,
                    LPN_SIZE_TYPE.OVERSIZE_FLAG,
                    LPN_SIZE_TYPE.LENGTH,
                    LPN_SIZE_TYPE.WIDTH,
                    LPN_SIZE_TYPE.HEIGHT,
                    LPN_SIZE_TYPE.LPN_DIM_UOM,
                    LPN_SIZE_TYPE.LPN_PER_TIER,
                    LPN_SIZE_TYPE.TIER_PER_PALLET,
                    LPN_SIZE_TYPE.PROC_ATTR_1,
                    LPN_SIZE_TYPE.PROC_ATTR_2,
                    LPN_SIZE_TYPE.PROC_ATTR_3,
                    LPN_SIZE_TYPE.PROC_ATTR_4,
                    LPN_SIZE_TYPE.PROC_ATTR_5,
                    LPN_SIZE_TYPE.USER_ID,
                    LPN_SIZE_TYPE.WM_VERSION_ID,
                    LPN_SIZE_TYPE.CREATED_DTTM,
                    LPN_SIZE_TYPE.LAST_UPDATED_DTTM,
                    LPN_SIZE_TYPE.HIBERNATE_VERSION,
                    LPN_SIZE_TYPE.TC_COMPANY_ID,
                    LPN_SIZE_TYPE.MARK_FOR_DELETION,
                    LPN_SIZE_TYPE.SHAPE_TYPE,
                    LPN_SIZE_TYPE.IS_STACKABLE,
                    LPN_SIZE_TYPE.STACK_RANK,
                    LPN_SIZE_TYPE.STACK_POSITION,
                    LPN_SIZE_TYPE.LOAD_BEARING_STRENGTH,
                    LPN_SIZE_TYPE.FACILITY_ID
                FROM LPN_SIZE_TYPE
                WHERE {Initial_Load}  (trunc(CREATED_DTTM)>= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (trunc(LAST_UPDATED_DTTM)>= trunc(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)"""

    SQ_Shortcut_to_LPN_SIZE_TYPE = gu.jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_LPN_SIZE_TYPE is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 33
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_LPN_SIZE_TYPE_temp = SQ_Shortcut_to_LPN_SIZE_TYPE.toDF(*["SQ_Shortcut_to_LPN_SIZE_TYPE___" + col for col in SQ_Shortcut_to_LPN_SIZE_TYPE.columns])
    
    EXPTRANS = SQ_Shortcut_to_LPN_SIZE_TYPE_temp.selectExpr( \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___sys_row_id as sys_row_id", \
    	f"{DC_NBR} as DC_NBR", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___LPN_SIZE_TYPE_ID as LPN_SIZE_TYPE_ID", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___LPN_DESC as LPN_DESC", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___LPN_SIZE_DESC as LPN_SIZE_DESC", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___LPN_SIZE_TYPE as LPN_SIZE_TYPE", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___LPN_TYPE_ID as LPN_TYPE_ID", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___PKG_DESC as PKG_DESC", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___OVERSIZE_FLAG as OVERSIZE_FLAG", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___LENGTH as LENGTH", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___WIDTH as WIDTH", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___HEIGHT as HEIGHT", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___LPN_DIM_UOM as LPN_DIM_UOM", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___LPN_PER_TIER as LPN_PER_TIER", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___TIER_PER_PALLET as TIER_PER_PALLET", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___PROC_ATTR_1 as PROC_ATTR_1", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___PROC_ATTR_2 as PROC_ATTR_2", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___PROC_ATTR_3 as PROC_ATTR_3", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___PROC_ATTR_4 as PROC_ATTR_4", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___PROC_ATTR_5 as PROC_ATTR_5", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___USER_ID as USER_ID", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___WM_VERSION_ID as WM_VERSION_ID", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___CREATED_DTTM as CREATED_DTTM", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___HIBERNATE_VERSION as HIBERNATE_VERSION", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___TC_COMPANY_ID as TC_COMPANY_ID", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___MARK_FOR_DELETION as MARK_FOR_DELETION", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___SHAPE_TYPE as SHAPE_TYPE", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___IS_STACKABLE as IS_STACKABLE", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___STACK_RANK as STACK_RANK", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___STACK_POSITION as STACK_POSITION", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___LOAD_BEARING_STRENGTH as LOAD_BEARING_STRENGTH", \
    	"SQ_Shortcut_to_LPN_SIZE_TYPE___FACILITY_ID as FACILITY_ID", \
    	"CURRENT_TIMESTAMP () as LOAD_TSTMP" \
    )
    
    
    # Processing node Shortcut_to_WM_LPN_SIZE_TYPE_PRE, type TARGET 
    # COLUMN COUNT: 33
    
    
    Shortcut_to_WM_LPN_SIZE_TYPE_PRE = EXPTRANS.selectExpr( \
    	"CAST(DC_NBR AS BIGINT) as DC_NBR", \
    	"CAST(LPN_SIZE_TYPE_ID AS BIGINT) as LPN_SIZE_TYPE_ID", \
    	"CAST(LPN_DESC AS STRING) as LPN_DESC", \
    	"CAST(LPN_SIZE_DESC AS STRING) as LPN_SIZE_DESC", \
    	"CAST(LPN_SIZE_TYPE AS STRING) as LPN_SIZE_TYPE", \
    	"CAST(LPN_TYPE_ID AS BIGINT) as LPN_TYPE_ID", \
    	"CAST(PKG_DESC AS STRING) as PKG_DESC", \
    	"CAST(OVERSIZE_FLAG AS STRING) as OVERSIZE_FLAG", \
    	"CAST(LENGTH AS BIGINT) as LENGTH", \
    	"CAST(WIDTH AS BIGINT) as WIDTH", \
    	"CAST(HEIGHT AS BIGINT) as HEIGHT", \
    	"CAST(LPN_DIM_UOM AS STRING) as LPN_DIM_UOM", \
    	"CAST(LPN_PER_TIER AS BIGINT) as LPN_PER_TIER", \
    	"CAST(TIER_PER_PALLET AS BIGINT) as TIER_PER_PALLET", \
    	"CAST(PROC_ATTR_1 AS STRING) as PROC_ATTR_1", \
    	"CAST(PROC_ATTR_2 AS STRING) as PROC_ATTR_2", \
    	"CAST(PROC_ATTR_3 AS STRING) as PROC_ATTR_3", \
    	"CAST(PROC_ATTR_4 AS STRING) as PROC_ATTR_4", \
    	"CAST(PROC_ATTR_5 AS STRING) as PROC_ATTR_5", \
    	"CAST(USER_ID AS STRING) as USER_ID", \
    	"CAST(WM_VERSION_ID AS BIGINT) as WM_VERSION_ID", \
    	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", \
    	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", \
    	"CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION", \
    	"CAST(TC_COMPANY_ID AS BIGINT) as TC_COMPANY_ID", \
    	"CAST(MARK_FOR_DELETION AS BIGINT) as MARK_FOR_DELETION", \
    	"CAST(SHAPE_TYPE AS BIGINT) as SHAPE_TYPE", \
    	"CAST(IS_STACKABLE AS BIGINT) as IS_STACKABLE", \
    	"CAST(STACK_RANK AS BIGINT) as STACK_RANK", \
    	"CAST(STACK_POSITION AS BIGINT) as STACK_POSITION", \
    	"CAST(LOAD_BEARING_STRENGTH AS BIGINT) as LOAD_BEARING_STRENGTH", \
    	"CAST(FACILITY_ID AS BIGINT) as FACILITY_ID", \
    	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
    )
    
    gu.overwriteDeltaPartition(Shortcut_to_WM_LPN_SIZE_TYPE_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_LPN_SIZE_TYPE_PRE is written to the target table - " + target_table_name)
