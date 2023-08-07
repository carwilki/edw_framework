#Code converted on 2023-06-22 15:26:03
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



def m_WM_Product_Class_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Product_Class_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_PRODUCT_CLASS_PRE"
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
                    PRODUCT_CLASS.TC_COMPANY_ID,
                    PRODUCT_CLASS.PRODUCT_CLASS,
                    PRODUCT_CLASS.DESCRIPTION,
                    PRODUCT_CLASS.MARK_FOR_DELETION,
                    PRODUCT_CLASS.PRODUCT_CLASS_ID,
                    PRODUCT_CLASS.HAS_SPLIT,
                    PRODUCT_CLASS.RANK,
                    PRODUCT_CLASS.MIN_THRESHOLD,
                    PRODUCT_CLASS.CREATED_DTTM,
                    PRODUCT_CLASS.LAST_UPDATED_DTTM,
                    PRODUCT_CLASS.STACKING_FACTOR
                FROM {source_schema}.PRODUCT_CLASS
                WHERE  (TRUNC(CREATED_DTTM) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC(LAST_UPDATED_DTTM) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""

    SQ_Shortcut_to_PRODUCT_CLASS = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_PRODUCT_CLASS is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 13
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_PRODUCT_CLASS_temp = SQ_Shortcut_to_PRODUCT_CLASS.toDF(*["SQ_Shortcut_to_PRODUCT_CLASS___" + col for col in SQ_Shortcut_to_PRODUCT_CLASS.columns])
    
    EXPTRANS = SQ_Shortcut_to_PRODUCT_CLASS_temp.selectExpr( 
    	"SQ_Shortcut_to_PRODUCT_CLASS___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___PRODUCT_CLASS_ID as PRODUCT_CLASS_ID", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___TC_COMPANY_ID as TC_COMPANY_ID", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___PRODUCT_CLASS as PRODUCT_CLASS", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___DESCRIPTION as DESCRIPTION", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___MARK_FOR_DELETION as MARK_FOR_DELETION", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___HAS_SPLIT as HAS_SPLIT", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___RANK as RANK", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___MIN_THRESHOLD as MIN_THRESHOLD", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"SQ_Shortcut_to_PRODUCT_CLASS___STACKING_FACTOR as STACKING_FACTOR", 
    	"CURRENT_TIMESTAMP () as LOAD_TSTMP" 
    )
    
    
    # Processing node Shortcut_to_WM_PRODUCT_CLASS_PRE, type TARGET 
    # COLUMN COUNT: 13
    
    
    Shortcut_to_WM_PRODUCT_CLASS_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(PRODUCT_CLASS_ID AS INT) as PRODUCT_CLASS_ID",
        "CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID",
        "CAST(PRODUCT_CLASS AS STRING) as PRODUCT_CLASS",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(MARK_FOR_DELETION AS TINYINT) as MARK_FOR_DELETION",
        "CAST(HAS_SPLIT AS SMALLINT) as HAS_SPLIT",
        "CAST(RANK AS SMALLINT) as RANK",
        "CAST(MIN_THRESHOLD AS SMALLINT) as MIN_THRESHOLD",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(STACKING_FACTOR AS INT) as STACKING_FACTOR",
        "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_PRODUCT_CLASS_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_PRODUCT_CLASS_PRE is written to the target table - " + target_table_name)
