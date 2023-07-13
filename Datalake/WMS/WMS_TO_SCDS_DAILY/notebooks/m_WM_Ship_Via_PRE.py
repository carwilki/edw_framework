#Code converted on 2023-06-22 21:00:02
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



def m_WM_Ship_Via_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Ship_Via_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_SHIP_VIA_PRE"
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
                    SHIP_VIA.SHIP_VIA_ID,
                    SHIP_VIA.TC_COMPANY_ID,
                    SHIP_VIA.CARRIER_ID,
                    SHIP_VIA.SERVICE_LEVEL_ID,
                    SHIP_VIA.MOT_ID,
                    SHIP_VIA.LABEL_TYPE,
                    SHIP_VIA.SERVICE_LEVEL_ICON,
                    SHIP_VIA.EXECUTION_LEVEL_ID,
                    SHIP_VIA.BILL_SHIP_VIA_ID,
                    SHIP_VIA.IS_TRACKING_NBR_REQ,
                    SHIP_VIA.MARKED_FOR_DELETION,
                    SHIP_VIA.DESCRIPTION,
                    SHIP_VIA.ACCESSORIAL_SEARCH_STRING,
                    SHIP_VIA.INS_COVER_TYPE_ID,
                    SHIP_VIA.MIN_DECLARED_VALUE,
                    SHIP_VIA.MAX_DECLARED_VALUE,
                    SHIP_VIA.SERVICE_LEVEL_INDICATOR,
                    SHIP_VIA.DECLARED_VALUE_CURRENCY,
                    SHIP_VIA.SHIP_VIA,
                    SHIP_VIA.CUSTOM_SHIPVIA_ATTRIB,
                    SHIP_VIA.CREATED_DTTM,
                    SHIP_VIA.LAST_UPDATED_DTTM
                FROM {source_schema}.SHIP_VIA
                WHERE  (TRUNC(CREATED_DTTM) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC(LAST_UPDATED_DTTM) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""
    

    SQ_Shortcut_to_SHIP_VIA = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_SHIP_VIA is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 24
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_SHIP_VIA_temp = SQ_Shortcut_to_SHIP_VIA.toDF(*["SQ_Shortcut_to_SHIP_VIA___" + col for col in SQ_Shortcut_to_SHIP_VIA.columns])
    
    EXPTRANS = SQ_Shortcut_to_SHIP_VIA_temp.selectExpr( 
    	"SQ_Shortcut_to_SHIP_VIA___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_SHIP_VIA___SHIP_VIA_ID as SHIP_VIA_ID", 
    	"SQ_Shortcut_to_SHIP_VIA___TC_COMPANY_ID as TC_COMPANY_ID", 
    	"SQ_Shortcut_to_SHIP_VIA___CARRIER_ID as CARRIER_ID", 
    	"SQ_Shortcut_to_SHIP_VIA___SERVICE_LEVEL_ID as SERVICE_LEVEL_ID", 
    	"SQ_Shortcut_to_SHIP_VIA___MOT_ID as MOT_ID", 
    	"SQ_Shortcut_to_SHIP_VIA___LABEL_TYPE as LABEL_TYPE", 
    	"SQ_Shortcut_to_SHIP_VIA___SERVICE_LEVEL_ICON as SERVICE_LEVEL_ICON", 
    	"SQ_Shortcut_to_SHIP_VIA___EXECUTION_LEVEL_ID as EXECUTION_LEVEL_ID", 
    	"SQ_Shortcut_to_SHIP_VIA___BILL_SHIP_VIA_ID as BILL_SHIP_VIA_ID", 
    	"SQ_Shortcut_to_SHIP_VIA___IS_TRACKING_NBR_REQ as IS_TRACKING_NBR_REQ", 
    	"SQ_Shortcut_to_SHIP_VIA___MARKED_FOR_DELETION as MARKED_FOR_DELETION", 
    	"SQ_Shortcut_to_SHIP_VIA___DESCRIPTION as DESCRIPTION", 
    	"SQ_Shortcut_to_SHIP_VIA___ACCESSORIAL_SEARCH_STRING as ACCESSORIAL_SEARCH_STRING", 
    	"SQ_Shortcut_to_SHIP_VIA___INS_COVER_TYPE_ID as INS_COVER_TYPE_ID", 
    	"SQ_Shortcut_to_SHIP_VIA___MIN_DECLARED_VALUE as MIN_DECLARED_VALUE", 
    	"SQ_Shortcut_to_SHIP_VIA___MAX_DECLARED_VALUE as MAX_DECLARED_VALUE", 
    	"SQ_Shortcut_to_SHIP_VIA___SERVICE_LEVEL_INDICATOR as SERVICE_LEVEL_INDICATOR", 
    	"SQ_Shortcut_to_SHIP_VIA___DECLARED_VALUE_CURRENCY as DECLARED_VALUE_CURRENCY", 
    	"SQ_Shortcut_to_SHIP_VIA___SHIP_VIA as SHIP_VIA", 
    	"SQ_Shortcut_to_SHIP_VIA___CUSTOM_SHIPVIA_ATTRIB as CUSTOM_SHIPVIA_ATTRIB", 
    	"SQ_Shortcut_to_SHIP_VIA___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_SHIP_VIA___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_SHIP_VIA_PRE, type TARGET 
    # COLUMN COUNT: 24
    
    
    Shortcut_to_WM_SHIP_VIA_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(SHIP_VIA_ID AS INT) as SHIP_VIA_ID",
        "CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID",
        "CAST(CARRIER_ID AS BIGINT) as CARRIER_ID",
        "CAST(SERVICE_LEVEL_ID AS BIGINT) as SERVICE_LEVEL_ID",
        "CAST(MOT_ID AS BIGINT) as MOT_ID",
        "CAST(LABEL_TYPE AS STRING) as LABEL_TYPE",
        "CAST(SERVICE_LEVEL_ICON AS STRING) as SERVICE_LEVEL_ICON",
        "CAST(EXECUTION_LEVEL_ID AS INT) as EXECUTION_LEVEL_ID",
        "CAST(BILL_SHIP_VIA_ID AS INT) as BILL_SHIP_VIA_ID",
        "CAST(IS_TRACKING_NBR_REQ AS TINYINT) as IS_TRACKING_NBR_REQ",
        "CAST(MARKED_FOR_DELETION AS SMALLINT) as MARKED_FOR_DELETION",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(ACCESSORIAL_SEARCH_STRING AS STRING) as ACCESSORIAL_SEARCH_STRING",
        "CAST(INS_COVER_TYPE_ID AS TINYINT) as INS_COVER_TYPE_ID",
        "CAST(MIN_DECLARED_VALUE AS DECIMAL(14,3)) as MIN_DECLARED_VALUE",
        "CAST(MAX_DECLARED_VALUE AS DECIMAL(14,3)) as MAX_DECLARED_VALUE",
        "CAST(SERVICE_LEVEL_INDICATOR AS STRING) as SERVICE_LEVEL_INDICATOR",
        "CAST(DECLARED_VALUE_CURRENCY AS STRING) as DECLARED_VALUE_CURRENCY",
        "CAST(SHIP_VIA AS STRING) as SHIP_VIA",
        "CAST(CUSTOM_SHIPVIA_ATTRIB AS STRING) as CUSTOM_SHIPVIA_ATTRIB",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_SHIP_VIA_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_SHIP_VIA_PRE is written to the target table - " + target_table_name)
