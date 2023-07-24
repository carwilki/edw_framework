#Code converted on 2023-06-22 20:59:59
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



def m_WM_Size_Uom_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Size_Uom_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_SIZE_UOM_PRE"
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
                    SIZE_UOM.TC_COMPANY_ID,
                    SIZE_UOM.SIZE_UOM,
                    SIZE_UOM.DESCRIPTION,
                    SIZE_UOM.CONSOLIDATION_CODE,
                    SIZE_UOM.DO_NOT_INHERIT_TO_ORDER,
                    SIZE_UOM.SIZE_MAPPING,
                    SIZE_UOM.STANDARD_UOM,
                    SIZE_UOM.STANDARD_UNITS,
                    SIZE_UOM.SPLITTER_CONS_CODE,
                    SIZE_UOM.APPLY_TO_VENDOR,
                    SIZE_UOM.SIZE_UOM_ID,
                    SIZE_UOM.MARK_FOR_DELETION,
                    SIZE_UOM.DISCRETE,
                    SIZE_UOM.ADJUSTMENT,
                    SIZE_UOM.ADJUSTMENT_SIZE_UOM_ID,
                    SIZE_UOM.HIBERNATE_VERSION,
                    SIZE_UOM.AUDIT_CREATED_SOURCE,
                    SIZE_UOM.AUDIT_CREATED_SOURCE_TYPE,
                    SIZE_UOM.AUDIT_CREATED_DTTM,
                    SIZE_UOM.AUDIT_LAST_UPDATED_SOURCE,
                    SIZE_UOM.AUDIT_LAST_UPDATED_SOURCE_TYPE,
                    SIZE_UOM.AUDIT_LAST_UPDATED_DTTM,
                    SIZE_UOM.CREATED_DTTM,
                    SIZE_UOM.LAST_UPDATED_DTTM
                FROM {source_schema}.SIZE_UOM
                WHERE  (TRUNC(CREATED_DTTM) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC(LAST_UPDATED_DTTM) >=  TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)"""
    

    SQ_Shortcut_to_SIZE_UOM = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_SIZE_UOM is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 26
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_SIZE_UOM_temp = SQ_Shortcut_to_SIZE_UOM.toDF(*["SQ_Shortcut_to_SIZE_UOM___" + col for col in SQ_Shortcut_to_SIZE_UOM.columns])
    
    EXPTRANS = SQ_Shortcut_to_SIZE_UOM_temp.selectExpr( 
    	"SQ_Shortcut_to_SIZE_UOM___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_SIZE_UOM___SIZE_UOM_ID as SIZE_UOM_ID", 
    	"SQ_Shortcut_to_SIZE_UOM___TC_COMPANY_ID as TC_COMPANY_ID", 
    	"SQ_Shortcut_to_SIZE_UOM___SIZE_UOM as SIZE_UOM", 
    	"SQ_Shortcut_to_SIZE_UOM___DESCRIPTION as DESCRIPTION", 
    	"SQ_Shortcut_to_SIZE_UOM___CONSOLIDATION_CODE as CONSOLIDATION_CODE", 
    	"SQ_Shortcut_to_SIZE_UOM___DO_NOT_INHERIT_TO_ORDER as DO_NOT_INHERIT_TO_ORDER", 
    	"SQ_Shortcut_to_SIZE_UOM___SIZE_MAPPING as SIZE_MAPPING", 
    	"SQ_Shortcut_to_SIZE_UOM___STANDARD_UOM as STANDARD_UOM", 
    	"SQ_Shortcut_to_SIZE_UOM___STANDARD_UNITS as STANDARD_UNITS", 
    	"SQ_Shortcut_to_SIZE_UOM___SPLITTER_CONS_CODE as SPLITTER_CONS_CODE", 
    	"SQ_Shortcut_to_SIZE_UOM___APPLY_TO_VENDOR as APPLY_TO_VENDOR", 
    	"SQ_Shortcut_to_SIZE_UOM___MARK_FOR_DELETION as MARK_FOR_DELETION", 
    	"SQ_Shortcut_to_SIZE_UOM___DISCRETE as DISCRETE", 
    	"SQ_Shortcut_to_SIZE_UOM___ADJUSTMENT as ADJUSTMENT", 
    	"SQ_Shortcut_to_SIZE_UOM___ADJUSTMENT_SIZE_UOM_ID as ADJUSTMENT_SIZE_UOM_ID", 
    	"SQ_Shortcut_to_SIZE_UOM___HIBERNATE_VERSION as HIBERNATE_VERSION", 
    	"SQ_Shortcut_to_SIZE_UOM___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", 
    	"SQ_Shortcut_to_SIZE_UOM___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_SIZE_UOM___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", 
    	"SQ_Shortcut_to_SIZE_UOM___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", 
    	"SQ_Shortcut_to_SIZE_UOM___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_SIZE_UOM___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", 
    	"SQ_Shortcut_to_SIZE_UOM___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_SIZE_UOM___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP" 
    )
    
    
    # Processing node Shortcut_to_WM_SIZE_UOM_PRE, type TARGET 
    # COLUMN COUNT: 26
    
    
    Shortcut_to_WM_SIZE_UOM_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(SIZE_UOM_ID AS INT) as SIZE_UOM_ID",
        "CAST(TC_COMPANY_ID AS INT) as TC_COMPANY_ID",
        "CAST(SIZE_UOM AS STRING) as SIZE_UOM",
        "CAST(DESCRIPTION AS STRING) as DESCRIPTION",
        "CAST(CONSOLIDATION_CODE AS STRING) as CONSOLIDATION_CODE",
        "CAST(DO_NOT_INHERIT_TO_ORDER AS SMALLINT) as DO_NOT_INHERIT_TO_ORDER",
        "CAST(SIZE_MAPPING AS STRING) as SIZE_MAPPING",
        "CAST(STANDARD_UOM AS SMALLINT) as STANDARD_UOM",
        "CAST(STANDARD_UNITS AS DECIMAL(16,8)) as STANDARD_UNITS",
        "CAST(SPLITTER_CONS_CODE AS STRING) as SPLITTER_CONS_CODE",
        "CAST(APPLY_TO_VENDOR AS INT) as APPLY_TO_VENDOR",
        "CAST(MARK_FOR_DELETION AS SMALLINT) as MARK_FOR_DELETION",
        "CAST(DISCRETE AS SMALLINT) as DISCRETE",
        "CAST(ADJUSTMENT AS DECIMAL(8,4)) as ADJUSTMENT",
        "CAST(ADJUSTMENT_SIZE_UOM_ID AS INT) as ADJUSTMENT_SIZE_UOM_ID",
        "CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION",
        "CAST(AUDIT_CREATED_SOURCE AS STRING) as AUDIT_CREATED_SOURCE",
        "CAST(AUDIT_CREATED_SOURCE_TYPE AS SMALLINT) as AUDIT_CREATED_SOURCE_TYPE",
        "CAST(AUDIT_CREATED_DTTM AS TIMESTAMP) as AUDIT_CREATED_DTTM",
        "CAST(AUDIT_LAST_UPDATED_SOURCE AS STRING) as AUDIT_LAST_UPDATED_SOURCE",
        "CAST(AUDIT_LAST_UPDATED_SOURCE_TYPE AS SMALLINT) as AUDIT_LAST_UPDATED_SOURCE_TYPE",
        "CAST(AUDIT_LAST_UPDATED_DTTM AS TIMESTAMP) as AUDIT_LAST_UPDATED_DTTM",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_SIZE_UOM_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_SIZE_UOM_PRE is written to the target table - " + target_table_name)
