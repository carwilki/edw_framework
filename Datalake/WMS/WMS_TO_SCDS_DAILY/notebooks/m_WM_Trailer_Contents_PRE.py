#Code converted on 2023-06-22 21:02:53
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



def m_WM_Trailer_Contents_PRE(dcnbr, env):
    from logging import getLogger, INFO

    logger = getLogger()
    spark: SparkSession = SparkSession.getActiveSession()
    logger.info("inside m_WM_Trailer_Contents_PRE")
    
    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "WM_TRAILER_CONTENTS_PRE"
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
                    TRAILER_CONTENTS.TRAILER_CONTENTS_ID,
                    TRAILER_CONTENTS.VISIT_DETAIL_ID,
                    TRAILER_CONTENTS.IS_PLANNED,
                    TRAILER_CONTENTS.SHIPMENT_ID,
                    TRAILER_CONTENTS.ASN_ID,
                    TRAILER_CONTENTS.PO_ID,
                    TRAILER_CONTENTS.CREATED_DTTM,
                    TRAILER_CONTENTS.CREATED_SOURCE_TYPE,
                    TRAILER_CONTENTS.CREATED_SOURCE,
                    TRAILER_CONTENTS.LAST_UPDATED_DTTM,
                    TRAILER_CONTENTS.LAST_UPDATED_SOURCE_TYPE,
                    TRAILER_CONTENTS.LAST_UPDATED_SOURCE
                FROM {source_schema}.TRAILER_CONTENTS
                WHERE  (TRUNC(TRAILER_CONTENTS.CREATED_DTTM) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14) OR (TRUNC(TRAILER_CONTENTS.LAST_UPDATED_DTTM) >= TRUNC(to_date('{Prev_Run_Dt}','YYYY-MM-DD')) - 14)"""
    

    SQ_Shortcut_to_TRAILER_CONTENTS = jdbcOracleConnection(query, username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())
    logger.info("SQL query for SQ_Shortcut_to_TRAILER_CONTENTS is executed and data is loaded using jdbc")
    
    
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 14
    
    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_TRAILER_CONTENTS_temp = SQ_Shortcut_to_TRAILER_CONTENTS.toDF(*["SQ_Shortcut_to_TRAILER_CONTENTS___" + col for col in SQ_Shortcut_to_TRAILER_CONTENTS.columns])
    
    EXPTRANS = SQ_Shortcut_to_TRAILER_CONTENTS_temp.selectExpr( 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___sys_row_id as sys_row_id", 
    	f"{dcnbr} as DC_NBR_EXP", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___TRAILER_CONTENTS_ID as TRAILER_CONTENTS_ID", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___IS_PLANNED as IS_PLANNED", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___SHIPMENT_ID as SHIPMENT_ID", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___ASN_ID as ASN_ID", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___PO_ID as PO_ID", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___CREATED_DTTM as CREATED_DTTM", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___CREATED_SOURCE as CREATED_SOURCE", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
    	"SQ_Shortcut_to_TRAILER_CONTENTS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
    	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
    )
    
    
    # Processing node Shortcut_to_WM_TRAILER_CONTENTS_PRE, type TARGET 
    # COLUMN COUNT: 14
    
    
    Shortcut_to_WM_TRAILER_CONTENTS_PRE = EXPTRANS.selectExpr(
        "CAST(DC_NBR_EXP AS SMALLINT) as DC_NBR",
        "CAST(TRAILER_CONTENTS_ID AS INT) as TRAILER_CONTENTS_ID",
        "CAST(VISIT_DETAIL_ID AS INT) as VISIT_DETAIL_ID",
        "CAST(IS_PLANNED AS TINYINT) as IS_PLANNED",
        "CAST(SHIPMENT_ID AS BIGINT) as SHIPMENT_ID",
        "CAST(ASN_ID AS BIGINT) as ASN_ID",
        "CAST(PO_ID AS BIGINT) as PO_ID",
        "CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
        "CAST(CREATED_SOURCE_TYPE AS SMALLINT) as CREATED_SOURCE_TYPE",
        "CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE",
        "CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
        "CAST(LAST_UPDATED_SOURCE_TYPE AS SMALLINT) as LAST_UPDATED_SOURCE_TYPE",
        "CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE",
        "CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
    )
    
    overwriteDeltaPartition(Shortcut_to_WM_TRAILER_CONTENTS_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_WM_TRAILER_CONTENTS_PRE is written to the target table - " + target_table_name)
