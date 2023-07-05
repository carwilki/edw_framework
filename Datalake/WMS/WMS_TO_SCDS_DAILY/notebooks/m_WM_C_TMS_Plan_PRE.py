#Code converted on 2023-06-24 13:42:08
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



def m_WM_C_TMS_Plan_PRE(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_WM_C_TMS_Plan_PRE function")

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)

    if env is None or env == '':
        raise ValueError('env is not set')

    refine = getEnvPrefix(env) + 'refine'
    raw = getEnvPrefix(env) + 'raw'
    tableName = "WM_C_TMS_PLAN_PRE"

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
    # Processing node SQ_Shortcut_to_C_TMS_PLAN, type SOURCE 
    # COLUMN COUNT: 42

    SQ_Shortcut_to_C_TMS_PLAN = jdbcOracleConnection(
        f"""SELECT
                C_TMS_PLAN.C_TMS_PLAN_ID,
                C_TMS_PLAN.PLAN_ID,
                C_TMS_PLAN.STO_NBR,
                C_TMS_PLAN.ROUTE_ID,
                C_TMS_PLAN.STOP_ID,
                C_TMS_PLAN.STOP_LOC,
                C_TMS_PLAN.ORIGIN,
                C_TMS_PLAN.DESTINATION,
                C_TMS_PLAN.CONSOLIDATOR,
                C_TMS_PLAN.CONS_ADDR_1,
                C_TMS_PLAN.CONS_ADDR_2,
                C_TMS_PLAN.CONS_ADDR_3,
                C_TMS_PLAN.CITY,
                C_TMS_PLAN.STATE,
                C_TMS_PLAN.ZIP,
                C_TMS_PLAN.CNTRY,
                C_TMS_PLAN.CONTACT_PERSON,
                C_TMS_PLAN.PHONE,
                C_TMS_PLAN.COMMODITY,
                C_TMS_PLAN.NUM_PALLETS,
                C_TMS_PLAN.WEIGHT,
                C_TMS_PLAN.VOLUME,
                C_TMS_PLAN.DROP_DEAD_DT,
                C_TMS_PLAN.ETA,
                C_TMS_PLAN.SPLIT_ROUTE,
                C_TMS_PLAN.TMS_CARRIER_ID,
                C_TMS_PLAN.TMS_CARRIER_NAME,
                C_TMS_PLAN.LOAD_STAT,
                C_TMS_PLAN.SHIP_VIA,
                C_TMS_PLAN.MILES,
                C_TMS_PLAN.DRIVE_TIME,
                C_TMS_PLAN.EQUIP_TYPE,
                C_TMS_PLAN.DEL_TYPE,
                C_TMS_PLAN.TMS_TRAILER_ID,
                C_TMS_PLAN.CARRIER_REF_NUM,
                C_TMS_PLAN.TMS_PLAN_TIME,
                C_TMS_PLAN.PLT_WEIGHT,
                C_TMS_PLAN.ERROR_SEQ_NBR,
                C_TMS_PLAN.STAT_CODE,
                C_TMS_PLAN.CREATE_DATE_TIME,
                C_TMS_PLAN.MOD_DATE_TIME,
                C_TMS_PLAN.USER_ID
            FROM {source_schema}.C_TMS_PLAN
            WHERE (TRUNC( CREATE_DATE_TIME) >= TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1) OR (TRUNC( MOD_DATE_TIME) >=  TRUNC( to_date('{Prev_Run_Dt}','YYYY-MM-DD'))-1)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node EXPTRANS, type EXPRESSION 
    # COLUMN COUNT: 44

    # for each involved DataFrame, append the dataframe name to each column
    SQ_Shortcut_to_C_TMS_PLAN_temp = SQ_Shortcut_to_C_TMS_PLAN.toDF(*["SQ_Shortcut_to_C_TMS_PLAN___" + col for col in SQ_Shortcut_to_C_TMS_PLAN.columns])

    EXPTRANS = SQ_Shortcut_to_C_TMS_PLAN_temp.selectExpr( 
        "SQ_Shortcut_to_C_TMS_PLAN___sys_row_id as sys_row_id", 
        f"{dcnbr} as DC_NBR_exp", 
        "SQ_Shortcut_to_C_TMS_PLAN___C_TMS_PLAN_ID as C_TMS_PLAN_ID", 
        "SQ_Shortcut_to_C_TMS_PLAN___PLAN_ID as PLAN_ID", 
        "SQ_Shortcut_to_C_TMS_PLAN___STO_NBR as STO_NBR", 
        "SQ_Shortcut_to_C_TMS_PLAN___ROUTE_ID as ROUTE_ID", 
        "SQ_Shortcut_to_C_TMS_PLAN___STOP_ID as STOP_ID", 
        "SQ_Shortcut_to_C_TMS_PLAN___STOP_LOC as STOP_LOC", 
        "SQ_Shortcut_to_C_TMS_PLAN___ORIGIN as ORIGIN", 
        "SQ_Shortcut_to_C_TMS_PLAN___DESTINATION as DESTINATION", 
        "SQ_Shortcut_to_C_TMS_PLAN___CONSOLIDATOR as CONSOLIDATOR", 
        "SQ_Shortcut_to_C_TMS_PLAN___CONS_ADDR_1 as CONS_ADDR_1", 
        "SQ_Shortcut_to_C_TMS_PLAN___CONS_ADDR_2 as CONS_ADDR_2", 
        "SQ_Shortcut_to_C_TMS_PLAN___CONS_ADDR_3 as CONS_ADDR_3", 
        "SQ_Shortcut_to_C_TMS_PLAN___CITY as CITY", 
        "SQ_Shortcut_to_C_TMS_PLAN___STATE as STATE", 
        "SQ_Shortcut_to_C_TMS_PLAN___ZIP as ZIP", 
        "SQ_Shortcut_to_C_TMS_PLAN___CNTRY as CNTRY", 
        "SQ_Shortcut_to_C_TMS_PLAN___CONTACT_PERSON as CONTACT_PERSON", 
        "SQ_Shortcut_to_C_TMS_PLAN___PHONE as PHONE", 
        "SQ_Shortcut_to_C_TMS_PLAN___COMMODITY as COMMODITY", 
        "SQ_Shortcut_to_C_TMS_PLAN___NUM_PALLETS as NUM_PALLETS", 
        "SQ_Shortcut_to_C_TMS_PLAN___WEIGHT as WEIGHT", 
        "SQ_Shortcut_to_C_TMS_PLAN___VOLUME as VOLUME", 
        "SQ_Shortcut_to_C_TMS_PLAN___DROP_DEAD_DT as DROP_DEAD_DT", 
        "SQ_Shortcut_to_C_TMS_PLAN___ETA as ETA", 
        "SQ_Shortcut_to_C_TMS_PLAN___SPLIT_ROUTE as SPLIT_ROUTE", 
        "SQ_Shortcut_to_C_TMS_PLAN___TMS_CARRIER_ID as TMS_CARRIER_ID", 
        "SQ_Shortcut_to_C_TMS_PLAN___TMS_CARRIER_NAME as TMS_CARRIER_NAME", 
        "SQ_Shortcut_to_C_TMS_PLAN___LOAD_STAT as LOAD_STAT", 
        "SQ_Shortcut_to_C_TMS_PLAN___SHIP_VIA as SHIP_VIA", 
        "SQ_Shortcut_to_C_TMS_PLAN___MILES as MILES", 
        "SQ_Shortcut_to_C_TMS_PLAN___DRIVE_TIME as DRIVE_TIME", 
        "SQ_Shortcut_to_C_TMS_PLAN___EQUIP_TYPE as EQUIP_TYPE", 
        "SQ_Shortcut_to_C_TMS_PLAN___DEL_TYPE as DEL_TYPE", 
        "SQ_Shortcut_to_C_TMS_PLAN___TMS_TRAILER_ID as TMS_TRAILER_ID", 
        "SQ_Shortcut_to_C_TMS_PLAN___CARRIER_REF_NUM as CARRIER_REF_NUM", 
        "SQ_Shortcut_to_C_TMS_PLAN___TMS_PLAN_TIME as TMS_PLAN_TIME", 
        "SQ_Shortcut_to_C_TMS_PLAN___PLT_WEIGHT as PLT_WEIGHT", 
        "SQ_Shortcut_to_C_TMS_PLAN___ERROR_SEQ_NBR as ERROR_SEQ_NBR", 
        "SQ_Shortcut_to_C_TMS_PLAN___STAT_CODE as STAT_CODE", 
        "SQ_Shortcut_to_C_TMS_PLAN___CREATE_DATE_TIME as CREATE_DATE_TIME", 
        "SQ_Shortcut_to_C_TMS_PLAN___MOD_DATE_TIME as MOD_DATE_TIME", 
        "SQ_Shortcut_to_C_TMS_PLAN___USER_ID as USER_ID", 
        "CURRENT_TIMESTAMP() as LOADTSTMP" 
    )

    # COMMAND ----------
    # Processing node Shortcut_to_WM_C_TMS_PLAN_PRE, type TARGET 
    # COLUMN COUNT: 44


    Shortcut_to_WM_C_TMS_PLAN_PRE = EXPTRANS.selectExpr( 
        "CAST(DC_NBR_exp AS BIGINT) as DC_NBR", 
        "CAST(C_TMS_PLAN_ID AS BIGINT) as C_TMS_PLAN_ID", 
        "CAST(PLAN_ID AS STRING) as PLAN_ID", 
        "CAST(STO_NBR AS STRING) as STO_NBR", 
        "CAST(ROUTE_ID AS STRING) as ROUTE_ID", 
        "CAST(STOP_ID AS BIGINT) as STOP_ID", 
        "CAST(STOP_LOC AS STRING) as STOP_LOC", 
        "CAST(ORIGIN AS STRING) as ORIGIN", 
        "CAST(DESTINATION AS STRING) as DESTINATION", 
        "CAST(CONSOLIDATOR AS STRING) as CONSOLIDATOR", 
        "CAST(CONS_ADDR_1 AS STRING) as CONS_ADDR_1", 
        "CAST(CONS_ADDR_2 AS STRING) as CONS_ADDR_2", 
        "CAST(CONS_ADDR_3 AS STRING) as CONS_ADDR_3", 
        "CAST(CITY AS STRING) as CITY", 
        "CAST(STATE AS STRING) as STATE", 
        "CAST(ZIP AS STRING) as ZIP", 
        "CAST(CNTRY AS STRING) as CNTRY", 
        "CAST(CONTACT_PERSON AS STRING) as CONTACT_PERSON", 
        "CAST(PHONE AS STRING) as PHONE", 
        "CAST(COMMODITY AS STRING) as COMMODITY", 
        "CAST(NUM_PALLETS AS BIGINT) as NUM_PALLETS", 
        "CAST(WEIGHT AS BIGINT) as WEIGHT", 
        "CAST(VOLUME AS BIGINT) as VOLUME", 
        "CAST(DROP_DEAD_DT AS TIMESTAMP) as DROP_DEAD_DT", 
        "CAST(ETA AS TIMESTAMP) as ETA", 
        "CAST(SPLIT_ROUTE AS STRING) as SPLIT_ROUTE", 
        "CAST(TMS_CARRIER_ID AS STRING) as TMS_CARRIER_ID", 
        "CAST(TMS_CARRIER_NAME AS STRING) as TMS_CARRIER_NAME", 
        "CAST(LOAD_STAT AS STRING) as LOAD_STAT", 
        "CAST(SHIP_VIA AS STRING) as SHIP_VIA", 
        "CAST(MILES AS BIGINT) as MILES", 
        "CAST(DRIVE_TIME AS STRING) as DRIVE_TIME", 
        "CAST(EQUIP_TYPE AS STRING) as EQUIP_TYPE", 
        "CAST(DEL_TYPE AS STRING) as DEL_TYPE", 
        "CAST(TMS_TRAILER_ID AS STRING) as TMS_TRAILER_ID", 
        "CAST(CARRIER_REF_NUM AS STRING) as CARRIER_REF_NUM", 
        "CAST(TMS_PLAN_TIME AS TIMESTAMP) as TMS_PLAN_TIME", 
        "CAST(PLT_WEIGHT AS BIGINT) as PLT_WEIGHT", 
        "CAST(ERROR_SEQ_NBR AS BIGINT) as ERROR_SEQ_NBR", 
        "CAST(STAT_CODE AS BIGINT) as STAT_CODE", 
        "CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME", 
        "CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME", 
        "CAST(USER_ID AS STRING) as USER_ID", 
        "CAST(LOADTSTMP AS TIMESTAMP) as LOAD_TSTMP" 
    )
    overwriteDeltaPartition(Shortcut_to_WM_C_TMS_PLAN_PRE,"DC_NBR",dcnbr,target_table_name)
    logger.info(
        "Shortcut_to_WM_C_TMS_PLAN_PRE is written to the target table - "
        + target_table_name
    )