#Code converted on 2023-07-11 16:22:59
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
#from pyspark.dbutils import DBUtils
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
# COMMAND ----------

def m_petshotel_exchange_rate_pre(dcnbr, env):
    from logging import getLogger, INFO
    logger = getLogger()
    logger.info("inside m_petshotel_exchange_rate_pre function")


    spark = SparkSession.getActiveSession()
    # dbutils = DBUtils(spark)

    if dcnbr is None or dcnbr == "":
        raise ValueError("DC_NBR is not set")
    if env is None or env == "":
        raise ValueError("env is not set")
    
    refine = getEnvPrefix(env) + "refine"
    raw = getEnvPrefix(env) + "raw"
    
    tableName = "PETSHOTEL_EXCH_RATE_PRE"
    schemaName = raw
    
    target_table_name = schemaName + "." + tableName
    refine_table_name = "PETSHOTEL_EXCH_RATE"
    prev_run_dt = gu.genPrevRunDt(refine_table_name, refine, raw)
    print("The prev run date is " + prev_run_dt)

    # Read in relation source variables
    (username, password, connection_string) = getConfig(dcnbr, env)
    

    # COMMAND ----------
    # Processing node SQ_Shortcut_to_CURRENCY_DAY, type SOURCE 
    # COLUMN COUNT: 3

    SQ_Shortcut_to_CURRENCY_DAY = jdbcOracleConnection(
        f"""
        SELECT day_dt, 'CA' country_cd, a.exchange_rate_pcnt
        FROM currency_day a
        WHERE day_dt > date'2005-01-01'
        UNION
        SELECT day_dt, 'US' country_cd, 1 exchange_rate_pcnt
        FROM currency_day a
        WHERE day_dt > date'2005-01-01'
        UNION
        SELECT a.day_dt, 'CA' country_cd, c.exchange_rate_pcnt
        FROM days a, (SELECT exchange_rate_pcnt, day_dt
                      FROM currency_day
                      WHERE day_dt = (SELECT MAX(day_dt) FROM currency_day)) c
        WHERE a.day_dt > c.day_dt
        UNION
        SELECT a.day_dt, 'US' country_cd, 1 exchange_rate_pcnt
        FROM days a
        WHERE a.day_dt > (SELECT MAX(day_dt) FROM currency_day)""",  
       username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())

    # COMMAND ----------
    # Processing node Shortcut_to_PETSHOTEL_EXCH_RATE_PRE, type TARGET 
    # COLUMN COUNT: 3


    Shortcut_to_PETSHOTEL_EXCH_RATE_PRE = SQ_Shortcut_to_CURRENCY_DAY.selectExpr( 
        "CAST(DAY_DT AS TIMESTAMP) as DAY_DT", 
        "CAST(STORE_CTRY_ABBR AS STRING) as COUNTRY_CD", 
        "CAST(EXCHANGE_RATE_PCNT AS DECIMAL(9,6)) as EXCH_RATE_PCT" 
    )
    
    overwriteDeltaPartition(Shortcut_to_PETSHOTEL_EXCH_RATE_PRE, "DC_NBR", dcnbr, target_table_name)
    logger.info("Shortcut_to_PETSHOTEL_EXCH_RATE_PRE is written to the target table - " + target_table_name)