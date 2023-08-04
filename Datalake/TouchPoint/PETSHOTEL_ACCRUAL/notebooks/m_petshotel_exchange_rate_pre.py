#Code converted on 2023-07-11 16:22:59
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
from Datalake.utils.logger import *

# COMMAND ----------
parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()

parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
enterprise = getEnvPrefix(env) + 'enterprise'

# COMMAND ----------


# Set global variables
starttime = datetime.now() #start timestamp of the script

# COMMAND ----------

# Processing node SQ_Shortcut_to_CURRENCY_DAY, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_CURRENCY_DAY = spark.sql(f"""SELECT day_dt, 'CA' country_cd, a.exchange_rate_pcnt
  FROM {enterprise}.currency_day a
 WHERE day_dt > TO_DATE ('01-01-2005', 'mm-dd-yyyy')
UNION
SELECT day_dt, 'US' country_cd, 1 exchange_rate_pcnt
  FROM {enterprise}.currency_day a
 WHERE day_dt > TO_DATE ('01-01-2005', 'mm-dd-yyyy')
UNION
SELECT a.day_dt, 'CA' country_cd, c.exchange_rate_pcnt
  FROM {enterprise}.days a, (SELECT exchange_rate_pcnt, day_dt
                             FROM {enterprise}.currency_day
                             WHERE day_dt = (SELECT MAX (day_dt) FROM {enterprise}.currency_day)) c
 WHERE a.day_dt > c.day_dt
UNION
SELECT a.day_dt, 'US' country_cd, 1 exchange_rate_pcnt
  FROM {enterprise}.days a
 WHERE a.day_dt > (SELECT MAX (day_dt) FROM {enterprise}.currency_day)""").withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_Shortcut_to_CURRENCY_DAY = (SQ_Shortcut_to_CURRENCY_DAY
	.withColumnRenamed(SQ_Shortcut_to_CURRENCY_DAY.columns[0],'DAY_DT')
	.withColumnRenamed(SQ_Shortcut_to_CURRENCY_DAY.columns[1],'STORE_CTRY_ABBR')
	.withColumnRenamed(SQ_Shortcut_to_CURRENCY_DAY.columns[2],'EXCHANGE_RATE_PCNT')
)
# COMMAND ----------

# Processing node Shortcut_to_PETSHOTEL_EXCH_RATE_PRE, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_PETSHOTEL_EXCH_RATE_PRE = SQ_Shortcut_to_CURRENCY_DAY.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(STORE_CTRY_ABBR AS STRING) as COUNTRY_CD",
	"CAST(EXCHANGE_RATE_PCNT AS DECIMAL(9,6)) as EXCH_RATE_PCT"
)

Shortcut_to_PETSHOTEL_EXCH_RATE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.PETSHOTEL_EXCH_RATE_PRE')