#Code converted on 2023-08-01 13:49:16
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

# Set global variables
starttime = datetime.now() #start timestamp of the script

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE = spark.sql(f"""SELECT
DAY_DT
FROM {raw}.WFA_TIME_SHEET_PUNCH_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node AGG_DELETE_7_DAYS, type AGGREGATOR 
# COLUMN COUNT: 1

AGG_DELETE_7_DAYS = SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE \
	.groupBy("DAY_DT") \
	.agg(count("*").alias("cnt")) \
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_STRATEGY, type UPDATE_STRATEGY 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
AGG_DELETE_7_DAYS_temp = AGG_DELETE_7_DAYS.toDF(*["AGG_DELETE_7_DAYS___" + col for col in AGG_DELETE_7_DAYS.columns])

UPD_STRATEGY = AGG_DELETE_7_DAYS_temp.selectExpr( \
	"AGG_DELETE_7_DAYS___DAY_DT as DAY_DT") \
	.withColumn('pyspark_data_action', lit(2))


UPD_STRATEGY.createOrReplaceTempView('UPD_STRATEGY')
# UPD_STRATEGY.show()

spark.sql(f"""
	MERGE INTO {legacy}.WFA_TIME_SHEET_PUNCH trg
    USING UPD_STRATEGY src
    ON src.DAY_DT = trg.DAY_DT
    WHEN MATCHED THEN DELETE
""")
#  from session this is a delete

# COMMAND ----------
# Processing node Shortcut_to_WFA_TIME_SHEET_PUNCH, type TARGET 
# COLUMN COUNT: 15


# Shortcut_to_WFA_TIME_SHEET_PUNCH = UPD_STRATEGY.selectExpr( \
# 	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT", \
# 	"CAST(NULL AS TIMESTAMP) as WEEK_DT", \
# 	"CAST(NULL AS DECIMAL(12,0)) as TIME_SHEET_ITEM_ID", \
# 	"CAST(NULL AS TIMESTAMP) as STRT_DTM", \
# 	"CAST(NULL AS TIMESTAMP) as END_DTM", \
# 	"CAST(NULL AS BIGINT) as LOCATION_ID", \
# 	"col(\"EMPLOYEE_ID\") as EMPLOYEE_ID", \
# 	"col(\"WFA_BUSN_AREA_ID\") as WFA_BUSN_AREA_ID", \
# 	"CAST(NULL AS STRING) as WFA_BUSN_AREA_DESC", \
# 	"col(\"WFA_DEPT_ID\") as WFA_DEPT_ID", \
# 	"CAST(NULL AS STRING) as WFA_DEPT_DESC", \
# 	"col(\"WFA_TASK_ID\") as WFA_TASK_ID", \
# 	"CAST(NULL AS STRING) as WFA_TASK_DESC", \
# 	"CAST(NULL AS TIMESTAMP) as UPDATE_DT", \
# 	"CAST(NULL AS TIMESTAMP) as LOAD_DT", \
# 	"pyspark_data_action as pyspark_data_action" \
# )
# Shortcut_to_WFA_TIME_SHEET_PUNCH.write.saveAsTable(f'{raw}.WFA_TIME_SHEET_PUNCH')