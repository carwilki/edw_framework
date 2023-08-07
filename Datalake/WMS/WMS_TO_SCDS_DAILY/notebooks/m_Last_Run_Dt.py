#Code converted on 2023-06-15 13:23:13
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

# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = getConfig(dcnbr, env)

# COMMAND ----------
# Variable_declaration_comment
Last_Run_Dt=args.Last_Run_Dt

# COMMAND ----------
# Processing node SQ_Shortcut_to_DAYS, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_DAYS = (spark.read.format('jdbc')
     .option('url', connection_string)
     .option('query', f"""SELECT
DAYS.DAY_DT
FROM DAYS
WHERE DAY_DT = CURRENT_DATE""")
     .option('user', username)
     .option('password', password)
     .option('numPartitions', 3)
     .option('driver', 'oracle.jdbc.OracleDriver')
     .option('fetchsize', 10000)
     .option('oracle.jdbc.timezoneAsRegion', 'false')
     .option(
         'sessionInitStatement',
         """begin 
         execute immediate 'alter session set time_zone=''-07:00''';
     end;
 """,
     )
     .load()).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LAST_RUN_DT, type EXPRESSION 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_DAYS_temp = SQ_Shortcut_to_DAYS.toDF(*["SQ_Shortcut_to_DAYS___" + col for col in SQ_Shortcut_to_DAYS.columns])

# SQ_Shortcut_to_DAYS_temp.withColumn("SET_LAST_RUN_DT", SET {Last_Run_Dt} = TRUNC ( SESSSTARTTIME ))
# Last_Run_Dt = 
EXP_LAST_RUN_DT = SQ_Shortcut_to_DAYS_temp.selectExpr( \
	"SQ_Shortcut_to_DAYS___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_DAYS___DAY_DT as DAY_DT" \
)

# COMMAND ----------
# Processing node FIL_FALSE, type FILTER 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
EXP_LAST_RUN_DT_temp = EXP_LAST_RUN_DT.toDF(*["EXP_LAST_RUN_DT___" + col for col in EXP_LAST_RUN_DT.columns])

FIL_FALSE = EXP_LAST_RUN_DT_temp.selectExpr( \
	"EXP_LAST_RUN_DT___DAY_DT as DAY_DT").filter("false").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_WM_YARD, type TARGET 
# COLUMN COUNT: 34


Shortcut_to_WM_YARD = FIL_FALSE.selectExpr( \
	"CAST(NULL AS BIGINT) as LOCATION_ID", \
	"CAST(NULL AS BIGINT) as WM_YARD_ID", \
	"CAST(NULL AS BIGINT) as WM_TC_COMPANY_ID", \
	"CAST(NULL AS STRING) as WM_YARD_NAME", \
	"CAST(NULL AS BIGINT) as WM_LOCATION_ID", \
	"CAST(NULL AS BIGINT) as WM_TIME_ZONE_ID", \
	"CAST(NULL AS BIGINT) as GENERATE_MOVE_TASK_FLAG", \
	"CAST(NULL AS BIGINT) as GENERATE_NEXT_EQUIP_FLAG", \
	"CAST(NULL AS BIGINT) as RANGE_TASKS_FLAG", \
	"CAST(NULL AS BIGINT) as SEAL_TASK_TRGD_FLAG", \
	"CAST(NULL AS BIGINT) as OVERRIDE_SYSTEM_TASKS_FLAG", \
	"CAST(NULL AS BIGINT) as TASKING_ALLOWED_FLAG", \
	"CAST(NULL AS BIGINT) as LOCK_TRAILER_ON_MOVE_TO_DOOR_FLAG", \
	"CAST(NULL AS STRING) as YARD_SVG_FILE", \
	"CAST(NULL AS STRING) as ADDRESS", \
	"CAST(NULL AS STRING) as CITY", \
	"CAST(NULL AS STRING) as STATE_PROV", \
	"CAST(NULL AS STRING) as POSTAL_CD", \
	"CAST(NULL AS STRING) as COUNTY", \
	"CAST(NULL AS STRING) as COUNTRY_CD", \
	"CAST(NULL AS BIGINT) as MAX_EQUIPMENT_ALLOWED", \
	"CAST(NULL AS BIGINT) as UPPER_CHECK_IN_TIME_MINS", \
	"CAST(NULL AS BIGINT) as LOWER_CHECK_IN_TIME_MINS", \
	"CAST(NULL AS BIGINT) as FIXED_TIME_MINS", \
	"CAST(NULL AS BIGINT) as THRESHOLD_PERCENT", \
	"CAST(NULL AS BIGINT) as MARK_FOR_DELETION", \
	"CAST(NULL AS BIGINT) as WM_CREATED_SOURCE_TYPE", \
	"CAST(NULL AS STRING) as WM_CREATED_SOURCE", \
	"CAST(NULL AS TIMESTAMP) as WM_CREATED_TSTMP", \
	"CAST(NULL AS BIGINT) as WM_LAST_UPDATED_SOURCE_TYPE", \
	"CAST(NULL AS STRING) as WM_LAST_UPDATED_SOURCE", \
	"CAST(NULL AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", \
	"CAST(NULL AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(DAY_DT AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_WM_YARD.write.saveAsTable(f"{raw}.WM_YARD", mode = 'append')