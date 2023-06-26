#Code converted on 2023-06-12 20:31:05
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

#<CHANGE_NEEDED>
#Thisi is how the parser is created and we spark session and dbutils
# parser = argparse.ArgumentParser()
# spark: SparkSession = SparkSession.getActiveSession()
# dbutils: DBUtils = DBUtils(spark)
#</CHANGE_NEEDED>

# COMMAND ----------

# Set global variables
starttime = datetime.now() #start timestamp of the script

#</CHANGE_NEEDED>

# COMMAND ----------
# Processing node FIL_FALSE, type FILTER 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
#<CHANGE_NEEDED>
#This should be pulled from environment variables similar to the following
#THis should be preamble that is added to each mapping to get common environment variables
# parser.add_argument("env", type=str, help="Env Variable")
# args = parser.parse_args()
# env = args.env
# # env = dbutils.widgets.get('env')

# if env is None or env == "":
#     raise ValueError("env is not set")

# refine = getEnvPrefix(env) + "refine"
# raw = getEnvPrefix(env) + "raw"
# legacy = getEnvPrefix(env) + "legacy"

# pre_dept_table = f"{raw}.WM_E_DEPT_PRE"
# refined_dept_table = f"{refine}.WM_E_DEPT"
# site_profile_table = f"{legacy}.SITE_PROFILE"
# prev_run_dt = spark.sql(
#        f"""select max(prev_run_date)
#     from {raw}.log_run_details
#     where table_name='{refine_table_name}' and lower(status)= 'completed'"""
# ).collect()[0][0]
# Read in job variables
# read_infa_paramfile('', 'm_Last_Run_Dt') ProcessingUtils

# COMMAND ----------
# Variable_declaration_comment
dbutils.widgets.text(name='Last_Run_Dt', defaultValue='1/1/1900')

# COMMAND ----------
# Processing node SQ_Shortcut_to_DAYS, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_DAYS = spark.read.jdbc(os.environ.get('BIW_Prod_CONNECT_STRING'), f"""SELECT
DAYS.DAY_DT
FROM DAYS
WHERE DAY_DT = CURRENT_DATE""", 
properties={
'user': os.environ.get('BIW_Prod_LOGIN'),
'password': os.environ.get('BIW_Prod_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LAST_RUN_DT, type EXPRESSION 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_DAYS_temp = SQ_Shortcut_to_DAYS.toDF(*["SQ_Shortcut_to_DAYS___" + col for col in SQ_Shortcut_to_DAYS.columns])

#<CHANGE_NEEDED>
# this causes a runtime error will not compile
.withColumn("SET_LAST_RUN_DT", SET $$Last_Run_Dt = TRUNC ( SESSSTARTTIME ))EXP_LAST_RUN_DT = SQ_Shortcut_to_DAYS_temp.selectExpr( \
	"SQ_Shortcut_to_DAYS___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_DAYS___DAY_DT as DAY_DT" \
)


EXP_LAST_RUN_DT_temp = EXP_LAST_RUN_DT.toDF(*["EXP_LAST_RUN_DT___" + col for col in EXP_LAST_RUN_DT.columns])

FIL_FALSE = EXP_LAST_RUN_DT_temp.selectExpr( \
	"EXP_LAST_RUN_DT___DAY_DT as DAY_DT").filter("false").withColumn("sys_row_id", monotonically_increasing_id())
#</CHANGE_NEEDED>

# COMMAND ----------
# Processing node Shortcut_to_WM_YARD, type TARGET 
# COLUMN COUNT: 34


Shortcut_to_WM_YARD = FIL_FALSE.selectExpr( 
	"CAST(lit(None) AS BIGINT) as LOCATION_ID", 
	"CAST(lit(None) AS BIGINT) as WM_YARD_ID", 
	"CAST(lit(None) AS BIGINT) as WM_TC_COMPANY_ID", \
	"CAST(lit(None) AS VARCHAR) as WM_YARD_NAME", \
	"CAST(lit(None) AS BIGINT) as WM_LOCATION_ID", \
	"CAST(lit(None) AS BIGINT) as WM_TIME_ZONE_ID", \
	"CAST(lit(None) AS BIGINT) as GENERATE_MOVE_TASK_FLAG", \
	"CAST(lit(None) AS BIGINT) as GENERATE_NEXT_EQUIP_FLAG", \
	"CAST(lit(None) AS BIGINT) as RANGE_TASKS_FLAG", \
	"CAST(lit(None) AS BIGINT) as SEAL_TASK_TRGD_FLAG", \
	"CAST(lit(None) AS BIGINT) as OVERRIDE_SYSTEM_TASKS_FLAG", \
	"CAST(lit(None) AS BIGINT) as TASKING_ALLOWED_FLAG", \
	"CAST(lit(None) AS BIGINT) as LOCK_TRAILER_ON_MOVE_TO_DOOR_FLAG", \
	"CAST(lit(None) AS VARCHAR) as YARD_SVG_FILE", \
	"CAST(lit(None) AS VARCHAR) as ADDRESS", \
	"CAST(lit(None) AS VARCHAR) as CITY", \
	"CAST(lit(None) AS VARCHAR) as STATE_PROV", \
	"CAST(lit(None) AS VARCHAR) as POSTAL_CD", \
	"CAST(lit(None) AS VARCHAR) as COUNTY", \
	"CAST(lit(None) AS VARCHAR) as COUNTRY_CD", \
	"CAST(lit(None) AS BIGINT) as MAX_EQUIPMENT_ALLOWED", \
	"CAST(lit(None) AS BIGINT) as UPPER_CHECK_IN_TIME_MINS", \
	"CAST(lit(None) AS BIGINT) as LOWER_CHECK_IN_TIME_MINS", \
	"CAST(lit(None) AS BIGINT) as FIXED_TIME_MINS", \
	"CAST(lit(None) AS BIGINT) as THRESHOLD_PERCENT", \
	"CAST(lit(None) AS BIGINT) as MARK_FOR_DELETION", \
	"CAST(lit(None) AS BIGINT) as WM_CREATED_SOURCE_TYPE", \
	"CAST(lit(None) AS VARCHAR) as WM_CREATED_SOURCE", \
	"CAST(lit(None) AS TIMESTAMP) as WM_CREATED_TSTMP", \
	"CAST(lit(None) AS BIGINT) as WM_LAST_UPDATED_SOURCE_TYPE", \
	"CAST(lit(None) AS VARCHAR) as WM_LAST_UPDATED_SOURCE", \
	"CAST(lit(None) AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", \
	"CAST(lit(None) AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(DAY_DT AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_WM_YARD.write.saveAsTable('WM_YARD', mode = 'append')