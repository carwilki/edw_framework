#Code converted on 2023-05-16 15:07:04
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from dbruntime import dbutils

# COMMAND ----------

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in job variables
# read_infa_paramfile('', 'm_Last_Run_Dt') ProcessingUtils

# COMMAND ----------
# Variable_declaration_comment
dbutils.widgets.text(name='Last_Run_Dt', defaultValue='1/1/1900')

# COMMAND ----------
# Processing node SQ_Shortcut_to_DAYS, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_DAYS = spark.read.jdbc(os.environ.get('NZ_EDW_CONNECT_STRING'), f"""SELECT
DAYS.DAY_DT
FROM DAYS
WHERE DAY_DT = CURRENT_DATE""", 
properties={
'user': os.environ.get('NZ_EDW_LOGIN'),
'password': os.environ.get('NZ_EDW_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LAST_RUN_DT, type EXPRESSION 
# COLUMN COUNT: 1

EXP_LAST_RUN_DT = SQ_Shortcut_to_DAYS.withColumn("SET_LAST_RUN_DT", SETVARIABLE ( '$$Last_Run_Dt' , trunc ( (to_timestamp(starttime)) ) )).selectExpr(
	"sys_row_id as sys_row_id",
	"SQ_Shortcut_to_DAYS.DAY_DT as DAY_DT"
)

# COMMAND ----------
# Processing node FIL_FALSE, type FILTER 
# COLUMN COUNT: 1

FIL_FALSE = EXP_LAST_RUN_DT.select(
	"EXP_LAST_RUN_DT.DAY_DT as DAY_DT").filter("false").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_WM_YARD, type TARGET 
# COLUMN COUNT: 34


Shortcut_to_WM_YARD = FIL_FALSE.selectExpr(
	"CAST(lit(None) AS BIGINT) as LOCATION_ID",
	"CAST(lit(None) AS BIGINT) as WM_YARD_ID",
	"CAST(lit(None) AS BIGINT) as WM_TC_COMPANY_ID",
	"CAST(lit(None) AS VARCHAR) as WM_YARD_NAME",
	"CAST(lit(None) AS BIGINT) as WM_LOCATION_ID",
	"CAST(lit(None) AS BIGINT) as WM_TIME_ZONE_ID",
	"CAST(lit(None) AS BIGINT) as GENERATE_MOVE_TASK_FLAG",
	"CAST(lit(None) AS BIGINT) as GENERATE_NEXT_EQUIP_FLAG",
	"CAST(lit(None) AS BIGINT) as RANGE_TASKS_FLAG",
	"CAST(lit(None) AS BIGINT) as SEAL_TASK_TRGD_FLAG",
	"CAST(lit(None) AS BIGINT) as OVERRIDE_SYSTEM_TASKS_FLAG",
	"CAST(lit(None) AS BIGINT) as TASKING_ALLOWED_FLAG",
	"CAST(lit(None) AS BIGINT) as LOCK_TRAILER_ON_MOVE_TO_DOOR_FLAG",
	"CAST(lit(None) AS VARCHAR) as YARD_SVG_FILE",
	"CAST(lit(None) AS VARCHAR) as ADDRESS",
	"CAST(lit(None) AS VARCHAR) as CITY",
	"CAST(lit(None) AS VARCHAR) as STATE_PROV",
	"CAST(lit(None) AS VARCHAR) as POSTAL_CD",
	"CAST(lit(None) AS VARCHAR) as COUNTY",
	"CAST(lit(None) AS VARCHAR) as COUNTRY_CD",
	"CAST(lit(None) AS BIGINT) as MAX_EQUIPMENT_ALLOWED",
	"CAST(lit(None) AS BIGINT) as UPPER_CHECK_IN_TIME_MINS",
	"CAST(lit(None) AS BIGINT) as LOWER_CHECK_IN_TIME_MINS",
	"CAST(lit(None) AS BIGINT) as FIXED_TIME_MINS",
	"CAST(lit(None) AS BIGINT) as THRESHOLD_PERCENT",
	"CAST(lit(None) AS BIGINT) as MARK_FOR_DELETION",
	"CAST(lit(None) AS BIGINT) as WM_CREATED_SOURCE_TYPE",
	"CAST(lit(None) AS VARCHAR) as WM_CREATED_SOURCE",
	"CAST(lit(None) AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(lit(None) AS BIGINT) as WM_LAST_UPDATED_SOURCE_TYPE",
	"CAST(lit(None) AS VARCHAR) as WM_LAST_UPDATED_SOURCE",
	"CAST(lit(None) AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(lit(None) AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(DAY_DT AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_WM_YARD.write.saveAsTable('WM_YARD', mode = 'append')

quit()