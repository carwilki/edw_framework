# COMMAND ----------
#Code converted on 2023-08-01 13:49:15
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

# dbutils.widgets.text(name = 'env', defaultValue = 'dev')
# print(env)
# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

enterprise = getEnvPrefix(env) + 'enterprise'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
# (username, password, connection_string) = getConfig('wfa_time_sheet_punch', env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_DEPARTMENT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_DEPARTMENT = spark.sql(f"""SELECT
WFA_DEPT_ID,
WFA_DEPT_DESC
FROM {legacy}.WFA_DEPARTMENT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_BUSINESS_AREA, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_BUSINESS_AREA = spark.sql(f"""SELECT
WFA_BUSN_AREA_ID,
WFA_BUSN_AREA_DESC
FROM {legacy}.WFA_BUSINESS_AREA""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TASK, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_TASK = spark.sql(f"""SELECT
WFA_TASK_ID,
WFA_TASK_DESC
FROM {legacy}.WFA_TASK""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_DAYS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_DAYS = spark.sql(f"""SELECT
DAY_DT,
WEEK_DT
FROM {enterprise}.DAYS""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE = spark.sql(f"""SELECT
DAY_DT,
TIME_SHEET_ITEM_ID,
STRT_DTM,
END_DTM,
STORE_NBR,
EMPLOYEE_ID,
WFA_BUSN_AREA_DESC,
WFA_DEPT_DESC,
WFA_TASK_DESC
FROM {raw}.WFA_TIME_SHEET_PUNCH_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_DAYS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_DAYS_temp = SQ_Shortcut_to_DAYS.toDF(*["SQ_Shortcut_to_DAYS___" + col for col in SQ_Shortcut_to_DAYS.columns])
SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE_temp = SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE.toDF(*["SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___" + col for col in SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE.columns])

JNR_DAYS = SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE_temp.join(SQ_Shortcut_to_DAYS_temp,[SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE_temp.SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___DAY_DT == SQ_Shortcut_to_DAYS_temp.SQ_Shortcut_to_DAYS___DAY_DT],'left_outer').selectExpr( \
	"SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___EMPLOYEE_ID as PERSONNUM", \
	"SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___DAY_DT as DAY_DT", \
	"SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___STRT_DTM as START_DTM", \
	"SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___END_DTM as END_DTM", \
	"SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___TIME_SHEET_ITEM_ID as TIMESHEETITEMID", \
	"SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___STORE_NBR as STORE_NUMBER", \
	"SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___WFA_BUSN_AREA_DESC as BUSN_AREA_DESC", \
	"SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___WFA_DEPT_DESC as DEPT_DESC", \
	"SQ_Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE___WFA_TASK_DESC as TASK_DESC", \
	"SQ_Shortcut_to_DAYS___DAY_DT as i_DAY_DT", \
	"SQ_Shortcut_to_DAYS___WEEK_DT as WEEK_DT")

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE_RPT = spark.sql(f"""SELECT
LOCATION_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_WFA_BUSINESS_AREA, type JOINER 
# COLUMN COUNT: 12

JNR_WFA_BUSINESS_AREA = JNR_DAYS.join(SQ_Shortcut_to_WFA_BUSINESS_AREA,[JNR_DAYS.BUSN_AREA_DESC == SQ_Shortcut_to_WFA_BUSINESS_AREA.WFA_BUSN_AREA_DESC],'left_outer')

# COMMAND ----------
# Processing node JNR_WFA_DEPARTMENT, type JOINER 
# COLUMN COUNT: 13

JNR_WFA_DEPARTMENT = JNR_WFA_BUSINESS_AREA.join(SQ_Shortcut_to_WFA_DEPARTMENT,[JNR_WFA_BUSINESS_AREA.DEPT_DESC == SQ_Shortcut_to_WFA_DEPARTMENT.WFA_DEPT_DESC],'left_outer')

# COMMAND ----------
# Processing node JNR_WFA_TASK, type JOINER 
# COLUMN COUNT: 14

JNR_WFA_TASK = JNR_WFA_DEPARTMENT.join(SQ_Shortcut_to_WFA_TASK,[JNR_WFA_DEPARTMENT.TASK_DESC == SQ_Shortcut_to_WFA_TASK.WFA_TASK_DESC],'left_outer')

# COMMAND ----------
# Processing node JNR_SITE_PROFILE_RPT, type JOINER 
# COLUMN COUNT: 15

JNR_SITE_PROFILE_RPT = JNR_WFA_TASK.join(SQ_Shortcut_to_SITE_PROFILE_RPT,[JNR_WFA_TASK.STORE_NUMBER == SQ_Shortcut_to_SITE_PROFILE_RPT.STORE_NBR],'left_outer')

# COMMAND ----------
# Processing node EXP_WFA_TIME_SHEET_PUNCH, type EXPRESSION 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_RPT_temp = JNR_SITE_PROFILE_RPT.toDF(*["JNR_SITE_PROFILE_RPT___" + col for col in JNR_SITE_PROFILE_RPT.columns])

EXP_WFA_TIME_SHEET_PUNCH = JNR_SITE_PROFILE_RPT_temp.selectExpr( \
	# "JNR_SITE_PROFILE_RPT___sys_row_id as sys_row_id", \
	"JNR_SITE_PROFILE_RPT___DAY_DT as DAY_DT", \
	"JNR_SITE_PROFILE_RPT___WEEK_DT as WEEK_DT", \
	"JNR_SITE_PROFILE_RPT___TIMESHEETITEMID as TIMESHEETITEMID", \
	"JNR_SITE_PROFILE_RPT___START_DTM as START_DTM", \
	"JNR_SITE_PROFILE_RPT___END_DTM as END_DTM", \
	"JNR_SITE_PROFILE_RPT___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE_RPT___PERSONNUM as PERSONNUM", \
	"JNR_SITE_PROFILE_RPT___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID", \
	"JNR_SITE_PROFILE_RPT___BUSN_AREA_DESC as BUSN_AREA_DESC", \
	"JNR_SITE_PROFILE_RPT___WFA_DEPT_ID as WFA_DEPT_ID", \
	"JNR_SITE_PROFILE_RPT___DEPT_DESC as DEPT_DESC", \
	"JNR_SITE_PROFILE_RPT___WFA_TASK_ID as WFA_TASK_ID", \
	"JNR_SITE_PROFILE_RPT___TASK_DESC as TASK_DESC", \
	"CURRENT_TIMESTAMP as UPDATE_DT", \
	"CURRENT_TIMESTAMP as LOAD_DT" \
)

# COMMAND ----------
# Processing node Shortcut_to_WFA_TIME_SHEET_PUNCH, type TARGET 
# COLUMN COUNT: 15


Shortcut_to_WFA_TIME_SHEET_PUNCH = EXP_WFA_TIME_SHEET_PUNCH.selectExpr( \
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT", \
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT", \
	"CAST(TIMESHEETITEMID AS BIGINT) as TIME_SHEET_ITEM_ID", \
	"CAST(START_DTM AS TIMESTAMP) as STRT_DTM", \
	"CAST(END_DTM AS TIMESTAMP) as END_DTM", \
	"CAST(LOCATION_ID AS INT) as LOCATION_ID", \
	"CAST(PERSONNUM AS BIGINT) as EMPLOYEE_ID ", \
	"CAST(WFA_BUSN_AREA_ID AS SMALLINT) as WFA_BUSN_AREA_ID", \
	"CAST(BUSN_AREA_DESC AS STRING) as WFA_BUSN_AREA_DESC", \
	"CAST(WFA_DEPT_ID AS SMALLINT) as WFA_DEPT_ID", \
	"CAST(DEPT_DESC AS STRING) as WFA_DEPT_DESC", \
	"CAST(WFA_TASK_ID AS SMALLINT) as WFA_TASK_ID", \
	"CAST(TASK_DESC AS STRING) as WFA_TASK_DESC", \
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT", \
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT" \
)

# is this an overwrite / Merge ?  ,mode='append' from session its an insert  prev step mapping would have done the delete.
Shortcut_to_WFA_TIME_SHEET_PUNCH.write.mode("append").saveAsTable(f'{legacy}.WFA_TIME_SHEET_PUNCH')
