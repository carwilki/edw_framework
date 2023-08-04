#Code converted on 2023-07-19 11:07:13
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
refined_perf_table = f"{legacy}.SVC_SERVICE_SUSPENSION_LOG"
raw_perf_table = f"{raw}.SVC_SERVICE_SUSPENSION_LOG_PRE"
site_profile_table = f"{legacy}.site_profile"

# COMMAND ----------

# Processing node LKP_SVC_SUSPENSION_SUBMITTER_TITLE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 3

query = f"SELECT SUBMITTED_BY_POSITION_ID, SUBMITTED_BY_POSITION_DESC FROM {legacy}.SVC_SUSPENSION_SUBMITTER_TITLE"
LKP_SVC_SUSPENSION_SUBMITTER_TITLE_SRC = spark.sql(query).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
LKP_SVC_SUSPENSION_SUBMITTER_TITLE_SRC = (LKP_SVC_SUSPENSION_SUBMITTER_TITLE_SRC
	.withColumnRenamed(LKP_SVC_SUSPENSION_SUBMITTER_TITLE_SRC.columns[0],'SUBMITTED_BY_POSITION_ID')
	.withColumnRenamed(LKP_SVC_SUSPENSION_SUBMITTER_TITLE_SRC.columns[1],'SUBMITTED_BY_POSITION_DESC')
#	.withColumnRenamed(LKP_SVC_SUSPENSION_SUBMITTER_TITLE_SRC.columns[2],'IN_SUBMITTED_BY_POSITION')
)

# COMMAND ----------

# Processing node LKP_SVC_SERVICE_SUSPENSION_REASON_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 3

query = f"SELECT SUSPENSION_REASON_ID, SUSPENSION_REASON_DESC FROM {legacy}.SVC_SERVICE_SUSPENSION_REASON"

LKP_SVC_SERVICE_SUSPENSION_REASON_SRC = spark.sql(query).withColumn("sys_row_id", monotonically_increasing_id())


# Conforming fields names to the component layout
LKP_SVC_SERVICE_SUSPENSION_REASON_SRC = (LKP_SVC_SERVICE_SUSPENSION_REASON_SRC
	.withColumnRenamed(LKP_SVC_SERVICE_SUSPENSION_REASON_SRC.columns[0],'SUSPENSION_REASON_ID')
	.withColumnRenamed(LKP_SVC_SERVICE_SUSPENSION_REASON_SRC.columns[1],'SUSPENSION_REASON_DESC')
	# .withColumnRenamed(LKP_SVC_SERVICE_SUSPENSION_REASON_SRC.columns[2],'IN_SUSPENSION_REASON')
 )

# COMMAND ----------

# Processing node LKP_SVC_SERVICE_AREA_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 3

query = f"SELECT SERVICE_AREA_ID, UPPER(TRIM(SERVICE_AREA_DESC)) FROM {legacy}.SVC_SERVICE_AREA"

LKP_SVC_SERVICE_AREA_SRC = spark.sql(query).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
LKP_SVC_SERVICE_AREA_SRC = (LKP_SVC_SERVICE_AREA_SRC
	.withColumnRenamed(LKP_SVC_SERVICE_AREA_SRC.columns[0],'SERVICE_AREA_ID')
	.withColumnRenamed(LKP_SVC_SERVICE_AREA_SRC.columns[1],'SERVICE_AREA_DESC')
	# .withColumnRenamed(LKP_SVC_SERVICE_AREA_SRC.columns[2],'IN_SERVICE_AREA')
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE = spark.sql(f"""SELECT
DAY_DT,
STORE_NUMBER,
CASE WHEN UPPER(TRIM(SERVICE_AREA)) = 'DAY DAY CAMP ONLY' THEN 'DAY CAMP ONLY' ELSE UPPER(TRIM(SERVICE_AREA)) END as SERVICE_AREA,
SUSPENSION_REASON,
SUBMITTED_BY,
SUBMITTED_BY_POSITION,
COMMENTS,
REVERSAL_TSTMP,
CREATE_TSTMP,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG = spark.sql(f"""SELECT
DAY_DT,
LOCATION_ID,
SERVICE_AREA_ID,
SUSPENSION_REASON_ID,
SUBMITTED_BY,
SUBMITTED_BY_POSITION_ID,
COMMENTS,
REVERSAL_TSTMP,
CREATE_TSTMP,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""")  #.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_SITE, type JOINER 
# COLUMN COUNT: 13

JNR_SITE = SQ_Shortcut_to_SITE_PROFILE.join(SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE.STORE_NUMBER],'inner')

# COMMAND ----------

# Processing node LKP_SVC_SUSPENSION_SUBMITTER_TITLE, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3


LKP_SVC_SUSPENSION_SUBMITTER_TITLE_lookup_result = JNR_SITE.selectExpr("SUBMITTED_BY_POSITION as IN_SUBMITTED_BY_POSITION", "sys_row_id").join(LKP_SVC_SUSPENSION_SUBMITTER_TITLE_SRC.selectExpr(
	"SUBMITTED_BY_POSITION_ID", "SUBMITTED_BY_POSITION_DESC"), (col('SUBMITTED_BY_POSITION_DESC') == col('IN_SUBMITTED_BY_POSITION')), 'left') \
.withColumn('row_num_SUBMITTED_BY_POSITION_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("SUBMITTED_BY_POSITION_ID")))

LKP_SVC_SUSPENSION_SUBMITTER_TITLE = LKP_SVC_SUSPENSION_SUBMITTER_TITLE_lookup_result.filter("row_num_SUBMITTED_BY_POSITION_ID = 1").select(
	LKP_SVC_SUSPENSION_SUBMITTER_TITLE_lookup_result.sys_row_id,
	col('SUBMITTED_BY_POSITION_ID')
)

# COMMAND ----------

# Processing node LKP_SVC_SERVICE_SUSPENSION_REASON, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

LKP_SVC_SERVICE_SUSPENSION_REASON_lookup_result = JNR_SITE.selectExpr("SUSPENSION_REASON as IN_SUSPENSION_REASON","sys_row_id").join(LKP_SVC_SERVICE_SUSPENSION_REASON_SRC.selectExpr('SUSPENSION_REASON_ID','SUSPENSION_REASON_DESC'), (col('SUSPENSION_REASON_DESC') == col('IN_SUSPENSION_REASON')), 'left') \
.withColumn('row_num_SUSPENSION_REASON_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("SUSPENSION_REASON_ID")))

LKP_SVC_SERVICE_SUSPENSION_REASON = LKP_SVC_SERVICE_SUSPENSION_REASON_lookup_result.filter("row_num_SUSPENSION_REASON_ID = 1").select(
	LKP_SVC_SERVICE_SUSPENSION_REASON_lookup_result.sys_row_id,
	col('SUSPENSION_REASON_ID')
)

# COMMAND ----------

# Processing node LKP_SVC_SERVICE_AREA, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

LKP_SVC_SERVICE_AREA_lookup_result = JNR_SITE.selectExpr(
	"SERVICE_AREA as IN_SERVICE_AREA", "sys_row_id").join(LKP_SVC_SERVICE_AREA_SRC.selectExpr("SERVICE_AREA_ID", "SERVICE_AREA_DESC"), (col('SERVICE_AREA_DESC') == col('IN_SERVICE_AREA')), 'left') \
.withColumn('row_num_SERVICE_AREA_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("SERVICE_AREA_ID")))

LKP_SVC_SERVICE_AREA = LKP_SVC_SERVICE_AREA_lookup_result.filter("row_num_SERVICE_AREA_ID = 1").select(
	LKP_SVC_SERVICE_AREA_lookup_result.sys_row_id,
	col('SERVICE_AREA_ID')
)

# COMMAND ----------

# Processing node EXP_PRE_FIELDS, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
LKP_SVC_SUSPENSION_SUBMITTER_TITLE_temp = LKP_SVC_SUSPENSION_SUBMITTER_TITLE.toDF(*["LKP_SVC_SUSPENSION_SUBMITTER_TITLE___" + col for col in LKP_SVC_SUSPENSION_SUBMITTER_TITLE.columns])
LKP_SVC_SERVICE_SUSPENSION_REASON_temp = LKP_SVC_SERVICE_SUSPENSION_REASON.toDF(*["LKP_SVC_SERVICE_SUSPENSION_REASON___" + col for col in LKP_SVC_SERVICE_SUSPENSION_REASON.columns])
JNR_SITE_temp = JNR_SITE.toDF(*["JNR_SITE___" + col for col in JNR_SITE.columns])
LKP_SVC_SERVICE_AREA_temp = LKP_SVC_SERVICE_AREA.toDF(*["LKP_SVC_SERVICE_AREA___" + col for col in LKP_SVC_SERVICE_AREA.columns])


JNR_SITE_alias = JNR_SITE.alias("JNR_SITE")
LKP_SVC_SUSPENSION_SUBMITTER_TITLE_alias = LKP_SVC_SUSPENSION_SUBMITTER_TITLE.alias("LKP_SVC_SUSPENSION_SUBMITTER_TITLE")
LKP_SVC_SERVICE_SUSPENSION_REASON_alias = LKP_SVC_SERVICE_SUSPENSION_REASON.alias("LKP_SVC_SERVICE_SUSPENSION_REASON")
LKP_SVC_SERVICE_AREA_alias = LKP_SVC_SERVICE_AREA.alias("LKP_SVC_SERVICE_AREA")

EXP_PRE_FIELDS_joined = JNR_SITE_alias.join(
    LKP_SVC_SUSPENSION_SUBMITTER_TITLE_alias,
    col("JNR_SITE.sys_row_id") == col("LKP_SVC_SUSPENSION_SUBMITTER_TITLE.sys_row_id"),
    "inner"
).join(
    LKP_SVC_SERVICE_SUSPENSION_REASON_alias,
    col("LKP_SVC_SUSPENSION_SUBMITTER_TITLE.sys_row_id") == col("LKP_SVC_SERVICE_SUSPENSION_REASON.sys_row_id"),
    "inner"
).join(
    LKP_SVC_SERVICE_AREA_alias,
    col("LKP_SVC_SERVICE_SUSPENSION_REASON.sys_row_id") == col("LKP_SVC_SERVICE_AREA.sys_row_id"),
    "inner"
)


# COMMAND ----------

EXP_PRE_FIELDS = EXP_PRE_FIELDS_joined.selectExpr(
	"JNR_SITE.sys_row_id as sys_row_id",
	"DAY_DT",
	"LOCATION_ID",
	"SERVICE_AREA_ID",
	"SUSPENSION_REASON_ID",
	"SUBMITTED_BY",
	"SUBMITTED_BY_POSITION_ID",
	"COMMENTS",
	"REVERSAL_TSTMP",
	"CREATE_TSTMP",
	"UPDATE_TSTMP",
	"LOAD_TSTMP"
)

# COMMAND ----------

# Processing node JNR_HIST, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_temp = SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG.toDF(*["SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___" + col for col in SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG.columns])
EXP_PRE_FIELDS_temp = EXP_PRE_FIELDS.toDF(*["EXP_PRE_FIELDS___" + col for col in EXP_PRE_FIELDS.columns])

JNR_HIST = SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_temp.join(EXP_PRE_FIELDS_temp,[SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_temp.SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___DAY_DT == EXP_PRE_FIELDS_temp.EXP_PRE_FIELDS___DAY_DT, SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_temp.SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___LOCATION_ID == EXP_PRE_FIELDS_temp.EXP_PRE_FIELDS___LOCATION_ID, SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_temp.SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___SERVICE_AREA_ID == EXP_PRE_FIELDS_temp.EXP_PRE_FIELDS___SERVICE_AREA_ID],'right_outer').selectExpr(
  "EXP_PRE_FIELDS___sys_row_id AS sys_row_id",
	"EXP_PRE_FIELDS___DAY_DT as DAY_DT",
	"EXP_PRE_FIELDS___LOCATION_ID as LOCATION_ID",
	"EXP_PRE_FIELDS___SERVICE_AREA_ID as SERVICE_AREA_ID",
	"EXP_PRE_FIELDS___SUSPENSION_REASON_ID as SUSPENSION_REASON_ID",
	"EXP_PRE_FIELDS___SUBMITTED_BY as SUBMITTED_BY",
	"EXP_PRE_FIELDS___SUBMITTED_BY_POSITION_ID as SUBMITTED_BY_POSITION_ID",
	"EXP_PRE_FIELDS___COMMENTS as COMMENTS",
	"EXP_PRE_FIELDS___REVERSAL_TSTMP as REVERSAL_TSTMP",
	"EXP_PRE_FIELDS___CREATE_TSTMP as CREATE_TSTMP",
	"EXP_PRE_FIELDS___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_PRE_FIELDS___LOAD_TSTMP as LOAD_TSTMP",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___DAY_DT as DAY_DT1",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___SERVICE_AREA_ID as SERVICE_AREA_ID1",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___SUSPENSION_REASON_ID as SUSPENSION_REASON_ID1",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___SUBMITTED_BY as SUBMITTED_BY1",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___SUBMITTED_BY_POSITION_ID as SUBMITTED_BY_POSITION_ID1",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___COMMENTS as COMMENTS1",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___REVERSAL_TSTMP as REVERSAL_TSTMP1",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___CREATE_TSTMP as CREATE_TSTMP1",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___UPDATE_TSTMP as UPDATE_TSTMP1",
	"SQ_Shortcut_to_SVC_SERVICE_SUSPENSION_LOG___LOAD_TSTMP as LOAD_TSTMP1")

# COMMAND ----------

# Processing node EXP_FIELDS, type EXPRESSION 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
JNR_HIST_temp = JNR_HIST.toDF(*["JNR_HIST___" + col for col in JNR_HIST.columns])

EXP_FIELDS = JNR_HIST_temp.selectExpr(
	"JNR_HIST___sys_row_id as sys_row_id",
	"JNR_HIST___DAY_DT1 as DAY_DT1",
	"JNR_HIST___LOCATION_ID1 as LOCATION_ID1",
	"JNR_HIST___SERVICE_AREA_ID1 as SERVICE_AREA_ID1",
	"JNR_HIST___DAY_DT as DAY_DT",
	"JNR_HIST___LOCATION_ID as LOCATION_ID",
	"JNR_HIST___SERVICE_AREA_ID as SERVICE_AREA_ID",
	"JNR_HIST___SUSPENSION_REASON_ID as SUSPENSION_REASON_ID",
	"JNR_HIST___SUBMITTED_BY as SUBMITTED_BY",
	"JNR_HIST___SUBMITTED_BY_POSITION_ID as SUBMITTED_BY_POSITION_ID",
	"JNR_HIST___COMMENTS as COMMENTS",
	"JNR_HIST___REVERSAL_TSTMP as REVERSAL_TSTMP",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
).withColumn("VAR_RECORD_FOUND", expr("""IF(( DAY_DT1 IS NULL and LOCATION_ID1 IS NULL and SERVICE_AREA_ID1 IS NULL ), 0, 1)""")) \
 .withColumn("CREATE_TSTMP", expr("IF(VAR_RECORD_FOUND = 0, CURRENT_TIMESTAMP, NULL)")) \
 .withColumn("UPDATE_TSTMP", expr("IF(VAR_RECORD_FOUND = 1, CURRENT_TIMESTAMP, NULL)")) \
 .withColumnRenamed("VAR_RECORD_FOUND", "RECORD_FOUND")


# COMMAND ----------

# Processing node UPD_ROW_ACTION, type UPDATE_STRATEGY 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
EXP_FIELDS_temp = EXP_FIELDS.toDF(*["EXP_FIELDS___" + col for col in EXP_FIELDS.columns])

UPD_ROW_ACTION = EXP_FIELDS_temp.selectExpr(
	"EXP_FIELDS___DAY_DT as DAY_DT",
	"EXP_FIELDS___LOCATION_ID as LOCATION_ID",
	"EXP_FIELDS___SERVICE_AREA_ID as SERVICE_AREA_ID",
	"EXP_FIELDS___SUSPENSION_REASON_ID as SUSPENSION_REASON_ID",
	"EXP_FIELDS___SUBMITTED_BY as SUBMITTED_BY",
	"EXP_FIELDS___SUBMITTED_BY_POSITION_ID as SUBMITTED_BY_POSITION_ID",
	"EXP_FIELDS___COMMENTS as COMMENTS",
	"EXP_FIELDS___REVERSAL_TSTMP as REVERSAL_TSTMP",
	"EXP_FIELDS___CREATE_TSTMP as CREATE_TSTMP",
	"EXP_FIELDS___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_FIELDS___LOAD_TSTMP as LOAD_TSTMP", 
	"IF(EXP_FIELDS___RECORD_FOUND=0, 0, 1) AS `pyspark_data_action`"
)

# COMMAND ----------

# Processing node Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_1, type TARGET 
# COLUMN COUNT: 11

try:
  primary_key = """source.DAY_DT = target.DAY_DT AND source.LOCATION_ID = target.LOCATION_ID AND source.SERVICE_AREA_ID = target.SERVICE_AREA_ID"""
  # refined_perf_table = "SVC_SERVICE_SUSPENSION_LOG"
  executeMerge(UPD_ROW_ACTION, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("SVC_SERVICE_SUSPENSION_LOG", "SVC_SERVICE_SUSPENSION_LOG", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("SVC_SERVICE_SUSPENSION_LOG", "SVC_SERVICE_SUSPENSION_LOG","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	
