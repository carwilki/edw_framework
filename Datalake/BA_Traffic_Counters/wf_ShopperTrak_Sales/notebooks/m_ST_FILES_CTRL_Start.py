# Databricks notebook source
#Code converted on 2023-11-29 14:36:07
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script


# COMMAND ----------

# Processing node SQ_Shortcut_to_ST_FILES_CTRL, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_ST_FILES_CTRL = spark.sql(f"""SELECT
ST_ID,
ST_DAY_DT
FROM {legacy}.ST_FILES_CTRL order by st_id desc , st_day_dt desc limit 1""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_ST_FILES_CTRL = SQ_Shortcut_to_ST_FILES_CTRL \
	.withColumnRenamed(SQ_Shortcut_to_ST_FILES_CTRL.columns[0],'ST_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ST_FILES_CTRL.columns[1],'ST_DAY_DT')

# COMMAND ----------

# Processing node Rnk_STCtrl, type RANK 
# COLUMN COUNT: 6

# Rnk_STCtrl = SQ_Shortcut_to_ST_FILES_CTRL.select(
# 	"SQ_Shortcut_to_ST_FILES_CTRL___RANKINDEX as RANKINDEX",
# 	"SQ_Shortcut_to_ST_FILES_CTRL___ST_ID as ST_ID",
# 	"SQ_Shortcut_to_ST_FILES_CTRL___ST_DAY_DT as ST_DAY_DT" \
# , rank().over(Window.partitionBy(SQ_Shortcut_to_ST_FILES_CTRL['ST_ID']).orderBy(SQ_Shortcut_to_ST_FILES_CTRL['ST_ID'].desc())).alias('RANKINDEX')).filter(col('RANKINDEX') <= 1)

# COMMAND ----------

# Processing node Exp_STCtrl, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
# Rnk_STCtrl_temp = Rnk_STCtrl.toDF(*["Rnk_STCtrl___" + col for col in Rnk_STCtrl.columns])

Exp_STCtrl = SQ_Shortcut_to_ST_FILES_CTRL.selectExpr(
	"ST_ID + 1 as ST_ID",
	"DATE_ADD(ST_DAY_DT ,1) as ST_DAY_DT",
	"'Y' as ST_CREATE_FILE_FLAG",
	"CURRENT_TIMESTAMP as LOAD_TSTMP",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP"
)

# COMMAND ----------

# Processing node Fil_ST_Ctrl, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
Exp_STCtrl_temp = Exp_STCtrl.toDF(*["Exp_STCtrl___" + col for col in Exp_STCtrl.columns])

Fil_ST_Ctrl = Exp_STCtrl_temp.selectExpr(
	"Exp_STCtrl___ST_ID as ST_ID",
	"Exp_STCtrl___ST_DAY_DT as ST_DAY_DT",
	"Exp_STCtrl___ST_CREATE_FILE_FLAG as ST_CREATE_FILE_FLAG",
	"Exp_STCtrl___LOAD_TSTMP as LOAD_TSTMP",
	"Exp_STCtrl___UPDATE_TSTMP as UPDATE_TSTMP").filter("ST_DAY_DT < DATE_TRUNC ( 'DD' , current_date )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_ST_FILES_CTRL, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_ST_FILES_CTRL = Fil_ST_Ctrl.selectExpr(
	"CAST(ST_ID AS INT) as ST_ID",
	"CAST(ST_DAY_DT AS DATE) as ST_DAY_DT",
	"CAST(ST_CREATE_FILE_FLAG AS STRING) as ST_CREATE_FILE_FLAG",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP"
)
try:
    # chk=DuplicateChecker()
    # chk.check_for_duplicate_primary_keys(Shortcut_to_ST_FILES_CTRL,[key])
    Shortcut_to_ST_FILES_CTRL.write.saveAsTable(f'{legacy}.ST_FILES_CTRL', mode = 'append')
except Exception as e:
    raise e
