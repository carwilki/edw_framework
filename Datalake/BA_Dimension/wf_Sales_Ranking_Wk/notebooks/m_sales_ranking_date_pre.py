# Databricks notebook source
#Code converted on 2023-11-06 15:44:05
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

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'
enterprise = getEnvPrefix(env) + 'enterprise'

# Set global variables
starttime = datetime.now() #start timestamp of the script


# COMMAND ----------

# Processing node SQ_Shortcut_to_WEEKS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WEEKS = spark.sql(f"""SELECT
WEEK_DT
FROM {enterprise}.WEEKS""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
# SQ_Shortcut_to_WEEKS = SQ_Shortcut_to_WEEKS \
# 	.withColumnRenamed(SQ_Shortcut_to_WEEKS.columns[0],'') \
# 	.withColumnRenamed(SQ_Shortcut_to_WEEKS.columns[1],'WEEK_DT')

# COMMAND ----------

# Processing node FIL_SALES_RANKING_WK_PRE, type FILTER 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WEEKS_temp = SQ_Shortcut_to_WEEKS.toDF(*["SQ_Shortcut_to_WEEKS___" + col for col in SQ_Shortcut_to_WEEKS.columns])

FIL_SALES_RANKING_WK_PRE = SQ_Shortcut_to_WEEKS_temp.selectExpr(
	"SQ_Shortcut_to_WEEKS___WEEK_DT as WEEK_DT").filter("WEEK_DT > DATE_ADD(CURRENT_TIMESTAMP , -364) AND WEEK_DT < CURRENT_TIMESTAMP").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_SET_RANKING_WK, type EXPRESSION 
# COLUMN COUNT: 2
# ADD_TO_DATE(TRUNC(SYSDATE), 'D',  -(IIF(TO_INTEGER(TO_CHAR(SYSDATE,'D')) = 1, 0, TO_INTEGER(TO_CHAR(SYSDATE,'D'))-1 )))
# --Always return last Sunday
# for each involved DataFrame, append the dataframe name to each column
FIL_SALES_RANKING_WK_PRE_temp = FIL_SALES_RANKING_WK_PRE.toDF(*["FIL_SALES_RANKING_WK_PRE___" + col for col in FIL_SALES_RANKING_WK_PRE.columns])

EXP_SET_RANKING_WK = FIL_SALES_RANKING_WK_PRE_temp.selectExpr(
	"FIL_SALES_RANKING_WK_PRE___sys_row_id as sys_row_id",
	"FIL_SALES_RANKING_WK_PRE___WEEK_DT as WEEK_DT",
 "DATE_ADD(DATE_TRUNC ('dd' ,CURRENT_TIMESTAMP ) , -( IF (dayofweek(CURRENT_DATE) = 1, 0, dayofweek(CURRENT_DATE) - 1) )) as RANKING_WEEK_DT"
)

# COMMAND ----------

# Processing node Shortcut_to_SALES_RANKING_DATE_PRE, type TARGET 
# COLUMN COUNT: 2


Shortcut_to_SALES_RANKING_DATE_PRE = EXP_SET_RANKING_WK.selectExpr(
	"CAST(WEEK_DT AS DATE) as WEEK_DT",
	"CAST(RANKING_WEEK_DT AS DATE) as RANKING_WEEK_DT"
)
try:
    chk=DuplicateChecker()
    chk.check_for_duplicate_primary_keys(Shortcut_to_SALES_RANKING_DATE_PRE,["WEEK_DT"])
    Shortcut_to_SALES_RANKING_DATE_PRE.write.saveAsTable(f'{raw}.SALES_RANKING_DATE_PRE', mode = 'overwrite')
except Exception as e:
    raise e

# COMMAND ----------


