# Databricks notebook source
#Code converted on 2023-08-09 13:02:48
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from source_file import get_source_file
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')

# uncomment before checking in
# args = parser.parse_args()
# env = args.env
# remove before checking in
env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node SQ_Shortcut_to_TRAIL_REC_FILE, type SOURCE 
# COLUMN COUNT: 5

key = "glpct"
_bucket=getParameterValue(raw,'BA_FINANCIALS_Parameter.prm','BA_FINANCIALS.WF:bs_GL_Daily','source_bucket')
file_path = get_source_file(key,_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file for {key}")
    
fixed_width_data = spark.read.text(file_path)

columns = [
    expr("substring(value, 1, 10)").alias("TRAILREC"),
    expr("substring(value, 11, 15)").alias("SOURCE_CNT"),
    expr("substring(value, 26, 15)").alias("INTRFACE_CNT"),
    expr("substring(value, 41, 2)").alias("FILE_SEQ_NBR"),
    expr("substring(value, 43, 40)").alias("SOURCE_FILENAME")
]


SQ_Shortcut_to_TRAIL_REC_FILE = fixed_width_data.select(*columns).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_prepare_target, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAIL_REC_FILE_temp = SQ_Shortcut_to_TRAIL_REC_FILE.toDF(*["SQ_Shortcut_to_TRAIL_REC_FILE___" + col for col in SQ_Shortcut_to_TRAIL_REC_FILE.columns])

EXP_prepare_target = SQ_Shortcut_to_TRAIL_REC_FILE_temp.selectExpr(
	"SQ_Shortcut_to_TRAIL_REC_FILE___sys_row_id as sys_row_id",
	"6 as BAL_FILE_ID",
	"SQ_Shortcut_to_TRAIL_REC_FILE___FILE_SEQ_NBR as FILE_SEQ_NBR",
	"DECODE ( TRUE , rtrim ( SQ_Shortcut_to_TRAIL_REC_FILE___TRAILREC ) != 'TRAILREC','1' , SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_CNT != SQ_Shortcut_to_TRAIL_REC_FILE___INTRFACE_CNT , 1 , 0 ) as BAL_FAIL_FLAG",
	"SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_CNT as SOURCE_CNT",
	"SQ_Shortcut_to_TRAIL_REC_FILE___INTRFACE_CNT as INTRFACE_CNT",
	"SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_FILENAME as SOURCE_FILENAME",
	"SQ_Shortcut_to_TRAIL_REC_FILE___DAY_DT_var as DAY_DT",
	"TRUNC ( CURRENT_TIMESTAMP ) as LOAD_DT"
).withColumn("FILENAME_position_var", CHARACTER_LENGTH(RTRIM ( SOURCE_FILENAME )) - 18) \
	.withColumn("DAY_DT_string_var", SUBSTR ( SOURCE_FILENAME , FILENAME_position_var , 8 )) \
	.withColumn("DAY_DT_var", expr("""IF (IS_DATE ( SQ_Shortcut_to_TRAIL_REC_FILE___DAY_DT_string_var , 'yyyyMMdd' ), TO_DATE ( SQ_Shortcut_to_TRAIL_REC_FILE___DAY_DT_string_var , 'yyyyMMdd' ), TRUNC ( CURRENT_TIMESTAMP ))"""))

EXP_prepare_target = SQ_Shortcut_to_TRAIL_REC_FILE_temp.selectExpr(
    "SQ_Shortcut_to_TRAIL_REC_FILE___sys_row_id as sys_row_id",
    "6 as BAL_FILE_ID",
    "SQ_Shortcut_to_TRAIL_REC_FILE___FILE_SEQ_NBR as FILE_SEQ_NBR",
    "CASE WHEN rtrim(SQ_Shortcut_to_TRAIL_REC_FILE___TRAILREC) != 'TRAILREC' THEN '1' "
    "ELSE CASE WHEN SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_CNT != SQ_Shortcut_to_TRAIL_REC_FILE___INTRFACE_CNT THEN '1' "
    "ELSE '0' END END as BAL_FAIL_FLAG",
    "SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_CNT as SOURCE_CNT",
    "SQ_Shortcut_to_TRAIL_REC_FILE___INTRFACE_CNT as INTRFACE_CNT",
    "SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_FILENAME as SOURCE_FILENAME",
    "SQ_Shortcut_to_TRAIL_REC_FILE___DAY_DT_var as DAY_DT",
    "date_trunc('DAY', current_timestamp()) as LOAD_DT"
).withColumn("FILENAME_position_var", expr("LENGTH(RTRIM(SOURCE_FILENAME)) - 18")) \
 .withColumn("DAY_DT_string_var", expr("SUBSTRING(SOURCE_FILENAME, FILENAME_position_var + 1, 8)")) \
 .withColumn("DAY_DT_var", expr("IF(IS_DATE(DAY_DT_string_var, 'yyyyMMdd'), TO_DATE(DAY_DT_string_var, 'yyyyMMdd'), current_date())"))



# COMMAND ----------

# Processing node Shortcut_to_BAL_FILE_INTRFACE, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_BAL_FILE_INTRFACE = EXP_prepare_target.selectExpr(
	"CAST(DAY_DT AS DATE) as DAY_DT",
	"CAST(BAL_FILE_ID AS INT) as BAL_FILE_ID",
	"CAST(BAL_SEQ_NBR AS INT) as BAL_SEQ_NBR",
	"CAST(BAL_FAIL_FLAG AS TINYINT) as BAL_FAIL_FLAG",
	"CAST(TRAIL_REC_CNT AS BIGINT) as TRAIL_REC_CNT",
	"CAST(INTRFACE_REC_CNT AS BIGINT) as INTRFACE_REC_CNT",
	"CAST(BAL_FILENAME_TX AS STRING) as BAL_FILENAME_TX",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
Shortcut_to_BAL_FILE_INTRFACE.write.saveAsTable(f'{legacy}.BAL_FILE_INTRFACE')
