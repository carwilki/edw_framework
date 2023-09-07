# Databricks notebook source
#Code converted on 2023-08-09 13:02:45
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from source_file import get_source_file, write_target_file
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

# Processing node EXP_prepare_inventory_records, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAIL_REC_FILE_temp = SQ_Shortcut_to_TRAIL_REC_FILE.toDF(*["SQ_Shortcut_to_TRAIL_REC_FILE___" + col for col in SQ_Shortcut_to_TRAIL_REC_FILE.columns])

EXP_prepare_inventory_records = SQ_Shortcut_to_TRAIL_REC_FILE_temp.selectExpr(
    "SQ_Shortcut_to_TRAIL_REC_FILE___sys_row_id as sys_row_id",
    "TO_DATE(SUBSTRING(SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_FILENAME, INSTR(SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_FILENAME, '20'), 8), 'yyyyMMdd') as DAY_DT",
    "0 as BAL_FILE_ID",
    "SQ_Shortcut_to_TRAIL_REC_FILE___FILE_SEQ_NBR as FILE_SEQ_NBR",
    "CASE WHEN TRIM(SQ_Shortcut_to_TRAIL_REC_FILE___TRAILREC) != 'TRAILREC' THEN '1' WHEN SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_CNT != SQ_Shortcut_to_TRAIL_REC_FILE___INTRFACE_CNT THEN '1' ELSE '0' END as BAL_FAIL_FLAG",
    "SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_CNT as SOURCE_CNT",
    "SQ_Shortcut_to_TRAIL_REC_FILE___INTRFACE_CNT as INTRFACE_CNT",
    "SQ_Shortcut_to_TRAIL_REC_FILE___SOURCE_FILENAME as SOURCE_FILENAME",
    "date_trunc('day', current_timestamp()).alias('LOAD_DT')"
)

# COMMAND ----------

# Processing node AGGTRANS, type AGGREGATOR 
# COLUMN COUNT: 5

AGGTRANS = EXP_prepare_inventory_records \
    .groupBy("DAY_DT", "BAL_FILE_ID") \
    .agg(
        min(col('FILE_SEQ_NBR_var')).alias("FILE_SEQ_NBR_out"),
        max(col("BAL_FAIL_FLAG")).alias("BAL_FAIL_FLAG_out"),
        min(col('SOURCE_FILENAME_var')).alias("SOURCE_FILENAME_out")
    ) \
    .withColumn("sys_row_id", monotonically_increasing_id()) \
    .withColumn(
        "FILE_SEQ_NBR_var",
        expr("IF(BAL_FAIL_FLAG_out = 1, FILE_SEQ_NBR_out, NULL)")
    ) \
    .withColumn(
        "SOURCE_FILENAME_var",
        expr("IF(BAL_FAIL_FLAG_out = 1, SOURCE_FILENAME_out, NULL)")
    )


# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
AGGTRANS_temp = AGGTRANS.toDF(*["AGGTRANS___" + col for col in AGGTRANS.columns])

EXPTRANS = AGGTRANS_temp.selectExpr(
	"AGGTRANS___DAY_DT as DAY_DT",
	"AGGTRANS___BAL_FILE_ID as BAL_FILE_ID",
	"AGGTRANS___FILE_SEQ_NBR_out as FILE_SEQ_NBR",
	"AGGTRANS___BAL_FAIL_FLAG_out as BAL_FAIL_FLAG",
	"AGGTRANS___SOURCE_FILENAME_out as SOURCE_FILENAME").selectExpr(
	"AGGTRANS___sys_row_id as sys_row_id",
	"AGGTRANS___DAY_DT as DAY_DT",
	"AGGTRANS___BAL_FILE_ID as BAL_FILE_ID",
	"AGGTRANS___FILE_SEQ_NBR as FILE_SEQ_NBR",
	"IF (AGGTRANS___BAL_FAIL_FLAG = 1, RAISE_ERROR(AGGTRANS___BAL_FAIL_FLAG_msg), AGGTRANS___BAL_FAIL_FLAG) as BAL_FAIL_FLAG_out"
).withColumn("BAL_FAIL_FLAG_msg", expr("""IF (AGGTRANS___BAL_FAIL_FLAG = 1, concat( 'A trailer record file in the Inventory business area failed to balance.  The filename is ' , AGGTRANS___SOURCE_FILENAME , '.  Other invday* files may also be out of balance.' ), NULL)"""))

# COMMAND ----------

# Processing node FILTRANS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])

FILTRANS = EXPTRANS_temp.selectExpr(
	"EXPTRANS___DAY_DT as DAY_DT",
	"EXPTRANS___BAL_FILE_ID as BAL_FILE_ID",
	"EXPTRANS___FILE_SEQ_NBR as FILE_SEQ_NBR",
	"EXPTRANS___BAL_FAIL_FLAG_out as BAL_FAIL_FLAG").filter("false").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_BAL_FILE_INTRFACE_TXT, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_BAL_FILE_INTRFACE_TXT = FILTRANS.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(BAL_FILE_ID AS DECIMAL(10)) as BAL_FILE_ID",
	"CAST(BAL_SEQ_NBR AS DECIMAL(10)) as BAL_SEQ_NBR",
	"CAST(BAL_FAIL_FLAG AS TINYINT) as BAL_FAIL_FLAG",
	"CAST(NULL AS BIGINT) as TRAIL_REC_CNT",
	"CAST(NULL AS BIGINT) as INTRFACE_REC_CNT",
	"CAST(NULL AS STRING) as BAL_FILENAME_TX",
	"CAST(NULL AS TIMESTAMP) as LOAD_DT"
)

write_target_file('shortcut_to_bal_file_intrface_txt1.out')
