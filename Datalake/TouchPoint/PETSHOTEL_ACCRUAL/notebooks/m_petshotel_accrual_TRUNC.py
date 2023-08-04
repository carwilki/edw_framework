#Code converted on 2023-07-19 14:19:36
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

schemaName = refine
tableName = 'PETSHOTEL_ACCRUAL'
target_table_name = schemaName + "." + tableName

# Set global variables
starttime = datetime.now() #start timestamp of the script


# Processing node SQ_Shortcut_to_PETSHOTEL_ACCRUAL, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_PETSHOTEL_ACCRUAL = spark.sql(f"""SELECT DAY_DT FROM {target_table_name} LIMIT 1""").withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_Shortcut_to_PETSHOTEL_ACCRUAL = SQ_Shortcut_to_PETSHOTEL_ACCRUAL.withColumnRenamed(SQ_Shortcut_to_PETSHOTEL_ACCRUAL.columns[0],'DAY_DT')


# Processing node FIL_PASS_NO_ROW, type FILTER 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_PETSHOTEL_ACCRUAL_temp = SQ_Shortcut_to_PETSHOTEL_ACCRUAL.toDF(*["SQ_Shortcut_to_PETSHOTEL_ACCRUAL___" + col for col in SQ_Shortcut_to_PETSHOTEL_ACCRUAL.columns])

FIL_PASS_NO_ROW = SQ_Shortcut_to_PETSHOTEL_ACCRUAL_temp.selectExpr(
	"SQ_Shortcut_to_PETSHOTEL_ACCRUAL___DAY_DT as DAY_DT").filter("false").withColumn("sys_row_id", monotonically_increasing_id())


# Processing node Shortcut_to_PETSHOTEL_ACCRUAL_truncate, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_PETSHOTEL_ACCRUAL_truncate = FIL_PASS_NO_ROW.selectExpr( 
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT", 
	"CAST(NULL AS TIMESTAMP) as ACCRUAL_DT", 
	"CAST(NULL AS BIGINT) as LOCATION_ID", 
	"CAST(NULL AS BIGINT) as STORE_NBR", 
	"CAST(NULL AS BIGINT) as TP_INVOICE_NBR",
	"CAST(NULL AS TIMESTAMP) as SERVICE_START_DT", 
	"CAST(NULL AS TIMESTAMP) as SERVICE_END_DT", 
	"CAST(NULL AS INT) as LENGTH_OF_STAY", 
	"CAST(NULL AS DECIMAL(38,2)) as TP_EXTENDED_PRICE", 
	"CAST(NULL AS INT) as PETCOUNT", 
	"CAST(NULL AS DECIMAL(38,6)) as ACCRUAL_AMT", 
	"CAST(NULL AS DECIMAL(15,6)) as EXCH_RATE_PCNT", 
	"CAST(NULL AS TIMESTAMP) as WEEK_DT", 
	"CAST(NULL AS INT) as FISCAL_YR", 
	"CAST(NULL AS BIGINT) as FISCAL_MO", 
	"CAST(NULL AS BIGINT) as FISCAL_WK", 
	"CAST(NULL AS TIMESTAMP) as LOAD_DT" 
)

Shortcut_to_PETSHOTEL_ACCRUAL_truncate.write.mode("overwrite").saveAsTable(f'{raw}.PETSHOTEL_ACCRUAL')
