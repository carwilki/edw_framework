#Code converted on 2023-08-08 15:41:33
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

# uncomment before checking in
args = parser.parse_args()
env = args.env
# remove before checking in
# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
empl_protected = getEnvPrefix(env) + 'empl_protected'


# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TSCHD, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_TSCHD = spark.sql(f"""SELECT
legacy_WFA_TSCHD.DAY_DT,
legacy_WFA_TSCHD.TSCHD_ID
FROM {empl_protected}.legacy_WFA_TSCHD
WHERE DAY_DT > CURRENT_DATE() - 36""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TSCHD_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_TSCHD_PRE = spark.sql(f"""SELECT
raw_WFA_TSCHD_PRE.TSCHD_ID,
raw_WFA_TSCHD_PRE.TSCHD_DAT
FROM {empl_protected}.raw_WFA_TSCHD_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_TSCHD_PRE_temp = SQ_Shortcut_to_WFA_TSCHD_PRE.toDF(*["SQ_Shortcut_to_WFA_TSCHD_PRE___" + col for col in SQ_Shortcut_to_WFA_TSCHD_PRE.columns])
SQ_Shortcut_to_WFA_TSCHD_temp = SQ_Shortcut_to_WFA_TSCHD.toDF(*["SQ_Shortcut_to_WFA_TSCHD___" + col for col in SQ_Shortcut_to_WFA_TSCHD.columns])

JNR_TRANS = SQ_Shortcut_to_WFA_TSCHD_PRE_temp.join(SQ_Shortcut_to_WFA_TSCHD_temp,[SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___TSCHD_ID == SQ_Shortcut_to_WFA_TSCHD_temp.SQ_Shortcut_to_WFA_TSCHD___TSCHD_ID, SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___TSCHD_DAT == SQ_Shortcut_to_WFA_TSCHD_temp.SQ_Shortcut_to_WFA_TSCHD___DAY_DT],'right_outer').selectExpr(
	"SQ_Shortcut_to_WFA_TSCHD___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_WFA_TSCHD___TSCHD_ID as TSCHD_ID",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___TSCHD_ID as TSCHD_ID1",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___TSCHD_DAT as TSCHD_DAT")

# COMMAND ----------
# Processing node FIL_TRANS, type FILTER 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
JNR_TRANS_temp = JNR_TRANS.toDF(*["JNR_TRANS___" + col for col in JNR_TRANS.columns])

FIL_TRANS = JNR_TRANS_temp.selectExpr(
	"JNR_TRANS___DAY_DT as DAY_DT",
	"JNR_TRANS___TSCHD_ID as TSCHD_ID",
	"JNR_TRANS___TSCHD_ID1 as TSCHD_ID1").filter("TSCHD_ID1 is null").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_TRANS, type UPDATE_STRATEGY 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
FIL_TRANS_temp = FIL_TRANS.toDF(*["FIL_TRANS___" + col for col in FIL_TRANS.columns])

UPD_TRANS = FIL_TRANS_temp.selectExpr(
	"FIL_TRANS___DAY_DT as DAY_DT",
	"FIL_TRANS___TSCHD_ID as TSCHD_ID") 

# COMMAND ----------
# Processing node Shortcut_to_WFA_TSCHD1, type TARGET 
# COLUMN COUNT: 32


Shortcut_to_WFA_TSCHD1 = UPD_TRANS.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(TSCHD_ID AS BIGINT) as TSCHD_ID"
)
# overwriteDeltaPartition(Shortcut_to_WFA_TSCHD1,'DC_NBR',dcnbr,f'{raw}.WFA_TSCHD')
#Shortcut_to_WFA_TSCHD1.write.mode("overwrite").saveAsTable(f'{empl_protected}.legacy_WFA_TSCHD')

Shortcut_to_WFA_TSCHD1.createOrReplaceTempView("Shortcut_to_WFA_TSCHD1")
# COMMAND ----------

spark.sql(f"""
MERGE INTO {empl_protected}.legacy_WFA_TSCHD target
USING Shortcut_to_WFA_TSCHD1 source
ON source.DAY_DT = target.DAY_DT AND source.TSCHD_ID = target.TSCHD_ID
WHEN MATCHED THEN
  DELETE """
)

