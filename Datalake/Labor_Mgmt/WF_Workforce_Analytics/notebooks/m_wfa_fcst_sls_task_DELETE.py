#Code converted on 2023-08-08 15:41:28
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

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_FCST_SLS_TASK, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_FCST_SLS_TASK = spark.sql(f"""SELECT
WFA_FCST_SLS_TASK.DAY_DT,
WFA_FCST_SLS_TASK.ORG_ID
FROM {legacy}.WFA_FCST_SLS_TASK
WHERE DAY_DT > CURRENT_DATE() - 36""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_FCST_SLS_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_FCST_SLS_PRE = spark.sql(f"""SELECT
WFA_FCST_SLS_PRE.BUSN_DAT,
WFA_ORG_FACT_PRE.ORG_ID
FROM {raw}.WFA_ORG_FACT_PRE, {raw}.WFA_FCST_SLS_PRE
WHERE wfa_fcst_sls_pre.org_skey = wfa_org_fact_pre.org_skey

AND wfa_org_fact_pre.org_lvl_nbr = 9""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_FCST_SLS_PRE_temp = SQ_Shortcut_to_WFA_FCST_SLS_PRE.toDF(*["SQ_Shortcut_to_WFA_FCST_SLS_PRE___" + col for col in SQ_Shortcut_to_WFA_FCST_SLS_PRE.columns])
SQ_Shortcut_to_WFA_FCST_SLS_TASK_temp = SQ_Shortcut_to_WFA_FCST_SLS_TASK.toDF(*["SQ_Shortcut_to_WFA_FCST_SLS_TASK___" + col for col in SQ_Shortcut_to_WFA_FCST_SLS_TASK.columns])

JNR_TRANS = SQ_Shortcut_to_WFA_FCST_SLS_TASK_temp.join(SQ_Shortcut_to_WFA_FCST_SLS_PRE_temp,[SQ_Shortcut_to_WFA_FCST_SLS_TASK_temp.SQ_Shortcut_to_WFA_FCST_SLS_TASK___DAY_DT == SQ_Shortcut_to_WFA_FCST_SLS_PRE_temp.SQ_Shortcut_to_WFA_FCST_SLS_PRE___BUSN_DAT, SQ_Shortcut_to_WFA_FCST_SLS_TASK_temp.SQ_Shortcut_to_WFA_FCST_SLS_TASK___ORG_ID == SQ_Shortcut_to_WFA_FCST_SLS_PRE_temp.SQ_Shortcut_to_WFA_FCST_SLS_PRE___ORG_ID],'left_outer').selectExpr(
	"SQ_Shortcut_to_WFA_FCST_SLS_TASK___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_WFA_FCST_SLS_TASK___ORG_ID as ORG_ID",
	"SQ_Shortcut_to_WFA_FCST_SLS_PRE___BUSN_DAT as BUSN_DAT",
	"SQ_Shortcut_to_WFA_FCST_SLS_PRE___ORG_ID as ORG_ID1")

# COMMAND ----------
# Processing node FIL_TRANS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
JNR_TRANS_temp = JNR_TRANS.toDF(*["JNR_TRANS___" + col for col in JNR_TRANS.columns])

FIL_TRANS = JNR_TRANS_temp.selectExpr(
	"JNR_TRANS___DAY_DT as DAY_DT",
	"JNR_TRANS___ORG_ID as ORG_ID1",
	"JNR_TRANS___ORG_ID1 as ORG_ID").filter("ORG_ID is null").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_DELETE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
FIL_TRANS_temp = FIL_TRANS.toDF(*["FIL_TRANS___" + col for col in FIL_TRANS.columns])

FIL_TRANS_temp.selectExpr(
	"FIL_TRANS___DAY_DT as DAY_DT",
	"FIL_TRANS___ORG_ID1 as ORG_ID").createOrReplaceTempView("Shortcut_to_WFA_FCST_SLS_TASK1")

# COMMAND ----------
# Processing node Shortcut_to_WFA_FCST_SLS_TASK1, type TARGET 
# COLUMN COUNT: 45

spark.sql(
    f"""
MERGE INTO {legacy}.WFA_FCST_SLS_TASK target
USING Shortcut_to_WFA_FCST_SLS_TASK1 source
ON source.DAY_DT= target.DAY_DT and source.ORG_ID = target.ORG_ID
WHEN MATCHED THEN
DELETE """
)