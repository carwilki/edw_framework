#Code converted on 2023-08-08 15:41:56
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
# Processing node SQ_Shortcut_to_WFA_ORG_PRE, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_WFA_ORG_PRE = spark.sql(f"""SELECT DISTINCT
WFA_ORG_PRE.ORG_LVL08_NAM
FROM {raw}.WFA_ORG_PRE
WHERE ORG_LVL_NBR > 7""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_DEPARTMENT, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_WFA_DEPARTMENT = spark.sql(f"""SELECT
WFA_DEPARTMENT.WFA_DEPT_DESC
FROM {legacy}.WFA_DEPARTMENT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRANS, type JOINER 
# COLUMN COUNT: 2

JNR_TRANS = SQ_Shortcut_to_WFA_ORG_PRE.join(SQ_Shortcut_to_WFA_DEPARTMENT,[SQ_Shortcut_to_WFA_ORG_PRE.ORG_LVL08_NAM == SQ_Shortcut_to_WFA_DEPARTMENT.WFA_DEPT_DESC],'left_outer')

# COMMAND ----------
# Processing node FIL_TRANS, type FILTER 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
JNR_TRANS_temp = JNR_TRANS.toDF(*["JNR_TRANS___" + col for col in JNR_TRANS.columns])

FIL_TRANS = JNR_TRANS_temp.selectExpr(
	"JNR_TRANS___ORG_LVL08_NAM as ORG_LVL08_NAM",
	"JNR_TRANS___WFA_DEPT_DESC as WFA_DEPT_DESC").filter("WFA_DEPT_DESC is null").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_WFA_DEPARTMENT, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
FIL_TRANS_temp = FIL_TRANS.toDF(*["FIL_TRANS___" + col for col in FIL_TRANS.columns])

EXP_WFA_DEPARTMENT = FIL_TRANS_temp\
	.withColumn("NEXTVAL", monotonically_increasing_id()) \
	.selectExpr(
	"FIL_TRANS___ORG_LVL08_NAM as WFA_DEPT_DESC",
	"FIL_TRANS___sys_row_id as sys_row_id",
	"DATE_TRUNC ('day', CURRENT_TIMESTAMP ) as LOAD_DT",
	"NEXTVAL as NEXTVAL"
)

# COMMAND ----------
# Processing node Shortcut_to_WFA_DEPARTMENT_1, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_WFA_DEPARTMENT_1 = EXP_WFA_DEPARTMENT.selectExpr(
	#"CAST(NEXTVAL AS SMALLINT) as WFA_DEPT_ID",
	"WFA_DEPT_DESC as WFA_DEPT_DESC",
	"CAST(LOAD_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
# overwriteDeltaPartition(Shortcut_to_WFA_DEPARTMENT_1,'DC_NBR',dcnbr,f'{raw}.WFA_DEPARTMENT')
Shortcut_to_WFA_DEPARTMENT_1.write.mode("append").saveAsTable(f'{legacy}.WFA_DEPARTMENT')