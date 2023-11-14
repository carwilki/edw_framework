#Code converted on 2023-08-08 15:41:44
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
# Processing node SQ_Shortcut_to_WFA_BUSINESS_AREA, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_WFA_BUSINESS_AREA = spark.sql(f"""SELECT
WFA_BUSINESS_AREA.WFA_BUSN_AREA_DESC
FROM {legacy}.WFA_BUSINESS_AREA""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_ORG_PRE, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_WFA_ORG_PRE = spark.sql(f"""SELECT DISTINCT
WFA_ORG_PRE.ORG_LVL07_NAM
FROM {raw}.WFA_ORG_PRE
WHERE ORG_LVL_NBR > 6""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRANS, type JOINER 
# COLUMN COUNT: 2

JNR_TRANS = SQ_Shortcut_to_WFA_ORG_PRE.join(SQ_Shortcut_to_WFA_BUSINESS_AREA,[SQ_Shortcut_to_WFA_ORG_PRE.ORG_LVL07_NAM == SQ_Shortcut_to_WFA_BUSINESS_AREA.WFA_BUSN_AREA_DESC],'left_outer')

# COMMAND ----------
# Processing node FIL_TRANS, type FILTER 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
JNR_TRANS_temp = JNR_TRANS.toDF(*["JNR_TRANS___" + col for col in JNR_TRANS.columns])

FIL_TRANS = JNR_TRANS_temp.selectExpr(
	"JNR_TRANS___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
	"JNR_TRANS___ORG_LVL07_NAM as ORG_LVL07_NAM").filter("WFA_BUSN_AREA_DESC is null").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_WFA_BUSINESS_AREA, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
FIL_TRANS_temp = FIL_TRANS.toDF(*["FIL_TRANS___" + col for col in FIL_TRANS.columns])

EXP_WFA_BUSINESS_AREA = FIL_TRANS_temp\
	.withColumn("NEXTVAL", monotonically_increasing_id()) \
	.selectExpr(
	"FIL_TRANS___ORG_LVL07_NAM as WFA_BUSN_DESC",
	"FIL_TRANS___sys_row_id as sys_row_id",
	"NEXTVAL as WFA_BUSN_ID",
	"DATE_TRUNC ('day', CURRENT_TIMESTAMP ) as LOAD_DT"
)

# COMMAND ----------
# Processing node Shortcut_to_WFA_BUSINESS_AREA1, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_WFA_BUSINESS_AREA1 = EXP_WFA_BUSINESS_AREA.selectExpr(
	#"CAST(WFA_BUSN_ID AS SMALLINT) as WFA_BUSN_AREA_ID",
	"WFA_BUSN_DESC as WFA_BUSN_AREA_DESC",
	"CAST(LOAD_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
# overwriteDeltaPartition(Shortcut_to_WFA_BUSINESS_AREA1,'DC_NBR',dcnbr,f'{raw}.WFA_BUSINESS_AREA')
Shortcut_to_WFA_BUSINESS_AREA1.write.mode("append").saveAsTable(f'{legacy}.WFA_BUSINESS_AREA')