#Code converted on 2023-07-24 08:28:15
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
# Processing node SQ_Shortcut_to_SDS_ORDER_STATUS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SDS_ORDER_STATUS = spark.sql(f"""SELECT
SDS_ORDER_STATUS_DESC,
LOAD_TSTMP
FROM {legacy}.SDS_ORDER_STATUS""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_TRIM2, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_ORDER_STATUS_temp = SQ_Shortcut_to_SDS_ORDER_STATUS.toDF(*["SQ_Shortcut_to_SDS_ORDER_STATUS___" + col for col in SQ_Shortcut_to_SDS_ORDER_STATUS.columns])

EXP_TRIM2 = SQ_Shortcut_to_SDS_ORDER_STATUS_temp.selectExpr(
	"SQ_Shortcut_to_SDS_ORDER_STATUS___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_SDS_ORDER_STATUS___SDS_ORDER_STATUS_DESC as SDS_ORDER_STATUS_DESC",
	"UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_SDS_ORDER_STATUS___SDS_ORDER_STATUS_DESC ) ) ) as o_SDS_ORDER_STATUS_DESC",
	"SQ_Shortcut_to_SDS_ORDER_STATUS___LOAD_TSTMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_PICKLIST_VALUE_INFO_PRE, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_SDS_PICKLIST_VALUE_INFO_PRE = spark.sql(f"""SELECT
VALUE
FROM {raw}.SDS_PICKLIST_VALUE_INFO_PRE
WHERE UPPER(ENTITY_PARTICLE_ID)='ORDER.STATUS'""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_TRIM1, type EXPRESSION 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_PICKLIST_VALUE_INFO_PRE_temp = SQ_Shortcut_to_SDS_PICKLIST_VALUE_INFO_PRE.toDF(*["SQ_Shortcut_to_SDS_PICKLIST_VALUE_INFO_PRE___" + col for col in SQ_Shortcut_to_SDS_PICKLIST_VALUE_INFO_PRE.columns])

EXP_TRIM1 = SQ_Shortcut_to_SDS_PICKLIST_VALUE_INFO_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SDS_PICKLIST_VALUE_INFO_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_SDS_PICKLIST_VALUE_INFO_PRE___VALUE as VALUE",
	"UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_SDS_PICKLIST_VALUE_INFO_PRE___VALUE ) ) ) as o_VALUE"
)

# COMMAND ----------
# Processing node JNR_SDS_ORDER_STATUS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
EXP_TRIM1_temp = EXP_TRIM1.toDF(*["EXP_TRIM1___" + col for col in EXP_TRIM1.columns])
EXP_TRIM2_temp = EXP_TRIM2.toDF(*["EXP_TRIM2___" + col for col in EXP_TRIM2.columns])

JNR_SDS_ORDER_STATUS = EXP_TRIM2_temp.join(EXP_TRIM1_temp,[EXP_TRIM2_temp.EXP_TRIM2___o_SDS_ORDER_STATUS_DESC == EXP_TRIM1_temp.EXP_TRIM1___o_VALUE],'left_outer').selectExpr(
	"EXP_TRIM1___VALUE as VALUE",
	"EXP_TRIM1___o_VALUE as o_VALUE",
	"EXP_TRIM2___o_SDS_ORDER_STATUS_DESC as i_SDS_ORDER_STATUS_DESC",
	"EXP_TRIM2___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node EXP_SDS_ORDER_STATUS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
JNR_SDS_ORDER_STATUS_temp = JNR_SDS_ORDER_STATUS.toDF(*["JNR_SDS_ORDER_STATUS___" + col for col in JNR_SDS_ORDER_STATUS.columns])

EXP_SDS_ORDER_STATUS = JNR_SDS_ORDER_STATUS_temp.selectExpr(
	"JNR_SDS_ORDER_STATUS___i_SDS_ORDER_STATUS_DESC as i_SDS_ORDER_STATUS_DESC",
	"JNR_SDS_ORDER_STATUS___i_LOAD_TSTMP as i_LOAD_TSTMP",
	"JNR_SDS_ORDER_STATUS___VALUE as SDS_ORDER_STATUS").selectExpr(
	"JNR_SDS_ORDER_STATUS___sys_row_id as sys_row_id",
	"LTRIM ( RTRIM ( JNR_SDS_ORDER_STATUS___SDS_ORDER_STATUS ) ) as o_SDS_ORDER_STATUS",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (JNR_SDS_ORDER_STATUS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNR_SDS_ORDER_STATUS___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF (JNR_SDS_ORDER_STATUS___i_SDS_ORDER_STATUS_DESC IS NULL, 1, 2) as o_UPDATE_VALIDATION"
)

# COMMAND ----------
# Processing node FLT_INSERTS_ONLY, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
EXP_SDS_ORDER_STATUS_temp = EXP_SDS_ORDER_STATUS.toDF(*["EXP_SDS_ORDER_STATUS___" + col for col in EXP_SDS_ORDER_STATUS.columns])

FLT_INSERTS_ONLY = EXP_SDS_ORDER_STATUS_temp.selectExpr(
	"EXP_SDS_ORDER_STATUS___o_SDS_ORDER_STATUS as SDS_ORDER_STATUS",
	"EXP_SDS_ORDER_STATUS___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_SDS_ORDER_STATUS___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_SDS_ORDER_STATUS___o_UPDATE_VALIDATION as o_UPDATE_VALIDATION").filter("o_UPDATE_VALIDATION = 1").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_SDS_ORDER_STATUS_4, type TARGET 
# COLUMN COUNT: 4

#TODO check if sequence
#  	"col("SDS_ORDER_STATUS_GID") as SDS_ORDER_STATUS_GID",

Shortcut_to_SDS_ORDER_STATUS_4 = FLT_INSERTS_ONLY.selectExpr(
	"CAST(SDS_ORDER_STATUS AS STRING) as SDS_ORDER_STATUS_DESC",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SDS_ORDER_STATUS_4.write.saveAsTable(f'{legacy}.SDS_ORDER_STATUS')