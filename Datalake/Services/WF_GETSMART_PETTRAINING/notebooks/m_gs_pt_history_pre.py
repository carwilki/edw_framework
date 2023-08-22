#Code converted on 2023-07-28 07:59:23
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

(username,password,connection_string) = PetTraining_prd_sqlServer(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_GS_History, type SOURCE 
# COLUMN COUNT: 8

query = f"""(SELECT
GS_History.HistoryID,
GS_History.TrainingID,
GS_History.FieldName,
GS_History.OldValue,
GS_History.NewValue,
GS_History.UpdateBy,
GS_History.UpdatedDate,
GS_History.Action
FROM PetTraining.dbo.GS_History) as src"""

SQ_Shortcut_to_GS_History = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_History_temp = SQ_Shortcut_to_GS_History.toDF(*["SQ_Shortcut_to_GS_History___" + col for col in SQ_Shortcut_to_GS_History.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_GS_History_temp.selectExpr(
	"SQ_Shortcut_to_GS_History___sys_row_id as sys_row_id",
	"CURRENT_TIMESTAMP as LOAD_TSTMP",
	"SQ_Shortcut_to_GS_History___HistoryID as HistoryID",
	"SQ_Shortcut_to_GS_History___TrainingID as TrainingID",
	"SQ_Shortcut_to_GS_History___FieldName as FieldName",
	"SQ_Shortcut_to_GS_History___OldValue as OldValue",
	"SQ_Shortcut_to_GS_History___NewValue as NewValue",
	"SQ_Shortcut_to_GS_History___UpdateBy as UpdateBy",
	"SQ_Shortcut_to_GS_History___UpdatedDate as UpdatedDate",
	"SQ_Shortcut_to_GS_History___Action as Action"
)

# COMMAND ----------
# Processing node Shortcut_to_GS_PT_HISTORY_PRE, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_GS_PT_HISTORY_PRE = EXP_LOAD_TSTMP.selectExpr(
	"CAST(HistoryID as INT) as GS_PT_HISTORY_ID",
	"CAST(TrainingID as INT) as GS_PT_TRAINING_ID",
	"CAST(FieldName AS STRING) as FIELD_NAME",
	"CAST(OldValue AS STRING) as OLD_VALUE",
	"CAST(NewValue AS STRING) as NEW_VALUE",
	"CAST(UpdateBy AS STRING) as UPDATE_BY",
	"CAST(UpdatedDate AS TIMESTAMP) as UPDATED_DT",
	"CAST(Action AS STRING) as ACTION",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_GS_PT_HISTORY_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GS_PT_HISTORY_PRE')
