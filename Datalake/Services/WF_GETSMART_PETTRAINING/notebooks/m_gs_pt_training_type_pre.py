#Code converted on 2023-07-28 07:59:25
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
# Processing node SQ_Shortcut_to_GS_TrainingType, type SOURCE 
# COLUMN COUNT: 4

query =f"""(SELECT
GS_TrainingType.TypeID,
GS_TrainingType.TrainingName,
GS_TrainingType.CutOffDays,
GS_TrainingType.IsActive
FROM PetTraining.dbo.GS_TrainingType) as src"""

SQ_Shortcut_to_GS_TrainingType = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_TrainingType_temp = SQ_Shortcut_to_GS_TrainingType.toDF(*["SQ_Shortcut_to_GS_TrainingType___" + col for col in SQ_Shortcut_to_GS_TrainingType.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_GS_TrainingType_temp.selectExpr(
	"SQ_Shortcut_to_GS_TrainingType___sys_row_id as sys_row_id",
	"CURRENT_TIMESTAMP as LOAD_TSTMP",
	"SQ_Shortcut_to_GS_TrainingType___TypeID as TypeID",
	"SQ_Shortcut_to_GS_TrainingType___TrainingName as TrainingName",
	"SQ_Shortcut_to_GS_TrainingType___CutOffDays as CutOffDays",
	"SQ_Shortcut_to_GS_TrainingType___IsActive as IsActive"
)

# COMMAND ----------
# Processing node Shortcut_to_GS_PT_TRAINING_TYPE_PRE, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_GS_PT_TRAINING_TYPE_PRE = EXP_LOAD_TSTMP.selectExpr(
	"CAST(TypeID AS BIGINT) as GS_PT_TRAINING_TYPE_ID",
	"CAST(TrainingName AS STRING) as TRAINING_NAME",
	"CAST(CutOffDays AS BIGINT) as CUT_OFF_DAYS",
	"CAST(IsActive as INT) as IS_ACTIVE",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_GS_PT_TRAINING_TYPE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GS_PT_TRAINING_TYPE_PRE')
