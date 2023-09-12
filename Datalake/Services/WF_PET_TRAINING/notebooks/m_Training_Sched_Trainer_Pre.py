#Code converted on 2023-08-03 13:28:35
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

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = pettraining_prd_sqlServer_trainingSched(env)

# COMMAND ----------
# Variable_declaration_comment
MAX_LOAD_DATE=genPrevRunDt('TRAINING_SCHED_TRAINER',legacy,raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_Trainer, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_Trainer = jdbcSqlServerConnection(f"""(SELECT
Trainer.TrainerId,
Trainer.StoreNumber,
Trainer.FirstName,
Trainer.LastName,
Trainer.AssociateId,
Trainer.IsActive,
Trainer.LastModified
FROM TrainingSched_PRD.dbo.Trainer
) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Trainer_temp = SQ_Shortcut_to_Trainer.toDF(*["SQ_Shortcut_to_Trainer___" + col for col in SQ_Shortcut_to_Trainer.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_Trainer_temp.selectExpr( \
	"SQ_Shortcut_to_Trainer___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_Trainer___TrainerId as TrainerId", \
	"SQ_Shortcut_to_Trainer___StoreNumber as StoreNumber", \
	"SQ_Shortcut_to_Trainer___FirstName as FirstName", \
	"SQ_Shortcut_to_Trainer___LastName as LastName", \
	"SQ_Shortcut_to_Trainer___AssociateId as AssociateId", \
	"CASE WHEN UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_Trainer___IsActive ) ) ) = 'T' THEN 1  ELSE 0  end as o_IsActive", \
	"SQ_Shortcut_to_Trainer___LastModified as LastModified", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_SCHED_TRAINER_PRE, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_TRAINING_SCHED_TRAINER_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(TrainerId AS INT) as TRAINER_ID", \
	"CAST(StoreNumber AS INT) as STORE_NUMBER", \
	"CAST(FirstName AS STRING) as FIRST_NAME", \
	"CAST(LastName AS STRING) as LAST_NAME", \
	"CAST(AssociateId AS INT) as ASSOCIATE_ID", \
	"CAST(o_IsActive AS TINYINT) as IS_ACTIVE", \
	"CAST(LastModified AS TIMESTAMP) as LAST_MODIFIED", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_TRAINING_SCHED_TRAINER_PRE.write.saveAsTable(f'{raw}.TRAINING_SCHED_TRAINER_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_SCHED_TRAINER_PRE", "TRAINING_SCHED_TRAINER_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_SCHED_TRAINER_PRE", "TRAINING_SCHED_TRAINER_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e


