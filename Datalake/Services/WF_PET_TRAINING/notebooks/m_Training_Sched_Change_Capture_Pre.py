#Code converted on 2023-08-03 13:28:28
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

# env = 'dev'

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
MAX_LOAD_DATE=genPrevRunDt('TRAINING_SCHED_CHANGE_CAPTURE',legacy,raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_ChangeCapture, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_ChangeCapture = jdbcSqlServerConnection(f"""(SELECT
ChangeCapture.ChangeId,
ChangeCapture.ChangeTypeId,
ChangeCapture.EntityId,
ChangeCapture.EntityTypeId,
ChangeCapture.StoreNumber,
ChangeCapture.ChangeUser,
ChangeCapture.ChangeDateTime,
ChangeCapture.ChangeStateId
FROM TrainingSched_PRD.dbo.ChangeCapture
WHERE ChangeDateTime > '{MAX_LOAD_DATE}') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ChangeCapture_temp = SQ_Shortcut_to_ChangeCapture.toDF(*["SQ_Shortcut_to_ChangeCapture___" + col for col in SQ_Shortcut_to_ChangeCapture.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_ChangeCapture_temp.selectExpr( \
	"SQ_Shortcut_to_ChangeCapture___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_ChangeCapture___ChangeId as ChangeId", \
	"SQ_Shortcut_to_ChangeCapture___ChangeTypeId as ChangeTypeId", \
	"SQ_Shortcut_to_ChangeCapture___EntityId as EntityId", \
	"SQ_Shortcut_to_ChangeCapture___EntityTypeId as EntityTypeId", \
	"SQ_Shortcut_to_ChangeCapture___StoreNumber as StoreNumber", \
	"SQ_Shortcut_to_ChangeCapture___ChangeUser as ChangeUser", \
	"SQ_Shortcut_to_ChangeCapture___ChangeDateTime as ChangeDateTime", \
	"SQ_Shortcut_to_ChangeCapture___ChangeStateId as ChangeStateId", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)
# .withColumn("v_MAX_LOAD_DATE", SET {MAX_LOAD_DATE} = GREATEST({MAX_LOAD_DATE}, ChangeDateTime))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_SCHED_CHANGE_CAPTURE_PRE, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_TRAINING_SCHED_CHANGE_CAPTURE_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(ChangeId AS INT) as CHANGE_ID", \
	"CAST(ChangeTypeId AS INT) as CHANGE_TYPE_ID", \
	"CAST(EntityId AS INT) as ENTITY_ID", \
	"CAST(EntityTypeId AS INT) as ENTITY_TYPE_ID", \
	"CAST(StoreNumber AS INT) as STORE_NUMBER", \
	"CAST(ChangeUser AS STRING) as CHANGE_USER", \
	"CAST(ChangeDateTime AS TIMESTAMP) as CHANGE_DATE_TIME", \
	"CAST(ChangeStateId AS INT) as CHANGE_STATE_ID", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_TRAINING_SCHED_CHANGE_CAPTURE_PRE.write.saveAsTable(f'{raw}.TRAINING_SCHED_CHANGE_CAPTURE_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_SCHED_CHANGE_CAPTURE_PRE", "TRAINING_SCHED_CHANGE_CAPTURE_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_SCHED_CHANGE_CAPTURE_PRE", "TRAINING_SCHED_CHANGE_CAPTURE_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e


