#Code converted on 2023-08-03 13:28:29
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
(username, password, connection_string) = mtx_prd_sqlServer(env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_ChangeState, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_ChangeState = jdbcSqlServerConnection(f"""(SELECT
ChangeState.ChangeStateId,
ChangeState.ChangeStateName
FROM TrainingSched_PRD.dbo.ChangeState) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ChangeState_temp = SQ_Shortcut_to_ChangeState.toDF(*["SQ_Shortcut_to_ChangeState___" + col for col in SQ_Shortcut_to_ChangeState.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_ChangeState_temp.selectExpr( \
	"SQ_Shortcut_to_ChangeState___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_ChangeState___ChangeStateId as ChangeStateId", \
	"SQ_Shortcut_to_ChangeState___ChangeStateName as ChangeStateName", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(ChangeStateId AS INT) as CHANGE_STATE_ID", \
	"CAST(ChangeStateName AS STRING) as CHANGE_STATE_NAME", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE.write.saveAsTable(f'{raw}.TRAINING_SCHED_CHANGE_STATE_PRE', mode = 'overwrite')