#Code converted on 2023-08-03 13:28:38
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

# Set parameter lookup values
#parameter_filename = 'TBD'
#parameter_section = 'TBD'
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
# Processing node SQ_Shortcut_to_Trainer, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_Trainer = jdbcSqlServerConnection(f"""(SELECT
Trainer.TrainerId,
Trainer.FormattedName,
Trainer.IsDeleted
FROM Training.dbo.Trainer) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Trainer_temp = SQ_Shortcut_to_Trainer.toDF(*["SQ_Shortcut_to_Trainer___" + col for col in SQ_Shortcut_to_Trainer.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_Trainer_temp.selectExpr( \
	"SQ_Shortcut_to_Trainer___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_Trainer___TrainerId as TrainerId", \
	"SQ_Shortcut_to_Trainer___FormattedName as FormattedName", \
	"CASE WHEN  UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_Trainer___IsDeleted ) ) ) =  True THEN 1 WHEN  UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_Trainer___IsDeleted ) ) ) =  False THEN 0 ELSE Null END as o_IsDeleted", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_TRAINER_PRE, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_TRAINING_TRAINER_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(TrainerId AS INT) as TRAINER_ID", \
	"CAST(FormattedName AS STRING) as FORMATTED_NAME", \
	"CAST(o_IsDeleted AS TINYINT) as IS_DELETED", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_TRAINING_TRAINER_PRE.write.saveAsTable(f'{raw}.TRAINING_TRAINER_PRE', mode = 'overwrite')