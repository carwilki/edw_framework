#Code converted on 2023-08-03 13:28:39
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
(username, password, connection_string) = pettraining_prd_sqlServer_training(env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_UnitOfMeasure, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_UnitOfMeasure = jdbcSqlServerConnection(f"""(SELECT
UnitOfMeasure.UnitOfMeasureId,
UnitOfMeasure.UnitOfMeasure,
UnitOfMeasure.UnitOfMeasurePlural
FROM Training.dbo.UnitOfMeasure) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_UnitOfMeasure_temp = SQ_Shortcut_to_UnitOfMeasure.toDF(*["SQ_Shortcut_to_UnitOfMeasure___" + col for col in SQ_Shortcut_to_UnitOfMeasure.columns])

EXPTRANS = SQ_Shortcut_to_UnitOfMeasure_temp.selectExpr( \
	"SQ_Shortcut_to_UnitOfMeasure___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_UnitOfMeasure___UnitOfMeasureId as UnitOfMeasureId", \
	"SQ_Shortcut_to_UnitOfMeasure___UnitOfMeasure as UnitOfMeasure", \
	"SQ_Shortcut_to_UnitOfMeasure___UnitOfMeasurePlural as UnitOfMeasurePlural" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE = EXPTRANS.selectExpr( \
	"CAST(UnitOfMeasureId AS INT) as UNIT_OF_MEASURE_ID", \
	"CAST(UnitOfMeasure AS STRING) as UNIT_OF_MEASURE", \
	"CAST(UnitOfMeasurePlural AS STRING) as UNIT_OF_MEASURE_PLURAL" \
)
Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE.write.saveAsTable(f'{raw}.TRAINING_UNIT_OF_MEASURE_PRE', mode = 'overwrite')