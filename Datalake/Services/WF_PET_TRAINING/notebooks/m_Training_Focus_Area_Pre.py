#Code converted on 2023-08-03 13:28:18
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
(username, password, connection_string) = pettraining_prd_sqlServer_training(env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_FocusArea, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_FocusArea = jdbcSqlServerConnection(f"""(SELECT
FocusArea.FocusAreaId,
FocusArea.FocusAreaName
FROM Training.dbo.FocusArea) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_FocusArea_temp = SQ_Shortcut_to_FocusArea.toDF(*["SQ_Shortcut_to_FocusArea___" + col for col in SQ_Shortcut_to_FocusArea.columns])

EXPTRANS = SQ_Shortcut_to_FocusArea_temp.selectExpr( \
	"SQ_Shortcut_to_FocusArea___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_FocusArea___FocusAreaId as FocusAreaId", \
	"SQ_Shortcut_to_FocusArea___FocusAreaName as FocusAreaName", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_FOCUS_AREA_PRE, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_TRAINING_FOCUS_AREA_PRE = EXPTRANS.selectExpr( \
	"CAST(FocusAreaId AS INT) as FOCUS_AREA_ID", \
	"FocusAreaName as FOCUS_AREA_NAME", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_TRAINING_FOCUS_AREA_PRE.write.saveAsTable(f'{raw}.TRAINING_FOCUS_AREA_PRE',mode='overwrite')