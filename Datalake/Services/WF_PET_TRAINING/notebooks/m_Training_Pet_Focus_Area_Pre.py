#Code converted on 2023-08-03 13:28:25
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
# Variable_declaration_comment
MAX_LOAD_DATE=genPrevRunDt('TRAINING_PET_FOCUS_AREA',legacy,raw)

# COMMAND ----------
# Processing node SQ_PetFocusArea, type SOURCE 
# COLUMN COUNT: 4

SQ_PetFocusArea = jdbcSqlServerConnection(f"""(SELECT
PetFocusArea.PetFocusAreaId,
PetFocusArea.PetId,
PetFocusArea.FocusAreaId,
PetFocusArea.LastModified
FROM Training.dbo.PetFocusArea) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_PET_FOCUS_AREA, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_PetFocusArea_temp = SQ_PetFocusArea.toDF(*["SQ_PetFocusArea___" + col for col in SQ_PetFocusArea.columns])

EXP_PET_FOCUS_AREA = SQ_PetFocusArea_temp.selectExpr( \
	"SQ_PetFocusArea___sys_row_id as sys_row_id", \
	"SQ_PetFocusArea___PetFocusAreaId as PetFocusAreaId", \
	"SQ_PetFocusArea___PetId as PetId", \
	"SQ_PetFocusArea___FocusAreaId as FocusAreaId", \
	"SQ_PetFocusArea___LastModified as LastModified", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)
# .withColumn("{MAX_LOAD_DATE}", SET {MAX_LOAD_DATE} = GREATEST({MAX_LOAD_DATE}, CURRENT_TIMESTAMP))

# COMMAND ----------
# Processing node TRAINING_PET_FOCUS_AREA_PRE, type TARGET 
# COLUMN COUNT: 5


TRAINING_PET_FOCUS_AREA_PRE = EXP_PET_FOCUS_AREA.selectExpr( \
	"CAST(PetFocusAreaId AS INT) as PET_FOCUS_AREA_ID", \
	"CAST(PetId AS INT) as PET_ID", \
	"CAST(FocusAreaId AS INT) as FOCUS_AREA_ID", \
	"CAST(LastModified AS TIMESTAMP) as LAST_MODIFIED", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	TRAINING_PET_FOCUS_AREA_PRE.write.saveAsTable(f'{raw}.TRAINING_PET_FOCUS_AREA_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_PET_FOCUS_AREA_PRE", "TRAINING_PET_FOCUS_AREA_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_PET_FOCUS_AREA_PRE", "TRAINING_PET_FOCUS_AREA_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e

