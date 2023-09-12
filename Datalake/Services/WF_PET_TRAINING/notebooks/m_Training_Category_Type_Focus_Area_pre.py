#Code converted on 2023-08-03 13:28:13
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
MAX_LOAD_DATE=genPrevRunDt('TRAINING_CATEGORY_TYPE_FOCUS_AREA',legacy,raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_CategoryTypeFocusArea, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_CategoryTypeFocusArea = jdbcSqlServerConnection(f"""(SELECT
CategoryTypeFocusArea.CategoryTypeFocusAreaId,
CategoryTypeFocusArea.CategoryId,
CategoryTypeFocusArea.FocusAreaId,
CategoryTypeFocusArea.LastModified
FROM Training.dbo.CategoryTypeFocusArea
WHERE LastModified > '{MAX_LOAD_DATE}') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_CategoryTypeFocusArea_temp = SQ_Shortcut_to_CategoryTypeFocusArea.toDF(*["SQ_Shortcut_to_CategoryTypeFocusArea___" + col for col in SQ_Shortcut_to_CategoryTypeFocusArea.columns])

EXPTRANS = SQ_Shortcut_to_CategoryTypeFocusArea_temp.selectExpr( \
	"SQ_Shortcut_to_CategoryTypeFocusArea___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_CategoryTypeFocusArea___CategoryTypeFocusAreaId as CategoryTypeFocusAreaId", \
	"SQ_Shortcut_to_CategoryTypeFocusArea___CategoryId as CategoryId", \
	"SQ_Shortcut_to_CategoryTypeFocusArea___FocusAreaId as FocusAreaId", \
	"SQ_Shortcut_to_CategoryTypeFocusArea___LastModified as LastModified", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE = EXPTRANS.selectExpr( \
	"CAST(CategoryTypeFocusAreaId AS INT) as CATEGORY_TYPE_FOCUS_AREA_ID", \
	"CAST(CategoryId AS INT) as CATEGORY_ID", \
	"CAST(FocusAreaId AS INT) as FOCUS_AREA_ID", \
	"CAST(LastModified AS TIMESTAMP) as LAST_MODIFIED", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE.write.saveAsTable(f'{raw}.TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE',mode='overwrite')
	logPrevRunDt("TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE", "TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE", "TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e
