
#Code converted on 2023-08-03 13:28:16
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
# Variable_declaration_comment
#  is this used  ?
# MAX_LOAD_DATE=genPrevRunDt('TRAINING_CLASS_TYPE',legacy,raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_ClassType, type SOURCE 
# COLUMN COUNT: 14

SQ_Shortcut_to_ClassType = jdbcSqlServerConnection(f"""(SELECT
ClassType.ClassTypeId,
ClassType.Name,
ClassType.ShortDescription,
ClassType.Duration,
ClassType.Price,
ClassType.UPC,
ClassType.InfoUrl,
ClassType.LastModified,
ClassType.SortOrderId,
ClassType.IsActive,
ClassType.DurationUnitOfMeasureId,
ClassType.SessionUnitOfMeasureId,
ClassType.CategoryID,
ClassType.SessionLength
FROM Training.dbo.ClassType) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ClassType_temp = SQ_Shortcut_to_ClassType.toDF(*["SQ_Shortcut_to_ClassType___" + col for col in SQ_Shortcut_to_ClassType.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_ClassType_temp.selectExpr( \
	"SQ_Shortcut_to_ClassType___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_ClassType___ClassTypeId as ClassTypeId", \
	"SQ_Shortcut_to_ClassType___Name as Name", \
	"SQ_Shortcut_to_ClassType___ShortDescription as ShortDescription", \
	"SQ_Shortcut_to_ClassType___Duration as Duration", \
	"SQ_Shortcut_to_ClassType___Price as Price", \
	"SQ_Shortcut_to_ClassType___UPC as UPC", \
	"SQ_Shortcut_to_ClassType___InfoUrl as InfoUrl", \
	"SQ_Shortcut_to_ClassType___LastModified as LastModified", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP", \
	"SQ_Shortcut_to_ClassType___SortOrderId as SortOrderId", \
	"IF (SQ_Shortcut_to_ClassType___IsActive = 'F', '0', '1') as o_IsActive", \
	"SQ_Shortcut_to_ClassType___CategoryID as CategoryID", \
	"SQ_Shortcut_to_ClassType___DurationUnitOfMeasureId as DurationUnitOfMeasureId", \
	"SQ_Shortcut_to_ClassType___sessionLength as sessionLength", \
	"SQ_Shortcut_to_ClassType___SessionUnitOfMeasureId as SessionUnitOfMeasureId" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_CLASS_TYPE_PRE, type TARGET 
# COLUMN COUNT: 15


Shortcut_to_TRAINING_CLASS_TYPE_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(ClassTypeId AS INT) as CLASS_TYPE_ID", \
	"CAST(Name AS STRING) as NAME", \
	"CAST(ShortDescription AS STRING) as SHORT_DESCRIPTION", \
	"CAST(Duration AS INT) as DURATION", \
	"CAST(Price AS DECIMAL(19,4)) as PRICE", \
	"CAST(UPC AS STRING) as UPC", \
	"CAST(InfoUrl AS STRING) as INFO_URL", \
	"CAST(LastModified AS TIMESTAMP) as LAST_MODIFIED", \
	"CAST(SortOrderId AS INT) as SORT_ORDER_ID", \
	"CAST(o_IsActive AS INT) as IS_ACTIVE", \
	"CAST(CategoryID AS INT) as CATEGORY_ID", \
	"CAST(DurationUnitOfMeasureId AS INT) as DURATION_UNIT_OF_MEASURE_ID", \
	"CAST(sessionLength AS INT) as SESSION_LENGTH", \
	"CAST(SessionUnitOfMeasureId AS INT) as SESSION_UNIT_OF_MEASURE_ID", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)
try:
	Shortcut_to_TRAINING_CLASS_TYPE_PRE.write.saveAsTable(f'{raw}.TRAINING_CLASS_TYPE_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_CLASS_TYPE_PRE", "TRAINING_CLASS_TYPE_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_CLASS_TYPE_PRE", "TRAINING_CLASS_TYPE_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e
