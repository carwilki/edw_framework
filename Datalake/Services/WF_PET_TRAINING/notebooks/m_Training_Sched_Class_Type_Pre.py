#Code converted on 2023-08-03 13:28:31
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
(username, password, connection_string) = pettraining_prd_sqlServer_trainingSched(env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_ClassType, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_ClassType = jdbcSqlServerConnection(f"""(SELECT
ClassType.ClassTypeId,
ClassType.Name,
ClassType.ShortDescription,
ClassType.Duration,
ClassType.Price,
ClassType.UPC,
ClassType.InfoPage,
ClassType.IsActive,
ClassType.VisibilityLevel
FROM TrainingSched_PRD.dbo.ClassType) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 10

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
	"SQ_Shortcut_to_ClassType___InfoPage as InfoPage", \
	"CASE WHEN UPPER ( ltrim ( rtrim ( SQ_Shortcut_to_ClassType___IsActive ) ) ) = 'T' or upper(trim(SQ_Shortcut_to_ClassType___IsActive))= 'TRUE' THEN 1 ELSE 0 END  as o_IsActive", \
	"SQ_Shortcut_to_ClassType___VisibilityLevel as VisibilityLevel", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_SCHED_CLASS_TYPE_PRE, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_TRAINING_SCHED_CLASS_TYPE_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(ClassTypeId AS INT) as CLASS_TYPE_ID", \
	"CAST(Name AS STRING) as NAME", \
	"CAST(ShortDescription AS STRING) as SHORT_DESCRIPTION", \
	"CAST(Duration AS INT) as DURATION", \
	"CAST(Price AS DECIMAL(8,2)) as PRICE", \
	"CAST(UPC AS STRING) as UPC", \
	"CAST(InfoPage AS STRING) as INFO_PAGE", \
	"CAST(o_IsActive AS TINYINT) as IS_ACTIVE", \
	"CAST(VisibilityLevel AS INT) as VISIBILITY_LEVEL", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_TRAINING_SCHED_CLASS_TYPE_PRE.write.saveAsTable(f'{raw}.TRAINING_SCHED_CLASS_TYPE_PRE', mode = 'overwrite')