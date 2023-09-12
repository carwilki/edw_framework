#Code converted on 2023-08-03 13:28:20
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
# Variable_declaration_comment
MAX_LOAD_DATE=genPrevRunDt('TRAINING_PACKAGE_OPTION_CLASS_TYPE',legacy,raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_PackageOptionClassType, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_PackageOptionClassType = jdbcSqlServerConnection(f"""(SELECT
PackageOptionClassType.PackageOptionClassTypeId,
PackageOptionClassType.PackageOptionId,
PackageOptionClassType.ClassTypeId,
PackageOptionClassType.LastModified
FROM Training.dbo.PackageOptionClassType
WHERE LastModified > '{MAX_LOAD_DATE}') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_PackageOptionClassType_temp = SQ_Shortcut_to_PackageOptionClassType.toDF(*["SQ_Shortcut_to_PackageOptionClassType___" + col for col in SQ_Shortcut_to_PackageOptionClassType.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_PackageOptionClassType_temp.selectExpr( \
	"SQ_Shortcut_to_PackageOptionClassType___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_PackageOptionClassType___PackageOptionClassTypeId as PackageOptionClassTypeId", \
	"SQ_Shortcut_to_PackageOptionClassType___PackageOptionId as PackageOptionId", \
	"SQ_Shortcut_to_PackageOptionClassType___ClassTypeId as ClassTypeId", \
	"SQ_Shortcut_to_PackageOptionClassType___LastModified as LastModified", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(PackageOptionClassTypeId AS INT) as PACKAGE_OPTION_CLASS_TYPE_ID", \
	"CAST(PackageOptionId AS INT) as PACKAGE_OPTION_ID", \
	"CAST(ClassTypeId AS INT) as CLASS_TYPE_ID", \
	"CAST(LastModified AS TIMESTAMP) as LAST_MODIFIED", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE.write.saveAsTable(f'{raw}.TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE", "TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE", "TRAINING_PACKAGE_OPTION_CLASS_TYPE_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e


