#Code converted on 2023-08-03 13:28:34
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
MAX_LOAD_DATE=genPrevRunDt('TRAINING_SCHED_STORE_CLASS',legacy,raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_StoreClass, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_StoreClass = jdbcSqlServerConnection(f"""(SELECT
StoreClass.StoreClassId,
StoreClass.ClassTypeId,
StoreClass.StoreNumber,
StoreClass.StartDateTime,
StoreClass.LastModified,
StoreClass.IsActive,
StoreClass.IsFull
FROM TrainingSched_PRD.dbo.StoreClass
) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_StoreClass_temp = SQ_Shortcut_to_StoreClass.toDF(*["SQ_Shortcut_to_StoreClass___" + col for col in SQ_Shortcut_to_StoreClass.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_StoreClass_temp.selectExpr( \
	"SQ_Shortcut_to_StoreClass___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_StoreClass___StoreClassId as StoreClassId", \
	"SQ_Shortcut_to_StoreClass___ClassTypeId as ClassTypeId", \
	"SQ_Shortcut_to_StoreClass___StoreNumber as StoreNumber", \
	"SQ_Shortcut_to_StoreClass___StartDateTime as StartDateTime", \
	"SQ_Shortcut_to_StoreClass___LastModified as LastModified", \
	"Case when UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_StoreClass___IsActive ) ) ) = True then 1 WHEN UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_StoreClass___IsActive ) ) ) = False then  0 ELSE Null end as o_IsActive", \
	"Case when UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_StoreClass___IsFull ) ) ) = True then 1 when UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_StoreClass___IsFull ) ) ) = False then 0 else  Null end as o_IsFull", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_SCHED_STORE_CLASS_PRE, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_TRAINING_SCHED_STORE_CLASS_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(StoreClassId AS INT) as STORE_CLASS_ID", \
	"CAST(ClassTypeId AS INT) as CLASS_TYPE_ID", \
	"CAST(StoreNumber AS INT) as STORE_NUMBER", \
	"CAST(StartDateTime AS TIMESTAMP) as START_DATE_TIME", \
	"CAST(LastModified AS TIMESTAMP) as LAST_MODIFIED", \
	"CAST(o_IsActive AS TINYINT) as IS_ACTIVE", \
	"CAST(o_IsFull AS TINYINT) as IS_FULL", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_TRAINING_SCHED_STORE_CLASS_PRE.write.saveAsTable(f'{raw}.TRAINING_SCHED_STORE_CLASS_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_SCHED_STORE_CLASS_PRE", "TRAINING_SCHED_STORE_CLASS_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_SCHED_STORE_CLASS_PRE", "TRAINING_SCHED_STORE_CLASS_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e


