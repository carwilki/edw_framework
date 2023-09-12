#Code converted on 2023-08-03 13:28:26
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
MAX_LOAD_DATE=genPrevRunDt('TRAINING_RESERVATION_HISTORY',legacy,raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_ReservationHistory, type SOURCE 
# COLUMN COUNT: 12

SQ_Shortcut_to_ReservationHistory = jdbcSqlServerConnection(f"""(SELECT
ReservationHistory.ReservationHistoryId,
ReservationHistory.ReservationId,
ReservationHistory.StoreClassId,
ReservationHistory.ClassStartDateTime,
ReservationHistory.ClassTypeName,
ReservationHistory.ClassDurationWeeks,
ReservationHistory.ClassPrice,
ReservationHistory.ClassUPC,
ReservationHistory.TrainerName,
ReservationHistory.CreateDateTime,
ReservationHistory.TrainingPackageName,
ReservationHistory.TrainingPackageOptionName
FROM Training.dbo.ReservationHistory
WHERE CreateDateTime > '{MAX_LOAD_DATE}') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ReservationHistory_temp = SQ_Shortcut_to_ReservationHistory.toDF(*["SQ_Shortcut_to_ReservationHistory___" + col for col in SQ_Shortcut_to_ReservationHistory.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_ReservationHistory_temp.selectExpr( \
	"SQ_Shortcut_to_ReservationHistory___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_ReservationHistory___ReservationHistoryId as ReservationHistoryId", \
	"SQ_Shortcut_to_ReservationHistory___ReservationId as ReservationId", \
	"SQ_Shortcut_to_ReservationHistory___StoreClassId as StoreClassId", \
	"SQ_Shortcut_to_ReservationHistory___ClassStartDateTime as ClassStartDateTime", \
	"SQ_Shortcut_to_ReservationHistory___ClassTypeName as ClassTypeName", \
	"SQ_Shortcut_to_ReservationHistory___ClassDurationWeeks as ClassDurationWeeks", \
	"SQ_Shortcut_to_ReservationHistory___ClassPrice as ClassPrice", \
	"SQ_Shortcut_to_ReservationHistory___ClassUPC as ClassUPC", \
	"SQ_Shortcut_to_ReservationHistory___TrainerName as TrainerName", \
	"SQ_Shortcut_to_ReservationHistory___CreateDateTime as CreateDateTime", \
	"SQ_Shortcut_to_ReservationHistory___TrainingPackageName as TrainingPackageName", \
	"SQ_Shortcut_to_ReservationHistory___TrainingPackageOptionName as TrainingPackageOptionName", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)
# .withColumn("v_MAX_LOAD_DATE", SET {MAX_LOAD_DATE} = GREATEST({MAX_LOAD_DATE}, CreateDateTime))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_RESERVATION_HISTORY_PRE, type TARGET 
# COLUMN COUNT: 13


Shortcut_to_TRAINING_RESERVATION_HISTORY_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(ReservationHistoryId AS INT) as RESERVATION_HISTORY_ID", \
	"CAST(ReservationId AS INT) as RESERVATION_ID", \
	"CAST(StoreClassId AS INT) as STORE_CLASS_ID", \
	"CAST(ClassStartDateTime AS TIMESTAMP) as CLASS_START_DATE_TIME", \
	"CAST(ClassTypeName AS STRING) as CLASS_TYPE_NAME", \
	"CAST(ClassDurationWeeks AS INT) as CLASS_DURATION_WEEKS", \
	"CAST(ClassPrice AS DECIMAL(19,4)) as CLASS_PRICE", \
	"CAST(ClassUPC AS STRING) as CLASS_UPC", \
	"CAST(TrainerName AS STRING) as TRAINER_NAME", \
	"CAST(CreateDateTime AS TIMESTAMP) as CREATE_DATE_TIME", \
	"CAST(TrainingPackageName AS STRING) as TRAINING_PACKAGE_NAME", \
	"CAST(TrainingPackageOptionName AS STRING) as TRAINING_PACKAGE_OPTION_NAME", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)


try:
	Shortcut_to_TRAINING_RESERVATION_HISTORY_PRE.write.saveAsTable(f'{raw}.TRAINING_RESERVATION_HISTORY_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_RESERVATION_HISTORY_PRE", "TRAINING_RESERVATION_HISTORY_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_RESERVATION_HISTORY_PRE", "TRAINING_RESERVATION_HISTORY_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e

