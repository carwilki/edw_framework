#Code converted on 2023-07-26 09:54:20
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

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

(username,password,connection_string) = SalonAcademy_prd_sqlServer(env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_GS_AssessmentActivityTracker, type SOURCE 
# COLUMN COUNT: 11

# SQ_Shortcut_to_GS_AssessmentActivityTracker = jdbcOracleConnection(f"""SELECT
query = f"""(SELECT
GS_AssessmentActivityTracker.ID,
GS_AssessmentActivityTracker.AssessmentId,
GS_AssessmentActivityTracker.AssessmentTrainerId,
GS_AssessmentActivityTracker.Activity,
GS_AssessmentActivityTracker.ActivityStatus,
GS_AssessmentActivityTracker.ActivityDate,
GS_AssessmentActivityTracker.ActivityNote,
GS_AssessmentActivityTracker.CreatedBy,
GS_AssessmentActivityTracker.CreatedDate,
GS_AssessmentActivityTracker.ModifiedBy,
GS_AssessmentActivityTracker.ModifiedDate
FROM SalonAcademy.dbo.GS_AssessmentActivityTracker) as src"""

SQ_Shortcut_to_GS_AssessmentActivityTracker = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_PRE, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_AssessmentActivityTracker_temp = SQ_Shortcut_to_GS_AssessmentActivityTracker.toDF(*["SQ_Shortcut_to_GS_AssessmentActivityTracker___" + col for col in SQ_Shortcut_to_GS_AssessmentActivityTracker.columns])

EXP_SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_PRE = SQ_Shortcut_to_GS_AssessmentActivityTracker_temp.selectExpr(
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___ID as ID",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___AssessmentId as AssessmentId",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___AssessmentTrainerId as AssessmentTrainerId",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___Activity as Activity",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___ActivityStatus as ActivityStatus",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___ActivityDate as ActivityDate",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___ActivityNote as ActivityNote",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___CreatedBy as CreatedBy",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___CreatedDate as CreatedDate",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___ModifiedBy as ModifiedBy",
	"SQ_Shortcut_to_GS_AssessmentActivityTracker___ModifiedDate as ModifiedDate",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node Shortcut_to_SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_PRE, type TARGET 
# COLUMN COUNT: 12


Shortcut_to_SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_PRE = EXP_SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_PRE.selectExpr(
  "CAST(ID AS BIGINT) as SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_ID", 
  "CAST(AssessmentId AS BIGINT) as SALON_ACADEMY_ASSESSMENT_ID", 
  "CAST(AssessmentTrainerId AS BIGINT) as SALON_ACADEMY_ASSESSMENT_TRAINER_ID", 
	"CAST(Activity AS STRING) as ACTIVITY",
	"CAST(ActivityStatus AS STRING) as ACTIVITY_STATUS",
	"CAST(ActivityDate AS TIMESTAMP) as ACTIVITY_TSTMP",
	"CAST(ActivityNote AS STRING) as ACTIVITY_NOTE",
	"CAST(CreatedDate AS TIMESTAMP) as CREATED_TSTMP",
	"CAST(CreatedBy AS STRING) as CREATED_BY",
	"CAST(ModifiedDate AS TIMESTAMP) as MODIFIED_TSTMP",
	"CAST(ModifiedBy AS STRING) as MODIFIED_BY",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SALON_ACADEMY_ASSESSMENT_ACTIVITY_TRACKER_PRE')
