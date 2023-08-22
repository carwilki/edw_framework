#Code converted on 2023-07-26 09:54:21
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

(username,password,connection_string) = mtx_prd_sqlServer(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_GS_Assessment, type SOURCE 
# COLUMN COUNT: 20

query = f"""(SELECT
GS_Assessment.ID,
GS_Assessment.AssessmentRequestDate,
GS_Assessment.ScheduledAssessmentDate,
GS_Assessment.ScheduledAssessmentTime,
GS_Assessment.PreAssessmentCompletionDate,
GS_Assessment.AssessmentPassDate,
GS_Assessment.AssessmentFailDate,
GS_Assessment.AcademyEnrollmentDate,
GS_Assessment.CreatedBy,
GS_Assessment.CreatedDate,
GS_Assessment.ModifiedBy,
GS_Assessment.ModifiedDate,
GS_Assessment.AssessmentStatus,
GS_Assessment.AssessmentCompletionDate,
GS_Assessment.ReasonComment,
GS_Assessment.AssessmentDenyId,
GS_Assessment.AssessmentFailId,
GS_Assessment.AcademyEnrollmentChangedDate,
GS_Assessment.AcademyCompletedDate,
GS_Assessment.ScheduleAssessmentChangedDate
FROM SalonAcademy.dbo.GS_Assessment) as src"""

SQ_Shortcut_to_GS_Assessment = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# COMMAND ----------
# Processing node EXP_SALON_ACADEMY_ASSESSMENT_PRE, type EXPRESSION 
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_Assessment_temp = SQ_Shortcut_to_GS_Assessment.toDF(*["SQ_Shortcut_to_GS_Assessment___" + col for col in SQ_Shortcut_to_GS_Assessment.columns])

EXP_SALON_ACADEMY_ASSESSMENT_PRE = SQ_Shortcut_to_GS_Assessment_temp.selectExpr(
	"SQ_Shortcut_to_GS_Assessment___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_GS_Assessment___ID as ID",
	"SQ_Shortcut_to_GS_Assessment___AssessmentRequestDate as AssessmentRequestDate",
	"SQ_Shortcut_to_GS_Assessment___ScheduledAssessmentDate as ScheduledAssessmentDate",
	"SQ_Shortcut_to_GS_Assessment___ScheduledAssessmentTime as ScheduledAssessmentTime",
	"SQ_Shortcut_to_GS_Assessment___PreAssessmentCompletionDate as PreAssessmentCompletionDate",
	"SQ_Shortcut_to_GS_Assessment___AssessmentPassDate as AssessmentPassDate",
	"SQ_Shortcut_to_GS_Assessment___AssessmentFailDate as AssessmentFailDate",
	"SQ_Shortcut_to_GS_Assessment___AcademyEnrollmentDate as AcademyEnrollmentDate",
	"SQ_Shortcut_to_GS_Assessment___CreatedBy as CreatedBy",
	"SQ_Shortcut_to_GS_Assessment___CreatedDate as CreatedDate",
	"SQ_Shortcut_to_GS_Assessment___ModifiedBy as ModifiedBy",
	"SQ_Shortcut_to_GS_Assessment___ModifiedDate as ModifiedDate",
	"SQ_Shortcut_to_GS_Assessment___AssessmentStatus as AssessmentStatus",
	"SQ_Shortcut_to_GS_Assessment___AssessmentCompletionDate as AssessmentCompletionDate",
	"SQ_Shortcut_to_GS_Assessment___ReasonComment as ReasonComment",
	"SQ_Shortcut_to_GS_Assessment___AssessmentDenyId as AssessmentDenyId",
	"SQ_Shortcut_to_GS_Assessment___AssessmentFailId as AssessmentFailId",
	"SQ_Shortcut_to_GS_Assessment___AcademyEnrollmentChangedDate as AcademyEnrollmentChangedDate",
	"SQ_Shortcut_to_GS_Assessment___AcademyCompletedDate as AcademyCompletedDate",
	"SQ_Shortcut_to_GS_Assessment___ScheduleAssessmentChangedDate as ScheduleAssessmentChangedDate",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node Shortcut_to_SALON_ACADEMY_ASSESSMENT_PRE, type TARGET 
# COLUMN COUNT: 21


Shortcut_to_SALON_ACADEMY_ASSESSMENT_PRE = EXP_SALON_ACADEMY_ASSESSMENT_PRE.selectExpr(
	"CAST(ID as BIGINT) as SALON_ACADEMY_ASSESSMENT_ID",
	"CAST(AssessmentRequestDate AS TIMESTAMP) as ASSESSMENT_REQUEST_TSTMP",
	"CAST(ScheduledAssessmentDate AS DATE) as SCHEDULED_ASSESSMENT_DT",
	"CAST(ScheduledAssessmentTime AS TIMESTAMP) as SCHEDULED_ASSESSMENT_TIME",
	"CAST(PreAssessmentCompletionDate AS TIMESTAMP) as PRE_ASSESSMENT_COMPLETION_TSTMP",
	"CAST(AssessmentPassDate AS TIMESTAMP) as ASSESSMENT_PASS_TSTMP",
	"CAST(AssessmentFailDate AS TIMESTAMP) as ASSESSMENT_FAIL_TSTMP",
	"CAST(AcademyEnrollmentDate AS TIMESTAMP) as ACADEMY_ENROLLMENT_TSTMP",
	"CAST(AssessmentStatus AS STRING) as ASSESSMENT_STATUS",
	"CAST(AssessmentCompletionDate AS TIMESTAMP) as ASSESSMENT_COMPLETION_TSTMP",
	"CAST(ReasonComment AS STRING) as REASON_COMMENT",
	"CAST(AssessmentDenyId AS BIGINT) as SALON_ACADEMY_ASSESSMENT_DENY_REASON_ID",
	"CAST(AssessmentFailId AS BIGINT) as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID",
	"CAST(AcademyEnrollmentChangedDate AS TIMESTAMP) as ACADEMY_ENROLLMENT_CHANGED_TSTMP",
	"CAST(AcademyCompletedDate AS TIMESTAMP) as ACADEMY_COMPLETED_TSTMP",
	"CAST(ScheduleAssessmentChangedDate AS TIMESTAMP) as SCHEDULE_ASSESSMENT_CHANGED_TSTMP",
	"CAST(CreatedDate AS TIMESTAMP) as CREATED_TSTMP",
	"CAST(CreatedBy AS STRING) as CREATED_BY",
	"CAST(ModifiedDate AS TIMESTAMP) as MODIFIED_TSTMP",
	"CAST(ModifiedBy AS STRING) as MODIFIED_BY",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SALON_ACADEMY_ASSESSMENT_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SALON_ACADEMY_ASSESSMENT_PRE')
