#Code converted on 2023-07-26 09:54:24
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
# Processing node SQ_Shortcut_To_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_To_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR,
SITE_PROFILE.STORE_NAME,
SITE_PROFILE.STATE_CD
FROM {legacy}.SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_GS_Training, type SOURCE 
# COLUMN COUNT: 27

query = f"""(SELECT
GS_Training.ID,
GS_Training.TravelID,
GS_Training.HomeStoreID,
GS_Training.AssociateID,
GS_Training.AssociateFirstName,
GS_Training.AssociateLastName,
GS_Training.SalonAcademyID,
GS_Training.DM,
GS_Training.SalonSafetyCertiDate,
GS_Training.SplashDate,
GS_Training.GroomingToolKit,
GS_Training.TrainingStatus,
GS_Training.TrainingType,
GS_Training.DateCreated,
GS_Training.CreatedBy,
GS_Training.ModifiedBy,
GS_Training.DateModified,
GS_Training.DogAcademyDateCompleted,
GS_Training.TrainingStoreRegionID,
GS_Training.DistrictID,
GS_Training.AssessmentID,
GS_Training.SplashExamPTSDate,
GS_Training.SplashObservPTSDate,
GS_Training.SafetyAllELearningDate,
GS_Training.SafetySalonELearningDate,
GS_Training.SafetyAllPTSDate,
GS_Training.SafetySalonPTSDate
FROM SalonAcademy.dbo.GS_Training) as src"""

SQ_Shortcut_to_GS_Training = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# COMMAND ----------
# Processing node SQ_Shortcut_to_SALON_ACADEMY, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_SALON_ACADEMY = spark.sql(f"""SELECT
SALON_ACADEMY.SALON_ACADEMY_ID,
SALON_ACADEMY.LOCATION_ID,
SALON_ACADEMY.START_DT,
SALON_ACADEMY.END_DT,
SALON_ACADEMY.SALON_ACADEMY_TYPE_ID,
SALON_ACADEMY.SALON_ACADEMY_TYPE_DESC
FROM {legacy}.SALON_ACADEMY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_FIELDS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SALON_ACADEMY_temp = SQ_Shortcut_to_SALON_ACADEMY.toDF(*["SQ_Shortcut_to_SALON_ACADEMY___" + col for col in SQ_Shortcut_to_SALON_ACADEMY.columns])
SQ_Shortcut_To_SITE_PROFILE_temp = SQ_Shortcut_To_SITE_PROFILE.toDF(*["SQ_Shortcut_To_SITE_PROFILE___" + col for col in SQ_Shortcut_To_SITE_PROFILE.columns])

JNR_SITE_FIELDS = SQ_Shortcut_To_SITE_PROFILE_temp.join(SQ_Shortcut_to_SALON_ACADEMY_temp,[SQ_Shortcut_To_SITE_PROFILE_temp.SQ_Shortcut_To_SITE_PROFILE___LOCATION_ID == SQ_Shortcut_to_SALON_ACADEMY_temp.SQ_Shortcut_to_SALON_ACADEMY___LOCATION_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_SALON_ACADEMY___SALON_ACADEMY_ID as SALON_ACADEMY_ID",
	"SQ_Shortcut_to_SALON_ACADEMY___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SALON_ACADEMY___START_DT as START_DT",
	"SQ_Shortcut_to_SALON_ACADEMY___END_DT as END_DT",
	"SQ_Shortcut_to_SALON_ACADEMY___SALON_ACADEMY_TYPE_ID as SALON_ACADEMY_TYPE_ID",
	"SQ_Shortcut_to_SALON_ACADEMY___SALON_ACADEMY_TYPE_DESC as SALON_ACADEMY_TYPE_DESC",
	"SQ_Shortcut_To_SITE_PROFILE___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_To_SITE_PROFILE___STORE_NBR as STORE_NBR",
	"SQ_Shortcut_To_SITE_PROFILE___STORE_NAME as STORE_NAME",
	"SQ_Shortcut_To_SITE_PROFILE___STATE_CD as STATE_CD")

# COMMAND ----------
# Processing node JNR_ACADEMY, type JOINER 
# COLUMN COUNT: 36

JNR_ACADEMY = JNR_SITE_FIELDS.join(SQ_Shortcut_to_GS_Training,[JNR_SITE_FIELDS.SALON_ACADEMY_ID == SQ_Shortcut_to_GS_Training.SalonAcademyID],'right_outer')

# COMMAND ----------
# Processing node EXP_FIELDS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 30

# for each involved DataFrame, append the dataframe name to each column
JNR_ACADEMY_temp = JNR_ACADEMY.toDF(*["JNR_ACADEMY___" + col for col in JNR_ACADEMY.columns])

EXP_FIELDS = JNR_ACADEMY_temp.selectExpr(
	"JNR_ACADEMY___ID as SALON_ACADEMY_TRAINING_ID",
	"JNR_ACADEMY___TravelID as TravelID",
	"JNR_ACADEMY___HomeStoreID as Home_LOCATION_ID",
	"JNR_ACADEMY___AssociateID as EMPLOYEE_ID",
	"JNR_ACADEMY___AssociateFirstName as AssociateFirstName",
	"JNR_ACADEMY___AssociateLastName as AssociateLastName",
	"JNR_ACADEMY___SalonAcademyID as SALON_ACADEMY_ID",
	"JNR_ACADEMY___DM as DISTRICTF_LEADER_NAME",
	"JNR_ACADEMY___SalonSafetyCertiDate as SalonSafetyCertiDate",
	"JNR_ACADEMY___SplashDate as SplashDate",
	"JNR_ACADEMY___GroomingToolKit as GroomingToolKit",
	"JNR_ACADEMY___TrainingStatus as TRAINING_STATUS_ID",
	"JNR_ACADEMY___TrainingType as TrainingType",
	"JNR_ACADEMY___DogAcademyDateCompleted as DogAcademyDateCompleted",
	"JNR_ACADEMY___DateCreated as DateCreated",
	"JNR_ACADEMY___CreatedBy as CreatedBy",
	"JNR_ACADEMY___ModifiedBy as ModifiedBy",
	"JNR_ACADEMY___DateModified as DateModified",
	"JNR_ACADEMY___DistrictID as DistrictID",
	"JNR_ACADEMY___AssessmentID as AssessmentID",
	"JNR_ACADEMY___SplashExamPTSDate as SplashExamPTSDate",
	"JNR_ACADEMY___SplashObservPTSDate as SplashObservPTSDate",
	"JNR_ACADEMY___SafetyAllELearningDate as SafetyAllELearningDate",
	"JNR_ACADEMY___SafetySalonELearningDate as SafetySalonELearningDate",
	"JNR_ACADEMY___SafetyAllPTSDate as SafetyAllPTSDate",
	"JNR_ACADEMY___SafetySalonPTSDate as SafetySalonPTSDate",
	"JNR_ACADEMY___LOCATION_ID as LOCATION_ID",
	"JNR_ACADEMY___START_DT as START_DT",
	"JNR_ACADEMY___END_DT as END_DT",
	"JNR_ACADEMY___SALON_ACADEMY_TYPE_ID as SALON_ACADEMY_TYPE_ID",
	"JNR_ACADEMY___SALON_ACADEMY_TYPE_DESC as SALON_ACADEMY_TYPE_DESC",
	"JNR_ACADEMY___STORE_NBR as STORE_NBR",
	"JNR_ACADEMY___STORE_NAME as STORE_NAME",
	"JNR_ACADEMY___STATE_CD as STATE_CD").selectExpr(
	"SALON_ACADEMY_TRAINING_ID as SALON_ACADEMY_TRAINING_ID",
	"TravelID as TravelID",
	"Home_LOCATION_ID as Home_LOCATION_ID",
	"EMPLOYEE_ID as EMPLOYEE_ID",
	"SALON_ACADEMY_ID as SALON_ACADEMY_ID",
	"DISTRICTF_LEADER_NAME as DISTRICTF_LEADER_NAME",
	"SalonSafetyCertiDate as SalonSafetyCertiDate",
	"SplashDate as SplashDate",
	"GroomingToolKit as GroomingToolKit",
	"TRAINING_STATUS_ID as TRAINING_STATUS_ID",
	"IF (TRAINING_STATUS_ID = 1, 'Pending Approval', IF (TRAINING_STATUS_ID = 2, 'Approved', IF (TRAINING_STATUS_ID = 3, 'Cancelled', IF (TRAINING_STATUS_ID = 4, 'Completed', IF (TRAINING_STATUS_ID = 5, 'Denied', IF (TRAINING_STATUS_ID = 6, 'Needs More Information', 'N/A')))))) as TRAINING_STATUS_DESC",
	"TrainingType as TrainingType",
	"DogAcademyDateCompleted as DogAcademyDateCompleted",
	"DateCreated as DateCreated",
	"CreatedBy as CreatedBy",
	"ModifiedBy as ModifiedBy",
	"DateModified as DateModified",
	"DistrictID as DistrictID",
	"AssessmentID as AssessmentID",
	"SplashExamPTSDate as SplashExamPTSDate",
	"SplashObservPTSDate as SplashObservPTSDate",
	"SafetyAllELearningDate as SafetyAllELearningDate",
	"SafetySalonELearningDate as SafetySalonELearningDate",
	"SafetyAllPTSDate as SafetyAllPTSDate",
	"SafetySalonPTSDate as SafetySalonPTSDate",
	"SALON_ACADEMY_TYPE_ID as SALON_ACADEMY_TYPE_ID",
	"SALON_ACADEMY_TYPE_DESC as SALON_ACADEMY_TYPE_DESC",
	"CURRENT_TIMESTAMP as LOAD_TSTMP",
	"IF (TRAINING_STATUS_ID = 3, 1, 0) as CANCELLED_FLAG",
	"concat(coalesce(STORE_NAME,'') , ' ' , coalesce(STATE_CD,'') , ' Store ' , coalesce(STORE_NBR,'') , ', ' ,coalesce(to_date(START_DT, 'YYYY-MM-DD'),'') ) as SALON_ACADEMY_CLASS"
)

# COMMAND ----------
# Processing node Shortcut_to_SALON_ACADEMY_TRAINING_PRE, type TARGET 
# COLUMN COUNT: 30


Shortcut_to_SALON_ACADEMY_TRAINING_PRE = EXP_FIELDS.selectExpr(
	"SALON_ACADEMY_TRAINING_ID as SALON_ACADEMY_TRAINING_ID",
	"SALON_ACADEMY_ID as SALON_ACADEMY_ID",
	"CAST(SALON_ACADEMY_TYPE_ID AS INT) as SALON_ACADEMY_TYPE_ID",
	"CAST(SALON_ACADEMY_TYPE_DESC AS STRING) as SALON_ACADEMY_TYPE_DESC",
	"CAST(SALON_ACADEMY_CLASS AS STRING) as SALON_ACADEMY_CLASS",
	"CAST(TravelID AS BIGINT) as SALON_ACADEMY_TRAVEL_ID",
	"CAST(AssessmentID AS BIGINT) as SALON_ACADEMY_ASSESSMENT_ID",
	"CAST(Home_LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(EMPLOYEE_ID AS INT) as EMPLOYEE_ID",
	"CAST(DISTRICTF_LEADER_NAME AS STRING) as DISTRICT_LEADER_NAME",
	"CAST(SalonSafetyCertiDate AS DATE) as SALON_SAFETY_CERTIFICATE_DT",
	"CAST(SplashDate AS DATE) as SPLASH_DT",
	"CAST(SplashExamPTSDate AS DATE) as SPLASH_EXAM_PTS_DT",
	"CAST(SplashObservPTSDate AS DATE) as SPLASH_OBSERV_PTS_DT",
	"CAST(GroomingToolKit AS INT) as GROOMING_TOOL_KIT_ID",
	"CAST(TRAINING_STATUS_ID AS INT) as TRAINING_STATUS_ID",
	"CAST(TRAINING_STATUS_DESC AS STRING) as TRAINING_STATUS_DESC",
	"CAST(TrainingType AS INT) as TRAINING_TYPE_ID",
	"CAST(DateCreated AS TIMESTAMP) as ENROLLED_TSTMP",
	"CAST(DogAcademyDateCompleted AS DATE) as DOG_ACADEMY_COMPLETED_DT",
	"CAST(SafetyAllELearningDate AS DATE) as SAFETY_ALL_E_LEARNING_DT",
	"CAST(SafetyAllPTSDate AS DATE) as SAFETY_ALL_PTS_DT",
	"CAST(SafetySalonELearningDate AS DATE) as SAFETY_SALON_E_LEARNING_DT",
	"CAST(SafetySalonPTSDate AS DATE) as SAFETY_SALON_PTS_DT",
	"CAST(CANCELLED_FLAG AS INT) as CANCELLED_FLAG",
	"CAST(DateCreated AS TIMESTAMP) as CREATED_TSTMP",
	"CAST(CreatedBy AS STRING) as CREATED_BY",
	"CAST(DateModified AS TIMESTAMP) as MODIFIED_TSTMP",
	"CAST(ModifiedBy AS STRING) as MODIFIED_BY",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SALON_ACADEMY_TRAINING_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SALON_ACADEMY_TRAINING_PRE')
