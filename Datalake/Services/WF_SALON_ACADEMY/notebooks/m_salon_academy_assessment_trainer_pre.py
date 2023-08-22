#Code converted on 2023-07-26 09:54:18
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
# Processing node SQ_Shortcut_to_GS_AssessmentTrainer, type SOURCE 
# COLUMN COUNT: 13

query = f"""(SELECT
GS_AssessmentTrainer.ID,
GS_AssessmentTrainer.AssessmentId,
GS_AssessmentTrainer.InstructorId,
GS_AssessmentTrainer.InstructorName,
GS_AssessmentTrainer.RegionId,
GS_AssessmentTrainer.RegionName,
GS_AssessmentTrainer.DistrictId,
GS_AssessmentTrainer.DistrictName,
GS_AssessmentTrainer.CreatedBy,
GS_AssessmentTrainer.CreatedDate,
GS_AssessmentTrainer.AssessmentTrainerText,
GS_AssessmentTrainer.ModifiedBy,
GS_AssessmentTrainer.ModifiedDate
FROM SalonAcademy.dbo.GS_AssessmentTrainer) as src"""

SQ_Shortcut_to_GS_AssessmentTrainer = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# COMMAND ----------
# Processing node EXP_SALON_ACADEMY_ASSESSMENT_TRAINER_PRE, type EXPRESSION 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_AssessmentTrainer_temp = SQ_Shortcut_to_GS_AssessmentTrainer.toDF(*["SQ_Shortcut_to_GS_AssessmentTrainer___" + col for col in SQ_Shortcut_to_GS_AssessmentTrainer.columns])

EXP_SALON_ACADEMY_ASSESSMENT_TRAINER_PRE = SQ_Shortcut_to_GS_AssessmentTrainer_temp.selectExpr(
	"SQ_Shortcut_to_GS_AssessmentTrainer___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_GS_AssessmentTrainer___ID as ID",
	"SQ_Shortcut_to_GS_AssessmentTrainer___AssessmentId as AssessmentId",
	"SQ_Shortcut_to_GS_AssessmentTrainer___InstructorId as InstructorId",
	"SQ_Shortcut_to_GS_AssessmentTrainer___InstructorName as InstructorName",
	"SQ_Shortcut_to_GS_AssessmentTrainer___RegionId as RegionId",
	"SQ_Shortcut_to_GS_AssessmentTrainer___RegionName as RegionName",
	"SQ_Shortcut_to_GS_AssessmentTrainer___DistrictId as DistrictId",
	"SQ_Shortcut_to_GS_AssessmentTrainer___DistrictName as DistrictName",
	"SQ_Shortcut_to_GS_AssessmentTrainer___CreatedBy as CreatedBy",
	"SQ_Shortcut_to_GS_AssessmentTrainer___CreatedDate as CreatedDate",
	"SQ_Shortcut_to_GS_AssessmentTrainer___AssessmentTrainerText as AssessmentTrainerText",
	"SQ_Shortcut_to_GS_AssessmentTrainer___ModifiedBy as ModifiedBy",
	"SQ_Shortcut_to_GS_AssessmentTrainer___ModifiedDate as ModifiedDate",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node Shortcut_to_SALON_ACADEMY_ASSESSMENT_TRAINER_PRE, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_SALON_ACADEMY_ASSESSMENT_TRAINER_PRE = EXP_SALON_ACADEMY_ASSESSMENT_TRAINER_PRE.selectExpr(
	"CAST(ID AS BIGINT) as SALON_ACADEMY_ASSESSMENT_TRAINER_ID",
	"CAST(AssessmentId AS BIGINT) as SALON_ACADEMY_ASSESSMENT_ID",
	"CAST(InstructorId AS STRING) as SALON_ACADEMY_INSTRUCTOR_ID",
	"CAST(InstructorName AS STRING) as SALON_ACADEMY_INSTRUCTOR_NAME",
	"CAST(AssessmentTrainerText AS STRING) as ASSESSMENT_TRAINER_TEXT",
	"CAST(RegionId AS BIGINT) as REGION_ID",
	"CAST(RegionName AS STRING) as REGION_NAME",
	"CAST(DistrictId AS BIGINT) as DISTRICT_ID",
	"CAST(DistrictName AS STRING) as DISTRICT_NAME",
	"CAST(CreatedDate AS TIMESTAMP) as CREATED_TSTMP",
	"CAST(CreatedBy AS STRING) as CREATED_BY",
	"CAST(ModifiedDate AS TIMESTAMP) as MODIFIED_TSTMP",
	"CAST(ModifiedBy AS STRING) as MODIFIED_BY",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SALON_ACADEMY_ASSESSMENT_TRAINER_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SALON_ACADEMY_ASSESSMENT_TRAINER_PRE')
