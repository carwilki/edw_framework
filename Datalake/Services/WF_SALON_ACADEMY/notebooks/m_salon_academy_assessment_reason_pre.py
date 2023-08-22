#Code converted on 2023-07-26 09:54:19
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
# Processing node SQ_Shortcut_to_GS_AssessmentReason, type SOURCE 
# COLUMN COUNT: 7

query = f"""(SELECT
GS_AssessmentReason.ID,
GS_AssessmentReason.AssessmentId,
GS_AssessmentReason.ReasonText,
GS_AssessmentReason.CreatedBy,
GS_AssessmentReason.CreatedDate,
GS_AssessmentReason.ModifiedBy,
GS_AssessmentReason.ModifiedDate
FROM SalonAcademy.dbo.GS_AssessmentReason) as src"""

SQ_Shortcut_to_GS_AssessmentReason = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SALON_ACADEMY_ASSESSMENT_REASON_PRE, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_AssessmentReason_temp = SQ_Shortcut_to_GS_AssessmentReason.toDF(*["SQ_Shortcut_to_GS_AssessmentReason___" + col for col in SQ_Shortcut_to_GS_AssessmentReason.columns])

EXP_SALON_ACADEMY_ASSESSMENT_REASON_PRE = SQ_Shortcut_to_GS_AssessmentReason_temp.selectExpr(
	"SQ_Shortcut_to_GS_AssessmentReason___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_GS_AssessmentReason___ID as ID",
	"SQ_Shortcut_to_GS_AssessmentReason___AssessmentId as AssessmentId",
	"SQ_Shortcut_to_GS_AssessmentReason___ReasonText as ReasonText",
	"SQ_Shortcut_to_GS_AssessmentReason___CreatedBy as CreatedBy",
	"SQ_Shortcut_to_GS_AssessmentReason___CreatedDate as CreatedDate",
	"SQ_Shortcut_to_GS_AssessmentReason___ModifiedBy as ModifiedBy",
	"SQ_Shortcut_to_GS_AssessmentReason___ModifiedDate as ModifiedDate",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node Shortcut_to_SALON_ACADEMY_ASSESSMENT_REASON_PRE, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_SALON_ACADEMY_ASSESSMENT_REASON_PRE = EXP_SALON_ACADEMY_ASSESSMENT_REASON_PRE.selectExpr(
	"CAST(ID as BIGINT) as SALON_ACADEMY_ASSESSMENT_REASON_ID",
	"CAST(AssessmentId as BIGINT) as SALON_ACADEMY_ASSESSMENT_ID",
	"CAST(ReasonText AS STRING) as REASON_TEXT",
	"CAST(CreatedDate AS TIMESTAMP) as CREATED_TSTMP",
	"CAST(CreatedBy AS STRING) as CREATED_BY",
	"CAST(ModifiedDate AS TIMESTAMP) as MODIFIED_TSTMP",
	"CAST(ModifiedBy AS STRING) as MODIFIED_BY",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SALON_ACADEMY_ASSESSMENT_REASON_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SALON_ACADEMY_ASSESSMENT_REASON_PRE')
