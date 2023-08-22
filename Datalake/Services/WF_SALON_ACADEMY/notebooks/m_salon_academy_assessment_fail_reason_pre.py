#Code converted on 2023-07-26 09:54:16
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
# Processing node SQ_Shortcut_to_GS_FailReasonType, type SOURCE 
# COLUMN COUNT: 2

query = f"""(SELECT
GS_FailReasonType.ID,
GS_FailReasonType.ReasonDesc
FROM SalonAcademy.dbo.GS_FailReasonType) as src"""

SQ_Shortcut_to_GS_FailReasonType = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# COMMAND ----------
# Processing node EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_FailReasonType_temp = SQ_Shortcut_to_GS_FailReasonType.toDF(*["SQ_Shortcut_to_GS_FailReasonType___" + col for col in SQ_Shortcut_to_GS_FailReasonType.columns])

EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE = SQ_Shortcut_to_GS_FailReasonType_temp.selectExpr(
	"SQ_Shortcut_to_GS_FailReasonType___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_GS_FailReasonType___ID as ID",
	"SQ_Shortcut_to_GS_FailReasonType___ReasonDesc as ReasonDesc",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE = EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE.selectExpr(
	"CAST(ID AS INT) as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID",
	"CAST(ReasonDesc AS STRING) as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE')
