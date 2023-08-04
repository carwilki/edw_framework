#Code converted on 2023-07-19 11:07:12
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
#from pyspark.dbutils import DBUtils
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

parser = argparse.ArgumentParser()

spark = SparkSession.getActiveSession()

#dbutils = DBUtils(spark)

parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

#env = 'dev'

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

# Processing node SQ_Shortcut_to_ServiceSuspensionLog, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_ServiceSuspensionLog = jdbcSqlServerConnection(f"""(SELECT CAST(DAYDT AS DATE) AS DayDt 
,StoreNumber
,ServiceArea
,SuspensionReason
,SubmittedBy
,SubmittedByPosition
,Comments
,ReversalDt
,CreateDt
,ModifyDt
  FROM (SELECT DayDt
,StoreNumber
,ServiceArea
,SuspensionReason
,SubmittedBy
,SubmittedByPosition
,Comments
,ReversalDt
,CreateDt
,ModifyDt
,ROW_NUMBER () OVER (PARTITION BY CAST(DAYDT AS DATE), STORENUMBER, SERVICEAREA ORDER BY MODIFYDT DESC, CREATEDT DESC) AS RNK
FROM ServiceSuspensionLog ) X
WHERE RNK = 1
) as src""", username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# _sql = "(SELECT MAX(CreateDt) max_create, MAX(ModifyDt) max_modify FROM ServiceSuspensionLog) as src"
# df =  jdbcSqlServerConnection(_sql, username,password,connection_string)
# df.show()

# COMMAND ----------

# Processing node EXP_FIELDS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ServiceSuspensionLog_temp = SQ_Shortcut_to_ServiceSuspensionLog.toDF(*["SQ_Shortcut_to_ServiceSuspensionLog___" + col for col in SQ_Shortcut_to_ServiceSuspensionLog.columns])

EXP_FIELDS = SQ_Shortcut_to_ServiceSuspensionLog_temp.selectExpr(
	"SQ_Shortcut_to_ServiceSuspensionLog___DayDt as DAY_DT",
	"SQ_Shortcut_to_ServiceSuspensionLog___StoreNumber as IN_STORE_NUMBER",
  "cast(SQ_Shortcut_to_ServiceSuspensionLog___StoreNumber as int) as STORE_NUMBER",
	"SQ_Shortcut_to_ServiceSuspensionLog___ServiceArea as SERVICE_AREA",
	"SQ_Shortcut_to_ServiceSuspensionLog___SuspensionReason as SUSPENSION_REASON",
	"SQ_Shortcut_to_ServiceSuspensionLog___SubmittedBy as SUBMITTED_BY",
	"SQ_Shortcut_to_ServiceSuspensionLog___SubmittedByPosition as SUBMITTED_BY_POSITION",
	"SQ_Shortcut_to_ServiceSuspensionLog___Comments as COMMENTS",
	"SQ_Shortcut_to_ServiceSuspensionLog___ReversalDt as REVERSAL_DT",
	"SQ_Shortcut_to_ServiceSuspensionLog___CreateDt as CREATE_TSTMP",
 	"SQ_Shortcut_to_ServiceSuspensionLog___sys_row_id as sys_row_id",
  "NULL AS UPDATE_TSTMP",
  "CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE, type TARGET 
# COLUMN COUNT: 11


Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE = EXP_FIELDS.selectExpr(
	"CAST(DAY_DT AS DATE) as DAY_DT",
	"CAST(STORE_NUMBER AS INT) as STORE_NUMBER",
	"CAST(SERVICE_AREA AS STRING) as SERVICE_AREA",
	"CAST(SUSPENSION_REASON AS STRING) as SUSPENSION_REASON",
	"CAST(SUBMITTED_BY AS STRING) as SUBMITTED_BY",
	"CAST(SUBMITTED_BY_POSITION AS STRING) as SUBMITTED_BY_POSITION",
	"CAST(COMMENTS AS STRING) as COMMENTS",
	"CAST(REVERSAL_DT AS TIMESTAMP) as REVERSAL_TSTMP",
	"CAST(CREATE_TSTMP AS TIMESTAMP) as CREATE_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)

Shortcut_to_SVC_SERVICE_SUSPENSION_LOG_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SVC_SERVICE_SUSPENSION_LOG_PRE')
