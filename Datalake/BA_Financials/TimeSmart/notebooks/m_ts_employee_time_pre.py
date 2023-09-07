#Code converted on 2023-08-07 16:26:09
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
# Processing node SQ_Shortcut_to_Employee_Time, type SOURCE 
# COLUMN COUNT: 7

query = f"""(SELECT
Employee_Time.EmpTimeID,
Employee_Time.EmpID,
Employee_Time.ActXRefID,
Employee_Time.DayDT,
Employee_Time.Hours,
Employee_Time.RFCNBR,
Employee_Time.CreateDate
FROM Time_Tracking.dbo.Employee_Time
WHERE daydt >= dateadd(day,-45,convert(varchar,getdate(),112))) as src"""

SQ_Shortcut_to_Employee_Time = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_Employee_Time_Comments, type SOURCE 
# COLUMN COUNT: 2

query = f"""(SELECT
Employee_Time_Comments.EmpTimeID,
Employee_Time_Comments.Comment
FROM Time_Tracking.dbo.Employee_Time_Comments) as src"""

SQ_Shortcut_to_Employee_Time_Comments = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_LEFTJOIN, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Employee_Time_Comments_temp = SQ_Shortcut_to_Employee_Time_Comments.toDF(*["SQ_Shortcut_to_Employee_Time_Comments___" + col for col in SQ_Shortcut_to_Employee_Time_Comments.columns])
SQ_Shortcut_to_Employee_Time_temp = SQ_Shortcut_to_Employee_Time.toDF(*["SQ_Shortcut_to_Employee_Time___" + col for col in SQ_Shortcut_to_Employee_Time.columns])

JNR_LEFTJOIN = SQ_Shortcut_to_Employee_Time_Comments_temp.join(SQ_Shortcut_to_Employee_Time_temp,[SQ_Shortcut_to_Employee_Time_Comments_temp.SQ_Shortcut_to_Employee_Time_Comments___EmpTimeID == SQ_Shortcut_to_Employee_Time_temp.SQ_Shortcut_to_Employee_Time___EmpTimeID],'right_outer').selectExpr(
	"SQ_Shortcut_to_Employee_Time___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_Employee_Time___EmpTimeID as EmpTimeID",
	"SQ_Shortcut_to_Employee_Time___DayDT as DayDT",
	"SQ_Shortcut_to_Employee_Time___EmpID as EmpID",
	"SQ_Shortcut_to_Employee_Time___ActXRefID as ActXRefID",
	"SQ_Shortcut_to_Employee_Time___Hours as Hours",
	"SQ_Shortcut_to_Employee_Time___RFCNBR as RFCNBR",
	"SQ_Shortcut_to_Employee_Time___CreateDate as CreateDate",
	"SQ_Shortcut_to_Employee_Time_Comments___EmpTimeID as M_EmpTimeID",
	"SQ_Shortcut_to_Employee_Time_Comments___Comment as M_Comment")

# COMMAND ----------
# Processing node EXP_RemoveCommentPipes, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2
 
# for each involved DataFrame, append the dataframe name to each column
JNR_LEFTJOIN_temp = JNR_LEFTJOIN.toDF(*["JNR_LEFTJOIN___" + col for col in JNR_LEFTJOIN.columns])
 
# This dataframe was casusing mismatches
# EXP_RemoveCommentPipes = JNR_LEFTJOIN_temp.selectExpr(
#   "JNR_LEFTJOIN___M_Comment as Comment",
#   "JNR_LEFTJOIN___sys_row_id as sys_row_id",
#   "REPLACE ( JNR_LEFTJOIN___M_Comment,'|',' ' ) as Comment_FXD"
# )
# COMMAND ----------
# Processing node Shortcut_to_TS_EMPLOYEE_TIME_PRE, type TARGET
# COLUMN COUNT: 7
 
# Joining dataframes JNR_LEFTJOIN, EXP_RemoveCommentPipes to form Shortcut_to_TS_EMPLOYEE_TIME_PRE
#Shortcut_to_TS_EMPLOYEE_TIME_PRE_joined = JNR_LEFTJOIN.join(EXP_RemoveCommentPipes, JNR_LEFTJOIN.sys_row_id == EXP_RemoveCommentPipes.sys_row_id, 'inner')
 
Shortcut_to_TS_EMPLOYEE_TIME_PRE = JNR_LEFTJOIN_temp.selectExpr(
  "CAST(JNR_LEFTJOIN___DayDT AS DATE) as DAYDT",
  "CAST(JNR_LEFTJOIN___EmpID AS INT) as EMPID",
  "CAST(JNR_LEFTJOIN___ActXRefID AS INT) as ACTXREFID",
  "CAST(JNR_LEFTJOIN___Hours AS DECIMAL(4,2)) as HOURS",
  "CAST(JNR_LEFTJOIN___RFCNBR AS STRING) as RFCNBR",
  "CAST(JNR_LEFTJOIN___CreateDate AS DATE) as CREATEDATE",
  "CAST(REPLACE ( JNR_LEFTJOIN___M_Comment,'|',' ' )AS STRING) as COMMENT"
)
# overwriteDeltaPartition(Shortcut_to_TS_EMPLOYEE_TIME_PRE,'DC_NBR',dcnbr,f'{raw}.TS_EMPLOYEE_TIME_PRE')
Shortcut_to_TS_EMPLOYEE_TIME_PRE.write.mode("overwrite").saveAsTable(f'{raw}.TS_EMPLOYEE_TIME_PRE')


#replacing from here
# # COMMAND ----------
# # Processing node EXP_RemoveCommentPipes, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# # COLUMN COUNT: 2

# # for each involved DataFrame, append the dataframe name to each column
# JNR_LEFTJOIN_temp = JNR_LEFTJOIN.toDF(*["JNR_LEFTJOIN___" + col for col in JNR_LEFTJOIN.columns])

# EXP_RemoveCommentPipes = JNR_LEFTJOIN_temp.selectExpr(
# 	"JNR_LEFTJOIN___M_Comment as Comment",
# 	"JNR_LEFTJOIN___sys_row_id as sys_row_id",
# 	"REPLACE ( JNR_LEFTJOIN___M_Comment,'|',' ' ) as Comment_FXD"
# )

# # COMMAND ----------
# # Processing node Shortcut_to_TS_EMPLOYEE_TIME_PRE, type TARGET 
# # COLUMN COUNT: 7

# # Joining dataframes JNR_LEFTJOIN, EXP_RemoveCommentPipes to form Shortcut_to_TS_EMPLOYEE_TIME_PRE
# Shortcut_to_TS_EMPLOYEE_TIME_PRE_joined = JNR_LEFTJOIN.join(EXP_RemoveCommentPipes, JNR_LEFTJOIN.sys_row_id == EXP_RemoveCommentPipes.sys_row_id, 'inner')

# Shortcut_to_TS_EMPLOYEE_TIME_PRE = Shortcut_to_TS_EMPLOYEE_TIME_PRE_joined.selectExpr(
# 	"CAST(DayDT AS DATE) as DAYDT",
# 	"CAST(EmpID AS INT) as EMPID",
# 	"CAST(ActXRefID AS INT) as ACTXREFID",
# 	"CAST(Hours AS DECIMAL(4,2)) as HOURS",
# 	"CAST(RFCNBR AS STRING) as RFCNBR",
# 	"CAST(CreateDate AS DATE) as CREATEDATE",
# 	"CAST(Comment_FXD AS STRING) as COMMENT"
# )
# # overwriteDeltaPartition(Shortcut_to_TS_EMPLOYEE_TIME_PRE,'DC_NBR',dcnbr,f'{raw}.TS_EMPLOYEE_TIME_PRE')
# Shortcut_to_TS_EMPLOYEE_TIME_PRE.write.mode("overwrite").saveAsTable(f'{raw}.TS_EMPLOYEE_TIME_PRE')