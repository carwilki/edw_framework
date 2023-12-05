# Databricks notebook source
#Code converted on 2023-10-17 15:29:11
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

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

def has_duplicates(df: DataFrame, key_columns: list) -> bool:
	"""
	Check if a DataFrame has duplicate rows based on the specified key columns.

	:param df: The DataFrame to check for duplicates.
	:param key_columns: A list of column names to use as the key for checking duplicates.
	:return: True if duplicates are found, False otherwise.
	"""
	if df.count() == 0:
		return False
	# Group by the key columns and count occurrences
	grouped = df.groupBy(key_columns).count()

	# Find the maximum count
	max_count = grouped.agg({"count": "max"}).collect()[0][0]

	# Return True if duplicates found, False otherwise
	return max_count > 1

# COMMAND ----------

parameter_file_name='wf_skillsoft_acm'
parameter_section='m_skillsoft_acm_asset_activity'
parameter_key='source_bucket'

# parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/employee/acm_group_report_training_status/'
# insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# COMMAND ----------

# Processing node LKP_EXISTS_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_EXISTS_SRC = spark.sql(f"""SELECT
STUDENT_REC_ID,
USERID,
USER_NAME,
REHIRE_DATE,
ACCOUNT_NUMBER,
COURSEID,
COURSE,
SKILLSOFT_COURSE_NUMBER,
METHOD,
ESTIMATED_COST,
ESTIMATED_DURATION,
CEU,
STATUS,
REQOPT,
DUE_DATE,
VALIDITYINDAYS,
BEGIN_DATE,
LAST_VISIT_DATE,
DATE_COMPLETED,
SCORE,
TIME_IN_COURSE_TO_COMPLETION,
TOTAL_TIME_IN_COURSE,
DATE_OPEN_FOR_TRAINING,
EXEMPTION_EXPIRATION_DATE,
USER_STATUS,
USER_EMAIL,
SUPERVISOR_EMAIL,
SUPERVISOR_LEVEL_2_EMAIL,
PRIORCOURSECOMPLETED,
PRIORCOURSETITLE,
COMMENTS,
MAXIMUM_ASSESSMENT_ATTEMPTS,
ACTUAL_ASSESSMENT_ATTEMPTS,
GROUP_FROM,
LOAD_DT,
UPDATE_DT
FROM {legacy}.SKILLSOFT_ACM_ASSET_ACTIVITY""")


# COMMAND ----------

source_bucket=getParameterValue(raw,parameter_file_name,parameter_section,parameter_key)
# print(source_bucket)
source_file = get_src_file('ACM_group_report_training_status', source_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_Report_ACM_Training_Status, type SOURCE 
# COLUMN COUNT: 34

SQ_Shortcut_to_Report_ACM_Training_Status = spark.read.csv(source_file, sep=',', header='true', quote='"').withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

new_columns = [col(c).alias(c.strip().replace(" ", "_")) for c in SQ_Shortcut_to_Report_ACM_Training_Status.columns]
SQ_Shortcut_to_Report_ACM_Training_Status = SQ_Shortcut_to_Report_ACM_Training_Status.select(*new_columns)

SQ_Shortcut_to_Report_ACM_Training_Status = SQ_Shortcut_to_Report_ACM_Training_Status.withColumnRenamed("User_Specific_Data_2", "Account_Number").withColumnRenamed("StudentReqID", "Student_Rec_ID")


# COMMAND ----------

# Processing node LKP_EXISTS, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 40


LKP_EXISTS_lookup_result = SQ_Shortcut_to_Report_ACM_Training_Status.selectExpr(
    "sys_row_id",
	"UserID as in_UserID",
	"CourseID as in_CourseID",
	"Student_Rec_ID as in_Student_Rec_ID") \
	.withColumn('in_Date_Open_For_Training', lit(None)) \
	.join(LKP_EXISTS_SRC, (col('STUDENT_REC_ID') == col('in_Student_Rec_ID')) & (col('USERID') == col('in_UserID')), 'left') \
    .withColumn('row_num_STUDENT_REC_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("STUDENT_REC_ID")))
    
LKP_EXISTS = LKP_EXISTS_lookup_result.filter(expr("row_num_STUDENT_REC_ID = 1")).select(
	col('sys_row_id'),
	col('STUDENT_REC_ID'),
	col('USERID'),
	col('COURSEID'),
	col('DATE_OPEN_FOR_TRAINING')
)

# COMMAND ----------

# Processing node EXP_UPDATE_FLAG, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 40

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Report_ACM_Training_Status_temp = SQ_Shortcut_to_Report_ACM_Training_Status.toDF(*["SQ_Shortcut_to_Report_ACM_Training_Status___" + col for col in SQ_Shortcut_to_Report_ACM_Training_Status.columns])
LKP_EXISTS_temp = LKP_EXISTS.toDF(*["LKP_EXISTS___" + col for col in LKP_EXISTS.columns])

# Joining dataframes SQ_Shortcut_to_Report_ACM_Training_Status, LKP_EXISTS to form EXP_UPDATE_FLAG
EXP_UPDATE_FLAG_joined = SQ_Shortcut_to_Report_ACM_Training_Status_temp.join(LKP_EXISTS_temp, col("SQ_Shortcut_to_Report_ACM_Training_Status___sys_row_id") == col("LKP_EXISTS___sys_row_id"), 'inner')

EXP_UPDATE_FLAG = EXP_UPDATE_FLAG_joined.selectExpr(
    "SQ_Shortcut_to_Report_ACM_Training_Status___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_Report_ACM_Training_Status___UserID as UserID",
	"SQ_Shortcut_to_Report_ACM_Training_Status___User_Name as User_Name",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Rehire_Date as Rehire_Date",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Account_Number as Account_Number",
	"SQ_Shortcut_to_Report_ACM_Training_Status___CourseID as CourseID",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Course as Course",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Skillsoft_Course_Number as Skillsoft_Course_Number",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Method as Method",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Estimated_Cost as Estimated_Cost",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Estimated_Duration as Estimated_Duration",
	"SQ_Shortcut_to_Report_ACM_Training_Status___CEU as CEU",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Status as Status",
	"SQ_Shortcut_to_Report_ACM_Training_Status___ReqOpt as ReqOpt",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Due_Date as Due_Date",
	"SQ_Shortcut_to_Report_ACM_Training_Status___ValidityInDays as ValidityInDays",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Begin_Date as Begin_Date",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Last_Visit_Date as Last_Visit_Date",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Date_Completed as Date_Completed",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Score as Score",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Time_in_Course_to_Completion as Time_in_Course_to_Completion",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Total_Time_in_Course as Total_Time_in_Course",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Date_Open_For_Training as Date_Open_For_Training",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Exemption_Expiration_Date as Exemption_Expiration_Date",
	"SQ_Shortcut_to_Report_ACM_Training_Status___User_Status as User_Status",
	"SQ_Shortcut_to_Report_ACM_Training_Status___User_Email as User_Email",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Supervisor_Email as Supervisor_Email",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Supervisor_Level_2_Email as Supervisor_Level_2_Email",
	"SQ_Shortcut_to_Report_ACM_Training_Status___PriorCourseCompleted as PriorCourseCompleted",
	"SQ_Shortcut_to_Report_ACM_Training_Status___PriorCourseTitle as PriorCourseTitle",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Comments as Comments",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Maximum_Assessment_Attempts as Maximum_Assessment_Attempts",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Actual_Assessment_Attempts as Actual_Assessment_Attempts",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Group_From as Group_From",
	"SQ_Shortcut_to_Report_ACM_Training_Status___Student_Rec_ID as Student_Rec_ID",
	"LKP_EXISTS___USERID as LKP_USERID",
	"LKP_EXISTS___COURSEID as LKP_COURSEID",
	"LKP_EXISTS___DATE_OPEN_FOR_TRAINING as LKP_DATE_OPEN_FOR_TRAINING",
	"LKP_EXISTS___STUDENT_REC_ID as LKP_STUDENT_REC_ID",
    "CURRENT_TIMESTAMP as UPDATE_DT").withColumn("UPDATE_FLAG", expr("IF (( LKP_STUDENT_REC_ID IS NULL and LKP_USERID IS NULL ), 1, 0)"))
	

# COMMAND ----------

# Processing node EXP_TO_DATE, type EXPRESSION 
# COLUMN COUNT: 13


EXP_TO_DATE = EXP_UPDATE_FLAG.selectExpr(
	"sys_row_id as sys_row_id",
	"Rehire_Date as Rehire_Date",
	"IF (instr ( Rehire_Date , '-' ) > 0, to_date ( Rehire_Date , 'yyyy-MM-dd' ), to_date ( Rehire_Date , 'MM/dd/yyyy' )) as o_Rehire_Date",
	"Due_Date as Due_Date",
	"IF (instr ( Due_Date , '-' ) > 0, to_date ( Due_Date , 'yyyy-MM-dd' ), to_date ( Due_Date , 'MM/dd/yyyy' )) as o_Due_Date",
	"Begin_Date as Begin_Date",
	"IF (instr ( Begin_Date , '-' ) > 0, to_date ( Begin_Date , 'yyyy-MM-dd' ), to_date ( Begin_Date , 'MM/dd/yyyy' )) as o_Begin_Date",
	"Last_Visit_Date as Last_Visit_Date",
	"IF (instr ( Last_Visit_Date , '-' ) > 0, to_date ( Last_Visit_Date , 'yyyy-MM-dd' ), to_date ( Last_Visit_Date , 'MM/dd/yyyy' )) as o_Last_Visit_Date",
	"Date_Completed as Date_Completed",
	"IF (instr ( Date_Completed , '-' ) > 0, to_date ( Date_Completed , 'yyyy-MM-dd' ), to_date ( Date_Completed , 'MM/dd/yyyy' )) as o_Date_Completed",
	"Date_Open_For_Training as Date_Open_For_Training",
	"IF (instr ( Date_Open_For_Training , '-' ) > 0, to_date ( Date_Open_For_Training , 'yyyy-MM-dd' ), to_date ( Date_Open_For_Training , 'MM/dd/yyyy' )) as o_Date_Open_For_Training",
	"IF (instr ( Exemption_Expiration_Date , '-' ) > 0, to_date ( Exemption_Expiration_Date , 'yyyy-MM-dd' ), to_date ( Exemption_Expiration_Date , 'MM/dd/yyyy' )) as o_Exemption_Expiration_Date"
)

# COMMAND ----------

# Processing node UPDTRANS, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 36


# for each involved DataFrame, append the dataframe name to each column
EXP_TO_DATE_temp = EXP_TO_DATE.toDF(*["EXP_TO_DATE___" + col for col in EXP_TO_DATE.columns])
EXP_UPDATE_FLAG_temp = EXP_UPDATE_FLAG.toDF(*["EXP_UPDATE_FLAG___" + col for col in EXP_UPDATE_FLAG.columns])

# Joining dataframes EXP_UPDATE_FLAG, EXP_TO_DATE to form UPDTRANS
UPDTRANS_joined = EXP_UPDATE_FLAG_temp.join(EXP_TO_DATE_temp, col("EXP_UPDATE_FLAG___sys_row_id") == col("EXP_TO_DATE___sys_row_id"), 'inner')


UPDTRANS = UPDTRANS_joined.selectExpr(
	"EXP_UPDATE_FLAG___UserID as UserID",
	"EXP_UPDATE_FLAG___User_Name as User_Name",
	"EXP_TO_DATE___o_Rehire_Date as Rehire_Date",
	"EXP_UPDATE_FLAG___Account_Number as Account_Number",
	"EXP_UPDATE_FLAG___CourseID as CourseID",
	"EXP_UPDATE_FLAG___Course as Course",
	"EXP_UPDATE_FLAG___Skillsoft_Course_Number as Skillsoft_Course_Number",
	"EXP_UPDATE_FLAG___Method as Method",
	"EXP_UPDATE_FLAG___Estimated_Cost as Estimated_Cost",
	"EXP_UPDATE_FLAG___Estimated_Duration as Estimated_Duration",
	"EXP_UPDATE_FLAG___CEU as CEU",
	"EXP_UPDATE_FLAG___Status as Status",
	"EXP_UPDATE_FLAG___ReqOpt as ReqOpt",
	"EXP_TO_DATE___o_Due_Date as Due_Date",
	"EXP_UPDATE_FLAG___ValidityInDays as ValidityInDays",
	"EXP_TO_DATE___o_Begin_Date as Begin_Date",
	"EXP_TO_DATE___o_Last_Visit_Date as Last_Visit_Date",
	"EXP_TO_DATE___o_Date_Completed as Date_Completed",
	"EXP_UPDATE_FLAG___Score as Score",
	"EXP_UPDATE_FLAG___Time_in_Course_to_Completion as Time_in_Course_to_Completion",
	"EXP_UPDATE_FLAG___Total_Time_in_Course as Total_Time_in_Course",
	"EXP_TO_DATE___o_Date_Open_For_Training as Date_Open_For_Training",
	"EXP_TO_DATE___o_Exemption_Expiration_Date as Exemption_Expiration_Date",
	"EXP_UPDATE_FLAG___User_Status as User_Status",
	"EXP_UPDATE_FLAG___User_Email as User_Email",
	"EXP_UPDATE_FLAG___Supervisor_Email as Supervisor_Email",
	"EXP_UPDATE_FLAG___Supervisor_Level_2_Email as Supervisor_Level_2_Email",
	"EXP_UPDATE_FLAG___PriorCourseCompleted as PriorCourseCompleted",
	"EXP_UPDATE_FLAG___PriorCourseTitle as PriorCourseTitle",
	"EXP_UPDATE_FLAG___Comments as Comments",
	"EXP_UPDATE_FLAG___Maximum_Assessment_Attempts as Maximum_Assessment_Attempts",
	"EXP_UPDATE_FLAG___Actual_Assessment_Attempts as Actual_Assessment_Attempts",
	"EXP_UPDATE_FLAG___Group_From as Group_From",
	"EXP_UPDATE_FLAG___Student_Rec_ID as Student_Rec_ID",
	"EXP_UPDATE_FLAG___UPDATE_DT as UPDATE_DT",
	"EXP_UPDATE_FLAG___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col("UPDATE_FLAG") ==(lit(1)), lit(0)).otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_SKILLSOFT_ACM_ASSET_ACTIVITY, type TARGET 
# COLUMN COUNT: 37


Shortcut_to_SKILLSOFT_ACM_ASSET_ACTIVITY = UPDTRANS.selectExpr(
	"CAST(Student_Rec_ID AS BIGINT) as STUDENT_REC_ID",
	"CAST(UserID AS BIGINT) as USERID",
	"CAST(User_Name AS STRING) as USER_NAME",
	"CAST(Rehire_Date AS TIMESTAMP) as REHIRE_DATE",
	"CAST(Account_Number AS STRING) as ACCOUNT_NUMBER",
	"CAST(CourseID AS STRING) as COURSEID",
	"CAST(Course AS STRING) as COURSE",
	"CAST(Skillsoft_Course_Number AS STRING) as SKILLSOFT_COURSE_NUMBER",
	"CAST(Method AS STRING) as METHOD",
	"CAST(Estimated_Cost AS STRING) as ESTIMATED_COST",
	"CAST(Estimated_Duration AS STRING) as ESTIMATED_DURATION",
	"CAST(CEU AS STRING) as CEU",
	"CAST(Status AS STRING) as STATUS",
	"CAST(ReqOpt AS STRING) as REQOPT",
	"CAST(Due_Date AS TIMESTAMP) as DUE_DATE",
	"CAST(ValidityInDays AS BIGINT) as VALIDITYINDAYS",
	"CAST(Begin_Date AS TIMESTAMP) as BEGIN_DATE",
	"CAST(Last_Visit_Date AS TIMESTAMP) as LAST_VISIT_DATE",
	"CAST(Date_Completed AS TIMESTAMP) as DATE_COMPLETED",
	"CAST(Score AS STRING) as SCORE",
	"CAST(Time_in_Course_to_Completion AS STRING) as TIME_IN_COURSE_TO_COMPLETION",
	"CAST(Total_Time_in_Course AS STRING) as TOTAL_TIME_IN_COURSE",
	"CAST(Date_Open_For_Training AS TIMESTAMP) as DATE_OPEN_FOR_TRAINING",
	"CAST(Exemption_Expiration_Date AS TIMESTAMP) as EXEMPTION_EXPIRATION_DATE",
	"CAST(User_Status AS STRING) as USER_STATUS",
	"CAST(User_Email AS STRING) as USER_EMAIL",
	"CAST(Supervisor_Email AS STRING) as SUPERVISOR_EMAIL",
	"CAST(Supervisor_Level_2_Email AS STRING) as SUPERVISOR_LEVEL_2_EMAIL",
	"CAST(PriorCourseCompleted AS STRING) as PRIORCOURSECOMPLETED",
	"CAST(PriorCourseTitle AS STRING) as PRIORCOURSETITLE",
	"CAST(Comments AS STRING) as COMMENTS",
	"CAST(Maximum_Assessment_Attempts AS STRING) as MAXIMUM_ASSESSMENT_ATTEMPTS",
	"CAST(Actual_Assessment_Attempts AS STRING) as ACTUAL_ASSESSMENT_ATTEMPTS",
	"CAST(Group_From AS STRING) as GROUP_FROM",
	"CAST(0 AS BIGINT) as DELETE_FLAG",
	"CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as LOAD_DT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"pyspark_data_action as pyspark_data_action"
).filter("USERID IS NOT NULL")  # some USERID values in UPDTRANS are not numeric, so cast makes them NULLs 

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

try:
	primary_key = """source.STUDENT_REC_ID = target.STUDENT_REC_ID AND source.USERID = target.USERID"""
	refined_perf_table = f"{legacy}.SKILLSOFT_ACM_ASSET_ACTIVITY"

	if has_duplicates(Shortcut_to_SKILLSOFT_ACM_ASSET_ACTIVITY, ["STUDENT_REC_ID", "USERID"]):
		raise Exception("Duplicates found in the dataset")

	executeMerge(Shortcut_to_SKILLSOFT_ACM_ASSET_ACTIVITY, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("SKILLSOFT_ACM_ASSET_ACTIVITY", "SKILLSOFT_ACM_ASSET_ACTIVITY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("SKILLSOFT_ACM_ASSET_ACTIVITY", "SKILLSOFT_ACM_ASSET_ACTIVITY","Failed",str(e), f"{raw}.log_run_details", )
	raise e

# COMMAND ----------

UPDTRANS.filter("CAST(userid as bigint) is null").select("USERID").show()

# COMMAND ----------


