# Databricks notebook source
#Code converted on 2023-10-17 15:29:13
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
parameter_section='m_skillsoft_acm_learning_program'
parameter_key='source_bucket'

# parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/employee/acm_learning_program/'
# insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# COMMAND ----------

# Processing node LKP_EXISTS_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_EXISTS_SRC = spark.sql(f"""SELECT
USERID,
USER_NAME,
REHIRE_DATE,
ACCOUNT_NUMBER,
COURSEID,
COURSE_TITLE,
SKILLSOFT_COURSE_NUMBER,
COURSE_STATUS,
SCORE,
TOTAL_TIME_IN_COURSE,
TOTAL_TIME_IN_COURSE_TO_COMPLETION,
COURSE_ORDER,
COURSE_BEGIN_DATE,
COURSE_LAST_VISIT_DATE,
COURSE_REQOPT,
COURSE_DATE_COMPLETED,
DELIVERY_METHOD,
LEARNING_PROGRAM_ID,
LEARNING_PROGRAM,
LEARNING_PROGRAM_STATUS,
REQOPT,
DUE_DATE,
VALIDITY_IN_DAYS,
LEARNING_PROGRAM_DATE_COMPLETED,
TOTAL_TIME_IN_LEARNING_PROGRAM,
TOTAL_TIME_IN_LEARNING_PROGRAM_TO_COMPLETION,
REQUIRED_TIME_IN_LEARNING_PROGRAM,
EXEMPTION_EXPIRATION_DATE,
USER_STATUS,
USER_EMAIL,
SUPERVISOR_EMAIL,
COURSE_COMMENTS,
LEARNING_PROGRAM_COMMENTS,
SUPERVISOR_LEVEL_2_EMAIL,
LOAD_DT,
UPDATE_DT
FROM {legacy}.SKILLSOFT_ACM_LEARNING_PROGRAM""")


# COMMAND ----------

source_bucket=getParameterValue(raw,parameter_file_name,parameter_section,parameter_key)
source_file = get_src_file('ACM_Learning_Program', source_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_Report_ACM_Leanring_Program, type SOURCE 
# COLUMN COUNT: 34

SQ_Shortcut_to_Report_ACM_Leanring_Program = spark.read.csv(source_file, sep=',', header='true', quote='"').withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

new_columns = [col(c).alias(c.replace(" (hh:mm:ss)", "").strip().replace(" ", "_")) for c in SQ_Shortcut_to_Report_ACM_Leanring_Program.columns]
SQ_Shortcut_to_Report_ACM_Leanring_Program = SQ_Shortcut_to_Report_ACM_Leanring_Program.select(*new_columns)

SQ_Shortcut_to_Report_ACM_Leanring_Program = SQ_Shortcut_to_Report_ACM_Leanring_Program.withColumnRenamed("User_Specific_Data_2", "Account_Number")


# COMMAND ----------

# Processing node LKP_EXISTS, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 39


LKP_EXISTS_lookup_result = SQ_Shortcut_to_Report_ACM_Leanring_Program.selectExpr(
	"sys_row_id",
    "UserID as in_UserID",
	"CourseID as in_CourseID",
	"Learning_Program_ID as in_Learning_Program_ID").join(LKP_EXISTS_SRC, (col('USERID') == col('in_UserID')) & (col('COURSEID') == col('in_CourseID')) & (col('LEARNING_PROGRAM_ID') == col('in_Learning_Program_ID')), 'left') \
.withColumn('row_num_USERID', row_number().over(Window.partitionBy("sys_row_id").orderBy("USERID")))

LKP_EXISTS = LKP_EXISTS_lookup_result.filter(col("row_num_USERID") == 1).select(
	col('sys_row_id'),
	col('USERID'),
	col('COURSEID'),
	col('LEARNING_PROGRAM_ID')
)

# COMMAND ----------

# Processing node EXP_UPDDATE_FLAG, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 39

# for each involved DataFrame, append the dataframe name to each column
LKP_EXISTS_temp = LKP_EXISTS.toDF(*["LKP_EXISTS___" + col for col in LKP_EXISTS.columns])
SQ_Shortcut_to_Report_ACM_Leanring_Program_temp = SQ_Shortcut_to_Report_ACM_Leanring_Program.toDF(*["SQ_Shortcut_to_Report_ACM_Leanring_Program___" + col for col in SQ_Shortcut_to_Report_ACM_Leanring_Program.columns])

# Joining dataframes SQ_Shortcut_to_Report_ACM_Leanring_Program, LKP_EXISTS to form EXP_UPDDATE_FLAG
EXP_UPDDATE_FLAG_joined = SQ_Shortcut_to_Report_ACM_Leanring_Program_temp.join(LKP_EXISTS_temp, col("SQ_Shortcut_to_Report_ACM_Leanring_Program___sys_row_id") == col("LKP_EXISTS___sys_row_id"), 'inner')
EXP_UPDDATE_FLAG = EXP_UPDDATE_FLAG_joined.selectExpr(
    "SQ_Shortcut_to_Report_ACM_Leanring_Program___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___UserID as UserID",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___User_Name as User_Name",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Rehire_Date as Rehire_Date",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Account_Number as Account_Number",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___CourseID as CourseID",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Course_Title as Course_Title",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Skillsoft_Course_Number as Skillsoft_Course_Number",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Course_Status as Course_Status",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Score as Score",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Total_Time_in_Course as Total_Time_in_Course",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Total_Time_in_Course_to_Completion as Total_Time_in_Course_to_Completion",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Course_Order as Course_Order",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Course_Begin_Date as Course_Begin_Date",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Course_Last_Visit_Date as Course_Last_Visit_Date",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Course_ReqOpt as Course_ReqOpt",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Course_Date_Completed as Course_Date_Completed",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Delivery_Method as Delivery_Method",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Learning_Program_ID as Learning_Program_ID",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Learning_Program as Learning_Program",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Learning_Program_Status as Learning_Program_Status",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___ReqOpt as ReqOpt",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Due_Date as Due_Date",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Validity_In_Days as Validity_In_Days",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Learning_Program_Date_Completed as Learning_Program_Date_Completed",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Total_Time_in_Learning_Program as Total_Time_in_Learning_Program",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Total_Time_in_Learning_Program_to_Completion as Total_Time_in_Learning_Program_to_Completion",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Required_Time_In_Learning_Program as Required_Time_In_Learning_Program",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Exemption_Expiration_Date as Exemption_Expiration_Date",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___User_Status as User_Status",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___User_Email as User_Email",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Supervisor_Email as Supervisor_Email",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Course_Comments as Course_Comments",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Learning_Program_Comments as Learning_Program_Comments",
	"SQ_Shortcut_to_Report_ACM_Leanring_Program___Supervisor_Level_2_Email as Supervisor_Level_2_Email",
	"LKP_EXISTS___USERID as LKP_USERID",
	"LKP_EXISTS___COURSEID as LKP_COURSEID",
	"LKP_EXISTS___LEARNING_PROGRAM_ID as LKP_LEARNING_PROGRAM_ID").selectExpr(
	"sys_row_id as sys_row_id",
	"UserID as UserID",
	"User_Name as User_Name",
	"Rehire_Date as Rehire_Date",
	"Account_Number as Account_Number",
	"CourseID as CourseID",
	"Course_Title as Course_Title",
	"Skillsoft_Course_Number as Skillsoft_Course_Number",
	"Course_Status as Course_Status",
	"Score as Score",
	"Total_Time_in_Course as Total_Time_in_Course",
	"Total_Time_in_Course_to_Completion as Total_Time_in_Course_to_Completion",
	"Course_Order as Course_Order",
	"Course_Begin_Date as Course_Begin_Date",
	"Course_Last_Visit_Date as Course_Last_Visit_Date",
	"Course_ReqOpt as Course_ReqOpt",
	"Course_Date_Completed as Course_Date_Completed",
	"Delivery_Method as Delivery_Method",
	"Learning_Program_ID as Learning_Program_ID",
	"Learning_Program as Learning_Program",
	"Learning_Program_Status as Learning_Program_Status",
	"ReqOpt as ReqOpt",
	"Due_Date as Due_Date",
	"Validity_In_Days as Validity_In_Days",
	"Learning_Program_Date_Completed as Learning_Program_Date_Completed",
	"Total_Time_in_Learning_Program as Total_Time_in_Learning_Program",
	"Total_Time_in_Learning_Program_to_Completion as Total_Time_in_Learning_Program_to_Completion",
	"Required_Time_In_Learning_Program as Required_Time_In_Learning_Program",
	"Exemption_Expiration_Date as Exemption_Expiration_Date",
	"User_Status as User_Status",
	"User_Email as User_Email",
	"Supervisor_Email as Supervisor_Email",
	"Course_Comments as Course_Comments",
	"Learning_Program_Comments as Learning_Program_Comments",
	"Supervisor_Level_2_Email as Supervisor_Level_2_Email",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"LKP_USERID as LKP_USERID",
	"LKP_COURSEID as LKP_COURSEID",
	"LKP_LEARNING_PROGRAM_ID as LKP_LEARNING_PROGRAM_ID",
	"IF (( LKP_USERID IS NULL and LKP_COURSEID IS NULL and LKP_LEARNING_PROGRAM_ID IS NULL ), 1, 0) as UPDATE_FLAG"
)

# COMMAND ----------

# Processing node EXP_TO_DATE, type EXPRESSION 
# COLUMN COUNT: 13

EXP_TO_DATE = EXP_UPDDATE_FLAG.selectExpr(
	"sys_row_id as sys_row_id",
	"Rehire_Date as Rehire_Date",
	"IF (instr ( Rehire_Date , '-' ) > 0, to_date ( Rehire_Date , 'yyyy-MM-dd' ), to_date ( Rehire_Date , 'MM/dd/yyyy' )) as o_Rehire_Date",
	"Course_Begin_Date as Course_Begin_Date",
	"IF (instr ( Course_Begin_Date , '-' ) > 0, to_date ( Course_Begin_Date , 'yyyy-MM-DD' ), to_date ( Course_Begin_Date , 'MM/dd/yyyy' )) as o_Course_Begin_Date",
	"Course_Last_Visit_Date as Course_Last_Visit_Date",
	"IF (instr ( Course_Last_Visit_Date , '-' ) > 0, to_date ( Course_Last_Visit_Date , 'yyyy-MM-dd' ), to_date ( Course_Last_Visit_Date , 'MM/dd/yyyy' )) as o_Course_Last_Visit_Date",
	"Course_Date_Completed as Course_Date_Completed",
	"IF (instr ( Course_Date_Completed , '-' ) > 0, to_date ( Course_Date_Completed , 'yyyy-MM-dd' ), to_date ( Course_Date_Completed , 'MM/dd/yyyy' )) as o_Course_Date_Completed",
	"Due_Date as Due_Date",
	"IF (instr ( Due_Date , '-' ) > 0, to_date ( Due_Date , 'yyyy-MM-dd' ), to_date ( Due_Date , 'MM/dd/yyyy' )) as o_Due_Date",
	"Learning_Program_Date_Completed as Learning_Program_Date_Completed",
	"IF (instr ( Learning_Program_Date_Completed , '-' ) > 0, to_date ( Learning_Program_Date_Completed , 'yyyy-MM-dd' ), to_date ( Learning_Program_Date_Completed , 'MM/dd/yyyy' )) as o_Learning_Program_Date_Completed",
	"IF (instr ( Exemption_Expiration_Date , '-' ) > 0, to_date ( Exemption_Expiration_Date , 'yyyy-MM-dd' ), to_date ( Exemption_Expiration_Date , 'MM/dd/yyyy' )) as o_Exemption_Expiration_Date"
)

# COMMAND ----------

# Processing node UPD_INS, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 36


# for each involved DataFrame, append the dataframe name to each column
EXP_UPDDATE_FLAG_temp = EXP_UPDDATE_FLAG.toDF(*["EXP_UPDDATE_FLAG___" + col for col in EXP_UPDDATE_FLAG.columns])
EXP_TO_DATE_temp = EXP_TO_DATE.toDF(*["EXP_TO_DATE___" + col for col in EXP_TO_DATE.columns])

# Joining dataframes EXP_UPDDATE_FLAG, EXP_TO_DATE to form UPD_INS
UPD_INS_joined = EXP_UPDDATE_FLAG_temp.join(EXP_TO_DATE_temp, col("EXP_UPDDATE_FLAG___sys_row_id") == col("EXP_TO_DATE___sys_row_id"), 'inner')

UPD_INS = UPD_INS_joined.selectExpr(
	"EXP_UPDDATE_FLAG___UserID as UserID",
	"EXP_UPDDATE_FLAG___User_Name as User_Name",
	"EXP_TO_DATE___o_Rehire_Date as Rehire_Date",
	"EXP_UPDDATE_FLAG___Account_Number as Account_Number",
	"EXP_UPDDATE_FLAG___CourseID as CourseID",
	"EXP_UPDDATE_FLAG___Course_Title as Course_Title",
	"EXP_UPDDATE_FLAG___Skillsoft_Course_Number as Skillsoft_Course_Number",
	"EXP_UPDDATE_FLAG___Course_Status as Course_Status",
	"EXP_UPDDATE_FLAG___Score as Score",
	"EXP_UPDDATE_FLAG___Total_Time_in_Course as Total_Time_in_Course",
	"EXP_UPDDATE_FLAG___Total_Time_in_Course_to_Completion as Total_Time_in_Course_to_Completion",
	"EXP_UPDDATE_FLAG___Course_Order as Course_Order",
	"EXP_TO_DATE___o_Course_Begin_Date as Course_Begin_Date",
	"EXP_TO_DATE___o_Course_Last_Visit_Date as Course_Last_Visit_Date",
	"EXP_UPDDATE_FLAG___Course_ReqOpt as Course_ReqOpt",
	"EXP_TO_DATE___o_Course_Date_Completed as Course_Date_Completed",
	"EXP_UPDDATE_FLAG___Delivery_Method as Delivery_Method",
	"EXP_UPDDATE_FLAG___Learning_Program_ID as Learning_Program_ID",
	"EXP_UPDDATE_FLAG___Learning_Program as Learning_Program",
	"EXP_UPDDATE_FLAG___Learning_Program_Status as Learning_Program_Status",
	"EXP_UPDDATE_FLAG___ReqOpt as ReqOpt",
	"EXP_TO_DATE___o_Due_Date as Due_Date",
	"EXP_UPDDATE_FLAG___Validity_In_Days as Validity_In_Days",
	"EXP_TO_DATE___o_Learning_Program_Date_Completed as Learning_Program_Date_Completed",
	"EXP_UPDDATE_FLAG___Total_Time_in_Learning_Program as Total_Time_in_Learning_Program",
	"EXP_UPDDATE_FLAG___Total_Time_in_Learning_Program_to_Completion as Total_Time_in_Learning_Program_to_Completion",
	"EXP_UPDDATE_FLAG___Required_Time_In_Learning_Program as Required_Time_In_Learning_Program",
	"EXP_TO_DATE___o_Exemption_Expiration_Date as Exemption_Expiration_Date",
	"EXP_UPDDATE_FLAG___User_Status as User_Status",
	"EXP_UPDDATE_FLAG___User_Email as User_Email",
	"EXP_UPDDATE_FLAG___Supervisor_Email as Supervisor_Email",
	"EXP_UPDDATE_FLAG___Course_Comments as Course_Comments",
	"EXP_UPDDATE_FLAG___Learning_Program_Comments as Learning_Program_Comments",
	"EXP_UPDDATE_FLAG___Supervisor_Level_2_Email as Supervisor_Level_2_Email",
	"EXP_UPDDATE_FLAG___UPDATE_DT as UPDATE_DT",
	"EXP_UPDDATE_FLAG___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col("UPDATE_FLAG") ==(lit(1)), lit(0)).otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_SKILLSOFT_ACM_LEARNING_PROGRAM, type TARGET 
# COLUMN COUNT: 36


Shortcut_to_SKILLSOFT_ACM_LEARNING_PROGRAM = UPD_INS.selectExpr(
	"CAST(UserID AS BIGINT) as USERID",
	"CAST(User_Name AS STRING) as USER_NAME",
	"CAST(Rehire_Date AS TIMESTAMP) as REHIRE_DATE",
	"CAST(Account_Number AS STRING) as ACCOUNT_NUMBER",
	"CAST(CourseID AS STRING) as COURSEID",
	"CAST(Course_Title AS STRING) as COURSE_TITLE",
	"CAST(Skillsoft_Course_Number AS STRING) as SKILLSOFT_COURSE_NUMBER",
	"CAST(Course_Status AS STRING) as COURSE_STATUS",
	"CAST(Score AS STRING) as SCORE",
	"CAST(Total_Time_in_Course AS STRING) as TOTAL_TIME_IN_COURSE",
	"CAST(Total_Time_in_Course_to_Completion AS STRING) as TOTAL_TIME_IN_COURSE_TO_COMPLETION",
	"CAST(Course_Order AS STRING) as COURSE_ORDER",
	"CAST(Course_Begin_Date AS TIMESTAMP) as COURSE_BEGIN_DATE",
	"CAST(Course_Last_Visit_Date AS TIMESTAMP) as COURSE_LAST_VISIT_DATE",
	"CAST(Course_ReqOpt AS STRING) as COURSE_REQOPT",
	"CAST(Course_Date_Completed AS TIMESTAMP) as COURSE_DATE_COMPLETED",
	"CAST(Delivery_Method AS STRING) as DELIVERY_METHOD",
	"CAST(Learning_Program_ID AS STRING) as LEARNING_PROGRAM_ID",
	"CAST(Learning_Program AS STRING) as LEARNING_PROGRAM",
	"CAST(Learning_Program_Status AS STRING) as LEARNING_PROGRAM_STATUS",
	"CAST(ReqOpt AS STRING) as REQOPT",
	"CAST(Due_Date AS TIMESTAMP) as DUE_DATE",
	"CAST(Validity_In_Days AS BIGINT) as VALIDITY_IN_DAYS",
	"CAST(Learning_Program_Date_Completed AS TIMESTAMP) as LEARNING_PROGRAM_DATE_COMPLETED",
	"CAST(Total_Time_in_Learning_Program AS STRING) as TOTAL_TIME_IN_LEARNING_PROGRAM",
	"CAST(Total_Time_in_Learning_Program_to_Completion AS STRING) as TOTAL_TIME_IN_LEARNING_PROGRAM_TO_COMPLETION",
	"CAST(Required_Time_In_Learning_Program AS STRING) as REQUIRED_TIME_IN_LEARNING_PROGRAM",
	"CAST(Exemption_Expiration_Date AS TIMESTAMP) as EXEMPTION_EXPIRATION_DATE",
	"CAST(User_Status AS STRING) as USER_STATUS",
	"CAST(User_Email AS STRING) as USER_EMAIL",
	"CAST(Supervisor_Email AS STRING) as SUPERVISOR_EMAIL",
	"CAST(Course_Comments AS STRING) as COURSE_COMMENTS",
	"CAST(Learning_Program_Comments AS STRING) as LEARNING_PROGRAM_COMMENTS",
	"CAST(Supervisor_Level_2_Email AS STRING) as SUPERVISOR_LEVEL_2_EMAIL",
	"CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as LOAD_DT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"pyspark_data_action as pyspark_data_action"
)


		

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

try:
	primary_key = """source.USERID = target.USERID AND source.COURSEID = target.COURSEID AND source.LEARNING_PROGRAM_ID = target.LEARNING_PROGRAM_ID"""
	refined_perf_table = f"{legacy}.SKILLSOFT_ACM_LEARNING_PROGRAM"

	if has_duplicates(Shortcut_to_SKILLSOFT_ACM_LEARNING_PROGRAM, ["USERID", "COURSEID", "LEARNING_PROGRAM_ID"]):
		raise Exception("Duplicates found in the dataset")

	executeMerge(Shortcut_to_SKILLSOFT_ACM_LEARNING_PROGRAM, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("SKILLSOFT_ACM_LEARNING_PROGRAM", "SKILLSOFT_ACM_LEARNING_PROGRAM", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("SKILLSOFT_ACM_LEARNING_PROGRAM", "SKILLSOFT_ACM_LEARNING_PROGRAM","Failed",str(e), f"{raw}.log_run_details", )
	raise e

# COMMAND ----------


