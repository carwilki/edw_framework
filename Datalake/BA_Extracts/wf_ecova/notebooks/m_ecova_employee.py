# Databricks notebook source
# Code converted on 2023-11-09 07:57:36
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
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
empl_protected = getEnvPrefix(env) + 'empl_protected'


# COMMAND ----------

# Processing node EMPL_PROFILE, type SOURCE 
# COLUMN COUNT: 7

EMPL_PROFILE = spark.sql(f"""SELECT
EMPLOYEE_PROFILE.EMPLOYEE_ID,
EMPLOYEE_PROFILE.PS_DEPT_CD,
EMPLOYEE_PROFILE.STORE_NBR,
EMPLOYEE_PROFILE.EMPL_FIRST_NAME,
EMPLOYEE_PROFILE.EMPL_LAST_NAME,
SITE_PROFILE.STORE_NAME,
RTRIM(SITE_PROFILE.STORE_TYPE_ID) as STORE_TYPE_ID
FROM {empl_protected}.LEGACY_EMPLOYEE_PROFILE EMPLOYEE_PROFILE, {legacy}.SITE_PROFILE
WHERE EMPLOYEE_PROFILE.STORE_NBR = SITE_PROFILE.STORE_NBR
AND EMPLOYEE_PROFILE.EMPL_STATUS_CD in ('A','L')""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Department, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 25

# for each involved DataFrame, append the dataframe name to each column
EMPL_PROFILE_temp = EMPL_PROFILE.toDF(*["EMPL_PROFILE___" + col for col in EMPL_PROFILE.columns])

EXP_Department = EMPL_PROFILE_temp.selectExpr(
	# "EMPL_PROFILE___EMPLOYEE_ID as EMPLOYEE_ID",
	# "EMPL_PROFILE___PS_DEPT_CD as PS_DEPT_CD",
	# "EMPL_PROFILE___STORE_NBR as i_STORE_NBR",
	# "EMPL_PROFILE___EMPL_FIRST_NAME as FIRST_NAME",
	# "EMPL_PROFILE___EMPL_LAST_NAME as LAST_NAME",
	# "EMPL_PROFILE___STORE_NAME as i_STORE_NAME",
	# "EMPL_PROFILE___STORE_TYPE_ID as i_STORE_TYPE_ID").selectExpr(
	# "EMPL_PROFILE___sys_row_id as sys_row_id",
	"EMPL_PROFILE___EMPLOYEE_ID as EMPLOYEE_ID",
	"EMPL_PROFILE___PS_DEPT_CD as PS_DEPT_CD",
	"EMPL_PROFILE___STORE_NBR as i_STORE_NBR",
	"EMPL_PROFILE___EMPL_FIRST_NAME as FIRST_NAME",
	"EMPL_PROFILE___EMPL_LAST_NAME as LAST_NAME",
	"NULL as TITLE",
	"concat(DECODE ( RTRIM ( EMPL_PROFILE___STORE_TYPE_ID ) , '100' , concat ( '38' , LPAD ( EMPL_PROFILE___STORE_NBR , 2 , '0' ) ) , LPAD ( EMPL_PROFILE___STORE_NBR , 4 , '0' ) ) , '-' , EMPL_PROFILE___STORE_NAME ) as DEPARTMENT_NAME",
	"DECODE ( RTRIM ( EMPL_PROFILE___STORE_TYPE_ID ) , '100' ,concat( '38' , LPAD ( EMPL_PROFILE___STORE_NBR , 2 , '0' ) ), LPAD ( EMPL_PROFILE___STORE_NBR , 4 , '0' ) ) as DEPARTMENT_ID",
	"concat(SUBSTR ( EMPL_PROFILE___EMPL_FIRST_NAME , 1 , 1 ) , EMPL_PROFILE___EMPL_LAST_NAME , '@ssg.petsmart.com' ) as EMAIL_ADDRESS",
	"NULL as DATE_HIRED",
	"NULL as TERMINATION_DATE",
	"NULL as STATUS_ID",
	"NULL as TYPE",
	"NULL as NOTES",
	"NULL as MGR_EMPLOYEE_ID",
	"concat(SUBSTR ( EMPL_PROFILE___EMPL_FIRST_NAME , 1 , 1 ) , EMPL_PROFILE___EMPL_LAST_NAME ) as LOGIN",
	"NULL as OFFICE_PHONE",
	"NULL as ADDRESS_LINE_1",
	"NULL as ADDRESS_LINE_2",
	"NULL as ADDRESS_LINE_3",
	"NULL as ADDRESS_CITY",
	"NULL as ADDRESS_STATE",
	"NULL as ADDRESS_COUNTRY",
	"NULL as ADDRESS_POSTAL_CODE",
	"NULL as PERMISSION"
)


# COMMAND ----------

current_date = datetime.today().strftime('%Y%m%d')
sub_folder="pet_empl"
target_bucket=getParameterValue(raw,'wf_ecova','m_ecova_employee','target_bucket')
key=getParameterValue(raw,'wf_ecova','m_ecova_employee','key')

key = key[:-4]+ f'_' + str(current_date) + key[-4:]
target_bucket=target_bucket+sub_folder+ f'/' + str(current_date) + f'/' 

target_file=target_bucket + key

nas_target_path=getParameterValue(raw,'wf_ecova','m_ecova_employee','nas_target_path')
nas_target_path=nas_target_path + sub_folder + '\\'

# COMMAND ----------

# Processing node Shortcut_to_ECOVA_EMPLOYEE, type TARGET 
# COLUMN COUNT: 23


Shortcut_to_ECOVA_EMPLOYEE = EXP_Department.selectExpr(
	"EMPLOYEE_ID as EMPLOYEE_ID",
	"CAST(FIRST_NAME AS STRING) as FIRST_NAME",
	"CAST(LAST_NAME AS STRING) as LAST_NAME",
	"CAST(TITLE AS STRING) as TITLE",
	"CAST(DEPARTMENT_NAME AS STRING) as DEPARTMENT_NAME",
	"CAST(DEPARTMENT_ID AS STRING) as DEPARTMENT_ID",
	"CAST(EMAIL_ADDRESS AS STRING) as EMAIL_ADDRESS",
	"CAST(DATE_HIRED AS STRING) as DATE_HIRED",
	"CAST(TERMINATION_DATE AS STRING) as TERMINATION_DATE",
	"CAST(STATUS_ID AS STRING) as STATUS_ID",
	"CAST(TYPE AS STRING) as TYPE",
	"CAST(NOTES AS STRING) as NOTES",
	"CAST(MGR_EMPLOYEE_ID AS STRING) as MGR_EMPLOYEE_ID",
	"CAST(LOGIN AS STRING) as LOGIN",
	"CAST(OFFICE_PHONE AS STRING) as OFFICE_PHONE",
	"CAST(ADDRESS_LINE_1 AS STRING) as ADDRESS_LINE_1",
	"CAST(ADDRESS_LINE_2 AS STRING) as ADDRESS_LINE_2",
	"CAST(ADDRESS_LINE_3 AS STRING) as ADDRESS_LINE_3",
	"CAST(ADDRESS_CITY AS STRING) as ADDRESS_CITY",
	"CAST(ADDRESS_STATE AS STRING) as ADDRESS_STATE",
	"CAST(ADDRESS_COUNTRY AS STRING) as ADDRESS_COUNTRY",
	"CAST(ADDRESS_POSTAL_CODE AS STRING) as ADDRESS_POSTAL_CODE",
	"CAST(PERMISSION AS STRING) as PERMISSION"
)

# COMMAND ----------

Shortcut_to_ECOVA_EMPLOYEE=Shortcut_to_ECOVA_EMPLOYEE.withColumnRenamed("PERMISSION","PERMISSION ")
cols = Shortcut_to_ECOVA_EMPLOYEE.columns
cols =[col.lower() for col in cols ]
Shortcut_to_ECOVA_EMPLOYEE=Shortcut_to_ECOVA_EMPLOYEE.toDF(*cols)



try:
	Shortcut_to_ECOVA_EMPLOYEE.repartition(1).write.mode('overwrite').option("ignoreLeadingWhiteSpace","false").option("ignoreTrailingWhiteSpace", "false").options(header='True', delimiter='|').csv(target_bucket.strip("/") + "/" + key[:-4])
	removeTransactionFiles(target_bucket.strip("/") + "/" + key[:-4])
	newFilePath = target_bucket.strip("/") + "/" + key[:-4]
	renamePartFileNames(newFilePath, newFilePath,'.txt')
	copy_file_to_nas(target_file, nas_target_path)
	logPrevRunDt("wf_ecova", "ecova_employee", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("wf_ecova", "ecova_employee","Failed",str(e), f"{raw}.log_run_details", )
	raise e

