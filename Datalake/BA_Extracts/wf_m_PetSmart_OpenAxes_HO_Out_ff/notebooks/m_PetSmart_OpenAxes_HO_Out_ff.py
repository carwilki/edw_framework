# Databricks notebook source
#Code converted on 2023-11-24 14:29:54
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
sensitive = getEnvPrefix(env) + 'empl_sensitive'
protected = getEnvPrefix(env) + 'empl_protected'

# Set global variables
starttime = datetime.now() #start timestamp of the script
current_date = datetime.today().strftime('%Y%m%d')
 
target_bucket=getParameterValue(raw,'wf_m_PetSmart_OpenAxes_HO_Out_ff','m_PetSmart_OpenAxes_HO_Out_ff','target_bucket')
target_bucket=target_bucket + str(current_date) + f'/'
key=getParameterValue(raw,'wf_m_PetSmart_OpenAxes_HO_Out_ff','m_PetSmart_OpenAxes_HO_Out_ff','key')
target_file=target_bucket + key
 
nas_target_path=getParameterValue(raw,'wf_m_PetSmart_OpenAxes_HO_Out_ff','m_PetSmart_OpenAxes_HO_Out_ff','nas_target_path')
nas_target_path=nas_target_path + str(current_date) + f'/'

# COMMAND ----------

# Processing node SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 18

SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT = spark.sql(f"""SELECT DISTINCT EMPL_LAST_NAME,

  EMPL_FIRST_NAME,

  ASSOC_ID,

  EMPL_EMAIL_ADDR,

  PS_PERSONNEL_AREA_DESC,

  PS_PERSONNEL_AREA_ID,

  STORENBR,

  STORE_NAME,

  STORE_STATE,

  STORE_CTRY,

  STATUS,

  JOB_CODE_DESC,

  PS_PERSONNEL_SUBAREA_DESC,

  STORE_DEPT_DESC,

  SUPERVISOR,

  EMPL_HIRE_DT,

  EMPL_TERM_DT,

  EMPL_REHIRE_DT

FROM

  (SELECT a11.EMPLOYEE_ID EMPLOYEE_ID,

    MAX(TRIM(TRIM(NVL(a11.EMPL_FIRST_NAME,''))

    ||' '

    ||TRIM(NVL(a11.EMPL_LAST_NAME,'')))) CustCol_274,

    MAX(

    CASE

      WHEN LENGTH(a11.EMPLOYEE_ID) < 6

      THEN LPAD(a11.EMPLOYEE_ID, 6, '0')

      ELSE CAST(a11.EMPLOYEE_ID AS VARCHAR(8))

    END) ASSOC_ID,

    a11.EMPL_LAST_NAME EMPL_LAST_NAME,

    a11.PS_PERSONNEL_AREA_ID PS_PERSONNEL_AREA_ID,

    MAX(a11.PS_PERSONNEL_AREA_DESC) PS_PERSONNEL_AREA_DESC,

    a11.EMPL_FIRST_NAME EMPL_FIRST_NAME,

    a11.EMPL_EMAIL_ADDR EMPL_EMAIL_ADDR,

    a12.STORE_DEPT_NBR Store_Dept_Nbr1,

    MAX(rtrim(a15.STORE_DEPT_DESC)) STORE_DEPT_DESC,

    a11.JOB_CODE JOB_CODE,

    MAX(rtrim(a14.JOB_CODE_DESC)) JOB_CODE_DESC,

    a11.STORE_NBR STORENBR,

    MAX(rtrim(a13.STORE_NAME)) STORE_NAME,

    MAX(SUBSTR(a13.STORE_NAME,1,6)) CustCol_23,

    MAX(a13.LOCATION_NBR) LOCATION_NBR,

    a11.PS_PERSONNEL_SUBAREA_ID PS_PERSONNEL_SUBAREA_ID,

    MAX(a11.PS_PERSONNEL_SUBAREA_DESC) PS_PERSONNEL_SUBAREA_DESC,

    rtrim(a13.COUNTRY_CD) STORE_CTRY,

    a11.PS_SUPERVISOR_ID PS_SUPERVISOR_ID,

    MAX(TRIM(TRIM(a16.EMPL_FIRST_NAME)

    ||' '

    ||TRIM(a16.EMPL_LAST_NAME)) ) SUPERVISOR,

    a11.EMPL_HIRE_DT EMPL_HIRE_DT1,

    a11.EMPL_TERM_DT EMPL_TERM_DT,

    a11.EMPL_REHIRE_DT EMPL_REHIRE_DT,

    a13.STATE_CD STORE_STATE,

    a11.EMPL_STATUS_CD EMPL_STATUS_CD,

    MAX(a11.EMPL_STATUS_DESC) STATUS,

    MAX(a11.EMPL_HIRE_DT) EMPL_HIRE_DT

  FROM {protected}.legacy_employee_profile_rpt a11

  JOIN {legacy}.EMPL_LABOR_WK_VW a12

  ON (a11.LOCATION_ID = a12.LOCATION_ID)

  JOIN {legacy}.SITE_PROFILE_RPT a13

  ON (a11.LOCATION_ID = a13.LOCATION_ID)

  JOIN {legacy}.JOB_CODE a14

  ON (a11.JOB_CODE = a14.JOB_CODE)

  JOIN {legacy}.STORE_DEPT a15

  ON (a11.STORE_DEPT_NBR = a15.STORE_DEPT_NBR

  AND a12.STORE_DEPT_NBR = a15.STORE_DEPT_NBR)

  JOIN {protected}.legacy_employee_profile_rpt a16

  ON (a11.PS_SUPERVISOR_ID        = a16.EMPLOYEE_ID)

  WHERE a11.PS_PERSONNEL_AREA_ID IN ('1020', '1030', '1130', '2030')

  GROUP BY a11.EMPLOYEE_ID,

    a11.EMPL_LAST_NAME,

    a11.PS_PERSONNEL_AREA_ID,

    a11.EMPL_FIRST_NAME,

    a11.EMPL_EMAIL_ADDR,

    a12.STORE_DEPT_NBR,

    a11.JOB_CODE,

    a11.STORE_NBR,

    a11.PS_PERSONNEL_SUBAREA_ID,

    rtrim(a13.COUNTRY_CD),

    a11.PS_SUPERVISOR_ID,

    a11.EMPL_HIRE_DT,

    a11.EMPL_TERM_DT,

    a11.EMPL_REHIRE_DT,

    a13.STATE_CD,

    a11.EMPL_STATUS_CD

  )A""").withColumn("sys_row_id", monotonically_increasing_id())
#{empl_protected}.legacy_EMPLOYEE_PROFILE_RPT
# Conforming fields names to the component layout
SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT = SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[0],'EMPL_LAST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[1],'EMPL_FIRST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[2],'ASSOC_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[3],'EMPL_EMAIL_ADDR') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[4],'PS_PERSONNEL_AREA_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[5],'PS_PERSONNEL_AREA_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[6],'STORENBR') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[7],'STORE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[8],'STORE_STATE') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[9],'STORE_CTRY') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[10],'EMPL_STATUS') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[11],'JOB_CODE_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[12],'PS_PERSONNEL_SUBAREA_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[13],'STORE_DEPT_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[14],'SUPERVISOR') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[15],'EMPL_HIRE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[16],'EMPL_TERM_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.columns[17],'EMPL_REHIRE_DT')

# COMMAND ----------

# SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.show()

# COMMAND ----------

# Processing node PetSmart_OpenAxes_HOOut_ff, type TARGET 
# COLUMN COUNT: 18


PetSmart_OpenAxes_HOOut_ff = SQ_Shortcut_to_EMPLOYEE_PROFILE_RPT.selectExpr(
	"CAST(EMPL_LAST_NAME AS STRING) as EMPL_LAST_NAME",
	"CAST(EMPL_FIRST_NAME AS STRING) as EMPL_FIRST_NAME",
	"CAST(ASSOC_ID as int) as EMPLOYEE_ID",
	"CAST(EMPL_EMAIL_ADDR AS STRING) as EMPL_EMAIL_ADDR",
	"CAST(PS_PERSONNEL_AREA_DESC AS STRING) as PS_PERSONNEL_AREA_DESC",
	"CAST(PS_PERSONNEL_AREA_ID AS STRING) as PS_PERSONNEL_AREA_ID",
	"CAST(STORENBR as int)as STORE_NBR",
	"CAST(STORE_NAME AS STRING) as STORE_NAME",
	"CAST(STORE_STATE AS STRING) as STORE_STATE",
	"CAST(STORE_CTRY AS STRING) as STORE_CTRY",
	"CAST(EMPL_STATUS AS STRING) as EMPL_STATUS",
	"CAST(JOB_CODE_DESC AS STRING) as JOB_CODE_DESC",
	"CAST(PS_PERSONNEL_SUBAREA_DESC AS STRING) as PS_PERSONNEL_SUBAREA_DESC",
	"CAST(STORE_DEPT_DESC AS STRING) as STORE_DEPT_DESC",
	"CAST(SUPERVISOR AS STRING) as Supervisor",
    "date_format(EMPL_HIRE_DT,'MM/dd/yyyy HH:mm:ss') as EMPL_HIRE_DT",
    "date_format(EMPL_TERM_DT,'MM/dd/yyyy HH:mm:ss')as EMPL_TERM_DT",
    "date_format(EMPL_REHIRE_DT,'MM/dd/yyyy HH:mm:ss') as EMPL_REHIRE_DT"
)

#filename ='PetSmart_OpenAxes_HOOut.TXT'
# PetSmart_OpenAxes_HOOut_ff.write.format('csv').option('header',True).option('sep','|').mode('overwrite').csv(filename)

# COMMAND ----------

#writeToFlatFile(PetSmart_OpenAxes_HOOut_ff, target_bucket, filename, 'overwrite')
try:
    PetSmart_OpenAxes_HOOut_ff.repartition(1).write.mode('overwrite').options(header='True', delimiter='|').csv(target_bucket.strip("/") + "/" + key[:-4])
    removeTransactionFiles(target_bucket.strip("/") + "/" + key[:-4])
    newFilePath = target_bucket.strip("/") + "/" + key[:-4]
    renamePartFileNames(newFilePath, newFilePath,'.TXT')
    copy_file_to_nas(target_file ,nas_target_path)
    logPrevRunDt("wf_PetSmart_OpenAxes_HOOut_ff", "m_PetSmart_OpenAxes_HOOut_ff", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
    logPrevRunDt("wf_PetSmart_OpenAxes_HOOut_ff", "m_PetSmart_OpenAxes_HOOut_ff","Failed",str(e), f"{raw}.log_run_details", )
    raise e
