# Databricks notebook source
#Code converted on 2023-09-22 13:59:03
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

def fileExists (pfile):
  try:
    data = dbutils.fs.head(pfile,1)
    if data == '':
      return False
  except:
    print(f"{pfile} doesn't exist")
    return False
  else:
    print(f'FILE {pfile} EXISTS  ')
    return True

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
	raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'
sensitive = getEnvPrefix(env) + 'empl_sensitive'
# Set global variables
starttime = datetime.now() #start timestamp of the script

# dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-qa-empl-sensitive-raw-p1-gcs-gbl/nas/employee/lifetime_discount/')
# file_path = dbutils.widgets.get('file_path')

source_bucket = getParameterValue(
    raw, "BA_HCM_Parameter.prm", "BA_HCM.WF:wf_Honorary_Designee", "source_bucket"
)
source_file = get_source_file(source_bucket)

print(source_file)

# COMMAND ----------

if not fileExists(f'{file_path}/Honorary_Designee_File.csv'):
  dbutils.notebook.exit(f'{file_path}/Honorary_Designee_File.csv not available')

# COMMAND ----------

# Processing node SQ_Shortcut_to_Honorary_Designee_File, type SOURCE 
# COLUMN COUNT: 16

SQ_Shortcut_to_Honorary_Designee_File = spark.read.option('header',True).option('inferSchema',True).csv(f'{file_path}/Honorary_Designee_File.csv').withColumn("sys_row_id_fl", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_Honorary_Designee_File = SQ_Shortcut_to_Honorary_Designee_File \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[0],'HD_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[1],'ID') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[2],'FIRST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[3],'LAST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[4],'SUFFIX') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[5],'BIRTH_MON') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[6],'BIRTH_DAY') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[7],'SPOUSE_FIRST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[8],'SPOUSE_LAST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[9],'ADDRESS') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[10],'CITY') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[11],'STATE') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[12],'ZIP5_CD') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[13],'COUNTRY') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[14],'PHONE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_Honorary_Designee_File.columns[15],'EXPIRATION_DT')
#  \
# 	.filter("DATE(EXPIRATION_DT) IS NOT NULL")

# COMMAND ----------

# Processing node SQ_Shortcut_to_HONORARY_DESIGNEE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_HONORARY_DESIGNEE = spark.sql(f"""SELECT
HD_ID,
HD_TYPE_CD,
HD_FIRST_NAME,
HD_LAST_NAME,
HD_SUFFIX,
HD_BIRTH_MON,
HD_BIRTH_DAY,
HD_SPOUSE_FIRST_NAME,
HD_SPOUSE_LAST_NAME,
HD_ADDRESS,
HD_CITY,
HD_STATE,
HD_ZIP_CD,
HD_COUNTRY,
HD_PHONE_NBR,
HD_EXP_DT,
LOAD_TSTMP
FROM {sensitive}.refine_HONORARY_DESIGNEE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_HONORARY_DESIGNEE = SQ_Shortcut_to_HONORARY_DESIGNEE \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[0],'HD_ID') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[1],'HD_TYPE_CD') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[2],'HD_FIRST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[3],'HD_LAST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[4],'HD_SUFFIX') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[5],'HD_BIRTH_MON') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[6],'HD_BIRTH_DAY') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[7],'HD_SPOUSE_FIRST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[8],'HD_SPOUSE_LAST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[9],'HD_ADDRESS') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[10],'HD_CITY') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[11],'HD_STATE') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[12],'HD_ZIP_CD') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[13],'HD_COUNTRY') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[14],'HD_PHONE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[15],'HD_EXP_DT') \
	.withColumnRenamed(SQ_Shortcut_to_HONORARY_DESIGNEE.columns[16],'LOAD_TSTMP')
 
# SQ_Shortcut_to_HONORARY_DESIGNEE.show()

# COMMAND ----------

# Processing node JNR_FF_HONORARY_DESIGNEE, type JOINER 
# COLUMN COUNT: 33

JNR_FF_HONORARY_DESIGNEE = SQ_Shortcut_to_Honorary_Designee_File.join(SQ_Shortcut_to_HONORARY_DESIGNEE,[SQ_Shortcut_to_Honorary_Designee_File.ID == SQ_Shortcut_to_HONORARY_DESIGNEE.HD_ID],'fullouter')

# COMMAND ----------

# Processing node EXP_Change_Flag, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
JNR_FF_HONORARY_DESIGNEE_temp = JNR_FF_HONORARY_DESIGNEE.toDF(*["JNR_FF_HONORARY_DESIGNEE___" + col for col in JNR_FF_HONORARY_DESIGNEE.columns])

EXP_Change_Flag = JNR_FF_HONORARY_DESIGNEE_temp.selectExpr(
	"JNR_FF_HONORARY_DESIGNEE___HD_TYPE as i_HD_TYPE",
	"JNR_FF_HONORARY_DESIGNEE___ID as i_ID",
	"JNR_FF_HONORARY_DESIGNEE___FIRST_NAME as i_FIRST_NAME",
	"JNR_FF_HONORARY_DESIGNEE___LAST_NAME as i_LAST_NAME",
	"JNR_FF_HONORARY_DESIGNEE___SUFFIX as i_SUFFIX",
	"JNR_FF_HONORARY_DESIGNEE___BIRTH_MON as i_BIRTH_MON",
	"JNR_FF_HONORARY_DESIGNEE___BIRTH_DAY as i_BIRTH_DAY",
	"JNR_FF_HONORARY_DESIGNEE___SPOUSE_FIRST_NAME as i_SPOUSE_FIRST_NAME",
	"JNR_FF_HONORARY_DESIGNEE___SPOUSE_LAST_NAME as i_SPOUSE_LAST_NAME",
	"JNR_FF_HONORARY_DESIGNEE___ADDRESS as i_ADDRESS",
	"JNR_FF_HONORARY_DESIGNEE___CITY as i_CITY",
	"JNR_FF_HONORARY_DESIGNEE___STATE as i_STATE",
	"JNR_FF_HONORARY_DESIGNEE___ZIP5_CD as i_ZIP5_CD",
	"JNR_FF_HONORARY_DESIGNEE___COUNTRY as i_COUNTRY",
	"JNR_FF_HONORARY_DESIGNEE___PHONE_NBR as i_PHONE_NBR",
	"JNR_FF_HONORARY_DESIGNEE___EXPIRATION_DT as i_EXPIRATION_DT",
	"JNR_FF_HONORARY_DESIGNEE___HD_ID as i_HD_ID",
	"JNR_FF_HONORARY_DESIGNEE___HD_TYPE_CD as i_HD_TYPE_CD",
	"JNR_FF_HONORARY_DESIGNEE___HD_FIRST_NAME as i_HD_FIRST_NAME",
	"JNR_FF_HONORARY_DESIGNEE___HD_LAST_NAME as i_HD_LAST_NAME",
	"JNR_FF_HONORARY_DESIGNEE___HD_SUFFIX as i_HD_SUFFIX",
	"JNR_FF_HONORARY_DESIGNEE___HD_BIRTH_MON as i_HD_BIRTH_MON",
	"JNR_FF_HONORARY_DESIGNEE___HD_BIRTH_DAY as i_HD_BIRTH_DAY",
	"JNR_FF_HONORARY_DESIGNEE___HD_SPOUSE_FIRST_NAME as i_HD_SPOUSE_FIRST_NAME",
	"JNR_FF_HONORARY_DESIGNEE___HD_SPOUSE_LAST_NAME as i_HD_SPOUSE_LAST_NAME",
	"JNR_FF_HONORARY_DESIGNEE___HD_ADDRESS as i_HD_ADDRESS",
	"JNR_FF_HONORARY_DESIGNEE___HD_CITY as i_HD_CITY",
	"JNR_FF_HONORARY_DESIGNEE___HD_STATE as i_HD_STATE",
	"JNR_FF_HONORARY_DESIGNEE___HD_ZIP_CD as i_HD_ZIP_CD",
	"JNR_FF_HONORARY_DESIGNEE___HD_COUNTRY as i_HD_COUNTRY",
	"JNR_FF_HONORARY_DESIGNEE___HD_PHONE_NBR as i_HD_PHONE_NBR",
	"JNR_FF_HONORARY_DESIGNEE___HD_EXP_DT as i_HD_EXP_DT",
	"JNR_FF_HONORARY_DESIGNEE___LOAD_TSTMP as i_HD_LOAD_TSTMP",
  "JNR_FF_HONORARY_DESIGNEE___sys_row_id as sys_row_id").selectExpr(
	"sys_row_id as sys_row_id",
	"i_HD_ID as i_HD_ID",
	"CURRENT_TIMESTAMP as i_UPDATE_TSTMP",
	"IF (i_ID IS NULL, i_HD_ID, i_ID) as ID",
	"IF (i_HD_ID IS NULL, CURRENT_TIMESTAMP, i_HD_LOAD_TSTMP) as LOAD_TSTMP",
	"IF (i_ID IS NULL AND i_HD_EXP_DT > CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TO_DATE ( i_EXPIRATION_DT , 'M/d/yyyy' )) as EXPIRATION_DT",
	# "IF (i_ID IS NULL AND i_HD_EXP_DT > CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, current_date) as EXPIRATION_DT_FAKE",
	"IF (i_ID IS NULL, i_HD_ZIP_CD, IF (i_COUNTRY = 'US', LPAD ( i_ZIP5_CD , 5 , '0' ), i_ZIP5_CD)) as ZIP5_CD",
	"IF (i_ID IS NULL, i_HD_TYPE_CD, i_HD_TYPE) as HD_TYPE_CD",
	"IF (i_ID IS NULL, i_HD_FIRST_NAME, i_FIRST_NAME) as HD_FIRST_NAME",
	"IF (i_ID IS NULL, i_HD_LAST_NAME, i_LAST_NAME) as HD_LAST_NAME",
	"IF (i_ID IS NULL, i_HD_SUFFIX, i_SUFFIX) as HD_SUFFIX",
	"IF (i_ID IS NULL, i_HD_BIRTH_MON, i_BIRTH_MON) as HD_BIRTH_MON",
	"IF (i_ID IS NULL, i_HD_BIRTH_DAY, i_BIRTH_DAY) as HD_BIRTH_DAY",
	"IF (i_ID IS NULL, i_HD_SPOUSE_FIRST_NAME, i_SPOUSE_FIRST_NAME) as HD_SPOUSE_FIRST_NAME",
	"IF (i_ID IS NULL, i_HD_SPOUSE_LAST_NAME, i_SPOUSE_LAST_NAME) as HD_SPOUSE_LAST_NAME",
	"IF (i_ID IS NULL, i_HD_ADDRESS, i_ADDRESS) as HD_ADDRESS",
	"IF (i_ID IS NULL, i_HD_CITY, i_CITY) as HD_CITY",
	"IF (i_ID IS NULL, i_HD_STATE, i_STATE) as HD_STATE",
	"IF (i_ID IS NULL, i_HD_COUNTRY, i_COUNTRY) as HD_COUNTRY",
	"IF (i_ID IS NULL, i_HD_PHONE_NBR, IF (i_PHONE_NBR = 0, NULL, i_PHONE_NBR)) as HD_PHONE_NBR",
	"IF (i_ID IS NULL and i_HD_EXP_DT > CURRENT_TIMESTAMP AND date_diff(i_HD_EXP_DT , TO_DATE ( '12/31/9999' , 'M/d/yyyy' )) = 0, 1, 0) as REMOVED_FLAG",
	"IF (i_HD_TYPE = 'R', 'Retiree', IF (i_HD_TYPE = 'B', 'Board Member', IF (i_HD_TYPE = 'O', 'Other', NULL))) as HD_TYPE_DESC",
	"IF (i_HD_ID IS NOT NULL AND i_ID IS NOT NULL AND \
 				( IF (i_HD_TYPE IS NULL, '.', i_HD_TYPE) <> IF (i_HD_TYPE_CD IS NULL, '.', i_HD_TYPE_CD) OR \
       	IF (i_FIRST_NAME IS NULL, 'xxNULLxx', i_FIRST_NAME) <> IF (i_HD_FIRST_NAME IS NULL, 'xxNULLxx', i_HD_FIRST_NAME) OR \
      	IF (i_LAST_NAME IS NULL, 'xxNULLxx', i_LAST_NAME) <> IF (i_HD_LAST_NAME IS NULL, 'xxNULLxx', i_HD_LAST_NAME) OR \
        IF (i_SUFFIX IS NULL, 'xxNULLxx', i_SUFFIX) <> IF (i_HD_SUFFIX IS NULL, 'xxNULLxx', i_HD_SUFFIX) OR \
        IF (i_BIRTH_MON IS NULL, 0, i_BIRTH_MON) <> IF (i_HD_BIRTH_MON IS NULL, 0, i_HD_BIRTH_MON) OR \
        IF (i_BIRTH_DAY IS NULL, 0, i_BIRTH_DAY) <> IF (i_HD_BIRTH_DAY IS NULL, 0, i_HD_BIRTH_DAY) OR \
        IF (i_SPOUSE_FIRST_NAME IS NULL, 'xxNULLxx', i_SPOUSE_FIRST_NAME) <> IF (i_HD_SPOUSE_FIRST_NAME IS NULL, 'xxNULLxx', i_HD_SPOUSE_FIRST_NAME) OR \
        IF (i_SPOUSE_LAST_NAME IS NULL, 'xxNULLxx', i_SPOUSE_LAST_NAME) <> IF (i_HD_SPOUSE_LAST_NAME IS NULL, 'xxNULLxx', i_HD_SPOUSE_LAST_NAME) OR \
        IF (i_ADDRESS IS NULL, 'xxNULLxx', i_ADDRESS) <> IF (i_HD_ADDRESS IS NULL, 'xxNULLxx', i_HD_ADDRESS) OR \
        IF (i_CITY IS NULL, 'xxNULLxx', i_CITY) <> IF (i_HD_CITY IS NULL, 'xxNULLxx', i_HD_CITY) OR \
        IF (i_STATE IS NULL, 'xxNULLxx', i_STATE) <> IF (i_HD_STATE IS NULL, 'xxNULLxx', i_HD_STATE) OR \
        IF (IF (i_ID IS NULL, i_HD_ZIP_CD, IF (i_COUNTRY = 'US', LPAD ( i_ZIP5_CD , 5 , '0' ), i_ZIP5_CD)) IS NULL, 'xxNULLxx', IF (i_ID IS NULL, i_HD_ZIP_CD, IF (i_COUNTRY = 'US', LPAD ( i_ZIP5_CD , 5 , '0' ), i_ZIP5_CD))) <> IF (i_HD_ZIP_CD IS NULL, 'xxNULLxx', i_HD_ZIP_CD) OR \
        IF (i_COUNTRY IS NULL, 'xxNULLxx', i_COUNTRY) <> IF (i_HD_COUNTRY IS NULL, 'xxNULLxx', i_HD_COUNTRY) OR \
        IF ( IF (i_PHONE_NBR = 0, NULL, i_PHONE_NBR) IS NULL, 0, \
        	IF (i_PHONE_NBR = 0, NULL, i_PHONE_NBR)) <> IF (i_HD_PHONE_NBR IS NULL, 0, i_HD_PHONE_NBR) OR \
          IF (IF (i_ID IS NULL AND i_HD_EXP_DT > CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TO_TIMESTAMP ( i_EXPIRATION_DT , 'M/d/yyyy' )) IS NULL, to_date ( '01-01-1400' , 'd-M-yyyy' ), \
            IF (i_ID IS NULL AND i_HD_EXP_DT > CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TO_TIMESTAMP ( i_EXPIRATION_DT , 'M/d/yyyy' ))) <> IF (i_HD_EXP_DT IS NULL, to_date ( '01-01-1400' , 'd-M-yyyy' ), i_HD_EXP_DT) ), 1, 0) as CHANGE_FLAG"
)
# EXP_Change_Flag.filter("DATE()").show(truncate=False)
#EXP_Change_Flag.show(truncate=False)

# .withColumn("v_PHONE_NBR", expr("""IF (i_PHONE_NBR = 0, NULL, i_PHONE_NBR)""")) \
# 	.withColumn("v_EXPIRATION_DT", expr("""IF (JNR_FF_HONORARY_DESIGNEE___i_ID IS NULL AND JNR_FF_HONORARY_DESIGNEE___i_HD_EXP_DT > CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TO_DATE ( JNR_FF_HONORARY_DESIGNEE___i_EXPIRATION_DT , 'MM/DD/YYYY' ))""")) \
# 	.withColumn("v_ZIP5_CD", expr("""IF (JNR_FF_HONORARY_DESIGNEE___i_ID IS NULL, JNR_FF_HONORARY_DESIGNEE___i_HD_ZIP_CD, IF (JNR_FF_HONORARY_DESIGNEE___i_COUNTRY = 'US', LPAD ( JNR_FF_HONORARY_DESIGNEE___i_ZIP5_CD , 5 , '0' ), JNR_FF_HONORARY_DESIGNEE___i_ZIP5_CD))"""))

# COMMAND ----------

# Processing node UPD_Ins_Upd, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
EXP_Change_Flag_temp = EXP_Change_Flag.toDF(*["EXP_Change_Flag___" + col for col in EXP_Change_Flag.columns])

UPD_Ins_Upd = EXP_Change_Flag_temp.selectExpr(
	"EXP_Change_Flag___ID as ID",
	"EXP_Change_Flag___HD_TYPE_CD as HD_TYPE_CD",
	"EXP_Change_Flag___HD_TYPE_DESC as HD_TYPE_DESC",
	"EXP_Change_Flag___HD_FIRST_NAME as HD_FIRST_NAME",
	"EXP_Change_Flag___HD_LAST_NAME as HD_LAST_NAME",
	"EXP_Change_Flag___HD_SUFFIX as HD_SUFFIX",
	"EXP_Change_Flag___HD_BIRTH_MON as HD_BIRTH_MON",
	"EXP_Change_Flag___HD_BIRTH_DAY as HD_BIRTH_DAY",
	"EXP_Change_Flag___HD_SPOUSE_FIRST_NAME as HD_SPOUSE_FIRST_NAME",
	"EXP_Change_Flag___HD_SPOUSE_LAST_NAME as HD_SPOUSE_LAST_NAME",
	"EXP_Change_Flag___HD_ADDRESS as HD_ADDRESS",
	"EXP_Change_Flag___HD_CITY as HD_CITY",
	"EXP_Change_Flag___HD_STATE as HD_STATE",
	"EXP_Change_Flag___ZIP5_CD as HD_ZIP_CD",
	"EXP_Change_Flag___HD_COUNTRY as HD_COUNTRY",
	"EXP_Change_Flag___HD_PHONE_NBR as HD_PHONE_NBR",
	"EXP_Change_Flag___EXPIRATION_DT as HD_EXP_DT",
	"EXP_Change_Flag___i_UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_Change_Flag___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_Change_Flag___CHANGE_FLAG as CHANGE_FLAG",
	"EXP_Change_Flag___i_HD_ID as HD_ID",
	"EXP_Change_Flag___REMOVED_FLAG as REMOVED_FLAG") \
	.withColumn('pyspark_data_action', when((col('HD_ID').isNull()) ,(lit(0))) .otherwise(when(((col('CHANGE_FLAG') == lit(1)) |(col('REMOVED_FLAG') == lit(1))) ,(lit(1))) .otherwise(lit(3))))

# COMMAND ----------

# Processing node Shortcut_to_HONORARY_DESIGNEE_INS_UPD, type TARGET 
# COLUMN COUNT: 19


Shortcut_to_HONORARY_DESIGNEE_INS_UPD = UPD_Ins_Upd.selectExpr(
	"CAST(ID AS INT) as HD_ID",
	"CAST(HD_TYPE_CD AS STRING) as HD_TYPE_CD",
	"CAST(HD_TYPE_DESC AS STRING) as HD_TYPE_DESC",
	"CAST(HD_FIRST_NAME AS STRING) as HD_FIRST_NAME",
	"CAST(HD_LAST_NAME AS STRING) as HD_LAST_NAME",
	"CAST(HD_SUFFIX AS STRING) as HD_SUFFIX",
	"CAST(HD_BIRTH_MON as tinyint) as HD_BIRTH_MON",
	"CAST(HD_BIRTH_DAY as tinyint) as HD_BIRTH_DAY",
	"CAST(HD_SPOUSE_FIRST_NAME AS STRING) as HD_SPOUSE_FIRST_NAME",
	"CAST(HD_SPOUSE_LAST_NAME AS STRING) as HD_SPOUSE_LAST_NAME",
	"CAST(HD_ADDRESS AS STRING) as HD_ADDRESS",
	"CAST(HD_CITY AS STRING) as HD_CITY",
	"CAST(HD_STATE AS STRING) as HD_STATE",
	"CAST(HD_ZIP_CD AS STRING) as HD_ZIP_CD",
	"CAST(HD_COUNTRY AS STRING) as HD_COUNTRY",
	"CAST(HD_PHONE_NBR AS DECIMAL(10,0)) as HD_PHONE_NBR",
	"CAST(HD_EXP_DT AS DATE) as HD_EXP_DT",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

# .filter("HD_EXP_DT < '1900-01-01' ")
# Shortcut_to_HONORARY_DESIGNEE_INS_UPD.write.saveAsTable(f'{refine}.HONORARY_DESIGNEE', mode = 'append')
# Shortcut_to_HONORARY_DESIGNEE_INS_UPD.show(truncate=False)
# need to due to data in file  = 12/31/999  

spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")

try:
  primary_key = """source.HD_ID = target.HD_ID"""
  refined_perf_table = f"{sensitive}.refine_HONORARY_DESIGNEE"
  executeMerge(Shortcut_to_HONORARY_DESIGNEE_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("HONORARY_DESIGNEE", "HONORARY_DESIGNEE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("HONORARY_DESIGNEE", "HONORARY_DESIGNEE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------


