# Databricks notebook source
#Code converted on 2023-10-17 09:36:47
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
refine = getEnvPrefix(env) + 'refine'

# Set global variables
starttime = datetime.now() #start timestamp of the script


# COMMAND ----------

# Processing node SQ_Shortcut_to_EDU_CERT_SUMMARY, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_EDU_CERT_SUMMARY = spark.sql(f"""SELECT
DAY_DT,
EMPLOYEE_ID,
MISSED_ASSESS_MID,
MISSED_ASSESS_LID,
MISSED_ASSESS_NAME,
JOB_CD,
LOCATION_ID,
CURR_COMPLIANCE_FLAG,
LOAD_DT
FROM {legacy}.EDU_CERT_SUMMARY
WHERE CURR_COMPLIANCE_FLAG <> 3""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EDU_CERT_SUMMARY = SQ_Shortcut_to_EDU_CERT_SUMMARY \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[1],'EMPLOYEE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[2],'MISSED_ASSESS_MID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[3],'MISSED_ASSESS_LID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[4],'MISSED_ASSESS_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[5],'JOB_CD') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[6],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[7],'CURR_COMPLIANCE_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[8],'LOAD_DT')

# COMMAND ----------

# Processing node SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS = spark.sql(f"""SELECT
SNAPSHOT_DT,
EMPLOYEE_ID,
LEARNING_ID,
LEARNING_NAME,
EXEMPT_FLAG
FROM {legacy}.EDU_SKILLSOFT_LEARNING_EXEMPTIONS""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS = SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS \
	.withColumnRenamed(SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS.columns[0],'SNAPSHOT_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS.columns[1],'EMPLOYEE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS.columns[2],'LEARNING_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS.columns[3],'LEARNING_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS.columns[4],'EXEMPT_FLAG')

# COMMAND ----------

# Processing node Exp_Concat_Id, type EXPRESSION 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_EDU_CERT_SUMMARY_temp = SQ_Shortcut_to_EDU_CERT_SUMMARY.toDF(*["SQ_Shortcut_to_EDU_CERT_SUMMARY___" + col for col in SQ_Shortcut_to_EDU_CERT_SUMMARY.columns])

Exp_Concat_Id = SQ_Shortcut_to_EDU_CERT_SUMMARY_temp.selectExpr(
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___EMPLOYEE_ID as EMPLOYEE_ID",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_MID as MISSED_ASSESS_MID",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_LID as MISSED_ASSESS_LID",
	"CONCAT ( IF (SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_MID = - 1, 0, SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_MID) , IF (SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_LID = - 1, 0, SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_LID) ) as Join_Learning_ID",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_NAME as MISSED_ASSESS_NAME",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___JOB_CD as JOB_CD",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___LOAD_DT as LOAD_DT"
)

# COMMAND ----------

# Processing node FTR_Snapshot_Dt, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS_temp = SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS.toDF(*["SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS___" + col for col in SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS.columns])

FTR_Snapshot_Dt = SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS_temp.selectExpr(
	"SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS___SNAPSHOT_DT as SNAPSHOT_DT",
	"SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS___EMPLOYEE_ID as EMPLOYEE_ID",
	"SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS___LEARNING_ID as LEARNING_ID",
	"SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS___LEARNING_NAME as LEARNING_NAME",
	"SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS___EXEMPT_FLAG as EXEMPT_FLAG").filter("True").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_FlatFile, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
FTR_Snapshot_Dt_temp = FTR_Snapshot_Dt.toDF(*["FTR_Snapshot_Dt___" + col for col in FTR_Snapshot_Dt.columns])
Exp_Concat_Id_temp = Exp_Concat_Id.toDF(*["Exp_Concat_Id___" + col for col in Exp_Concat_Id.columns])

JNR_FlatFile = FTR_Snapshot_Dt_temp.join(Exp_Concat_Id_temp,[FTR_Snapshot_Dt_temp.FTR_Snapshot_Dt___EMPLOYEE_ID == Exp_Concat_Id_temp.Exp_Concat_Id___EMPLOYEE_ID, FTR_Snapshot_Dt_temp.FTR_Snapshot_Dt___LEARNING_ID == Exp_Concat_Id_temp.Exp_Concat_Id___Join_Learning_ID],'inner').selectExpr(
	"Exp_Concat_Id___DAY_DT as DAY_DT",
	"Exp_Concat_Id___EMPLOYEE_ID as EMPLOYEE_ID",
	"Exp_Concat_Id___MISSED_ASSESS_MID as MISSED_ASSESS_MID",
	"Exp_Concat_Id___MISSED_ASSESS_LID as MISSED_ASSESS_LID",
	"Exp_Concat_Id___Join_Learning_ID as Learning_ID",
	"Exp_Concat_Id___MISSED_ASSESS_NAME as MISSED_ASSESS_NAME",
	"Exp_Concat_Id___JOB_CD as JOB_CD",
	"Exp_Concat_Id___LOCATION_ID as LOCATION_ID",
	"Exp_Concat_Id___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"Exp_Concat_Id___LOAD_DT as LOAD_DT",
	"FTR_Snapshot_Dt___EMPLOYEE_ID as employee_id_ff",
	"FTR_Snapshot_Dt___LEARNING_ID as learning_id_ff",
	"FTR_Snapshot_Dt___LEARNING_NAME as learning_name",
	"FTR_Snapshot_Dt___EXEMPT_FLAG as exempt_flag")

# COMMAND ----------

# Processing node EXP_Flag_Update, type EXPRESSION 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
JNR_FlatFile_temp = JNR_FlatFile.toDF(*["JNR_FlatFile___" + col for col in JNR_FlatFile.columns])

EXP_Flag_Update = JNR_FlatFile_temp.selectExpr(
	# "JNR_FlatFile___sys_row_id as sys_row_id",
	"JNR_FlatFile___DAY_DT as DAY_DT",
	"JNR_FlatFile___EMPLOYEE_ID as EMPLOYEE_ID",
	"JNR_FlatFile___MISSED_ASSESS_MID as MISSED_ASSESS_MID",
	"JNR_FlatFile___MISSED_ASSESS_LID as MISSED_ASSESS_LID",
	"JNR_FlatFile___Learning_ID as Learning_ID",
	"JNR_FlatFile___MISSED_ASSESS_NAME as MISSED_ASSESS_NAME",
	"JNR_FlatFile___JOB_CD as JOB_CD",
	"JNR_FlatFile___LOCATION_ID as LOCATION_ID",
	"JNR_FlatFile___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"JNR_FlatFile___LOAD_DT as LOAD_DT",
	"JNR_FlatFile___employee_id_ff as employee_id_ff",
	"JNR_FlatFile___learning_id_ff as learning_id_ff",
	"JNR_FlatFile___learning_name as learning_name",
	"JNR_FlatFile___exempt_flag as exempt_flag",
	"IF (JNR_FlatFile___exempt_flag = 1, 3, JNR_FlatFile___CURR_COMPLIANCE_FLAG) as Exempt_Flag_exp"
)

# COMMAND ----------

# Processing node UPD_Flag, type UPDATE_STRATEGY 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
EXP_Flag_Update_temp = EXP_Flag_Update.toDF(*["EXP_Flag_Update___" + col for col in EXP_Flag_Update.columns])

UPD_Flag = EXP_Flag_Update_temp.selectExpr(
	"EXP_Flag_Update___DAY_DT as DAY_DT",
	"EXP_Flag_Update___EMPLOYEE_ID as EMPLOYEE_ID",
	"EXP_Flag_Update___MISSED_ASSESS_MID as MISSED_ASSESS_MID",
	"EXP_Flag_Update___MISSED_ASSESS_LID as MISSED_ASSESS_LID",
	"EXP_Flag_Update___Learning_ID as Learning_ID",
	"EXP_Flag_Update___MISSED_ASSESS_NAME as MISSED_ASSESS_NAME",
	"EXP_Flag_Update___JOB_CD as JOB_CD",
	"EXP_Flag_Update___LOCATION_ID as LOCATION_ID",
	"EXP_Flag_Update___Exempt_Flag_exp as CURR_COMPLIANCE_FLAG",
	"EXP_Flag_Update___LOAD_DT as LOAD_DT",
	"EXP_Flag_Update___employee_id_ff as employee_id_ff",
	"EXP_Flag_Update___learning_id_ff as learning_id_ff",
	"EXP_Flag_Update___learning_name as learning_name",
	"EXP_Flag_Update___exempt_flag as exempt_flag",
	"EXP_Flag_Update___Exempt_Flag_exp as Exempt_Flag_exp")

# COMMAND ----------

# Processing node Shortcut_to_EDU_CERT_SUMMARY1, type TARGET 
# COLUMN COUNT: 9

try:
    Shortcut_to_EDU_CERT_SUMMARY1 = UPD_Flag.selectExpr(
        "CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
        "CAST(EMPLOYEE_ID as bigint) as EMPLOYEE_ID",
        "CAST(MISSED_ASSESS_MID as bigint) as MISSED_ASSESS_MID",
        "CAST(MISSED_ASSESS_LID as bigint) as MISSED_ASSESS_LID",
        "CAST(CURR_COMPLIANCE_FLAG as tinyint) as CURR_COMPLIANCE_FLAG"
    )
    Shortcut_to_EDU_CERT_SUMMARY1.dropDuplicates().createOrReplaceTempView('EDU_CERT_SUMMARY_UPD')
    spark.sql(f"""
            MERGE INTO {legacy}.EDU_CERT_SUMMARY trg
            USING EDU_CERT_SUMMARY_UPD src
            ON (src.DAY_DT =  trg.DAY_DT AND src.EMPLOYEE_ID =  trg.EMPLOYEE_ID AND src.MISSED_ASSESS_MID =  trg.MISSED_ASSESS_MID AND src.MISSED_ASSESS_LID =  trg.MISSED_ASSESS_LID  )
            WHEN MATCHED THEN UPDATE SET trg.CURR_COMPLIANCE_FLAG = src.CURR_COMPLIANCE_FLAG
            """)
    logPrevRunDt("EDU_CERT_SUMMARY", "EDU_CERT_SUMMARY", "Completed", "N/A", f"{raw}.log_run_details")
    
except Exception as e:
    logPrevRunDt("EDU_CERT_SUMMARY", "EDU_CERT_SUMMARY","Failed",str(e), f"{raw}.log_run_details")
    raise e

	

# COMMAND ----------


