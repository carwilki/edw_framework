# Databricks notebook source
# Code converted on 2023-09-25 11:57:26
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

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == "":
    raise ValueError("env is not set")

raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"
refine = getEnvPrefix(env) + "refine"
sensitive = getEnvPrefix(env) + "empl_sensitive"

# Set global variables
starttime = datetime.now()  # start timestamp of the script
# currdate = starttime.strftime('%Y%m%d')
# currdate='20230921'

# dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-prod-empl-sensitive-raw-p1-gcs-gbl/nas/employee/banfield_employee')
# file_path = dbutils.widgets.get('file_path') + f'/{currdate}'

# COMMAND ----------


def get_source_file(_bucket):
    import builtins

    lst = dbutils.fs.ls(_bucket)
    dirs = [item for item in lst if item.isDir()]
    fldr = builtins.max(dirs, key=lambda x: x.name).name
    lst = dbutils.fs.ls(_bucket + fldr)
    files = [x.path for x in lst if x.name]
    return files[0] if files else None


source_bucket = getParameterValue(
    raw, "BA_HCM_Parameter.prm", "BA_HCM.WF:wf_EDW_Banfield_Employee", "source_bucket"
)
source_file = get_source_file(source_bucket)

print(source_file)

# COMMAND ----------

# if not fileExists(f'{file_path}/tp_asc.dat'):
#   # trunc and reload  if no file trunc table
#   spark.sql(f"TRUNCATE TABLE {raw}.TP_ASSOCIATE_PRE")
#   dbutils.notebook.exit('tp_asc.dat not available')


# COMMAND ----------

# Processing node SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT, type SOURCE
# COLUMN COUNT: 5

SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT = (
    spark.read.option("header", False)
    .option("skipRows", 1)
    .option("sep", "|")
    .option("inferSchema", True)
    .csv(source_file)
)  # .withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT = (
    SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT.withColumnRenamed(
        SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT.columns[0], "Record_Type"
    )
    .withColumnRenamed(
        SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT.columns[1], "Associate_Number_IN"
    )
    .withColumnRenamed(
        SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT.columns[2], "Associate_LastName"
    )
    .withColumnRenamed(
        SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT.columns[3], "Associate_First_Name"
    )
    .withColumn("Associate_PetPerks_ID", lit(None))
    .withColumn("Associate_Number", expr("BIGINT(Associate_Number_IN)"))
    .filter("Record_Type = 'D'")
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_BANFIELD_EMPLOYEE, type SOURCE
# COLUMN COUNT: 6

SQ_Shortcut_to_BANFIELD_EMPLOYEE = spark.sql(
    f"""SELECT
BANF_EMPL_ID,
BANF_EMPL_FIRST_NAME,
BANF_EMPL_LAST_NAME,
BANF_EMPL_PETPERKS_ID,
BANF_EMPL_STATUS_CD,
LOAD_TSTMP
FROM {sensitive}.refine_BANFIELD_EMPLOYEE"""
).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_BANFIELD_EMPLOYEE = (
    SQ_Shortcut_to_BANFIELD_EMPLOYEE.withColumnRenamed(
        SQ_Shortcut_to_BANFIELD_EMPLOYEE.columns[0], "BANF_EMPL_ID"
    )
    .withColumnRenamed(
        SQ_Shortcut_to_BANFIELD_EMPLOYEE.columns[1], "BANF_EMPL_FIRST_NAME"
    )
    .withColumnRenamed(
        SQ_Shortcut_to_BANFIELD_EMPLOYEE.columns[2], "BANF_EMPL_LAST_NAME"
    )
    .withColumnRenamed(
        SQ_Shortcut_to_BANFIELD_EMPLOYEE.columns[3], "BANF_EMPL_PETPERKS_ID"
    )
    .withColumnRenamed(
        SQ_Shortcut_to_BANFIELD_EMPLOYEE.columns[4], "BANF_EMPL_STATUS_CD"
    )
    .withColumnRenamed(SQ_Shortcut_to_BANFIELD_EMPLOYEE.columns[5], "LOAD_TSTMP")
)

# # COMMAND ----------
# # Processing node EXP_BanfieldEmpID, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# # COLUMN COUNT: 6

# # for each involved DataFrame, append the dataframe name to each column
# SQ_Shortcut_to_BANFIELD_EMPLOYEE_temp = SQ_Shortcut_to_BANFIELD_EMPLOYEE.toDF(*["SQ_Shortcut_to_BANFIELD_EMPLOYEE___" + col for col in SQ_Shortcut_to_BANFIELD_EMPLOYEE.columns])

# EXP_BanfieldEmpID = SQ_Shortcut_to_BANFIELD_EMPLOYEE_temp.selectExpr(
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___BANF_EMPL_ID as BANF_EMPL_ID",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___BANF_EMPL_FIRST_NAME as Banf_Empl_First_Name",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___BANF_EMPL_LAST_NAME as Banf_Empl_Last_Name",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___BANF_EMPL_PETPERKS_ID as Banf_Empl_Petperks_Id",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___BANF_EMPL_STATUS_CD as Banf_Empl_Status_Cd",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___LOAD_TSTMP as Load_Tstmp").selectExpr(
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___sys_row_id as sys_row_id",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___BANF_EMPL_ID as BANF_EMPL_ID",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___Banf_Empl_First_Name as Banf_Empl_First_Name",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___Banf_Empl_Last_Name as Banf_Empl_Last_Name",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___Banf_Empl_Petperks_Id as Banf_Empl_Petperks_Id",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___Banf_Empl_Status_Cd as Banf_Empl_Status_Cd",
# 	"SQ_Shortcut_to_BANFIELD_EMPLOYEE___Load_Tstmp as Load_Tstmp"
# )

# COMMAND ----------

# Processing node EXP_Associate_Number, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
# FIL_RecordType_temp = FIL_RecordType.toDF(*["FIL_RecordType___" + col for col in FIL_RecordType.columns])

# EXP_Associate_Number = FIL_RecordType_temp.selectExpr(
# 	"FIL_RecordType___Associate_Number as i_Associate_Number",
# 	"FIL_RecordType___Associate_LastName as Associate_LastName",
# 	"FIL_RecordType___Associate_First_Name as Associate_First_Name",
# 	"FIL_RecordType___Associate_PetPerks_ID as Associate_PetPerks_ID").selectExpr(
# 	"FIL_RecordType___sys_row_id as sys_row_id",
# 	"BIGINT(FIL_RecordType___i_Associate_Number) as Associate_Number",
# 	"FIL_RecordType___Associate_LastName as Associate_LastName",
# 	"FIL_RecordType___Associate_First_Name as Associate_First_Name",
# 	"FIL_RecordType___Associate_PetPerks_ID as Associate_PetPerks_ID"
# )

# COMMAND ----------

# Processing node JNR_FF_BanfieldEmployee, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_BANFIELD_EMPLOYEE_temp = SQ_Shortcut_to_BANFIELD_EMPLOYEE.toDF(
    *[
        "SQ_Shortcut_to_BANFIELD_EMPLOYEE___" + col
        for col in SQ_Shortcut_to_BANFIELD_EMPLOYEE.columns
    ]
)
SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT_temp = SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT.toDF(
    *[
        "SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT___" + col
        for col in SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT.columns
    ]
)

JNR_FF_BanfieldEmployee = SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT_temp.join(
    SQ_Shortcut_to_BANFIELD_EMPLOYEE_temp,
    [
        SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT_temp.SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT___Associate_Number
        == SQ_Shortcut_to_BANFIELD_EMPLOYEE_temp.SQ_Shortcut_to_BANFIELD_EMPLOYEE___BANF_EMPL_ID
    ],
    "fullouter",
).selectExpr(
    "SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT___Associate_Number as Associate_Number",
    "SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT___Associate_LastName as Associate_LastName",
    "SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT___Associate_First_Name as Associate_First_Name",
    "SQ_Shortcut_to_BANFIELD_EMPLOYEE_FLAT___Associate_PetPerks_ID as Associate_PetPerks_ID",
    "SQ_Shortcut_to_BANFIELD_EMPLOYEE___BANF_EMPL_ID as Banf_Empl_Id",
    "SQ_Shortcut_to_BANFIELD_EMPLOYEE___Banf_Empl_First_Name as Banf_Empl_First_Name",
    "SQ_Shortcut_to_BANFIELD_EMPLOYEE___Banf_Empl_Last_Name as Banf_Empl_Last_Name",
    "SQ_Shortcut_to_BANFIELD_EMPLOYEE___Banf_Empl_Petperks_Id as Banf_Empl_Petperks_Id",
    "SQ_Shortcut_to_BANFIELD_EMPLOYEE___Banf_Empl_Status_Cd as Banf_Empl_Status_Cd",
    "SQ_Shortcut_to_BANFIELD_EMPLOYEE___Load_Tstmp as Load_Tstmp",
)

# COMMAND ----------

# Processing node EXP_Defaults, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_FF_BanfieldEmployee_temp = JNR_FF_BanfieldEmployee.toDF(
    *["JNR_FF_BanfieldEmployee___" + col for col in JNR_FF_BanfieldEmployee.columns]
)

# .selectExpr(
# 	"JNR_FF_BanfieldEmployee___Associate_Number as Associate_Number",
# 	"JNR_FF_BanfieldEmployee___Associate_LastName as i_Associate_LastName",
# 	"JNR_FF_BanfieldEmployee___Associate_First_Name as i_Associate_First_Name",
# 	"JNR_FF_BanfieldEmployee___Associate_PetPerks_ID as i_Associate_PetPerks_ID",
# 	"JNR_FF_BanfieldEmployee___Banf_Empl_Id as Banf_Empl_Id",
# 	"JNR_FF_BanfieldEmployee___Banf_Empl_First_Name as Banf_Empl_First_Name",
# 	"JNR_FF_BanfieldEmployee___Banf_Empl_Last_Name as Banf_Empl_Last_Name",
# 	"JNR_FF_BanfieldEmployee___Banf_Empl_Petperks_Id as Banf_Empl_Petperks_Id",
# 	"JNR_FF_BanfieldEmployee___Banf_Empl_Status_Cd as Banf_Empl_Status_Cd",
# 	"JNR_FF_BanfieldEmployee___Load_Tstmp as LOAD_TSTMP1")

EXP_Defaults = JNR_FF_BanfieldEmployee_temp.selectExpr(
    # "JNR_FF_BanfieldEmployee___sys_row_id as sys_row_id",
    "IF (JNR_FF_BanfieldEmployee___Banf_Empl_Id IS NULL, JNR_FF_BanfieldEmployee___Associate_Number, JNR_FF_BanfieldEmployee___Banf_Empl_Id) as O_Banf_Empl_Id",
    "IF (JNR_FF_BanfieldEmployee___Associate_Number IS NULL, JNR_FF_BanfieldEmployee___Banf_Empl_Last_Name, JNR_FF_BanfieldEmployee___Associate_LastName) as Associate_LastName",
    "IF (JNR_FF_BanfieldEmployee___Associate_Number IS NULL, JNR_FF_BanfieldEmployee___Banf_Empl_First_Name, JNR_FF_BanfieldEmployee___Associate_First_Name) as Associate_First_Name",
    "IF (JNR_FF_BanfieldEmployee___Associate_Number IS NULL, JNR_FF_BanfieldEmployee___Banf_Empl_Petperks_Id, JNR_FF_BanfieldEmployee___Associate_PetPerks_ID) as Associate_PetPerks_ID",
    "IF (JNR_FF_BanfieldEmployee___Associate_Number IS NULL, 'I', 'A') as O_Associate_Empl_Status_Cd",
    "'2263' as Banf_Empl_PIN",
    "CURRENT_TIMESTAMP as Update_Tstmp",
    "IF (JNR_FF_BanfieldEmployee___Banf_Empl_Id IS NULL, CURRENT_TIMESTAMP, JNR_FF_BanfieldEmployee___LOAD_TSTMP) as Load_Tstmp",
    "IF (JNR_FF_BanfieldEmployee___Banf_Empl_Id IS NULL, 0, IF (JNR_FF_BanfieldEmployee___Associate_Number IS NULL AND JNR_FF_BanfieldEmployee___Banf_Empl_Status_Cd = 'I', 3, IF (IF (JNR_FF_BanfieldEmployee___Banf_Empl_First_Name IS NULL, 'zzz', JNR_FF_BanfieldEmployee___Banf_Empl_First_Name) <> IF (JNR_FF_BanfieldEmployee___Associate_First_Name IS NULL, 'zzz', JNR_FF_BanfieldEmployee___Associate_First_Name) OR IF (JNR_FF_BanfieldEmployee___Banf_Empl_Last_Name IS NULL, 'zzz', JNR_FF_BanfieldEmployee___Banf_Empl_Last_Name) <> IF (JNR_FF_BanfieldEmployee___Associate_LastName IS NULL, 'zzz', JNR_FF_BanfieldEmployee___Associate_LastName) OR IF (JNR_FF_BanfieldEmployee___Banf_Empl_Petperks_Id IS NULL, -1, JNR_FF_BanfieldEmployee___Banf_Empl_Petperks_Id) <> IF (JNR_FF_BanfieldEmployee___Associate_PetPerks_ID IS NULL, -1, JNR_FF_BanfieldEmployee___Associate_PetPerks_ID) OR IF (JNR_FF_BanfieldEmployee___Banf_Empl_Status_Cd IS NULL, 'z', JNR_FF_BanfieldEmployee___Banf_Empl_Status_Cd) <> IF (IF (JNR_FF_BanfieldEmployee___Associate_Number IS NULL, 'I', 'A') IS NULL, 'z', IF (JNR_FF_BanfieldEmployee___Associate_Number IS NULL, 'I', 'A')), 1, 3))) as Update_Strategy",
)
# .withColumn("v_Associate_Empl_Status_Cd", expr("""IF (JNR_FF_BanfieldEmployee___Associate_Number IS NULL, 'I', 'A')"""))

# COMMAND ----------

# Processing node FIL_Reject, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
EXP_Defaults_temp = EXP_Defaults.toDF(
    *["EXP_Defaults___" + col for col in EXP_Defaults.columns]
)

FIL_Reject = (
    EXP_Defaults_temp.selectExpr(
        "EXP_Defaults___O_Banf_Empl_Id as o_Banf_Empl_Id",
        "EXP_Defaults___Associate_LastName as Associate_LastName",
        "EXP_Defaults___Associate_First_Name as Associate_First_Name",
        "EXP_Defaults___Associate_PetPerks_ID as Associate_PetPerks_ID",
        "EXP_Defaults___O_Associate_Empl_Status_Cd as Associate_Empl_Status_Cd",
        "EXP_Defaults___Banf_Empl_PIN as Banf_Empl_PIN",
        "EXP_Defaults___Update_Tstmp as Update_Tstmp",
        "EXP_Defaults___Load_Tstmp as Load_Tstmp",
        "EXP_Defaults___Update_Strategy as Update_Strategy",
    )
    .filter("Update_Strategy != 3")
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node UPD_Update_Strategy, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
FIL_Reject_temp = FIL_Reject.toDF(
    *["FIL_Reject___" + col for col in FIL_Reject.columns]
)

UPD_Update_Strategy = FIL_Reject_temp.selectExpr(
    "FIL_Reject___o_Banf_Empl_Id as Banf_Empl_Id",
    "FIL_Reject___Associate_First_Name as Banf_Empl_First_Name",
    "FIL_Reject___Associate_LastName as Banf_Empl_Last_Name",
    "FIL_Reject___Associate_PetPerks_ID as Banf_Empl_Petperks_Id",
    "FIL_Reject___Associate_Empl_Status_Cd as Banf_Empl_Status_Cd",
    "FIL_Reject___Banf_Empl_PIN as Banf_Empl_PIN",
    "FIL_Reject___Update_Tstmp as Update_Tstmp",
    "FIL_Reject___Load_Tstmp as Load_Tstmp",
    "FIL_Reject___Update_Strategy as Update_Strategy",
).withColumn("pyspark_data_action", col("Update_Strategy"))

# Processing node Shortcut_to_BANFIELD_EMPLOYEE1, type TARGET
# COLUMN COUNT: 8


Shortcut_to_BANFIELD_EMPLOYEE1 = UPD_Update_Strategy.selectExpr(
    "CAST(BANF_EMPL_ID as bigint) as BANF_EMPL_ID",
    "CAST(Banf_Empl_First_Name AS STRING) as BANF_EMPL_FIRST_NAME",
    "CAST(Banf_Empl_Last_Name AS STRING) as BANF_EMPL_LAST_NAME",
    "CAST(BANF_EMPL_PETPERKS_ID as bigint) as BANF_EMPL_PETPERKS_ID",
    "CAST(Banf_Empl_Status_Cd AS STRING) as BANF_EMPL_STATUS_CD",
    "CAST(Banf_Empl_PIN AS STRING) as BANF_EMPL_PIN",
    "CAST(Update_Tstmp AS TIMESTAMP) as UPDATE_TSTMP",
    "CAST(Load_Tstmp AS TIMESTAMP) as LOAD_TSTMP",
    "pyspark_data_action as pyspark_data_action",
).distinct()

# COMMAND ----------


# Shortcut_to_BANFIELD_EMPLOYEE1.write.saveAsTable(f'{refine}.BANFIELD_EMPLOYEE', mode = 'append')
try:
    primary_key = """source.BANF_EMPL_ID = target.BANF_EMPL_ID"""
    refined_perf_table = f"{sensitive}.refine_BANFIELD_EMPLOYEE"
    executeMerge(Shortcut_to_BANFIELD_EMPLOYEE1, refined_perf_table, primary_key)
    logger.info(f"Merge with {refined_perf_table} completed]")
    logPrevRunDt(
        "BANFIELD_EMPLOYEE",
        "BANFIELD_EMPLOYEE",
        "Completed",
        "N/A",
        f"{raw}.log_run_details",
    )
except Exception as e:
    logPrevRunDt(
        "BANFIELD_EMPLOYEE",
        "BANFIELD_EMPLOYEE",
        "Failed",
        str(e),
        f"{raw}.log_run_details",
    )
    raise e


# COMMAND ----------
