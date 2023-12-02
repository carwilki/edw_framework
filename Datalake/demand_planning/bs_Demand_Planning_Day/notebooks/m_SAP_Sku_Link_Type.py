# Databricks notebook source
#Code converted on 2023-10-06 15:46:05
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

# Variable_declaration_comment
EDW_SKU_LINK_TYPE_CD=getParameterValue(raw,'wf_PMSourceFileDir_RH01.prm','BA_Demand_Planning.WF:bs_Demand_Planning_Day.ST:s_SAP_Sku_Link_Type','EDW_SKU_LINK_TYPE_CD')

source_bucket=getParameterValue(raw,'bs_Demand_Planning_Day.prm','Demand_Planning.WF:bs_Demand_Planning_Day.M:m_SAP_Sku_Link_Type','source_bucket')
key=getParameterValue(raw,'bs_Demand_Planning_Day.prm','Demand_Planning.WF:bs_Demand_Planning_Day.M:m_SAP_Sku_Link_Type','key')

source_file=get_src_file(key,source_bucket)
print(source_file)

# COMMAND ----------

# Processing node SQ_Shortcut_to_WRF_FOLUP_TYPT, type APPLICATION_SOURCE_QUALIFIER 
# COLUMN COUNT: 2

# ---------- No handler defined for type APPLICATION_SOURCE_QUALIFIER, node SQ_Shortcut_to_WRF_FOLUP_TYPT, job m_SAP_Sku_Link_Type ---------- #

SQ_Shortcut_to_WRF_FOLUP_TYPT = spark.read.csv(source_file,sep='|',header=True,inferSchema=True)



# COMMAND ----------

# Processing node SQ_Shortcut_to_SAP_SKU_LINK_TYPE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_SAP_SKU_LINK_TYPE = spark.sql(f"""SELECT
SAP_SKU_LINK_TYPE_CD,
SAP_SKU_LINK_TYPE_DESC,
LOAD_TSTMP
FROM {legacy}.SAP_SKU_LINK_TYPE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SAP_SKU_LINK_TYPE = SQ_Shortcut_to_SAP_SKU_LINK_TYPE \
	.withColumnRenamed(SQ_Shortcut_to_SAP_SKU_LINK_TYPE.columns[0],'SAP_SKU_LINK_TYPE_CD') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_SKU_LINK_TYPE.columns[1],'SAP_SKU_LINK_TYPE_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_SKU_LINK_TYPE.columns[2],'LOAD_TSTMP')

# COMMAND ----------

# Processing node JNR_SAP_SKU_LINK_TYPE, type JOINER 
# COLUMN COUNT: 5

JNR_SAP_SKU_LINK_TYPE = SQ_Shortcut_to_SAP_SKU_LINK_TYPE.join(SQ_Shortcut_to_WRF_FOLUP_TYPT,[SQ_Shortcut_to_SAP_SKU_LINK_TYPE.SAP_SKU_LINK_TYPE_CD == SQ_Shortcut_to_WRF_FOLUP_TYPT.FOLLOWUP_TYP_NR],'right_outer')

# COMMAND ----------

# Processing node FIL_NO_CHANGE_REC, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
JNR_SAP_SKU_LINK_TYPE_temp = JNR_SAP_SKU_LINK_TYPE.toDF(*["JNR_SAP_SKU_LINK_TYPE___" + col for col in JNR_SAP_SKU_LINK_TYPE.columns])

FIL_NO_CHANGE_REC = JNR_SAP_SKU_LINK_TYPE_temp.selectExpr(
	"JNR_SAP_SKU_LINK_TYPE___FOLLOWUP_TYP_NR as FOLLOWUP_TYP_NR",
	"JNR_SAP_SKU_LINK_TYPE___FOLLOWUP_TYP_DE as FOLLOWUP_TYP_DE",
	"JNR_SAP_SKU_LINK_TYPE___SAP_SKU_LINK_TYPE_CD as SAP_SKU_LINK_TYPE_CD",
	"JNR_SAP_SKU_LINK_TYPE___SAP_SKU_LINK_TYPE_DESC as SAP_SKU_LINK_TYPE_DESC",
	"JNR_SAP_SKU_LINK_TYPE___LOAD_TSTMP as LOAD_TSTMP").filter("SAP_SKU_LINK_TYPE_CD IS NULL OR ( SAP_SKU_LINK_TYPE_CD IS NOT NULL AND FOLLOWUP_TYP_DE != SAP_SKU_LINK_TYPE_DESC )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_EVAL_VALUES, type EXPRESSION 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
FIL_NO_CHANGE_REC_temp = FIL_NO_CHANGE_REC.toDF(*["FIL_NO_CHANGE_REC___" + col for col in FIL_NO_CHANGE_REC.columns])

EXP_EVAL_VALUES = FIL_NO_CHANGE_REC_temp.selectExpr(
	"FIL_NO_CHANGE_REC___sys_row_id as sys_row_id",
	"FIL_NO_CHANGE_REC___FOLLOWUP_TYP_NR as FOLLOWUP_TYP_NR",
	"FIL_NO_CHANGE_REC___FOLLOWUP_TYP_DE as FOLLOWUP_TYP_DE",
	f"{EDW_SKU_LINK_TYPE_CD} as o_EDW_SKU_LINK_TYPE_CD",
	"FIL_NO_CHANGE_REC___SAP_SKU_LINK_TYPE_CD as SAP_SKU_LINK_TYPE_CD",
	"CURRENT_TIMESTAMP as o_UPDATE_TSTMP",
	"IF (FIL_NO_CHANGE_REC___SAP_SKU_LINK_TYPE_CD IS NULL, CURRENT_TIMESTAMP, FIL_NO_CHANGE_REC___LOAD_TSTMP) as o_LOAD_TSTMP"
)

# COMMAND ----------

# Processing node UPD_VALIDATE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXP_EVAL_VALUES_temp = EXP_EVAL_VALUES.toDF(*["EXP_EVAL_VALUES___" + col for col in EXP_EVAL_VALUES.columns])

UPD_VALIDATE = EXP_EVAL_VALUES_temp.selectExpr(
	"EXP_EVAL_VALUES___FOLLOWUP_TYP_NR as FOLLOWUP_TYP_NR",
	"EXP_EVAL_VALUES___FOLLOWUP_TYP_DE as FOLLOWUP_TYP_DE",
	"EXP_EVAL_VALUES___o_EDW_SKU_LINK_TYPE_CD as EDW_SKU_LINK_TYPE_CD",
	"EXP_EVAL_VALUES___SAP_SKU_LINK_TYPE_CD as SAP_SKU_LINK_TYPE_CD",
	"EXP_EVAL_VALUES___o_UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_EVAL_VALUES___o_LOAD_TSTMP as LOAD_TSTMP") \
	.withColumn('pyspark_data_action', when((col('SAP_SKU_LINK_TYPE_CD').isNull()) ,(lit(0))) .otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_SAP_SKU_LINK_TYPE, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_SAP_SKU_LINK_TYPE = UPD_VALIDATE.selectExpr(
	"CAST(FOLLOWUP_TYP_NR AS STRING) as SAP_SKU_LINK_TYPE_CD",
	"CAST(FOLLOWUP_TYP_DE AS STRING) as SAP_SKU_LINK_TYPE_DESC",
	"CAST(EDW_SKU_LINK_TYPE_CD AS STRING) as EDW_SKU_LINK_TYPE_CD",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.SAP_SKU_LINK_TYPE_CD = target.SAP_SKU_LINK_TYPE_CD"""
  refined_perf_table = f"{legacy}.SAP_SKU_LINK_TYPE"
  chk=DuplicateChecker()
  chk.check_for_duplicate_primary_keys(Shortcut_to_SAP_SKU_LINK_TYPE,['SAP_SKU_LINK_TYPE_CD'])  
  executeMerge(Shortcut_to_SAP_SKU_LINK_TYPE, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("SAP_SKU_LINK_TYPE", "SAP_SKU_LINK_TYPE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("SAP_SKU_LINK_TYPE", "SAP_SKU_LINK_TYPE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------


