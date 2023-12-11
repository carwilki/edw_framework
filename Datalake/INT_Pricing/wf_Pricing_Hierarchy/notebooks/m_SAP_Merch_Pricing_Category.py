# Databricks notebook source
#Code converted on 2023-09-19 11:15:46
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

# Processing node SQ_Shortcut_to_SAP_CATEGORY, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SAP_CATEGORY = spark.sql(f"""SELECT
SAP_CATEGORY_ID
FROM {legacy}.SAP_CATEGORY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE = spark.sql(f"""SELECT
HIER_NODE,
END_DT,
HIER_LEVEL,
PARENT_NODE
FROM {raw}.SAP_PRICING_HIERARCHY_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_LVL6, type FILTER 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE_temp = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE.toDF(*["SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___" + col for col in SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE.columns])

FIL_LVL6 = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___HIER_NODE as HIER_NODE",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___END_DT as END_DT",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___HIER_LEVEL as HIER_LEVEL",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___PARENT_NODE as PARENT_NODE").filter("HIER_LEVEL = 6 AND END_DT = date'9999-12-31'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_LVL6, type EXPRESSION 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
FIL_LVL6_temp = FIL_LVL6.toDF(*["FIL_LVL6___" + col for col in FIL_LVL6.columns])

EXP_LVL6 = FIL_LVL6_temp.selectExpr(
	"FIL_LVL6___sys_row_id as sys_row_id",
	"REGEXP_EXTRACT(FIL_LVL6___HIER_NODE, '([A-Z]+)(\\\\d+)', '2') as HIER_NODE",
	"REGEXP_EXTRACT(FIL_LVL6___PARENT_NODE, '([A-Z]+)(\\\\d+)', '2') as PARENT_NODE"
)

# COMMAND ----------

# Processing node JNR_SRC_TGT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SAP_CATEGORY_temp = SQ_Shortcut_to_SAP_CATEGORY.toDF(*["SQ_Shortcut_to_SAP_CATEGORY___" + col for col in SQ_Shortcut_to_SAP_CATEGORY.columns])
EXP_LVL6_temp = EXP_LVL6.toDF(*["EXP_LVL6___" + col for col in EXP_LVL6.columns])

JNR_SRC_TGT = EXP_LVL6_temp.join(SQ_Shortcut_to_SAP_CATEGORY_temp,[EXP_LVL6_temp.EXP_LVL6___HIER_NODE == SQ_Shortcut_to_SAP_CATEGORY_temp.SQ_Shortcut_to_SAP_CATEGORY___SAP_CATEGORY_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_SAP_CATEGORY___sys_row_id as sys_row_id",
  "EXP_LVL6___HIER_NODE as HIER_NODE",
	"EXP_LVL6___PARENT_NODE as PARENT_NODE",
	"SQ_Shortcut_to_SAP_CATEGORY___SAP_CATEGORY_ID as lkp_SAP_CATEGORY_ID")

# COMMAND ----------

# Processing node EXP_NULL_CHK, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
JNR_SRC_TGT_temp = JNR_SRC_TGT.toDF(*["JNR_SRC_TGT___" + col for col in JNR_SRC_TGT.columns])

EXP_NULL_CHK = JNR_SRC_TGT_temp.selectExpr(
	"JNR_SRC_TGT___sys_row_id as sys_row_id",
	"JNR_SRC_TGT___lkp_SAP_CATEGORY_ID as lkp_SAP_CATEGORY_ID",
	"IF (JNR_SRC_TGT___HIER_NODE IS NULL, - 1, JNR_SRC_TGT___PARENT_NODE) as PARENT_NODE",
	"CURRENT_TIMESTAMP () as UPD_TSTMP"
)

# COMMAND ----------

# Processing node UPD_SAP_PRICNG_CTGRY_ID, type UPDATE_STRATEGY 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
EXP_NULL_CHK_temp = EXP_NULL_CHK.toDF(*["EXP_NULL_CHK___" + col for col in EXP_NULL_CHK.columns])

UPD_SAP_PRICNG_CTGRY_ID = EXP_NULL_CHK_temp.selectExpr(
	"EXP_NULL_CHK___lkp_SAP_CATEGORY_ID as lkp_SAP_CATEGORY_ID",
	"EXP_NULL_CHK___PARENT_NODE as PARENT_NODE",
	"EXP_NULL_CHK___UPD_TSTMP as UPD_TSTMP") 

# COMMAND ----------

# Processing node Shortcut_to_SAP_CATEGORY, type TARGET 
# COLUMN COUNT: 6

Shortcut_to_SAP_CATEGORY = UPD_SAP_PRICNG_CTGRY_ID.selectExpr(
	"CAST(lkp_SAP_CATEGORY_ID AS INT) as SAP_CATEGORY_ID",
	"CAST(PARENT_NODE AS INT) as SAP_PRICING_CATEGORY_ID",
	"CAST(UPD_TSTMP AS TIMESTAMP) as UPD_TSTMP"
)


# COMMAND ----------

try:
    refined_perf_table = f"{legacy}.SAP_CATEGORY"
    Shortcut_to_SAP_CATEGORY.createOrReplaceTempView('temp_SAP_CATEGORY')
    primary_key = """source.SAP_CATEGORY_ID = target.SAP_CATEGORY_ID"""

    merge_sql = f"""MERGE INTO {refined_perf_table} as target
                    USING temp_SAP_CATEGORY as source
                    ON {primary_key}
                    WHEN MATCHED THEN
                    UPDATE SET target.SAP_PRICING_CATEGORY_ID=source.SAP_PRICING_CATEGORY_ID, target.UPD_TSTMP=source.UPD_TSTMP
                    """
    spark.sql(merge_sql)
    logger.info(f"Merge with {refined_perf_table} completed]")
    logPrevRunDt("SAP_CATEGORY", "SAP_CATEGORY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
    logPrevRunDt("SAP_CATEGORY", "SAP_CATEGORY","Failed",str(e), f"{raw}.log_run_details", )
    raise e
