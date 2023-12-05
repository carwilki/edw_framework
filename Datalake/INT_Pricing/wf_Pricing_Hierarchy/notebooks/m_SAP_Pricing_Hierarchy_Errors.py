# Databricks notebook source
#Code converted on 2023-09-19 11:15:43
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

# Processing node SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS, type SOURCE 
# COLUMN COUNT: 3

_sql = f"""
SELECT DISTINCT SAP_PRICING_CATEGORY_ID AS Missing_Node
	,'SAP_PRICING_CATEGORY' AS LOAD_TABLE
	,'PRICING CATEGORY Missing from SAP_PRICING_CATEGORY table, Hierarchy Level 5' AS Error_Message
FROM {legacy}.SAP_CATEGORY
WHERE SAP_PRICING_CATEGORY_ID NOT IN (
SELECT DISTINCT SAP_PRICING_CATEGORY_ID
FROM {legacy}.SAP_PRICING_CATEGORY
)
UNION
SELECT DISTINCT SAP_PRICING_CLASS_ID AS Missing_Node
	,'SAP_PRICING_CLASS' AS LOAD_TABLE
	,'PRICING CLASS Missing from SAP_PRICING_CLASS table, Hierarchy Level 4' AS Error_Message
FROM {legacy}.SAP_PRICING_CATEGORY
WHERE SAP_PRICING_CLASS_ID NOT IN (
SELECT DISTINCT SAP_PRICING_CLASS_ID
FROM {legacy}.SAP_PRICING_CLASS
)
UNION
SELECT DISTINCT SAP_PRICING_DEPT_ID AS Missing_Node
	,'SAP_PRICING_DEPT' AS LOAD_TABLE
	,'PRICING DEPARTMENT Missing from SAP_PRICING_DEPT table, Hierarchy Level 3' AS Error_Message
FROM {legacy}.SAP_PRICING_CLASS
WHERE SAP_PRICING_DEPT_ID NOT IN (
	SELECT DISTINCT SAP_PRICING_DEPT_ID
	FROM {legacy}.SAP_PRICING_DEPT
)
UNION
SELECT DISTINCT SAP_PRICING_DIVISION_ID AS Missing_Node
	,'SAP_PRICING_DIVISION' AS LOAD_TABLE
	,'PRICING DIVISION Missing from SAP_PRICING_DIVISION table, Hierarchy Level 2' AS Error_Message
FROM {legacy}.SAP_PRICING_DEPT
WHERE SAP_PRICING_DIVISION_ID NOT IN (
	SELECT DISTINCT SAP_PRICING_DIVISION_ID
	FROM {legacy}.SAP_PRICING_DIVISION
)"""

SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS \
	.withColumnRenamed(SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS.columns[0],'MISSING_NODE') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS.columns[1],'LOAD_TABLE') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS.columns[2],'ERROR_MESSAGE')

# COMMAND ----------

# Processing node EXP_BUS_LOGIC, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS_temp = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS.toDF(*["SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS___" + col for col in SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS.columns])

EXP_BUS_LOGIC = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS_temp.selectExpr(
	# "SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS___MISSING_NODE as MISSING_NODE",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS___LOAD_TABLE as LOAD_TABLE",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS___ERROR_MESSAGE as ERROR_MESSAGE",
	"CURRENT_TIMESTAMP () as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS1, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS1 = EXP_BUS_LOGIC.selectExpr(
	"CAST(MISSING_NODE AS INT) as MISSING_NODE",
	"CAST(LOAD_TABLE AS STRING) as LOAD_TABLE",
	"CAST(ERROR_MESSAGE AS STRING) as ERROR_MESSAGE",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SAP_PRICING_HIERARCHY_ERRORS1.write.mode("overwrite").saveAsTable(f'{legacy}.SAP_PRICING_HIERARCHY_ERRORS')

# COMMAND ----------


