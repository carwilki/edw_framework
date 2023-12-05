# Databricks notebook source
#Code converted on 2023-09-19 11:15:45
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

# Processing node SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE = spark.sql(f"""SELECT
HIER_NODE,
END_DT,
HIER_LEVEL,
PARENT_NODE,
NODE_DESC
FROM {raw}.SAP_PRICING_HIERARCHY_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_DUMMY, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_DUMMY = spark.sql("SELECT -1 AS HIER_NODE, -1 AS PARENT_NODE,'DUMMY' AS NODE_DESC").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node FIL_LVL4, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE_temp = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE.toDF(*["SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___" + col for col in SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE.columns])

FIL_LVL4 = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___HIER_NODE as HIER_NODE",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___END_DT as END_DT",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___HIER_LEVEL as HIER_LEVEL",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___PARENT_NODE as PARENT_NODE",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___NODE_DESC as NODE_DESC").filter("HIER_LEVEL = 4 AND END_DT = date'9999-12-31'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Union_INS_DUMMY_REC, type UNION_TRANSFORMATION 
# COLUMN COUNT: 3

Union_INS_DUMMY_REC = FIL_LVL4.select(
	col('HIER_NODE'),
	col('PARENT_NODE'),
	col('NODE_DESC')).unionAll(
SQ_Shortcut_to_DUMMY.select(
	col('HIER_NODE'),
	col('PARENT_NODE'),
	col('NODE_DESC')))

# COMMAND ----------

# Processing node EXP_LVL4, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
Union_INS_DUMMY_REC_temp = Union_INS_DUMMY_REC.toDF(*["Union_INS_DUMMY_REC___" + col for col in Union_INS_DUMMY_REC.columns])

EXP_LVL4 = Union_INS_DUMMY_REC_temp.selectExpr(
	# "Union_INS_DUMMY_REC___sys_row_id as sys_row_id",
	"IF (Union_INS_DUMMY_REC___HIER_NODE = '-1', - 1, cast(REGEXP_EXTRACT(Union_INS_DUMMY_REC___HIER_NODE, '([A-Z]+)(\\\\d+)', '2') as int)) as o_HIER_NODE",
	"IF (Union_INS_DUMMY_REC___PARENT_NODE = '-1', - 1, cast(REGEXP_EXTRACT(Union_INS_DUMMY_REC___PARENT_NODE, '([A-Z]+)(\\\\d+)', '2') as int)) as o_PARENT_NODE",
	"Union_INS_DUMMY_REC___NODE_DESC as NODE_DESC",
	"CURRENT_TIMESTAMP () as o_LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_SAP_PRICING_CLASS, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_SAP_PRICING_CLASS = EXP_LVL4.selectExpr(
	"CAST(o_HIER_NODE AS INT) as SAP_PRICING_CLASS_ID",
	"CAST(o_PARENT_NODE AS INT) as SAP_PRICING_DEPT_ID",
	"CAST(NODE_DESC AS STRING) as SAP_PRICING_CLASS_DESC",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SAP_PRICING_CLASS.write.mode("overwrite").saveAsTable(f'{legacy}.SAP_PRICING_CLASS')

# COMMAND ----------


