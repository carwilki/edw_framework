# Databricks notebook source
#Code converted on 2023-09-19 11:15:47
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

# Processing node SQ_Shortcut_to_DUMMY, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_DUMMY = spark.sql("SELECT -1 AS HIER_NODE, -1 AS PARENT_NODE,'DUMMY' AS NODE_DESC").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE = spark.sql(f"""SELECT
HIER_NODE,
END_DT,
HIER_LEVEL,
NODE_DESC
FROM {raw}.SAP_PRICING_HIERARCHY_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_LVL2, type FILTER 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE_temp = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE.toDF(*["SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___" + col for col in SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE.columns])

FIL_LVL2 = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___HIER_NODE as HIER_NODE",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___END_DT as END_DT",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___HIER_LEVEL as HIER_LEVEL",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___NODE_DESC as NODE_DESC").filter("HIER_LEVEL = 2 AND END_DT = date'9999-12-31'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Union_INS_DUMMY_REC, type UNION_TRANSFORMATION 
# COLUMN COUNT: 2

Union_INS_DUMMY_REC = FIL_LVL2.select(
	col('HIER_NODE'),
	col('NODE_DESC')).unionAll(
SQ_Shortcut_to_DUMMY.select(
	col('HIER_NODE'),
	col('NODE_DESC')))

# COMMAND ----------

# Processing node EXP_LVL2, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
Union_INS_DUMMY_REC_temp = Union_INS_DUMMY_REC.toDF(*["Union_INS_DUMMY_REC___" + col for col in Union_INS_DUMMY_REC.columns])

EXP_LVL2 = Union_INS_DUMMY_REC_temp.selectExpr(
	# "Union_INS_DUMMY_REC___sys_row_id as sys_row_id",
	"IF (Union_INS_DUMMY_REC___HIER_NODE = '-1', - 1, cast(REGEXP_EXTRACT(Union_INS_DUMMY_REC___HIER_NODE, '([A-Z]+)(\\\\d+)', '2') as int)) as o_HIER_NODE",
	"Union_INS_DUMMY_REC___NODE_DESC as NODE_DESC",
	"CURRENT_TIMESTAMP () as o_LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_SAP_PRICING_DIVISION, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_SAP_PRICING_DIVISION = EXP_LVL2.selectExpr(
	"CAST(o_HIER_NODE AS INT) as SAP_PRICING_DIVISION_ID",
	"CAST(NODE_DESC AS STRING) as SAP_PRICING_DIVISION_DESC",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SAP_PRICING_DIVISION.write.mode("overwrite").saveAsTable(f'{legacy}.SAP_PRICING_DIVISION')
