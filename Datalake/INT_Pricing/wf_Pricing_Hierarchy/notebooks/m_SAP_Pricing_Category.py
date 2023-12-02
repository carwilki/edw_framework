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

(username, password, connection_string) = UserDataFeed_prd_sqlServer(env)

# COMMAND ----------

# Processing node LKP_UDH_ONLINE_ROLE_LKUP_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 1

_sql = f"""
SELECT
    ONLINE_CATEGORY_ROLE_ID,
    ONLINE_CATEGORY_ROLE_DESC
FROM userdatafeed.dbo.UDH_ONLINE_ROLE_LKUP
"""

LKP_UDH_ONLINE_ROLE_LKUP_SRC = jdbcSqlServerConnection(f"({_sql}) as src", username, password, connection_string)


# COMMAND ----------

# Processing node LKP_UDH_PRICING_ROLE_LKUP_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 1

_sql = f"""
SELECT
    PRICING_CATEGORY_ROLE_ID,
    PRICING_CATEGORY_ROLE_DESC
FROM userdatafeed.dbo.UDH_PRICING_ROLE_LKUP
"""

LKP_UDH_PRICING_ROLE_LKUP_SRC = jdbcSqlServerConnection(f"({_sql}) as src", username, password, connection_string)


# COMMAND ----------

# Processing node SQ_Shortcut_to_DUMMY, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_DUMMY = spark.sql("SELECT -1 AS HIER_NODE, -1 AS PARENT_NODE, 'DUMMY' AS NODE_DESC").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node SQ_Shortcut_to_UDH_PRICING_ROLES, type SOURCE 
# COLUMN COUNT: 3

_sql = f"""
SELECT
    UDH_PRICING_ROLES.SAP_PRICING_CATEGORY_ID,
    UDH_PRICING_ROLES.PRICING_CATEGORY_ROLE_ID,
    UDH_PRICING_ROLES.ONLINE_CATEGORY_ROLE_ID
FROM userdatafeed.dbo.UDH_PRICING_ROLES
"""

SQ_Shortcut_to_UDH_PRICING_ROLES = jdbcSqlServerConnection(f"({_sql}) as src", username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())

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

# Processing node EXP_LOOKUP_VALUES, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
# LKP_temp = LKP.toDF(*["LKP___" + col for col in LKP.columns])

# EXP_LOOKUP_VALUES = SQ_Shortcut_to_UDH_PRICING_ROLES.selectExpr(
	# "SQ_Shortcut_to_UDH_PRICING_ROLES___sys_row_id as sys_row_id",
	# "SQ_Shortcut_to_UDH_PRICING_ROLES___SAP_PRICING_CATEGORY_ID as SAP_PRICING_CATEGORY_ID",
	# "SQ_Shortcut_to_UDH_PRICING_ROLES___PRICING_CATEGORY_ROLE_ID as PRICING_CATEGORY_ROLE_ID",
	# "SQ_Shortcut_to_UDH_PRICING_ROLES___ONLINE_CATEGORY_ROLE_ID as ONLINE_CATEGORY_ROLE_ID",
	# ":LKP___LKP_UDH_PRICING_ROLE_LKUP ( SQ_Shortcut_to_UDH_PRICING_ROLES___PRICING_CATEGORY_ROLE_ID ) as o_PRICING_CATEG_DESC",
	# ":LKP___LKP_UDH_ONLINE_ROLE_LKUP ( SQ_Shortcut_to_UDH_PRICING_ROLES___ONLINE_CATEGORY_ROLE_ID ) as o_ONLINE_CATEG_DESC"
# )

# Adding deferred logic for dataframe EXP_LOOKUP_VALUES, column o_PRICING_CATEG_DESC
EXP_LOOKUP_VALUES = SQ_Shortcut_to_UDH_PRICING_ROLES.join(
    LKP_UDH_PRICING_ROLE_LKUP_SRC, (LKP_UDH_PRICING_ROLE_LKUP_SRC.PRICING_CATEGORY_ROLE_ID == SQ_Shortcut_to_UDH_PRICING_ROLES.PRICING_CATEGORY_ROLE_ID),'left'
).select(
    SQ_Shortcut_to_UDH_PRICING_ROLES['*'], 
    LKP_UDH_PRICING_ROLE_LKUP_SRC['PRICING_CATEGORY_ROLE_DESC'].alias('PRICING_CATEG_DESC') 
)

# EXP_LOOKUP_VALUES = EXP_LOOKUP_VALUES.withColumn('o_PRICING_CATEG_DESC',col('ULKP_RETURN_1'))

# Adding deferred logic for dataframe EXP_LOOKUP_VALUES, column o_ONLINE_CATEG_DESC
EXP_LOOKUP_VALUES = EXP_LOOKUP_VALUES.join(LKP_UDH_ONLINE_ROLE_LKUP_SRC, (LKP_UDH_ONLINE_ROLE_LKUP_SRC.ONLINE_CATEGORY_ROLE_ID == EXP_LOOKUP_VALUES.ONLINE_CATEGORY_ROLE_ID),'left'
).select(
    EXP_LOOKUP_VALUES['*'], 
    LKP_UDH_ONLINE_ROLE_LKUP_SRC['ONLINE_CATEGORY_ROLE_DESC'].alias('ONLINE_CATEG_DESC') 
)

# EXP_LOOKUP_VALUES = EXP_LOOKUP_VALUES.withColumn('o_ONLINE_CATEG_DESC',col('ULKP_RETURN_2'))

# COMMAND ----------

# Processing node FIL_LVL5, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE_temp = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE.toDF(*["SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___" + col for col in SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE.columns])

FIL_LVL5 = SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___HIER_NODE as HIER_NODE",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___END_DT as END_DT",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___HIER_LEVEL as HIER_LEVEL",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___PARENT_NODE as PARENT_NODE",
	"SQ_Shortcut_to_SAP_PRICING_HIERARCHY_PRE___NODE_DESC as NODE_DESC").filter("HIER_LEVEL = 5 AND END_DT = DATE'9999-12-31'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Union_INS_DUMMY_REC, type UNION_TRANSFORMATION 
# COLUMN COUNT: 3

Union_INS_DUMMY_REC = FIL_LVL5.select(
	col('HIER_NODE'),
	col('PARENT_NODE'),
	col('NODE_DESC')).unionAll(
SQ_Shortcut_to_DUMMY.select(
	col('HIER_NODE'),
	col('PARENT_NODE'),
	col('NODE_DESC')))

# COMMAND ----------

# Processing node JNR_SAP_PRICING_CATEGORY, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
Union_INS_DUMMY_REC_temp = Union_INS_DUMMY_REC.toDF(*["Union_INS_DUMMY_REC___" + col for col in Union_INS_DUMMY_REC.columns])
EXP_LOOKUP_VALUES_temp = EXP_LOOKUP_VALUES.toDF(*["EXP_LOOKUP_VALUES___" + col for col in EXP_LOOKUP_VALUES.columns])

JNR_SAP_PRICING_CATEGORY = Union_INS_DUMMY_REC_temp.join(EXP_LOOKUP_VALUES_temp,[Union_INS_DUMMY_REC_temp.Union_INS_DUMMY_REC___HIER_NODE == EXP_LOOKUP_VALUES_temp.EXP_LOOKUP_VALUES___SAP_PRICING_CATEGORY_ID],'left_outer').selectExpr(
	"Union_INS_DUMMY_REC___HIER_NODE as HIER_NODE",
	"Union_INS_DUMMY_REC___PARENT_NODE as PARENT_NODE",
	"Union_INS_DUMMY_REC___NODE_DESC as NODE_DESC",
	"EXP_LOOKUP_VALUES___SAP_PRICING_CATEGORY_ID as SAP_PRICING_CATEGORY_ID",
	"EXP_LOOKUP_VALUES___PRICING_CATEGORY_ROLE_ID as PRICING_CATEGORY_ROLE_ID",
	"EXP_LOOKUP_VALUES___ONLINE_CATEGORY_ROLE_ID as ONLINE_CATEGORY_ROLE_ID",
	"EXP_LOOKUP_VALUES___PRICING_CATEG_DESC as PRICING_CATEG_DESC",
	"EXP_LOOKUP_VALUES___ONLINE_CATEG_DESC as ONLINE_CATEG_DESC")

# COMMAND ----------

# Processing node EXP_LVL5, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_SAP_PRICING_CATEGORY_temp = JNR_SAP_PRICING_CATEGORY.toDF(*["JNR_SAP_PRICING_CATEGORY___" + col for col in JNR_SAP_PRICING_CATEGORY.columns])

EXP_LVL5 = JNR_SAP_PRICING_CATEGORY_temp.selectExpr(
	"IF (JNR_SAP_PRICING_CATEGORY___HIER_NODE = '-1', - 1, cast(REGEXP_EXTRACT(JNR_SAP_PRICING_CATEGORY___HIER_NODE, '([A-Z]+)(\\\\d+)', '2') as int)) as o_HIER_NODE",
	"IF (JNR_SAP_PRICING_CATEGORY___PARENT_NODE = '-1', - 1, cast(REGEXP_EXTRACT(JNR_SAP_PRICING_CATEGORY___PARENT_NODE, '([A-Z]+)(\\\\d+)', '2') as int)) as o_PARENT_NODE",
	"JNR_SAP_PRICING_CATEGORY___NODE_DESC as NODE_DESC",
	"CURRENT_TIMESTAMP () as o_LOAD_TSTMP",
	"JNR_SAP_PRICING_CATEGORY___PRICING_CATEGORY_ROLE_ID as PRICING_CATEGORY_ROLE_ID",
	"JNR_SAP_PRICING_CATEGORY___ONLINE_CATEGORY_ROLE_ID as ONLINE_CATEGORY_ROLE_ID",
	"JNR_SAP_PRICING_CATEGORY___PRICING_CATEG_DESC as PRICING_CATEG_DESC",
	"JNR_SAP_PRICING_CATEGORY___ONLINE_CATEG_DESC as ONLINE_CATEG_DESC"
)

# COMMAND ----------

# Processing node Shortcut_to_SAP_PRICING_CATEGORY, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_SAP_PRICING_CATEGORY = EXP_LVL5.selectExpr(
	"CAST(o_HIER_NODE AS INT) as SAP_PRICING_CATEGORY_ID",
	"CAST(o_PARENT_NODE AS INT) as SAP_PRICING_CLASS_ID",
	"CAST(NODE_DESC AS STRING) as SAP_PRICING_CATEGORY_DESC",
	"CAST(PRICING_CATEGORY_ROLE_ID AS INT) as PRICING_CATEGORY_ROLE_ID",
	"CAST(PRICING_CATEG_DESC AS STRING) as PRICING_CATEGORY_ROLE_DESC",
	"CAST(ONLINE_CATEGORY_ROLE_ID AS INT) as ONLINE_CATEGORY_ROLE_ID",
	"CAST(ONLINE_CATEG_DESC AS STRING) as ONLINE_CATEGORY_ROLE_DESC",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)

Shortcut_to_SAP_PRICING_CATEGORY.write.mode("overwrite").saveAsTable(f'{legacy}.SAP_PRICING_CATEGORY')

# COMMAND ----------


