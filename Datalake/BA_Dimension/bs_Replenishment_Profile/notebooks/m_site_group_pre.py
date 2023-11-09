# Databricks notebook source
#Code converted on 2023-09-26 15:05:32
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
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/replenishment_profile/')
# file_path = dbutils.widgets.get('file_path') # + f'/{currdate}'


file_path=getParameterValue(raw,'BA_Dimension_Parameter.prm','BA_Dimension.WF:bs_Replenishment_Profile.M:m_site_group_pre','source_bucket')
source_file = get_src_file('sitegroup', file_path)

#source_file= 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/replenishment_profile/20231017/sitegroup20231016_170603.dat'
#source_file= 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/replenishment_profile/20231028/sitegroup20231027_170503.dat'


# COMMAND ----------

if source_file  is None:
  dbutils.notebook.exit(f'{source_file} does not exist.')

# COMMAND ----------

# Processing node SQ_Shortcut_To_SITEGROUP, type SOURCE 
# COLUMN COUNT: 4
# source_file='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/replenishment_profile/20230830/sitegroup20230829_170528.dat'
fixed_width_data = spark.read.text(source_file)
columns = [
	expr('substring(value, 1, 1)').alias('DELETE_IND'),
	expr('substring(value, 2, 18)').alias('SITE_GROUP_CD'),
	expr('substring(value, 20, 4)').alias('STORE_NBR'),
	expr('substring(value, 24, 40)').alias('SITE_GROUP_DESC'),
]

# fixed_width_data.show()
SQ_Shortcut_To_SITEGROUP = fixed_width_data.select(*columns).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_SITEGROUP = SQ_Shortcut_To_SITEGROUP \
	.withColumnRenamed(SQ_Shortcut_To_SITEGROUP.columns[0],'DELETE_IND') \
	.withColumnRenamed(SQ_Shortcut_To_SITEGROUP.columns[1],'SITE_GROUP_CD') \
	.withColumnRenamed(SQ_Shortcut_To_SITEGROUP.columns[2],'STORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_SITEGROUP.columns[3],'SITE_GROUP_DESC')

# SQ_Shortcut_To_SITEGROUP.show(truncate=False)

# COMMAND ----------

# Processing node EXP_TRIM_DESC, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_SITEGROUP_temp = SQ_Shortcut_To_SITEGROUP.toDF(*["SQ_Shortcut_To_SITEGROUP___" + col for col in SQ_Shortcut_To_SITEGROUP.columns])

EXP_TRIM_DESC = SQ_Shortcut_To_SITEGROUP_temp.selectExpr(
	"SQ_Shortcut_To_SITEGROUP___sys_row_id as sys_row_id",
	"SQ_Shortcut_To_SITEGROUP___STORE_NBR as STORE_NBR",
	"RTRIM ( SQ_Shortcut_To_SITEGROUP___SITE_GROUP_CD ) as SITE_GROUP_CD_OUT",
	"SQ_Shortcut_To_SITEGROUP___DELETE_IND as DELETE_IND",
	"RTRIM ( SQ_Shortcut_To_SITEGROUP___SITE_GROUP_DESC ) as SITE_GROUP_DESC_OUT"
)

# COMMAND ----------

# Processing node Shortcut_To_SITE_GROUP_PRE, type TARGET 
# COLUMN COUNT: 4


Shortcut_To_SITE_GROUP_PRE = EXP_TRIM_DESC.selectExpr(
	"CAST(STORE_NBR AS INT) as STORE_NBR",
	"CAST(SITE_GROUP_CD_OUT AS STRING) as SITE_GROUP_CD",
	"CAST(DELETE_IND AS STRING) as DELETE_IND",
	"CAST(SITE_GROUP_DESC_OUT AS STRING) as SITE_GROUP_DESC"
)
Shortcut_To_SITE_GROUP_PRE.write.saveAsTable(f'{raw}.SITE_GROUP_PRE', mode = 'overwrite')
