# Databricks notebook source
#Code converted on 2023-09-27 16:52:38
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

# dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/masterdata/ztb_art_loc_site/')
# file_path = dbutils.widgets.get('file_path') #+ f'/{currdate}'

bucket=getParameterValue(raw,'BA_Dimension_Parameter.prm','BA_Dimension.WF:bs_Replenishment_Profile.M:m_ZTB_ART_LOC_SITE_PRE','source_bucket')
source_file = get_src_file('ZTB_ART_LOC_SITE', bucket)


if source_file != None and not fileExists(source_file):
  # print('2')
  # overwrite so trunc for missing data
  spark.sql(f'truncate table {raw}.ZTB_ART_LOC_SITE_PRE')
  dbutils.notebooks.exit(f'{file_path} does not exist')
  



# COMMAND ----------

# Processing node SQ_Shortcut_to_ZTB_ART_LOC_SITE, type APPLICATION_SOURCE_QUALIFIER 
# COLUMN COUNT: 14

# ---------- No handler defined for type APPLICATION_SOURCE_QUALIFIER, node SQ_Shortcut_to_ZTB_ART_LOC_SITE, job m_ZTB_ART_LOC_SITE_PRE ---------- #


SQ_Shortcut_to_ZTB_ART_LOC_SITE = spark.read.option('header',True).option('sep', '|').option('inferSchema',True).csv(source_file).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# SQ_Shortcut_to_ZTB_ART_LOC_SITE.show(truncate=False)

# COMMAND ----------

# Processing node EXP_Load_Tstmp, type EXPRESSION 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ZTB_ART_LOC_SITE_temp = SQ_Shortcut_to_ZTB_ART_LOC_SITE.toDF(*["SQ_Shortcut_to_ZTB_ART_LOC_SITE___" + col for col in SQ_Shortcut_to_ZTB_ART_LOC_SITE.columns])

EXP_Load_Tstmp = SQ_Shortcut_to_ZTB_ART_LOC_SITE_temp.selectExpr(
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___ARTICLE as ARTICLE",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___SITE as SITE",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___POG_ID as POG_ID",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___POG_DBKEY as POG_DBKEY",
	"TO_DATE(SQ_Shortcut_to_ZTB_ART_LOC_SITE___EFFECTIVE_DT,'M/d/yyyy HH:mm:ss') as EFFECTIVE_DT",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___NUMBER_OF_POSITIONS as NUMBER_OF_POSITIONS",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___NUMBER_OF_FACINGS as NUMBER_OF_FACINGS",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___CAPACITY as CAPACITY",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___PRESENTATION_QUANTITY as PRESENTATION_QUANTITY",
	"TO_DATE(SQ_Shortcut_to_ZTB_ART_LOC_SITE___LAST_CHG_DATE,'M/d/yyyy HH:mm:ss') as LAST_CHG_DATE",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___LAST_CHG_TIME as LAST_CHG_TIME",
	"TO_DATE(SQ_Shortcut_to_ZTB_ART_LOC_SITE___EFFECTIVE_ENDT,'M/d/yyyy HH:mm:ss') as EFFECTIVE_ENDT",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___POSITION_STATUS as POSITION_STATUS",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE___POG_TYPE as POG_TYPE",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# EXP_Load_Tstmp.show()

# COMMAND ----------

# Processing node Shortcut_to_ZTB_ART_LOC_SITE_PRE, type TARGET 
# COLUMN COUNT: 15


Shortcut_to_ZTB_ART_LOC_SITE_PRE = EXP_Load_Tstmp.selectExpr(
	"CAST(ARTICLE AS INT) as ARTICLE",
	"CAST(SITE AS INT) as SITE",
	"CAST(POG_ID AS STRING) as POG_ID",
	"CAST(POG_DBKEY AS INT) as POG_DBKEY",
	"CAST(EFFECTIVE_DT AS DATE) as EFFECTIVE_DT",
	"CAST(EFFECTIVE_ENDT AS DATE) as EFFECTIVE_ENDT",
	"CAST(NUMBER_OF_POSITIONS AS INT) as NUMBER_OF_POSITIONS",
	"CAST(NUMBER_OF_FACINGS AS INT) as NUMBER_OF_FACINGS",
	"CAST(CAPACITY AS INT) as CAPACITY",
	"CAST(PRESENTATION_QUANTITY AS INT) as PRESENTATION_QUANTITY",
	"CAST(POSITION_STATUS AS TINYINT) as POSITION_STATUS",
	"CAST(POG_TYPE AS STRING) as POG_TYPE",
	"CAST(LAST_CHG_DATE AS DATE) as LAST_CHG_DATE",
	"CAST(LAST_CHG_TIME AS STRING) as LAST_CHG_TIME",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
# Shortcut_to_ZTB_ART_LOC_SITE_PRE.show()
Shortcut_to_ZTB_ART_LOC_SITE_PRE.write.saveAsTable(f'{raw}.ZTB_ART_LOC_SITE_PRE', mode = 'overwrite')

# COMMAND ----------


