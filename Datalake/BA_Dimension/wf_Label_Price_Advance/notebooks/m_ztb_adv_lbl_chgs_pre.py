# Databricks notebook source
#Code converted on 2023-10-24 21:17:02
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

parameter_file_name='BA_Dimension_Parameter.prm'
parameter_section='BA_Dimension.WF:wf_Label_Price_ADVANCE'
parameter_key='source_bucket'

# parameter_value='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/masterdata/ztb_adv_lbl_chgs/'
# insert_param_config(raw,parameter_file_name,parameter_section,parameter_key,parameter_value)

# COMMAND ----------

source_bucket=getParameterValue(raw,parameter_file_name,parameter_section,parameter_key)
key=getParameterValue(raw,parameter_file_name,parameter_section,'key')
source_file = get_src_file(key, source_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_ZTB_LABEL_CHGS, type APPLICATION_SOURCE_QUALIFIER 
# COLUMN COUNT: 12

SQ_Shortcut_to_ZTB_ADV_LBL_CHGS = spark.read.csv(source_file, sep='|', header='true').withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node EXP_EFFECTIVE_DATE, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_temp = SQ_Shortcut_to_ZTB_ADV_LBL_CHGS.toDF(*["SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___" + col for col in SQ_Shortcut_to_ZTB_ADV_LBL_CHGS.columns])

EXP_EFFECTIVE_DATE = SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_temp.selectExpr(
    "SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___MANDT as MANDT",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___EFFECTIVE_DATE as in_EFFECTIVE_DATE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___ARTICLE as ARTICLE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___SITE as SITE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___POG_TYPE as POG_TYPE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___LABEL_SIZE as LABEL_SIZE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___LABEL_TYPE as LABEL_TYPE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___EXP_LABEL_TYPE as i_EXP_LABEL_TYPE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___SUPPRESS_IND as SUPPRESS_IND",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___NUM_LABELS as NUM_LABELS",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___CREATE_DATE as in_CREATE_DATE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS___ENH_LBL_ID as ENH_LBL_ID").selectExpr(
	"sys_row_id as sys_row_id",
	"MANDT as MANDT",
	"in_EFFECTIVE_DATE as EFFECTIVE_DATE",
	"ARTICLE as ARTICLE",
	"SITE as SITE",
	"POG_TYPE as POG_TYPE",
	"LABEL_SIZE as LABEL_SIZE",
	"LABEL_TYPE as LABEL_TYPE",
	"IF (i_EXP_LABEL_TYPE IS NOT NULL AND UPPER ( i_EXP_LABEL_TYPE ) != 'X', ' ', UPPER ( i_EXP_LABEL_TYPE )) as EXP_LABEL_TYPE",
	"SUPPRESS_IND as SUPPRESS_IND",
	"NUM_LABELS as NUM_LABELS",
	"date_format(in_CREATE_DATE, 'yyyyMMdd') as CREATE_DATE",
	"ENH_LBL_ID as ENH_LBL_ID"
)

# COMMAND ----------

def has_duplicates(df: DataFrame, key_columns: list) -> bool:
	"""
	Check if a DataFrame has duplicate rows based on the specified key columns.

	:param df: The DataFrame to check for duplicates.
	:param key_columns: A list of column names to use as the key for checking duplicates.
	:return: True if duplicates are found, False otherwise.
	"""
	if df.count() == 0:
		return False
	# Group by the key columns and count occurrences
	grouped = df.groupBy(key_columns).count()

	# Find the maximum count
	max_count = grouped.agg({"count": "max"}).collect()[0][0]

	# Return True if duplicates found, False otherwise
	return max_count > 1
    

# COMMAND ----------

# Processing node Shortcut_to_ZTB_ADV_LBL_CHGS_PRE1, type TARGET 
# COLUMN COUNT: 12


Shortcut_to_ZTB_ADV_LBL_CHGS_PRE1 = EXP_EFFECTIVE_DATE.selectExpr(
	"CAST(MANDT AS STRING) as MANDT",
	"CAST(EFFECTIVE_DATE AS STRING) as EFFECTIVE_DATE",
	"CAST(ARTICLE AS STRING) as ARTICLE",
	"CAST(SITE AS STRING) as SITE",
	"CAST(POG_TYPE AS STRING) as POG_TYPE",
	"CAST(LABEL_SIZE AS STRING) as LABEL_SIZE",
	"CAST(LABEL_TYPE AS STRING) as LABEL_TYPE",
	"CAST(EXP_LABEL_TYPE AS STRING) as EXP_LABEL_TYPE",
	"CAST(SUPPRESS_IND AS STRING) as SUPPRESS_IND",
	"CAST(NUM_LABELS AS STRING) as NUM_LABELS",
	"CAST(CREATE_DATE AS STRING) as CREATE_DATE",
	"CAST(ENH_LBL_ID AS STRING) as ENH_LBL_ID"
)

# COMMAND ----------

# DuplicateChecker.check_for_duplicate_primary_keys(Shortcut_to_ZTB_LABEL_CHGS_PRE, ["MANDT", "EFFECTIVE_DATE", "ARTICLE", "SITE", "POG_TYPE", "LABEL_SIZE", "LABEL_TYPE", "EXP_LABEL_TYPE"])

if has_duplicates(Shortcut_to_ZTB_ADV_LBL_CHGS_PRE1, ["MANDT", "EFFECTIVE_DATE", "ARTICLE", "SITE",
                                                      "POG_TYPE", "LABEL_SIZE", "LABEL_TYPE", "EXP_LABEL_TYPE"]):
    raise Exception("Duplicates found in the dataset")
    
    
Shortcut_to_ZTB_ADV_LBL_CHGS_PRE1.write.mode('overwrite').saveAsTable(f'{raw}.ZTB_ADV_LBL_CHGS_PRE')

# COMMAND ----------


