# Databricks notebook source
#Code converted on 2023-09-11 16:18:55
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
empl_protected = getEnvPrefix(env) + 'empl_protected'

# COMMAND ----------

# Processing node SQ_Shortcut_to_IC_CA_CLAIMS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_IC_CA_CLAIMS = spark.sql(f"""SELECT
     icc.CLAIM_NBR, icc.INJURED_DT,
     icc.NATYRE_OF_INJURY_DESC,
     icc.CLAIM_STATUS,
     sp.LOCATION_ID
FROM {legacy}.IC_CA_CLAIMS icc, {legacy}.SITE_PROFILE sp
where icc.store_nbr = sp.store_nbr 
  and icc.load_dt > current_date - INTERVAL 1 DAY""").withColumn("sys_row_id", monotonically_increasing_id())
  
# Conforming fields names to the component layout
SQ_Shortcut_to_IC_CA_CLAIMS = SQ_Shortcut_to_IC_CA_CLAIMS \
	.withColumnRenamed(SQ_Shortcut_to_IC_CA_CLAIMS.columns[0],'CLAIM_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_IC_CA_CLAIMS.columns[1],'INJURED_DT') \
	.withColumnRenamed(SQ_Shortcut_to_IC_CA_CLAIMS.columns[2],'NATYRE_OF_INJURY_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_IC_CA_CLAIMS.columns[3],'CLAIM_STATUS') \
	.withColumnRenamed(SQ_Shortcut_to_IC_CA_CLAIMS.columns[4],'LOCATION_ID')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_IC_CA_CLAIMS_temp = SQ_Shortcut_to_IC_CA_CLAIMS.toDF(*["SQ_Shortcut_to_IC_CA_CLAIMS___" + col for col in SQ_Shortcut_to_IC_CA_CLAIMS.columns])

EXPTRANS = SQ_Shortcut_to_IC_CA_CLAIMS_temp.selectExpr(
	"SQ_Shortcut_to_IC_CA_CLAIMS___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_IC_CA_CLAIMS___INJURED_DT as DAY_DT",
	"SQ_Shortcut_to_IC_CA_CLAIMS___CLAIM_NBR as CLAIM_NBR",
	"SQ_Shortcut_to_IC_CA_CLAIMS___LOCATION_ID as LOCATION_ID",
	"2 as SOURCE_TYPE_ID",
	"'WC' as LINE_TYPE_CD",
	"CASE SQ_Shortcut_to_IC_CA_CLAIMS___NATYRE_OF_INJURY_DESC WHEN 'Bite, Insect' THEN '1' ELSE '0' END AS ANIMAL_IND",
	"SUBSTR ( SQ_Shortcut_to_IC_CA_CLAIMS___CLAIM_STATUS , 1 , 1 ) as CLAIM_SUB_STATUS",
	"0 as INCURRED_MEDICAL_AMT",
	"0 as INCURRED_EXP_AMT",
	"0 as INCURRED_INDEM_LOSS_AMT",
	"0 as INCURRED_OTHER_EXP_AMT",
	"0 as INCURRED_TOTAL_AMT",
	"CURRENT_TIMESTAMP as LOAD_DT",
	"CURRENT_TIMESTAMP as UPDATE_DT"
)

# COMMAND ----------

# Processing node UPD_ca_ic_wc_claims, type UPDATE_STRATEGY 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])

UPD_ca_ic_wc_claims = EXPTRANS_temp.selectExpr(
	"EXPTRANS___DAY_DT as DAY_DT",
	"EXPTRANS___CLAIM_NBR as CLAIM_NBR",
	"EXPTRANS___LOCATION_ID as LOCATION_ID",
	"EXPTRANS___SOURCE_TYPE_ID as SOURCE_TYPE_ID",
	"EXPTRANS___LINE_TYPE_CD as LINE_TYPE_CD",
	"EXPTRANS___ANIMAL_IND as ANIMAL_IND",
	"EXPTRANS___CLAIM_SUB_STATUS as CLAIM_SUB_STATUS",
	"EXPTRANS___INCURRED_MEDICAL_AMT as INCURRED_MEDICAL_AMT",
	"EXPTRANS___INCURRED_EXP_AMT as INCURRED_EXP_AMT",
	"EXPTRANS___INCURRED_INDEM_LOSS_AMT as INCURRED_INDEM_LOSS_AMT",
	"EXPTRANS___INCURRED_OTHER_EXP_AMT as INCURRED_OTHER_EXP_AMT",
	"EXPTRANS___INCURRED_TOTAL_AMT as INCURRED_TOTAL_AMT",
	"EXPTRANS___LOAD_DT as LOAD_DT",
	"EXPTRANS___UPDATE_DT as UPDATE_DT") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------

# Processing node Shortcut_to_IC_WC_CLAIMS, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_IC_WC_CLAIMS = UPD_ca_ic_wc_claims.selectExpr(
	"CAST(CLAIM_NBR AS STRING) as CLAIM_NBR",
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(SOURCE_TYPE_ID AS INT) as SOURCE_TYPE_ID",
	"CAST(LINE_TYPE_CD AS STRING) as LINE_TYPE_CD",
	"CAST(ANIMAL_IND AS INT) as ANIMAL_IND",
	"CAST(CLAIM_SUB_STATUS AS STRING) as CLAIM_SUB_STATUS_CD",
	"CAST(INCURRED_MEDICAL_AMT AS DECIMAL(11,2)) as INCURRED_EXP_AMT",
	"CAST(INCURRED_EXP_AMT AS DECIMAL(11,2)) as INCURRED_MEDICAL_AMT",
	"CAST(INCURRED_INDEM_LOSS_AMT AS DECIMAL(11,2)) as INCURRED_INDEM_LOSS_AMT",
	"CAST(INCURRED_OTHER_EXP_AMT AS DECIMAL(11,2)) as INCURRED_OTHER_EXP_AMT",
	"CAST(INCURRED_TOTAL_AMT AS DECIMAL(11,2)) as INCURRED_TOTAL_AMT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"pyspark_data_action as pyspark_data_action"
)




# COMMAND ----------

try:
	primary_key = "source.CLAIM_NBR = target.CLAIM_NBR"
	refined_perf_table = f"{empl_protected}.legacy_IC_WC_CLAIMS"
	executeMerge(Shortcut_to_IC_WC_CLAIMS, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("IC_WC_CLAIMS", "IC_WC_CLAIMS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("IC_WC_CLAIMS", "IC_WC_CLAIMS","Failed",str(e), f"{raw}.log_run_details", )
	raise e
