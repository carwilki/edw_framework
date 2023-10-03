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


# COMMAND ----------

# Processing node SQ_ic_wc_ca, type SOURCE 
# COLUMN COUNT: 24

#dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-qa-raw-p1-gcs-gbl/nas/incident_management/canada_loss/')
#source_bucket = dbutils.widgets.get('source_bucket')
source_bucket = getParameterValue(raw,'BA_Incident_Management_Parameter.prm','BA_Incident_Management.WF:wf_ic_wc_claims_CA','source_bucket')


def get_source_file(key, _bucket):
  import builtins

  lst = dbutils.fs.ls(_bucket)
  fldr = builtins.max(lst, key=lambda x: x.name).name
  lst = dbutils.fs.ls(_bucket + fldr)
  files = [x.path for x in lst if x.name.startswith(key)]
  return files[0] if files else None
  
source_file = get_source_file('Canada_Loss', source_bucket)
print(source_file)
SQ_ic_wc_ca = spark.read.csv(source_file, sep=',', header=True, multiLine=True,escape= '\"').withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node Shortcut_to_IC_CA_CLAIMS, type TARGET 
# COLUMN COUNT: 26


Shortcut_to_IC_CA_CLAIMS = SQ_ic_wc_ca \
    .withColumn("v_STORE_NBR", expr("ifnull(int(substring(substring(LocationName, 1, instr(LocationName, ' - ') - 1), 3, 4)),0)")) \
    .selectExpr(
	"CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as LOAD_DT",
	"CAST(CaseNo AS STRING) as CLAIM_NBR",
	"CAST(v_STORE_NBR AS STRING) as STORE_NBR",
	"CAST(`Geographic Location` AS STRING) as GEO_LOCATION_NAME",
	"CAST(Jurisdiction AS STRING) as JURISDICTION",
	"CAST(LocationName AS STRING) as LOCATION_NAME",
	"CAST(Classification AS STRING) as CLASSIFICATION",
	"CAST(EMPLOYEE AS STRING) as EMPLOYEE_NAME",
	"to_timestamp(`Date Injured`,'d/M/y') as INJURED_DT", 
	"to_timestamp(`Date Reported`,'d/M/y') as REPORTED_DT",
	"to_timestamp(`Date Opened`,'d/M/y') as OPENED_DT",
	"CAST(Description AS STRING) as DESCRIPTION",
	"CAST(`Part Of Body` AS STRING) as BODY_PART",
	"CAST(`Nature Of Injury` AS STRING) as NATYRE_OF_INJURY_DESC",
	"CAST(`Claim Status` AS STRING) as CLAIM_STATUS",
	"CAST(`Case Manager` AS STRING) as CASE_MANAGER",
	"CAST(`Current Status` AS STRING) as CURRENT_STATUS",
	"CAST(`Accident Location` AS STRING) as ACCIDENT_LOCATION",
  "REGEXP_REPLACE(`Current Position`, '(' || CHR ( 10 ) || '|' ||CHR ( 13 ) || ')', ' ' ) as EMPLOYEE_JOB_TITLE",
	"CAST(Department AS STRING) as DEPARTMENT",
	"CAST(EmployeeNo AS STRING) as EMPLOYEE_ID",
	"CAST(`Accident Activity` AS STRING) as CLAIMANT_ACTIVITY_AT_INCIDENT",
	"CAST(`Animal Incident Type` AS STRING) as ANIMAL_INCIDENT_TYPE",
	"CAST(`Animal Type` AS STRING) as PET_BREED_TYPE",
  "IF (`Time of Incident` IS NOT NULL, LPAD(trim(`Time of Incident`), 11 , '0' ), NULL) as TIME_OF_INCIDENT",
	"CAST(`Animal Care Custody` AS STRING) as ANIMAL_CARE_CUSTODY"
)



# COMMAND ----------

# get pyspark_data_action column: 0 - for new row1, 1 - for existing 
# Added a filter as the source files had NULLs in the CaseNo column

Shortcut_to_IC_CA_CLAIMS = Shortcut_to_IC_CA_CLAIMS.withColumn("pyspark_data_action", lit(0)).filter("CLAIM_NBR is not null")

# COMMAND ----------

try:
	primary_key = "source.CLAIM_NBR = target.CLAIM_NBR AND source.LOAD_DT = target.LOAD_DT"
	refined_perf_table = f"{legacy}.IC_CA_CLAIMS"
	executeMerge(Shortcut_to_IC_CA_CLAIMS, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("IC_CA_CLAIMS", "IC_CA_CLAIMS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("IC_CA_CLAIMS", "IC_CA_CLAIMS","Failed",str(e), f"{raw}.log_run_details", )
	raise e
    
