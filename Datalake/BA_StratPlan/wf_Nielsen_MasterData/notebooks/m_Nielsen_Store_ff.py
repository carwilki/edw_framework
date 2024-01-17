# Databricks notebook source
# Code converted on 2023-12-07 08:16:56
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
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

# Processing node SQ_Shortcut_to_SALES_INSTANCE_SKEY, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SALES_INSTANCE_SKEY = spark.sql(f"""SELECT DISTINCT
SALES_INSTANCE_SKEY.LOCATION_ID,
SALES_INSTANCE_SKEY.REGISTER_NBR
FROM {legacy}.SALES_INSTANCE_SKEY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node AGGTRANS, type AGGREGATOR 
# COLUMN COUNT: 2

AGGTRANS = SQ_Shortcut_to_SALES_INSTANCE_SKEY \
	.groupBy("LOCATION_ID") \
	.agg( \
	count(col("REGISTER_NBR")).alias("NUM_OF_REGS")
	) \
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_SITE_PROFILE_RPT = spark.sql(f"""SELECT
SITE_PROFILE_RPT.LOCATION_ID,
SITE_PROFILE_RPT.STORE_NBR,
SITE_PROFILE_RPT.STORE_NAME,
SITE_PROFILE_RPT.SITE_ADDRESS,
SITE_PROFILE_RPT.SITE_CITY,
SITE_PROFILE_RPT.STATE_CD,
SITE_PROFILE_RPT.POSTAL_CD,
SITE_PROFILE_RPT.SITE_MAIN_TELE_NO,
SITE_PROFILE_RPT.GR_OPEN_DT,
SITE_PROFILE_RPT.CLOSE_DT,
SITE_PROFILE_RPT.SQ_FEET_RETAIL
FROM {legacy}.SITE_PROFILE_RPT
WHERE TRIM(COUNTRY_CD) = 'US' AND

TRIM(STATE_CD) <> 'PR' AND

LOCATION_TYPE_ID in (6,8)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNRTRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_RPT_temp = SQ_Shortcut_to_SITE_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SITE_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SITE_PROFILE_RPT.columns])
AGGTRANS_temp = AGGTRANS.toDF(*["AGGTRANS___" + col for col in AGGTRANS.columns])

JNRTRANS = AGGTRANS_temp.join(SQ_Shortcut_to_SITE_PROFILE_RPT_temp,[AGGTRANS_temp.AGGTRANS___LOCATION_ID == SQ_Shortcut_to_SITE_PROFILE_RPT_temp.SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID],'inner').selectExpr(
	"SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___STORE_NBR as STORE_NBR",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___STORE_NAME as STORE_NAME",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___SITE_ADDRESS as SITE_ADDRESS",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___SITE_CITY as SITE_CITY",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___STATE_CD as STATE_CD",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___POSTAL_CD as POSTAL_CD",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___SITE_MAIN_TELE_NO as SITE_MAIN_TELE_NO",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___SQ_FEET_RETAIL as SQ_FEET_RETAIL",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___GR_OPEN_DT as GR_OPEN_DT",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___CLOSE_DT as CLOSE_DT",
	"AGGTRANS___LOCATION_ID as LOCATION_ID1",
	"AGGTRANS___NUM_OF_REGS as NUM_OF_REGS")

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
JNRTRANS_temp = JNRTRANS.toDF(*["JNRTRANS___" + col for col in JNRTRANS.columns])

EXPTRANS = JNRTRANS_temp.selectExpr(
	"JNRTRANS___STORE_NBR as STORE_NBR",
	"JNRTRANS___STORE_NAME as STORE_NAME",
	"JNRTRANS___SITE_ADDRESS as SITE_ADDRESS",
	"JNRTRANS___SITE_CITY as SITE_CITY",
	"RTRIM(JNRTRANS___STATE_CD) as STATE_CD",
	"JNRTRANS___POSTAL_CD as POSTAL_CD",
	"JNRTRANS___SITE_MAIN_TELE_NO as SITE_MAIN_TELE_NO",
	"JNRTRANS___SQ_FEET_RETAIL as SQ_FEET_RETAIL",
	"IF (JNRTRANS___GR_OPEN_DT = to_date ( '12/31/9999' , 'MM/dd/yyyy' ), '', date_format(JNRTRANS___GR_OPEN_DT, 'MM/dd/yyyy')) as GR_OPEN_DT",
	"IF (JNRTRANS___CLOSE_DT = to_date ( '12/31/9999' , 'MM/dd/yyyy' ), '', date_format(JNRTRANS___CLOSE_DT, 'MM/dd/yyyy')) as CLOSE_DT",
	"JNRTRANS___NUM_OF_REGS as NUM_OF_REGS",
	"'N' as ANSWER_N"
)

# COMMAND ----------

# Processing node SRTTRANS, type SORTER 
# COLUMN COUNT: 13

SRTTRANS = EXPTRANS.sort(col('STORE_NBR').asc())

# COMMAND ----------

# Processing node Shortcut_to_Nielsen_Store_Flat, type TARGET 
# COLUMN COUNT: 22


Shortcut_to_Nielsen_Store_Flat = SRTTRANS.selectExpr(
	"STORE_NBR as STORE_NBR",
	"STORE_NAME as STORE_NAME",
	"'' as BANNER_NAME",
	"SITE_ADDRESS	 as STREET_ADDRESS",
	"SITE_CITY as CITY",
	"STATE_CD as STATE",
	"POSTAL_CD as ZIP",
	"SITE_MAIN_TELE_NO as PHONE",
	"CAST(SQ_FEET_RETAIL as INT) as SQ_FEET_RETAIL",
	"NUM_OF_REGS as NO_OF_REGS",
	"ANSWER_N as IS_BEER_SOLD",
	"ANSWER_N as IS_WINE_SOLD",
	"ANSWER_N as IS_LIQUOR_SOLD",
	"ANSWER_N as IS_GAS_STATION",
	"GR_OPEN_DT as GRAND_OPEN_DATE",
	"CLOSE_DT as CLOSING_DATE",
	"cast(null as string) as DIVESTITURES_DATE",
	"cast(null as string) as REMODELING_COMP_DATE",
	"cast(null as string) as OPEN_DURING_REMODELING"
)

target_bucket=getParameterValue(raw,'wf_Nielsen_MasterData','m_Nielsen_Store_ff','target_bucket')
target_file=getParameterValue(raw,'wf_Nielsen_MasterData','m_Nielsen_Store_ff','key')

target_bucket=target_bucket+datetime.now().strftime("%Y%m%d")+'/'
target_file=target_file.replace('.txt','')+'_'+datetime.now().strftime("%Y%m%d")+'.txt'

# writeToFlatFile(Shortcut_to_Nielsen_Store_Flat, target_bucket, target_file, 'overwrite' )
writeToFlatFile_withoutQuotes(Shortcut_to_Nielsen_Store_Flat, target_bucket, target_file, 'overwrite' )

# COMMAND ----------

print(target_bucket+target_file)

# COMMAND ----------

if env == "prod":
    gs_source_path = target_bucket + target_file
    today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/nas05/edwshare/DataLake/Temp_NZ_Migration/Nielsen/petm_store/" + today + '/'
else:
    gs_source_path = target_bucket + target_file
    #today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test/Nielsen/petm_store/"
try:
    writeToFlatFile_withoutQuotes(Shortcut_to_Nielsen_Store_Flat, target_bucket, target_file, 'overwrite' )
    copy_file_to_nas(gs_source_path, nas_target_path)
 
except Exception as e:
    raise e

# COMMAND ----------


