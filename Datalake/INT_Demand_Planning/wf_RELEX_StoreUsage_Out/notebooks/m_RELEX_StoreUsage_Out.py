# Databricks notebook source
# Code converted on 2023-11-27 11:22:11
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

# Variable_declaration_comment

target_bucket=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','target_bucket')
key=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','key')
target_file=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','key')

Movement_Filter=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','Movement_Filter')
Site_Filter=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','Site_Filter')
LAST_RUN_DATE=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','LAST_RUN_DATE')
Delta_Filter=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','Delta_Filter')
Exclude_Flag=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','Exclude_Flag')

# COMMAND ----------

# Variable_declaration_comment
target_bucket=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','target_bucket')
target_file=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','key')

#target_file=target_bucket + key
Last_Run_date=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','LAST_RUN_DATE')
Delta_Filter=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','Delta_Filter')

if Last_Run_date=='12/20/2023' :
    v_max_update_dt=str(spark.sql(f"Select max(UPDATE_DT) as max_dt from {legacy}.MOVEMENT_DAY").first()[0])
    Last_Run_date=v_max_update_dt
    Delta_Filter=f"MOVEMENT_DAY.UPDATE_DT>=to_date({v_max_update_dt},'MM/dd/yyyy')"
else:
    Last_Run_date=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','LAST_RUN_DATE')
    Delta_Filter=getParameterValue(raw,'wf_RELEX_StoreUsage_Out','m_RELEX_StoreUsage_Out','Delta_Filter')

print(Last_Run_date)
print(Delta_Filter)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_SKU_PROFILE_RPT = spark.sql(f"""SELECT
SKU_PROFILE_RPT.PRODUCT_ID,
SKU_PROFILE_RPT.SKU_NBR,
SKU_PROFILE_RPT.SAP_DEPT_ID
FROM {legacy}.SKU_PROFILE_RPT
""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE_RPT = spark.sql(f"""SELECT
SITE_PROFILE_RPT.LOCATION_ID,
SITE_PROFILE_RPT.STORE_NBR
FROM {legacy}.SITE_PROFILE_RPT
WHERE {Site_Filter}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

Site_Filter

# COMMAND ----------

# Processing node SQ_Shortcut_to_MOVEMENT_DAY, type SOURCE 
# COLUMN COUNT: 6

_sql = f"""SELECT MOVEMENT_DAY.DAY_DT, MOVEMENT_DAY.PRODUCT_ID, MOVEMENT_DAY.LOCATION_ID, 

MOVEMENT_DAY.TRANS_QTY, MOVEMENT_DAY.UPDATE_DT ,MOVEMENT_INFO.MOVE_REASON_ID

FROM
  {legacy}.MOVEMENT_DAY

JOIN 

 (

	SELECT DISTINCT DAY_DT,PRODUCT_ID,LOCATION_ID FROM {legacy}.MOVEMENT_DAY
 
    
	WHERE {Delta_Filter}

 ) U ON MOVEMENT_DAY.DAY_DT = U.DAY_DT 

	AND MOVEMENT_DAY.PRODUCT_ID = U.PRODUCT_ID

	AND MOVEMENT_DAY.LOCATION_ID = U.LOCATION_ID

JOIN {legacy}.MOVEMENT_INFO ON MOVEMENT_DAY.MOVEMENT_ID=MOVEMENT_INFO.MOVEMENT_ID

	AND {Movement_Filter}"""

 
SQ_Shortcut_to_MOVEMENT_DAY = spark.sql(f"""{_sql}""").withColumn("sys_row_id", monotonically_increasing_id())
#Conforming fields names to the component layout
SQ_Shortcut_to_MOVEMENT_DAY = SQ_Shortcut_to_MOVEMENT_DAY \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[1],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[2],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[3],'TRANS_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[4],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[5],'MOVE_REASON_ID')

# COMMAND ----------

# Processing node JNR_Location, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_RPT_temp = SQ_Shortcut_to_SITE_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SITE_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SITE_PROFILE_RPT.columns])
SQ_Shortcut_to_MOVEMENT_DAY_temp = SQ_Shortcut_to_MOVEMENT_DAY.toDF(*["SQ_Shortcut_to_MOVEMENT_DAY___" + col for col in SQ_Shortcut_to_MOVEMENT_DAY.columns])

JNR_Location = SQ_Shortcut_to_SITE_PROFILE_RPT_temp.join(SQ_Shortcut_to_MOVEMENT_DAY_temp,[SQ_Shortcut_to_SITE_PROFILE_RPT_temp.SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID == SQ_Shortcut_to_MOVEMENT_DAY_temp.SQ_Shortcut_to_MOVEMENT_DAY___LOCATION_ID],'inner').selectExpr(
	"SQ_Shortcut_to_MOVEMENT_DAY___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_MOVEMENT_DAY___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_MOVEMENT_DAY___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_MOVEMENT_DAY___TRANS_QTY as TRANS_QTY",
	"SQ_Shortcut_to_MOVEMENT_DAY___UPDATE_DT as UPDATE_DT",
	"SQ_Shortcut_to_MOVEMENT_DAY___MOVE_REASON_ID as MOVE_REASON_ID",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___STORE_NBR as STORE_NBR")

# COMMAND ----------

# Processing node JNR_Product, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_Location_temp = JNR_Location.toDF(*["JNR_Location___" + col for col in JNR_Location.columns])
SQ_Shortcut_to_SKU_PROFILE_RPT_temp = SQ_Shortcut_to_SKU_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT.columns])

JNR_Product = SQ_Shortcut_to_SKU_PROFILE_RPT_temp.join(JNR_Location_temp,[SQ_Shortcut_to_SKU_PROFILE_RPT_temp.SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID == JNR_Location_temp.JNR_Location___PRODUCT_ID],'inner').selectExpr(
	"JNR_Location___DAY_DT as DAY_DT",
	"JNR_Location___PRODUCT_ID as PRODUCT_ID",
	"JNR_Location___TRANS_QTY as TRANS_QTY",
	"JNR_Location___STORE_NBR as STORE_NBR",
	"JNR_Location___UPDATE_DT as UPDATE_DT",
	"JNR_Location___MOVE_REASON_ID as MOVE_REASON_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID as PRODUCT_ID1",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SKU_NBR as SKU_NBR",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_DEPT_ID as SAP_DEPT_ID")

# COMMAND ----------

# Processing node EXP_EXCLUDE_FLAG, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_Product_temp = JNR_Product.toDF(*["JNR_Product___" + col for col in JNR_Product.columns])

EXP_EXCLUDE_FLAG = JNR_Product_temp.selectExpr(
	"JNR_Product___DAY_DT as DAY_DT",
	"JNR_Product___TRANS_QTY as TRANS_QTY",
	"JNR_Product___STORE_NBR as STORE_NBR",
	"JNR_Product___UPDATE_DT as UPDATE_DT",
	"JNR_Product___MOVE_REASON_ID as MOVE_REASON_ID",
	"JNR_Product___SKU_NBR as SKU_NBR",
	"JNR_Product___SAP_DEPT_ID as SAP_DEPT_ID",
	f"{Exclude_Flag} as EXCLUDE_FLAG"
)

# COMMAND ----------

# Processing node FIL_EXCLUDE_FLAG, type FILTER 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXP_EXCLUDE_FLAG_temp = EXP_EXCLUDE_FLAG.toDF(*["EXP_EXCLUDE_FLAG___" + col for col in EXP_EXCLUDE_FLAG.columns])

FIL_EXCLUDE_FLAG = EXP_EXCLUDE_FLAG_temp.selectExpr(
	"EXP_EXCLUDE_FLAG___DAY_DT as DAY_DT",
	"EXP_EXCLUDE_FLAG___TRANS_QTY as TRANS_QTY",
	"EXP_EXCLUDE_FLAG___STORE_NBR as STORE_NBR",
	"EXP_EXCLUDE_FLAG___UPDATE_DT as UPDATE_DT",
	"EXP_EXCLUDE_FLAG___SKU_NBR as SKU_NBR",
	"EXP_EXCLUDE_FLAG___EXCLUDE_FLAG as EXCLUDE_FLAG").filter("EXCLUDE_FLAG = '0'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SRT_DAY_STORE_SKU, type SORTER 
# COLUMN COUNT: 5

SRT_DAY_STORE_SKU = FIL_EXCLUDE_FLAG.sort(col('DAY_DT').asc(), col('STORE_NBR').asc(), col('SKU_NBR').asc())

# COMMAND ----------

# Processing node AGG_DAY_STORE_SKU, type AGGREGATOR 
# COLUMN COUNT: 6

AGG_DAY_STORE_SKU = SRT_DAY_STORE_SKU \
	.groupBy("DAY_DT","STORE_NBR","SKU_NBR") \
	.agg( \
	min(col("UPDATE_DT")).alias('UPDATE_DT'),
	sum(col("TRANS_QTY")).alias("TRANS_QTY")
	) \
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Derived_Fields, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
AGG_DAY_STORE_SKU_temp = AGG_DAY_STORE_SKU.toDF(*["AGG_DAY_STORE_SKU___" + col for col in AGG_DAY_STORE_SKU.columns])

EXP_Derived_Fields = AGG_DAY_STORE_SKU_temp.selectExpr(
	"AGG_DAY_STORE_SKU___DAY_DT as DAY_DT",
	"AGG_DAY_STORE_SKU___TRANS_QTY as TRANS_QTY",
	"AGG_DAY_STORE_SKU___STORE_NBR as STORE_NBR",
	"AGG_DAY_STORE_SKU___SKU_NBR as SKU_NBR",
	"AGG_DAY_STORE_SKU___UPDATE_DT as UPDATE_DT",
	"'SALE' as TYPE",
	"'SWO' as PARTNER_CODE",
	"LPAD ( AGG_DAY_STORE_SKU___STORE_NBR , 4 , '0' ) as LOCATION_CODE",
	"date_format(AGG_DAY_STORE_SKU___DAY_DT, 'yyyy-MM-dd') as DATE"
)

# COMMAND ----------

# Processing node Shortcut_to_RELEX_StoreUsage_WO_FF, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_RELEX_StoreUsage_WO_FF = EXP_Derived_Fields.selectExpr(
	"CAST(DATE AS STRING) as `Date`",
	"CAST(LOCATION_CODE AS STRING) as `Location Code`",
	"CAST(SKU_NBR AS STRING) as `Product Code`",
	"CAST(TYPE AS STRING) as `Type`",
	"CAST(TRANS_QTY AS STRING) as `Transaction Quantity`",
	"CAST(PARTNER_CODE AS STRING) as `Partner Code`"
)
target_bucket=target_bucket+datetime.now().strftime("%Y%m%d")+'/'
target_file =  target_file.replace('.csv','') + datetime.today().strftime("_%Y-%m-%d-%H-%m-%S") + '.csv'
writeToFlatFile(Shortcut_to_RELEX_StoreUsage_WO_FF, target_bucket, target_file, 'overwrite' )

# COMMAND ----------

# updating parm config table with latest run date
param_file_name='wf_RELEX_StoreUsage_Out'
param_section='m_RELEX_StoreUsage_Out'

v_param_value_Last_Run_date=spark.sql(f"Select max(UPDATE_DT) as max_dt from {legacy}.MOVEMENT_DAY").first()[0]
v_param_value_Delta_Filter=f"MOVEMENT_DAY.UPDATE_DT > to_date('{v_param_value_Last_Run_date}','MM/dd/yyyy')"

print(v_param_value_Last_Run_date)
print(v_param_value_Delta_Filter)

#spark.sql(f"""Update {raw}.parameter_config set parameter_value="SKU_PROFILE_RPT.UPDATE_DT > '{v_parameter_value}'" where parameter_file_name='{param_file_name}' and parameter_section='{param_section}' and parameter_key in ('SKU_Date_Filter')""")

#spark.sql(f"""Update {raw}.parameter_config set parameter_value='{v_parameter_value}' where parameter_file_name='{param_file_name}' and parameter_section='{param_section}' and parameter_key in ('Last_Run_date')""")

#raw, parameter_file_name, parameter_section, parameter_key, parameter_value
update_param_config(raw, param_file_name, param_section, 'Delta_Filter',v_param_value_Delta_Filter)
update_param_config(raw, param_file_name, param_section, 'LAST_RUN_DATE',v_param_value_Last_Run_date)

# COMMAND ----------

# print(target_bucket)
# print(target_file)

# COMMAND ----------

if env == "prod":
    gs_source_path = target_bucket + target_file
    today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/nas05/edwshare/DataLake/Temp_NZ_Migration/Relex/StoreUsage_Out/" + today + '/'
else:
    gs_source_path = target_bucket + target_file
    #today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test/Relex/StoreUsage_Out/"

# COMMAND ----------

copy_file_to_nas(gs_source_path, nas_target_path)
