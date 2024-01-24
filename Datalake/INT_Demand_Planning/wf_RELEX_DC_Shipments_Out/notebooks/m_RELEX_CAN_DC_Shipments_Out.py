# Databricks notebook source
# Code converted on 2023-11-27 11:20:39
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
target_bucket=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_CAN_DC_Shipments_Out','target_bucket')
target_file=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_CAN_DC_Shipments_Out','key')
Move_Type_Filter=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_CAN_DC_Shipments_Out','Move_Type_Filter')
Store_Filter=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_CAN_DC_Shipments_Out','Store_Filter')
#target_file=target_bucket + key
Last_Run_date=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_CAN_DC_Shipments_Out','Last_Run_Date')
Delta_Filter=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_CAN_DC_Shipments_Out','Delta_Filter')

if Last_Run_date=='1900/01/01' :
    v_max_update_dt=str(spark.sql(f"Select max(UPDATE_DT) as max_dt from {legacy}.MOVEMENT_DAY").first()[0])
    Last_Run_date=v_max_update_dt
    Delta_Filter=f"MOVEMENT_DAY.UPDATE_DT>=to_date({v_max_update_dt},'MM/dd/yyyy')"
else:
    Last_Run_date=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_CAN_DC_Shipments_Out','Last_Run_Date')
    Delta_Filter=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_CAN_DC_Shipments_Out','Delta_Filter')

print(Last_Run_date)
print(Delta_Filter)

# COMMAND ----------

# Processing node SQ_Shortcut_to_MOVEMENT_DAY, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_MOVEMENT_DAY = spark.sql(f"""SELECT MOVEMENT_DAY.DAY_DT, MOVEMENT_DAY.PRODUCT_ID, MOVEMENT_DAY.LOCATION_ID, MOVEMENT_DAY.TRANS_QTY, MOVEMENT_DAY.UPDATE_DT 

FROM {legacy}.MOVEMENT_DAY 

JOIN (SELECT DISTINCT DAY_DT,PRODUCT_ID,LOCATION_ID FROM {legacy}.MOVEMENT_DAY

 WHERE {Delta_Filter}) U

 ON MOVEMENT_DAY.DAY_DT = U.DAY_DT

 AND MOVEMENT_DAY.PRODUCT_ID = U.PRODUCT_ID

 AND MOVEMENT_DAY.LOCATION_ID = U.LOCATION_ID

JOIN {legacy}.MOVEMENT_INFO ON 

MOVEMENT_DAY.MOVEMENT_ID=MOVEMENT_INFO.MOVEMENT_ID

AND {Move_Type_Filter}""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_MOVEMENT_DAY = SQ_Shortcut_to_MOVEMENT_DAY \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[1],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[2],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[3],'TRANS_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_MOVEMENT_DAY.columns[4],'UPDATE_DT')

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE_RPT = spark.sql(f"""SELECT
SKU_PROFILE_RPT.PRODUCT_ID,
SKU_PROFILE_RPT.SKU_NBR
FROM {legacy}.SKU_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE_RPT = spark.sql(f"""SELECT
SITE_PROFILE_RPT.LOCATION_ID,
SITE_PROFILE_RPT.STORE_NBR
FROM {legacy}.SITE_PROFILE_RPT
WHERE {Store_Filter}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_RPT_temp = SQ_Shortcut_to_SITE_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SITE_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SITE_PROFILE_RPT.columns])
SQ_Shortcut_to_MOVEMENT_DAY_temp = SQ_Shortcut_to_MOVEMENT_DAY.toDF(*["SQ_Shortcut_to_MOVEMENT_DAY___" + col for col in SQ_Shortcut_to_MOVEMENT_DAY.columns])

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE_RPT_temp.join(SQ_Shortcut_to_MOVEMENT_DAY_temp,[SQ_Shortcut_to_SITE_PROFILE_RPT_temp.SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID == SQ_Shortcut_to_MOVEMENT_DAY_temp.SQ_Shortcut_to_MOVEMENT_DAY___LOCATION_ID],'inner').selectExpr(
	"SQ_Shortcut_to_MOVEMENT_DAY___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_MOVEMENT_DAY___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_MOVEMENT_DAY___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_MOVEMENT_DAY___TRANS_QTY as TRANS_QTY",
	"SQ_Shortcut_to_MOVEMENT_DAY___UPDATE_DT as UPDATE_DT",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___STORE_NBR as STORE_NBR")

# COMMAND ----------

# Processing node JNR_SKU_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_SKU_PROFILE_RPT_temp = SQ_Shortcut_to_SKU_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT.columns])

JNR_SKU_PROFILE = SQ_Shortcut_to_SKU_PROFILE_RPT_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_SKU_PROFILE_RPT_temp.SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___PRODUCT_ID],'inner').selectExpr(
	"JNR_SITE_PROFILE___DAY_DT as DAY_DT",
	"JNR_SITE_PROFILE___PRODUCT_ID as PRODUCT_ID",
	"JNR_SITE_PROFILE___TRANS_QTY as TRANS_QTY",
	"JNR_SITE_PROFILE___UPDATE_DT as UPDATE_DT",
	"JNR_SITE_PROFILE___STORE_NBR as STORE_NBR",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID as PRODUCT_ID1",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SKU_NBR as SKU_NBR")

# COMMAND ----------

# Processing node AGG_TRANS_QTY, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

AGG_TRANS_QTY = JNR_SKU_PROFILE.selectExpr(
	"DAY_DT as DAY_DT",
	"STORE_NBR as STORE_NBR",
	"SKU_NBR as SKU_NBR",
	"TRANS_QTY as i_TRANS_QTY",
	"UPDATE_DT as UPDATE_DT") \
	.groupBy("DAY_DT","STORE_NBR","SKU_NBR") \
	.agg( \
	sum(col('i_TRANS_QTY')).alias("TRANS_QTY"),
	min(col('UPDATE_DT')).alias('UPDATE_DT')
	) \
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SRT_DAY_SKU_STR, type SORTER 
# COLUMN COUNT: 5

SRT_DAY_SKU_STR = AGG_TRANS_QTY.sort(col('DAY_DT').asc(), col('STORE_NBR').asc(), col('SKU_NBR').asc())

# COMMAND ----------

# Processing node EXP_CONVERSION, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SRT_DAY_SKU_STR_temp = SRT_DAY_SKU_STR.toDF(*["SRT_DAY_SKU_STR___" + col for col in SRT_DAY_SKU_STR.columns])

EXP_CONVERSION = SRT_DAY_SKU_STR_temp.selectExpr(
	"SRT_DAY_SKU_STR___SKU_NBR as SKU_NBR",
	"cast(SRT_DAY_SKU_STR___TRANS_QTY as int) as TRANS_QTY",
	"LPAD ( SRT_DAY_SKU_STR___STORE_NBR , 4 , '0' ) as STORE_NBR",
	"date_format(SRT_DAY_SKU_STR___DAY_DT, 'yyyy-MM-dd') as DAY_DT",
	"'SALE' as TYPE",
	"'' as VALUE",
	"'' as CURRENCY",
	"'' as PURCHASE_PRICE",
	"'' as SECONDARY_CURR_VALUE",
	"'' as SECONDARY_CURR_PURCHASE"
)

# COMMAND ----------

# Processing node FIL_ZERO_TRANS, type FILTER 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
EXP_CONVERSION_temp = EXP_CONVERSION.toDF(*["EXP_CONVERSION___" + col for col in EXP_CONVERSION.columns])

FIL_ZERO_TRANS = EXP_CONVERSION_temp.selectExpr(
	"EXP_CONVERSION___DAY_DT as DAY_DT",
	"EXP_CONVERSION___STORE_NBR as STORE_NBR",
	"EXP_CONVERSION___SKU_NBR as SKU_NBR",
	"EXP_CONVERSION___TYPE as TYPE",
	"EXP_CONVERSION___TRANS_QTY as TRANS_QTY",
	"EXP_CONVERSION___VALUE as VALUE",
	"EXP_CONVERSION___CURRENCY as CURRENCY",
	"EXP_CONVERSION___PURCHASE_PRICE as PURCHASE_PRICE",
	"EXP_CONVERSION___SECONDARY_CURR_VALUE as SECONDARY_CURR_VALUE",
	"EXP_CONVERSION___SECONDARY_CURR_PURCHASE as SECONDARY_CURR_PURCHASE").filter("TRANS_QTY != '0'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_RELEX_DC_Shipments_FF, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_RELEX_DC_Shipments_FF = FIL_ZERO_TRANS.selectExpr(
	"CAST(DAY_DT AS STRING) as DATE",
	"CAST(STORE_NBR AS STRING) as LOCATION_CODE",
	"CAST(SKU_NBR AS STRING) as PRODUCT_CODE",
	"CAST(TYPE AS STRING) as TYPE",
	"TRANS_QTY as TRANSACTION_QUANTITY",
	"CAST(VALUE AS STRING) as VALUE",
	"CAST(CURRENCY AS STRING) as CURRENCY",
	"CAST(PURCHASE_PRICE AS STRING) as PURCHASE_PRICE",
	"CAST(SECONDARY_CURR_VALUE AS STRING) as SECONDARY_CURRENCY_VALUE",
	"CAST(SECONDARY_CURR_PURCHASE AS STRING) as SECONDARY_CURRENCY_PURCHASE_PRICE"
)
input_path=target_bucket+datetime.now().strftime("%Y%m%d")+'/'

pre_session_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter","|")
    .load(input_path)
    ).withColumn("LOCATION_CODE",lpad(col("LOCATION_CODE"),4,'0'))


Shortcut_to_RELEX_DC_Shipments_FF = Shortcut_to_RELEX_DC_Shipments_FF.unionByName(pre_session_df)
# display(pre_session_df)
target_bucket=target_bucket+datetime.now().strftime("%Y%m%d")+'/'
target_file =  target_file.replace('.csv','') + datetime.today().strftime("_%Y-%m-%d-%H-%m-%S") + '.csv'

writeToFlatFile(Shortcut_to_RELEX_DC_Shipments_FF, target_bucket, target_file, 'append' )


# COMMAND ----------

print(target_bucket)
print(target_file)

# COMMAND ----------

# Deleting the 1st notebook as it is appending to 2nd file in the same location

file_lst = dbutils.fs.ls(input_path)
for file in file_lst:
    if file.name!=target_file:
       dbutils.fs.rm(input_path+file.name)


# COMMAND ----------

if env == "prod":
    gs_source_path = target_bucket + target_file
    today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/nas05/edwshare/DataLake/Temp_NZ_Migration/Relex/DC_Shipments_Out/" + today + '/'
else:
    gs_source_path = target_bucket + target_file
    #today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test/Relex/DC_Shipments_Out"

# COMMAND ----------

gs_source_path

# COMMAND ----------

copy_file_to_nas(gs_source_path, nas_target_path)


# COMMAND ----------

# updating parm config table with latest run date
param_file_name='wf_RELEX_DC_Shipments_Out'
param_section='m_RELEX_CAN_DC_Shipments_Out'

v_param_value_Last_Run_date=spark.sql(f"Select max(UPDATE_DT) as max_dt from {legacy}.MOVEMENT_DAY").first()[0]
v_param_value_Delta_Filter=f"MOVEMENT_DAY.UPDATE_DT > to_date('{v_param_value_Last_Run_date}','MM/dd/yyyy')"

print(v_param_value_Last_Run_date)
print(v_param_value_Delta_Filter)

#spark.sql(f"""Update {raw}.parameter_config set parameter_value="SKU_PROFILE_RPT.UPDATE_DT > '{v_parameter_value}'" where parameter_file_name='{param_file_name}' and parameter_section='{param_section}' and parameter_key in ('SKU_Date_Filter')""")

#spark.sql(f"""Update {raw}.parameter_config set parameter_value='{v_parameter_value}' where parameter_file_name='{param_file_name}' and parameter_section='{param_section}' and parameter_key in ('Last_Run_date')""")

#raw, parameter_file_name, parameter_section, parameter_key, parameter_value
update_param_config(raw, param_file_name, param_section, 'Delta_Filter',v_param_value_Delta_Filter)
update_param_config(raw, param_file_name, param_section, 'LAST_RUN_DATE',v_param_value_Last_Run_date)
