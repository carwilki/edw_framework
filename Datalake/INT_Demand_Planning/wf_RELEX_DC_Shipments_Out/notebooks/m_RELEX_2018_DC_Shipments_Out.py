# Databricks notebook source
# Code converted on 2023-11-27 11:20:37
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
target_bucket=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_2018_DC_Shipments_Out','target_bucket')
key=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_2018_DC_Shipments_Out','key')
Filter=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_2018_DC_Shipments_Out','Filter')
target_file=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_2018_DC_Shipments_Out','key')

#target_file=target_bucket + key
Last_Run_date=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_2018_DC_Shipments_Out','Last_Run_Date')
Delta_Filter1=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_2018_DC_Shipments_Out','Delta_Filter1')
Delta_Filter2=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_2018_DC_Shipments_Out','Delta_Filter2')

if Last_Run_date=='01/01/1900' :
    v_max_update_dt1=str(spark.sql(f"Select max(UPDATE_TSTMP) as max_dt from {legacy}.WM_ORDERS").first()[0])
    v_max_update_dt2=str(spark.sql(f"Select max(UPDATE_TSTMP) as max_dt from {legacy}.WM_ORDER_LINE_ITEM").first()[0])
    Last_Run_date=v_max_update_dt1
    Delta_Filter1=f"WM_ORDERS.UPDATE_TSTMP>=to_date({v_max_update_dt1},'MM/dd/yyyy')"
    Delta_Filter2=f"WM_ORDER_LINE_ITEM.UPDATE_TSTMP>=to_date({v_max_update_dt2},'MM/dd/yyyy')"
else:
    Last_Run_date=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_2018_DC_Shipments_Out','Last_Run_Date')
    Delta_Filter1=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_2018_DC_Shipments_Out','Delta_Filter1')
    Delta_Filter2=getParameterValue(raw,'wf_RELEX_DC_Shipments_Out','m_RELEX_2018_DC_Shipments_Out','Delta_Filter2')

print(Last_Run_date)
print(Delta_Filter1)
print(Delta_Filter2)

# COMMAND ----------

# Processing node SQ_Shortcut_To_SKU_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_SKU_PROFILE = spark.sql(f"""SELECT
SKU_PROFILE.PRODUCT_ID,
SKU_PROFILE.SKU_NBR
FROM {legacy}.SKU_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_To_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM {legacy}.SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_WM_ORDERS, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_WM_ORDERS = spark.sql(f"""select O.LOCATION_ID , O.WM_ORDER_ID , O.WM_ACTUAL_SHIPPED_TSTMP , O.UPDATE_TSTMP,

 OLI.LOCATION_ID AS OLN_LOCATION_ID, OLI.WM_ORDER_ID AS OLN_WM_ORDER_ID, OLI.WM_ITEM_NAME AS OLN_WM_ITEM_NAME, OLI.SHIPPED_QTY AS OLN_SHIPPED_QTY,OLI.UPDATE_TSTMP AS OLN_UPDATE_TSTMP

	 from 

	  (

SELECT A.LOCATION_ID, A.WM_ORDER_ID, A.WM_ITEM_NAME, A.SHIPPED_QTY,A.UPDATE_TSTMP 

FROM

{legacy}.WM_ORDER_LINE_ITEM A

JOIN ( 

SELECT DISTINCT LOCATION_ID,WM_ITEM_NAME

		FROM {legacy}.WM_ORDER_LINE_ITEM 

		WHERE {Delta_Filter2}

		)B

		ON A.LOCATION_ID = B.LOCATION_ID

		AND A.WM_ITEM_NAME = B.WM_ITEM_NAME ) OLI

		JOIN 

		(

		SELECT

	 DISTINCT WM_ORDERS.LOCATION_ID , WM_ORDERS.WM_ORDER_ID , WM_ORDERS.WM_ACTUAL_SHIPPED_TSTMP , WM_ORDERS.UPDATE_TSTMP

FROM

	{legacy}.WM_ORDERS 

LEFT OUTER JOIN {legacy}.WM_LPN ON WM_ORDERS.LOCATION_ID=WM_ORDERS.LOCATION_ID 

AND WM_ORDERS.WM_TC_ORDER_ID=WM_LPN.WM_TC_ORDER_ID 

WHERE

	 {Delta_Filter1}  

AND {Filter}

		) O

		on OLI.location_id=O.location_id 

and 

		OLI.WM_ORDER_ID=O.WM_ORDER_ID""").withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_Shortcut_to_WM_ORDERS = SQ_Shortcut_to_WM_ORDERS \
	.withColumnRenamed(SQ_Shortcut_to_WM_ORDERS.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_WM_ORDERS.columns[1],'WM_ORDER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_WM_ORDERS.columns[2],'WM_ACTUAL_SHIPPED_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_WM_ORDERS.columns[3],'UPDATE_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_WM_ORDERS.columns[4],'oln_LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_WM_ORDERS.columns[5],'oln_WM_ORDER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_WM_ORDERS.columns[6],'oln_WM_ITEM_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_WM_ORDERS.columns[7],'oln_SHIPPED_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_WM_ORDERS.columns[8],'oln_UPDATE_TSTMP')

# COMMAND ----------

# Processing node EXP_TO_CHAR, type EXPRESSION 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_SITE_PROFILE_temp = SQ_Shortcut_To_SITE_PROFILE.toDF(*["SQ_Shortcut_To_SITE_PROFILE___" + col for col in SQ_Shortcut_To_SITE_PROFILE.columns])

EXP_TO_CHAR = SQ_Shortcut_To_SITE_PROFILE_temp.selectExpr(
	"SQ_Shortcut_To_SITE_PROFILE___sys_row_id as sys_row_id",
	"SQ_Shortcut_To_SITE_PROFILE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_To_SITE_PROFILE___STORE_NBR as o_STORE_NBR"
)

# COMMAND ----------

# Processing node EXP_INT_CONV, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ORDERS_temp = SQ_Shortcut_to_WM_ORDERS.toDF(*["SQ_Shortcut_to_WM_ORDERS___" + col for col in SQ_Shortcut_to_WM_ORDERS.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_ORDERS_temp.selectExpr(
	"SQ_Shortcut_to_WM_ORDERS___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_WM_ORDERS___WM_ORDER_ID as WM_ORDER_ID",
	"SQ_Shortcut_to_WM_ORDERS___WM_ACTUAL_SHIPPED_TSTMP as o_WM_ACTUAL_SHIPPED_TSTMP",
	"SQ_Shortcut_to_WM_ORDERS___oln_LOCATION_ID as oln_LOCATION_ID1",
	"SQ_Shortcut_to_WM_ORDERS___oln_WM_ORDER_ID as oln_WM_ORDER_ID1",
	"cast(SQ_Shortcut_to_WM_ORDERS___oln_WM_ITEM_NAME as int) as o_oln_WM_ITEM_NAME",
	"cast(SQ_Shortcut_to_WM_ORDERS___oln_SHIPPED_QTY as int) as oln_SHIPPED_QTY",
	"SQ_Shortcut_to_WM_ORDERS___UPDATE_TSTMP  as UPDATE_TSTMP",
	"SQ_Shortcut_to_WM_ORDERS___oln_UPDATE_TSTMP  as oln_UPDATE_TSTMP"
)

# COMMAND ----------

# Processing node JNR_SKU_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_SKU_PROFILE_temp = SQ_Shortcut_To_SKU_PROFILE.toDF(*["SQ_Shortcut_To_SKU_PROFILE___" + col for col in SQ_Shortcut_To_SKU_PROFILE.columns])
EXP_INT_CONV_temp = EXP_INT_CONV.toDF(*["EXP_INT_CONV___" + col for col in EXP_INT_CONV.columns])

JNR_SKU_PROFILE = SQ_Shortcut_To_SKU_PROFILE_temp.join(EXP_INT_CONV_temp,[SQ_Shortcut_To_SKU_PROFILE_temp.SQ_Shortcut_To_SKU_PROFILE___SKU_NBR == EXP_INT_CONV_temp.EXP_INT_CONV___o_oln_WM_ITEM_NAME],'inner').selectExpr(
	"EXP_INT_CONV___LOCATION_ID as LOCATION_ID",
	"EXP_INT_CONV___WM_ORDER_ID as WM_ORDER_ID",
	"EXP_INT_CONV___o_WM_ACTUAL_SHIPPED_TSTMP as WM_ACTUAL_SHIPPED_TSTMP",
	"EXP_INT_CONV___oln_LOCATION_ID1 as oln_LOCATION_ID1",
	"EXP_INT_CONV___oln_WM_ORDER_ID1 as oln_WM_ORDER_ID1",
	"EXP_INT_CONV___o_oln_WM_ITEM_NAME as oln_WM_ITEM_NAME",
	"EXP_INT_CONV___oln_SHIPPED_QTY as oln_SHIPPED_QTY",
	"SQ_Shortcut_To_SKU_PROFILE___PRODUCT_ID as sku_PRODUCT_ID",
	"SQ_Shortcut_To_SKU_PROFILE___SKU_NBR as sku_SKU_NBR",
	"EXP_INT_CONV___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_INT_CONV___oln_UPDATE_TSTMP as oln_UPDATE_TSTMP")

# COMMAND ----------

# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_SKU_PROFILE_temp = JNR_SKU_PROFILE.toDF(*["JNR_SKU_PROFILE___" + col for col in JNR_SKU_PROFILE.columns])
EXP_TO_CHAR_temp = EXP_TO_CHAR.toDF(*["EXP_TO_CHAR___" + col for col in EXP_TO_CHAR.columns])

JNR_SITE_PROFILE = EXP_TO_CHAR_temp.join(JNR_SKU_PROFILE_temp,[EXP_TO_CHAR_temp.EXP_TO_CHAR___LOCATION_ID == JNR_SKU_PROFILE_temp.JNR_SKU_PROFILE___oln_LOCATION_ID1],'inner').selectExpr(
	"JNR_SKU_PROFILE___LOCATION_ID as LOCATION_ID",
	"JNR_SKU_PROFILE___WM_ORDER_ID as WM_ORDER_ID",
	"JNR_SKU_PROFILE___WM_ACTUAL_SHIPPED_TSTMP as WM_ACTUAL_SHIPPED_TSTMP",
	"JNR_SKU_PROFILE___oln_LOCATION_ID1 as oln_LOCATION_ID1",
	"JNR_SKU_PROFILE___oln_SHIPPED_QTY as oln_SHIPPED_QTY",
	"JNR_SKU_PROFILE___sku_SKU_NBR as sku_SKU_NBR",
	"EXP_TO_CHAR___LOCATION_ID as site_LOCATION_ID1",
	"EXP_TO_CHAR___o_STORE_NBR as site_STORE_NBR",
	"JNR_SKU_PROFILE___UPDATE_TSTMP as UPDATE_TSTMP",
	"JNR_SKU_PROFILE___oln_UPDATE_TSTMP as oln_UPDATE_TSTMP") \
	.withColumn('lpn_LOCATION_ID1', lit(None))
	

# COMMAND ----------

# Processing node SRT_ORDER_QTY, type SORTER 
# COLUMN COUNT: 6

SRT_ORDER_QTY = JNR_SITE_PROFILE.sort(col('WM_ACTUAL_SHIPPED_TSTMP').asc(), col('site_STORE_NBR').asc(), col('sku_SKU_NBR').asc())

# COMMAND ----------

# Processing node AGG_ORDER_QTY, type AGGREGATOR 
# COLUMN COUNT: 6

AGG_ORDER_QTY = SRT_ORDER_QTY \
	.groupBy(to_date("WM_ACTUAL_SHIPPED_TSTMP").alias("WM_ACTUAL_SHIPPED_TSTMP"),"site_STORE_NBR","sku_SKU_NBR") \
	.agg( \
	sum(when(col("oln_SHIPPED_QTY").isNull() ,0) .otherwise(col("oln_SHIPPED_QTY")) *(-1)).alias("SHIPPED_QTY"),
	min(col("UPDATE_TSTMP")).alias('UPDATE_TSTMP'),
	min(col("oln_UPDATE_TSTMP")).alias('oln_UPDATE_TSTMP')
	) \
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Conversion, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
AGG_ORDER_QTY_temp = AGG_ORDER_QTY.toDF(*["AGG_ORDER_QTY___" + col for col in AGG_ORDER_QTY.columns])

EXP_Conversion = AGG_ORDER_QTY_temp.selectExpr(
	"AGG_ORDER_QTY___sku_SKU_NBR as sku_SKU_NBR",
	"AGG_ORDER_QTY___SHIPPED_QTY as SHIPPED_QTY",
	"AGG_ORDER_QTY___WM_ACTUAL_SHIPPED_TSTMP as DAY_DT",
	"LPAD ( AGG_ORDER_QTY___site_STORE_NBR , 4 , '0' ) as STORE_NBR",
	"'SALE' as TYPE",
	"'' as VALUE",
	"'' as CURRENCY",
	"'' as PURCHASE_PRICE",
	"'' as SECONDARY_CURRENCY_VALUE",
	"'' as SECONDARY_PURCHASE_PRICE"
)

# COMMAND ----------

# Processing node FIL_ZERO_TRANS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
EXP_Conversion_temp = EXP_Conversion.toDF(*["EXP_Conversion___" + col for col in EXP_Conversion.columns])

FIL_ZERO_TRANS = EXP_Conversion_temp.selectExpr(
	"EXP_Conversion___DAY_DT as DAY_DT",
	"EXP_Conversion___STORE_NBR as STORE_NBR",
	"EXP_Conversion___sku_SKU_NBR as SKU_NBR",
	"EXP_Conversion___TYPE as TYPE",
	"EXP_Conversion___SHIPPED_QTY as SHIPPED_QTY",
	"EXP_Conversion___VALUE as VALUE",
	"EXP_Conversion___CURRENCY as CURRENCY",
	"EXP_Conversion___PURCHASE_PRICE as PURCHASE_PRICE",
	"EXP_Conversion___SECONDARY_CURRENCY_VALUE as SECONDARY_CURR_VALUE",
	"EXP_Conversion___SECONDARY_PURCHASE_PRICE as SECONDARY_CURR_PURCHASE").filter("SHIPPED_QTY != '0'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_RELEX_DC_Shipments_FF, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_RELEX_DC_Shipments_FF = FIL_ZERO_TRANS.selectExpr(
	"CAST(DAY_DT AS STRING) as DATE",
	"CAST(STORE_NBR AS STRING) as LOCATION_CODE",
	"CAST(SKU_NBR AS STRING) as PRODUCT_CODE",
	"CAST(TYPE AS STRING) as TYPE",
	"SHIPPED_QTY as TRANSACTION_QUANTITY",
	"CAST(VALUE AS STRING) as VALUE",
	"CAST(CURRENCY AS STRING) as CURRENCY",
	"CAST(PURCHASE_PRICE AS STRING) as PURCHASE_PRICE",
	"CAST(SECONDARY_CURR_VALUE AS STRING) as SECONDARY_CURRENCY_VALUE",
	"CAST(SECONDARY_CURR_PURCHASE AS STRING) as SECONDARY_CURRENCY_PURCHASE_PRICE"
)

target_bucket=target_bucket+datetime.now().strftime("%Y%m%d")+'/'
target_file =  target_file.replace('.csv','') + datetime.today().strftime("_%Y-%m-%d-%H-%m-%S") + '.csv'
writeToFlatFile(Shortcut_to_RELEX_DC_Shipments_FF, target_bucket, target_file, 'overwrite' )

# COMMAND ----------

# updating parm config table with latest run date
param_file_name='wf_RELEX_DC_Shipments_Out'
param_section='m_RELEX_2018_DC_Shipments_Out'

v_param_value_Last_Run_date1=str(spark.sql(f"Select max(UPDATE_TSTMP) as max_dt from {legacy}.WM_ORDERS").first()[0])
v_param_value_Last_Run_date2=str(spark.sql(f"Select max(UPDATE_TSTMP) as max_dt from {legacy}.WM_ORDER_LINE_ITEM").first()[0])
v_param_value_Delta_Filter1=f"WM_ORDERS.UPDATE_TSTMP > to_date('{v_param_value_Last_Run_date1}','MM/dd/yyyy')"
v_param_value_Delta_Filter2=f"WM_ORDER_LINE_ITEM.UPDATE_TSTMP > to_date('{v_param_value_Last_Run_date2}','MM/dd/yyyy')"

# print(v_param_value_Last_Run_date)
# print(v_param_value_Delta_Filter)

#spark.sql(f"""Update {raw}.parameter_config set parameter_value="SKU_PROFILE_RPT.UPDATE_DT > '{v_parameter_value}'" where parameter_file_name='{param_file_name}' and parameter_section='{param_section}' and parameter_key in ('SKU_Date_Filter')""")

#spark.sql(f"""Update {raw}.parameter_config set parameter_value='{v_parameter_value}' where parameter_file_name='{param_file_name}' and parameter_section='{param_section}' and parameter_key in ('Last_Run_date')""")

#raw, parameter_file_name, parameter_section, parameter_key, parameter_value
update_param_config(raw, param_file_name, param_section, 'Delta_Filter1',v_param_value_Delta_Filter1)
update_param_config(raw, param_file_name, param_section, 'Delta_Filter2',v_param_value_Delta_Filter2)
update_param_config(raw, param_file_name, param_section, 'LAST_RUN_DATE',v_param_value_Last_Run_date1)

# COMMAND ----------

print(target_bucket)
print(target_file)
