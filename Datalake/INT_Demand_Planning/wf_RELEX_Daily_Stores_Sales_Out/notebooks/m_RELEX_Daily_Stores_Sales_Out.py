# Databricks notebook source
# Code converted on 2023-11-27 11:18:53
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *
from datetime import date

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

target_bucket = getParameterValue(raw,'wf_RELEX_Daily_Stores_Sales_Out','m_RELEX_Daily_Stores_Sales_Out','target_bucket')
nas_target = getParameterValue(raw,'wf_RELEX_Daily_Stores_Sales_Out','m_RELEX_Daily_Stores_Sales_Out','nas_target')
key = getParameterValue(raw,'wf_RELEX_Daily_Stores_Sales_Out','m_RELEX_Daily_Stores_Sales_Out','key')
target_file = target_bucket + key
DC_Filter = getParameterValue(raw,'wf_RELEX_Daily_Stores_Sales_Out','m_RELEX_Daily_Stores_Sales_Out','DC_Filter')
#Delta_Filter=getParameterValue(raw,'wf_RELEX_Daily_Stores_Sales_Out','m_RELEX_Daily_Stores_Sales_Out','Delta_Filter')
#Last_Run_Date=getParameterValue(raw,'wf_RELEX_Daily_Stores_Sales_Out','m_RELEX_Daily_Stores_Sales_Out','Last_Run_Date')
Sku_Type_Filter = getParameterValue(raw,'wf_RELEX_Daily_Stores_Sales_Out','m_RELEX_Daily_Stores_Sales_Out','Sku_Type_Filter')
Last_Run_Date = str(date.today() - timedelta(days= 1))
#Last_Run_Date = '2023-12-19'

Delta_Filter = "DATE(SALES_DAY_SKU_STORE_RPT.UPDATE_DT)>to_date('"+Last_Run_Date+"','yyyy-MM-dd')"


# COMMAND ----------



# COMMAND ----------

# Processing node SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT, type SOURCE 
# COLUMN COUNT: 12

SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT = spark.sql(f"""SELECT S.DAY_DT, S.LOCATION_ID, S.SKU_NBR, S.STORE_NBR, S.NET_SALES_AMT, S.NET_SALES_QTY, S.SALES_COST, S.RETURN_COST, S.SPECIAL_SALES_AMT, S.SPECIAL_SALES_QTY, S.EXCH_RATE_PCT, S.UPDATE_DT 

 FROM

 {legacy}.SALES_DAY_SKU_STORE_RPT S,
  
(SELECT DISTINCT DAY_DT, PRODUCT_ID, LOCATION_ID

 FROM {legacy}.SALES_DAY_SKU_STORE_RPT

 WHERE {Delta_Filter} ) Z

 WHERE S.DAY_DT = Z.DAY_DT

 AND S.PRODUCT_ID = Z.PRODUCT_ID

 AND S.LOCATION_ID = Z.LOCATION_ID

ORDER BY S.LOCATION_ID,S.STORE_NBR""").withColumn("sys_row_id", monotonically_increasing_id())

#Conforming fields names to the component layout
SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT = SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[2],'SKU_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[3],'STORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[4],'NET_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[5],'NET_SALES_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[6],'SALES_COST') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[7],'RETURN_COST') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[8],'SPECIAL_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[9],'SPECIAL_SALES_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[10],'EXCH_RATE_PCT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[11],'UPDATE_DT')
SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.createOrReplaceTempView('table')

# COMMAND ----------

#spark.sql('select * from table').display()

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2
print(Sku_Type_Filter)
SQ_Shortcut_to_SKU_PROFILE_RPT = spark.sql(f'''SELECT
SKU_PROFILE_RPT.SKU_NBR,
SKU_PROFILE_RPT.SAP_DEPT_ID
FROM {legacy}.SKU_PROFILE_RPT
WHERE {Sku_Type_Filter}''').withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE_RPT = spark.sql(f"""SELECT SITE_PROFILE_RPT.LOCATION_ID, SITE_PROFILE_RPT.STORE_NBR 

FROM

{legacy}.SITE_PROFILE_RPT 
WHERE

 {DC_Filter}

ORDER BY  SITE_PROFILE_RPT.LOCATION_ID,SITE_PROFILE_RPT.STORE_NBR""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SITE_PROFILE_RPT = SQ_Shortcut_to_SITE_PROFILE_RPT \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE_RPT.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE_RPT.columns[1],'STORE_NBR')

# COMMAND ----------

# Processing node JNR_SITE_PROFILE_RPT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_RPT_temp = SQ_Shortcut_to_SITE_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SITE_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SITE_PROFILE_RPT.columns])
SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_temp = SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.toDF(*["SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___" + col for col in SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns])

JNR_SITE_PROFILE_RPT = SQ_Shortcut_to_SITE_PROFILE_RPT_temp.join(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_temp,[SQ_Shortcut_to_SITE_PROFILE_RPT_temp.SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID == SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_temp.SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___LOCATION_ID, SQ_Shortcut_to_SITE_PROFILE_RPT_temp.SQ_Shortcut_to_SITE_PROFILE_RPT___STORE_NBR == SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_temp.SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___STORE_NBR],'inner').selectExpr(
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___STORE_NBR as STORE_NBR",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___SKU_NBR as SKU_NBR",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___NET_SALES_QTY as NET_SALES_QTY",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___NET_SALES_AMT as NET_SALES_AMT",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___SALES_COST as SALES_COST",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___STORE_NBR as STORE_NBR1",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___RETURN_COST as RETURN_COST",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___SPECIAL_SALES_AMT as SPECIAL_SALES_AMT",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___SPECIAL_SALES_QTY as SPECIAL_SALES_QTY",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___EXCH_RATE_PCT as EXCH_RATE_PCT",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___UPDATE_DT as UPDATE_DT")

# COMMAND ----------

# Processing node JNR_SKU_PROFILE_RPT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_RPT_temp = JNR_SITE_PROFILE_RPT.toDF(*["JNR_SITE_PROFILE_RPT___" + col for col in JNR_SITE_PROFILE_RPT.columns])
SQ_Shortcut_to_SKU_PROFILE_RPT_temp = SQ_Shortcut_to_SKU_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT.columns])

JNR_SKU_PROFILE_RPT = SQ_Shortcut_to_SKU_PROFILE_RPT_temp.join(JNR_SITE_PROFILE_RPT_temp,[SQ_Shortcut_to_SKU_PROFILE_RPT_temp.SQ_Shortcut_to_SKU_PROFILE_RPT___SKU_NBR == JNR_SITE_PROFILE_RPT_temp.JNR_SITE_PROFILE_RPT___SKU_NBR],'inner').selectExpr(
	"JNR_SITE_PROFILE_RPT___DAY_DT as DAY_DT",
	"JNR_SITE_PROFILE_RPT___STORE_NBR as STORE_NBR",
	"JNR_SITE_PROFILE_RPT___SKU_NBR as SKU_NBR",
	"JNR_SITE_PROFILE_RPT___NET_SALES_QTY as NET_SALES_QTY",
	"JNR_SITE_PROFILE_RPT___NET_SALES_AMT as NET_SALES_AMT",
	"JNR_SITE_PROFILE_RPT___SALES_COST as SALES_COST",
	"JNR_SITE_PROFILE_RPT___UPDATE_DT as UPDATE_DT",
	"JNR_SITE_PROFILE_RPT___RETURN_COST as RETURN_COST",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SKU_NBR as SKU_NBR1",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_DEPT_ID as SAP_DEPT_ID",
	"JNR_SITE_PROFILE_RPT___SPECIAL_SALES_AMT as SPECIAL_SALES_AMT",
	"JNR_SITE_PROFILE_RPT___SPECIAL_SALES_QTY as SPECIAL_SALES_QTY",
	"JNR_SITE_PROFILE_RPT___EXCH_RATE_PCT as EXCH_RATE_PCT")

# COMMAND ----------

# Processing node AGG_SALES, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

AGG_SALES = JNR_SKU_PROFILE_RPT.selectExpr(
	"NET_SALES_QTY as i_NET_SALES_QTY",
	"NET_SALES_AMT as i_NET_SALES_AMT",
	"SALES_COST as i_SALES_COST",
	"RETURN_COST as i_RETURN_COST",
	"SPECIAL_SALES_AMT as i_SPECIAL_SALES_AMT",
	"SPECIAL_SALES_QTY as i_SPECIAL_SALES_QTY",
	"EXCH_RATE_PCT as EXCH_RATE_PCT",
	"SAP_DEPT_ID as SAP_DEPT_ID",
	"DAY_DT as DAY_DT",
	"STORE_NBR as STORE_NBR",
	"SKU_NBR as SKU_NBR",
	"UPDATE_DT as UPDATE_DT") \
	.withColumn("sys_row_id", monotonically_increasing_id()) \
	.withColumn("NET_SALES_QTY", expr("""IF (i_NET_SALES_QTY IS NULL, '', i_NET_SALES_QTY)""")) \
	.withColumn("NET_SALES_AMT", expr("""IF (i_NET_SALES_AMT IS NULL, '', i_NET_SALES_AMT)""")) \
	.withColumn("SALES_COST", expr("""IF (i_SALES_COST IS NULL, '', i_SALES_COST)""")) \
	.withColumn("RETURN_COST", expr("""IF (i_RETURN_COST IS NULL, '', i_RETURN_COST)""")) \
	.withColumn("SPECIAL_SALES_AMT", expr("""IF (i_SPECIAL_SALES_AMT IS NULL, '', i_SPECIAL_SALES_AMT)""")) \
	.withColumn("SPECIAL_SALES_QTY", expr("""IF (i_SPECIAL_SALES_QTY IS NULL, '', i_SPECIAL_SALES_QTY)"""))   \
	.groupBy("DAY_DT","STORE_NBR","SKU_NBR","SAP_DEPT_ID") \
	.agg( \
	min(col("NET_SALES_QTY")).alias("i_NET_SALES_QTY"),
	min(col("NET_SALES_AMT")).alias("i_NET_SALES_AMT"),
	min(col("SALES_COST")).alias("i_SALES_COST"),
	min(col('UPDATE_DT')).alias('UPDATE_DT'),
	(when(col("SAP_DEPT_ID") == 73 , sum(col('SPECIAL_SALES_QTY')) *(-1)) .otherwise(sum(col('NET_SALES_QTY')) *(- 1))).alias("o_TRANS_QTY"),
	(when((col("SAP_DEPT_ID") == 73) & (sum(col('SPECIAL_SALES_QTY')) == 0) , sum(col('SPECIAL_SALES_AMT') * col('EXCH_RATE_PCT'))) \
    	.otherwise(when((col("SAP_DEPT_ID") == 73) & (sum(col('SPECIAL_SALES_QTY')) != 0) , abs(sum(col('SPECIAL_SALES_AMT') * col('EXCH_RATE_PCT')))) \
         	.otherwise(when(sum(col('NET_SALES_QTY')) == 0 , sum(col('NET_SALES_AMT') * col('EXCH_RATE_PCT'))) \
              	.otherwise(abs(sum(col('NET_SALES_AMT') * col('EXCH_RATE_PCT'))))))).alias("o_VALUE"),
	(when((col("SAP_DEPT_ID") == 73) & (sum(col('SPECIAL_SALES_QTY')) == 0) , sum(col('SALES_COST') * col('EXCH_RATE_PCT')) - sum(col('RETURN_COST') * col('EXCH_RATE_PCT'))) \
    	.otherwise(when((col("SAP_DEPT_ID") == 73) & (sum(col('SPECIAL_SALES_QTY')) != 0) , abs(sum(col('SALES_COST') * col('EXCH_RATE_PCT')) - sum(col('RETURN_COST') * col('EXCH_RATE_PCT')))) \
         	.otherwise(when(sum(col('NET_SALES_QTY')) == 0 , sum(col('SALES_COST') * col('EXCH_RATE_PCT')) - sum(col('RETURN_COST') * col('EXCH_RATE_PCT'))) \
              	.otherwise(abs(sum(col('SALES_COST') * col('EXCH_RATE_PCT')) - sum(col('RETURN_COST') * col('EXCH_RATE_PCT'))))))).alias("o_PURCHASE_PRICE"), 
	(when(col("SAP_DEPT_ID") == 73 , sum(col('SPECIAL_SALES_AMT'))).otherwise(sum(col('NET_SALES_AMT')))).alias("o_SEC_VALUE"),
	sum(col('SALES_COST') - col('RETURN_COST')).alias("o_SEC_PURCHASE_PRICE")
	)

# COMMAND ----------

# Processing node SRT_STR_NBR, type SORTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

SRT_STR_NBR = AGG_SALES.selectExpr(
	"DAY_DT as DAY_DT",
	"STORE_NBR as STORE_NBR",
	"SKU_NBR as SKU_NBR",
	"o_TRANS_QTY as TRANS_QTY",
	"o_VALUE as VALUE",
	"o_PURCHASE_PRICE as PURCHASE_PRICE",
	"UPDATE_DT as UPDATE_DT",
	"o_SEC_VALUE as SEC_VALUE",
	"o_SEC_PURCHASE_PRICE as SEC_PURCHASE_PRICE").sort(col('DAY_DT').asc(), col('STORE_NBR').asc(), col('SKU_NBR').asc())

# COMMAND ----------

# Processing node EXP_CONVERSIONS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SRT_STR_NBR_temp = SRT_STR_NBR.toDF(*["SRT_STR_NBR___" + col for col in SRT_STR_NBR.columns])

EXP_CONVERSIONS2 = SRT_STR_NBR_temp.selectExpr(
	"SRT_STR_NBR___DAY_DT as i_DAY_DT",
	"SRT_STR_NBR___UPDATE_DT as UPDATE_DT",
	"SRT_STR_NBR___STORE_NBR as i_STORE_NBR",
	"SRT_STR_NBR___PURCHASE_PRICE as i_PURCHASE_PRICE",
	"SRT_STR_NBR___SEC_PURCHASE_PRICE as i_SEC_PURCHASE_PRICE",
	"SRT_STR_NBR___SKU_NBR as SKU_NBR",
	"SRT_STR_NBR___TRANS_QTY as TRANS_QTY",
	"SRT_STR_NBR___VALUE as VALUE",
	"SRT_STR_NBR___SEC_VALUE as SEC_VALUE")
	
SRT_STR_NBR_temp = EXP_CONVERSIONS2.toDF(*["SRT_STR_NBR___" + col for col in EXP_CONVERSIONS2.columns])

EXP_CONVERSIONS = SRT_STR_NBR_temp.selectExpr(
	"SRT_STR_NBR___SKU_NBR as SKU_NBR",
	"SRT_STR_NBR___TRANS_QTY as TRANS_QTY",
	"SRT_STR_NBR___VALUE as VALUE",
	"SRT_STR_NBR___SEC_VALUE as SEC_VALUE",
	"IF (ROUND ( SRT_STR_NBR___i_SEC_PURCHASE_PRICE , 2 ) = - 00, ABS ( SRT_STR_NBR___i_SEC_PURCHASE_PRICE ), SRT_STR_NBR___i_SEC_PURCHASE_PRICE) as SEC_PURCHASE_PRICE",
	"IF (ROUND ( SRT_STR_NBR___i_PURCHASE_PRICE , 2 ) = - 00, ABS ( SRT_STR_NBR___i_PURCHASE_PRICE ), SRT_STR_NBR___i_PURCHASE_PRICE) as PURCHASE_PRICE",
	"date_format(SRT_STR_NBR___i_DAY_DT, 'yyyy-MM-dd') as DAY_DT",
	"LPAD ( SRT_STR_NBR___i_STORE_NBR , 4 , '0' ) as STORE_NBR",
	"'SALE' as TYPE",
	"'USD' as CURRENCY"
)

# COMMAND ----------

# Processing node Shortcut_to_RELEX_Stores_Sales_FF, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_RELEX_Stores_Sales_FF = EXP_CONVERSIONS.selectExpr(
	"CAST(DAY_DT AS STRING) as DATE",
	"CAST(STORE_NBR AS STRING) as LOCATION_CODE",
	"CAST(SKU_NBR AS STRING) as PRODUCT_CODE",
	"CAST(TYPE AS STRING) as TYPE",
	"TRANS_QTY as TRANSACTION_QUANTITY",
	"VALUE as VALUE",
	"CAST(CURRENCY AS STRING) as CURRENCY",
	"ROUND(PURCHASE_PRICE,2) as PURCHASE_PRICE",
	"ROUND(SEC_VALUE ,2) as SECONDARY_CURRENCY_VALUE",
	"ROUND(SEC_PURCHASE_PRICE,2) as SECONDARY_CURRENCY_PURCHASE"
)
target_file_path = target_bucket + key.split('.')[0] + '/'+ date.today().strftime('%Y%m%d') + '/'
print(target_file_path)
writeToFlatFile(Shortcut_to_RELEX_Stores_Sales_FF, target_file_path, key, 'overwrite')

# COMMAND ----------

nas_target_path = nas_target  + date.today().strftime('%Y%m%d') + '/'
gs_source_path = target_file_path + '*.csv'
copy_file_to_nas(gs_source_path, nas_target_path)


# COMMAND ----------

#Shortcut_to_RELEX_Stores_Sales_FF.display()
