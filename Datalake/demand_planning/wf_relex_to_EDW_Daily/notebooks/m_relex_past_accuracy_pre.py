# Databricks notebook source
#Code converted on 2023-08-25 11:19:46
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = '')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'

# Set global variables
starttime = datetime.now() #start timestamp of the script
currdate = starttime.strftime('%Y%m%d')
#currdate = '20230901' #only for validation, after validation remove this line

#dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/demand_planing/past_accuracy')
#file_path = dbutils.widgets.get('file_path') + f'/{currdate}'

#_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily.M:m_relex_past_accuracy_pre','source_bucket')
#file_path = _bucket + f'/{currdate}'

_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily','source_bucket')
complete_bucket = _bucket + 'past_accuracy/'
key ="past_accuracy"

source_file = get_src_file(key, complete_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_past_accuracy_FF, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_past_accuracy_FF = spark.read.option('header',False).option('sep', ';').option('skipRows',1).option('ignoreTrailingWhiteSpace', True).option('inferSchema',True).csv(source_file).withColumn("sys_row_id", monotonically_increasing_id())\
.withColumnRenamed('_c0' ,'Date')\
.withColumnRenamed('_c1' ,'Product_code')\
.withColumnRenamed('_c2' ,'Location_code')\
.withColumnRenamed('_c3' ,'Purchase_Group')\
.withColumnRenamed('_c4' ,'Supplier_code')\
.withColumnRenamed('_c5' ,'Effective_forecast')\
.withColumnRenamed('_c6' ,'Count_of_stock_outs')\
.withColumnRenamed('_c7' ,'Estimated_lost_sales')\
.withColumnRenamed('_c8' ,'Estimated_lost_sales__value')

# COMMAND ----------

# Processing node FIL_UNKNOWN_RECORDS, type FILTER 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_past_accuracy_FF_temp = SQ_Shortcut_to_past_accuracy_FF.toDF(*["SQ_Shortcut_to_past_accuracy_FF___" + col for col in SQ_Shortcut_to_past_accuracy_FF.columns])

FIL_UNKNOWN_RECORDS = SQ_Shortcut_to_past_accuracy_FF_temp.selectExpr(
	"SQ_Shortcut_to_past_accuracy_FF___Date as Date",
	"SQ_Shortcut_to_past_accuracy_FF___Product_code as Product_code",
	"SQ_Shortcut_to_past_accuracy_FF___Location_code as Location_code",
	"SQ_Shortcut_to_past_accuracy_FF___Purchase_Group as Purchase_Group",
	"SQ_Shortcut_to_past_accuracy_FF___Supplier_code as Supplier_code",
	"SQ_Shortcut_to_past_accuracy_FF___Effective_forecast as Effective_forecast",
	"SQ_Shortcut_to_past_accuracy_FF___Count_of_stock_outs as Count_of_stock_outs",
	"SQ_Shortcut_to_past_accuracy_FF___Estimated_lost_sales as Estimated_lost_sales",
	"SQ_Shortcut_to_past_accuracy_FF___Estimated_lost_sales__value as Estimated_lost_sales__value").filter("Supplier_code != '<UNKNOWN>' AND Supplier_code IS NOT NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_TO_DATE, type EXPRESSION 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
FIL_UNKNOWN_RECORDS_temp = FIL_UNKNOWN_RECORDS.toDF(*["FIL_UNKNOWN_RECORDS___" + col for col in FIL_UNKNOWN_RECORDS.columns])

EXP_TO_DATE = FIL_UNKNOWN_RECORDS_temp.selectExpr(
	"FIL_UNKNOWN_RECORDS___sys_row_id as sys_row_id",
	"TO_DATE ( FIL_UNKNOWN_RECORDS___Date , 'yyyy-MM-dd' ) as o_Date",
	"FIL_UNKNOWN_RECORDS___Product_code as Product_code",
	"FIL_UNKNOWN_RECORDS___Location_code as Location_code",
	"FIL_UNKNOWN_RECORDS___Purchase_Group as Purchase_Group",
	"IF (FIL_UNKNOWN_RECORDS___Supplier_code IS NULL, ' ', FIL_UNKNOWN_RECORDS___Supplier_code) as o_Supplier_code",
	"FIL_UNKNOWN_RECORDS___Effective_forecast as Effective_forecast",
	"FIL_UNKNOWN_RECORDS___Estimated_lost_sales as Estimated_lost_sales",
	"FIL_UNKNOWN_RECORDS___Estimated_lost_sales__value as Estimated_lost_sales__value",
	"FIL_UNKNOWN_RECORDS___Count_of_stock_outs as Count_of_stock_outs",
	"CURRENT_TIMESTAMP as o_LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_RELEX_PAST_ACCURACY_PRE, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_RELEX_PAST_ACCURACY_PRE = EXP_TO_DATE.selectExpr(
	"CAST(o_Date AS DATE) as DAY_DATE",
	"CAST(Location_code AS INT) as LOCATION_CODE",
	"CAST(Product_code AS INT) as PRODUCT_CODE",
	"CAST(o_Supplier_code AS STRING) as SUPPLIER_CODE",
	"CAST(Purchase_Group AS STRING) as PURCHASE_GROUP",
	"CAST(Effective_forecast AS DECIMAL(12,4)) as EFFECTIVE_FORECAST",
	"CAST(Estimated_lost_sales AS DECIMAL(12,4)) as ESTIMATED_LOST_SALES",
	"CAST(Estimated_lost_sales__value AS DECIMAL(12,4)) as ESTIMATED_LOST_SALES_VALUE",
	"CAST(Count_of_stock_outs AS TINYINT) as COUNT_OF_STOCK_OUTS",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_RELEX_PAST_ACCURACY_PRE.write.saveAsTable(f'{raw}.RELEX_PAST_ACCURACY_PRE', mode = 'overwrite')
