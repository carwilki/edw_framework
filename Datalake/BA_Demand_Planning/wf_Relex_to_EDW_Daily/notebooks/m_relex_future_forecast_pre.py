# Databricks notebook source
#Code converted on 2023-08-25 11:19:45
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from pyspark.sql.types import *

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
#currdate = '20230901' #only for validation, after validation please remove this line

#dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-qa-raw-p1-gcs-gbl/nas/demand_planing/future_forecast')
#file_path = dbutils.widgets.get('file_path') + f'/{currdate}'

#_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily','source_bucket')
#file_path = _bucket + f'/{currdate}'

_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily','source_bucket')
complete_bucket = _bucket + 'future_forecast/'
key ="future_forecast"

source_file = get_src_file(key, complete_bucket)

# COMMAND ----------

# Used struct schema instead of using infered schema as it was taking long time
struct_schema = StructType([StructField("Product_code", DoubleType(), True), 
                    StructField("Location_code", DoubleType(), True), 
                    StructField("Purchase_Group", DoubleType(), True),
                    StructField("Supplier_code", DoubleType(), True),  
                    StructField("Date", DateType(), True),    
                    StructField("Weekly_Baseline_Forecast", DoubleType(), True), 
                    StructField("Weekly_Campaign_Increase", DoubleType(), True),   
                    StructField("Weekly_Calculated_Forecast", DoubleType(), True),
                    StructField("Weekly_Effective_Forecast", DoubleType(), True),   
                    StructField("Weekly_Corrected_Effective_Forecast", DoubleType(), True) ]) 
                
#print(struct_schema)

# COMMAND ----------

# Processing node SQ_Shortcut_to_future_forecast_FF, type SOURCE 
# COLUMN COUNT: 10

SQ_Shortcut_to_future_forecast_FF = spark.read.option('header',False).option('sep', ';').option('skipRows',1).option('ignoreTrailingWhiteSpace', True).option('schema',struct_schema).csv(source_file).withColumn("sys_row_id", monotonically_increasing_id())\
.withColumnRenamed('_c0' ,'Product_code')\
.withColumnRenamed('_c1' ,'Location_code')\
.withColumnRenamed('_c2' ,'Purchase_Group')\
.withColumnRenamed('_c3' ,'Supplier_code')\
.withColumnRenamed('_c4' ,'Date')\
.withColumnRenamed('_c5' ,'Weekly_Baseline_Forecast')\
.withColumnRenamed('_c6' ,'Weekly_Campaign_Increase')\
.withColumnRenamed('_c7' ,'Weekly_Calculated_Forecast')\
.withColumnRenamed('_c8' ,'Weekly_Effective_Forecast')\
.withColumnRenamed('_c9' ,'Weekly_Corrected_Effective_Forecast')

# COMMAND ----------

# Processing node FIL_UNKNOWN_RECORDS, type FILTER 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_future_forecast_FF_temp = SQ_Shortcut_to_future_forecast_FF.toDF(*["SQ_Shortcut_to_future_forecast_FF___" + col for col in SQ_Shortcut_to_future_forecast_FF.columns])

FIL_UNKNOWN_RECORDS = SQ_Shortcut_to_future_forecast_FF_temp.selectExpr(
	"SQ_Shortcut_to_future_forecast_FF___Product_code as Product_code",
	"SQ_Shortcut_to_future_forecast_FF___Location_code as Location_code",
	"SQ_Shortcut_to_future_forecast_FF___Purchase_Group as Purchase_Group",
	"SQ_Shortcut_to_future_forecast_FF___Supplier_code as Supplier_code",
	"SQ_Shortcut_to_future_forecast_FF___Date as Date",
	"SQ_Shortcut_to_future_forecast_FF___Weekly_Baseline_Forecast as Weekly_Baseline_Forecast",
	"SQ_Shortcut_to_future_forecast_FF___Weekly_Campaign_Increase as Weekly_Campaign_Increase",
	"SQ_Shortcut_to_future_forecast_FF___Weekly_Calculated_Forecast as Weekly_Calculated_Forecast",
	"SQ_Shortcut_to_future_forecast_FF___Weekly_Effective_Forecast as Weekly_Effective_Forecast",
	"SQ_Shortcut_to_future_forecast_FF___Weekly_Corrected_Effective_Forecast as Weekly_Corrected_Effective_Forecast").filter("Supplier_code != '<UNKNOWN>' AND Supplier_code IS NOT NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_DATE_CONV, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
FIL_UNKNOWN_RECORDS_temp = FIL_UNKNOWN_RECORDS.toDF(*["FIL_UNKNOWN_RECORDS___" + col for col in FIL_UNKNOWN_RECORDS.columns])

EXP_DATE_CONV = FIL_UNKNOWN_RECORDS_temp.selectExpr(
	"FIL_UNKNOWN_RECORDS___sys_row_id as sys_row_id",
	"FIL_UNKNOWN_RECORDS___Product_code as Product_code",
	"FIL_UNKNOWN_RECORDS___Location_code as Location_code",
	"FIL_UNKNOWN_RECORDS___Purchase_Group as Purchase_Group",
	"IF (FIL_UNKNOWN_RECORDS___Supplier_code IS NULL, ' ', FIL_UNKNOWN_RECORDS___Supplier_code) as o_Supplier_code",
	"TO_DATE ( FIL_UNKNOWN_RECORDS___Date , 'yyyy-MM-dd' ) as o_Date",
	"FIL_UNKNOWN_RECORDS___Weekly_Baseline_Forecast as Weekly_Baseline_Forecast",
	"FIL_UNKNOWN_RECORDS___Weekly_Campaign_Increase as Weekly_Campaign_Increase",
	"FIL_UNKNOWN_RECORDS___Weekly_Calculated_Forecast as Weekly_Calculated_Forecast",
	"FIL_UNKNOWN_RECORDS___Weekly_Effective_Forecast as Weekly_Effective_Forecast",
	"FIL_UNKNOWN_RECORDS___Weekly_Corrected_Effective_Forecast as Weekly_Corrected_Effective_Forecast",
	"CURRENT_TIMESTAMP as o_LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_RELEX_FUTURE_FORECAST_PRE, type TARGET 
# COLUMN COUNT: 11


Shortcut_to_RELEX_FUTURE_FORECAST_PRE = EXP_DATE_CONV.selectExpr(
	"CAST(o_Date AS DATE) as WEEK_DATE",
	"CAST(Location_code AS INT) as LOCATION_CODE",
	"CAST(Product_code AS INT) as PRODUCT_CODE",
	"CAST(o_Supplier_code AS STRING) as SUPPLIER_CODE",
	"CAST(Purchase_Group AS STRING) as PURCHASE_GROUP",
	"CAST(Weekly_Baseline_Forecast AS DECIMAL(12,4)) as BASELINE_FORECAST",
	"CAST(Weekly_Campaign_Increase AS DECIMAL(12,4)) as CAMPAIGN_INCREASE",
	"CAST(Weekly_Calculated_Forecast AS DECIMAL(12,4)) as CALCULATED_FORECAST",
	"CAST(Weekly_Effective_Forecast AS DECIMAL(12,4)) as EFFECTIVE_FORECAST",
	"CAST(Weekly_Corrected_Effective_Forecast AS DECIMAL(12,4)) as CORRECTED_EFFECTIVE_FORECAST",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_RELEX_FUTURE_FORECAST_PRE.repartition(32).write.saveAsTable(f'{raw}.RELEX_FUTURE_FORECAST_PRE', mode = 'overwrite')

# COMMAND ----------


