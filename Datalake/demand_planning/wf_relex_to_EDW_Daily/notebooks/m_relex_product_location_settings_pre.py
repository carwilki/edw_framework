# Databricks notebook source
#Code converted on 2023-08-25 11:19:47
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
#currdate = '20230901' #only for validation, after validation please remove this line

#dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/demand_planing/product_location_settings')
#file_path = dbutils.widgets.get('file_path') + f'/{currdate}'

#_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily.M:m_relex_product_location_settings_pre','source_bucket')
#file_path = _bucket + f'/{currdate}'

_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily','source_bucket')
complete_bucket = _bucket + 'product_location_settings/'
key ="product_location_settings"

source_file = get_src_file(key, complete_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_product_location_settings, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_product_location_settings = spark.read.option('header',False).option('sep', ';').option('skipRows',1).option('ignoreTrailingWhiteSpace', True).option('inferSchema',True).csv(source_file).withColumn("sys_row_id", monotonically_increasing_id())\
.withColumnRenamed('_c0' ,'Product_code')\
.withColumnRenamed('_c1' ,'Location_code')\
.withColumnRenamed('_c2' ,'Purchase_Group')\
.withColumnRenamed('_c3' ,'Supplier_code')\
.withColumnRenamed('_c4' ,'Order_model')\
.withColumnRenamed('_c5' ,'Forecast_model')\
.withColumnRenamed('_c6' ,'ABC_class')\
.withColumnRenamed('_c7' ,'Product__ABC_class')\
.withColumnRenamed('_c8' ,'XYZ_class')\
.withColumnRenamed('_c9' ,'Introduction_date')\
.withColumnRenamed('_c10' ,'Termination_date')\
.withColumnRenamed('_c11' ,'Product__XYZ_class')\
.withColumnRenamed('_c12' ,'Reference_product')\
.withColumnRenamed('_c13' ,'Reference_scaling_factor')\
.withColumnRenamed('_c14' ,'Product_location__Replaces_products')\
.withColumnRenamed('_c15' ,'Product_location__Replacing_products')\
.withColumnRenamed('_c16' ,'Demand_satisfied')

# COMMAND ----------

# Processing node FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS, type FILTER 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_product_location_settings_temp = SQ_Shortcut_to_product_location_settings.toDF(*["SQ_Shortcut_to_product_location_settings___" + col for col in SQ_Shortcut_to_product_location_settings.columns])

FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS = SQ_Shortcut_to_product_location_settings_temp.selectExpr(
	"SQ_Shortcut_to_product_location_settings___Product_code as Product_code",
	"SQ_Shortcut_to_product_location_settings___Location_code as Location_code",
	"SQ_Shortcut_to_product_location_settings___Purchase_Group as Purchase_Group",
	"SQ_Shortcut_to_product_location_settings___Supplier_code as Supplier_code",
	"SQ_Shortcut_to_product_location_settings___Order_model as Order_model",
	"SQ_Shortcut_to_product_location_settings___Forecast_model as Forecast_model",
	"SQ_Shortcut_to_product_location_settings___ABC_class as ABC_class",
	"SQ_Shortcut_to_product_location_settings___Product__ABC_class as Product__ABC_class",
	"SQ_Shortcut_to_product_location_settings___XYZ_class as XYZ_class",
	"SQ_Shortcut_to_product_location_settings___Introduction_date as Introduction_date",
	"SQ_Shortcut_to_product_location_settings___Termination_date as Termination_date",
	"SQ_Shortcut_to_product_location_settings___Product__XYZ_class as Product__XYZ_class",
	"SQ_Shortcut_to_product_location_settings___Reference_product as Reference_product",
	"SQ_Shortcut_to_product_location_settings___Reference_scaling_factor as Reference_scaling_factor",
	"SQ_Shortcut_to_product_location_settings___Product_location__Replaces_products as Product_location__Replaces_products",
	"SQ_Shortcut_to_product_location_settings___Product_location__Replacing_products as Product_location__Replacing_products",
	"SQ_Shortcut_to_product_location_settings___Demand_satisfied as Demand_satisfied").filter("Supplier_code IS NOT NULL AND Supplier_code != '<UNKNOWN>'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_DEFAULTS, type EXPRESSION 
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS_temp = FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS.toDF(*["FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___" + col for col in FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS.columns])

EXP_DEFAULTS = FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS_temp.selectExpr(
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___sys_row_id as sys_row_id",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Product_code as Product_code",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Location_code as Location_code",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Purchase_Group as Purchase_Group",
	"IF (FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Supplier_code IS NULL, ' ', FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Supplier_code) as o_Supplier_code",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Order_model as Order_model",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Forecast_model as Forecast_model",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___ABC_class as ABC_class",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Product__ABC_class as Product__ABC_class",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___XYZ_class as XYZ_class",
	"TO_DATE ( FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Introduction_date , 'yyyy-MM-dd' ) as o_Introduction_date",
	"TO_DATE ( FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Termination_date , 'yyyy-MM-dd' ) as o_Termination_date",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Product__XYZ_class as Product__XYZ_class",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Reference_product as Reference_product",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Reference_scaling_factor as Reference_scaling_factor",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Product_location__Replaces_products as Product_location__Replaces_products",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Product_location__Replacing_products as Product_location__Replacing_products",
	"FIL_NULL_UNKNOWN_SUPPLIER_CODE_RECS___Demand_satisfied as Demand_satisfied",
	"CURRENT_TIMESTAMP as o_LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE, type TARGET 
# COLUMN COUNT: 18


Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE = EXP_DEFAULTS.selectExpr(
	"CAST(Location_code AS INT) as LOCATION_CODE",
	"CAST(Product_code AS INT) as PRODUCT_CODE",
	"CAST(o_Supplier_code AS STRING) as SUPPLIER_CODE",
	"CAST(Purchase_Group AS STRING) as PURCHASE_GROUP",
	"CAST(o_Introduction_date AS DATE) as INTRODUCTION_DATE",
	"CAST(o_Termination_date AS DATE) as TERMINATION_DATE",
	"CAST(Product_location__Replaces_products AS INT) as REPLACES_PRODUCT",
	"CAST(Product_location__Replacing_products AS INT) as REPLACING_PRODUCT",
	"CAST(Reference_product AS INT) as REFERENCE_PRODUCT",
	"CAST(Reference_scaling_factor AS DECIMAL(8,4)) as REFERENCE_SCALING_FACTOR",
	"CAST(Order_model AS STRING) as ORDER_MODEL",
	"CAST(Forecast_model AS STRING) as FORECAST_MODEL",
	"CAST(ABC_class AS STRING) as ABC_CLASS",
	"CAST(Product__ABC_class AS STRING) as PRODUCT_ABC_CLASS",
	"CAST(XYZ_class AS STRING) as XYZ_CLASS",
	"CAST(Product__XYZ_class AS STRING) as PRODUCT_XYZ_CLASS",
	"CAST(Demand_satisfied AS DECIMAL(8,4)) as DEMAND_SATISFIED_PCT",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE.write.saveAsTable(f'{raw}.RELEX_PRODUCT_LOCATION_SETTINGS_PRE', mode = 'overwrite')
