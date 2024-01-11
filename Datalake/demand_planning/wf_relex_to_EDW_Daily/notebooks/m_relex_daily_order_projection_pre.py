# Databricks notebook source
#Code converted on 2023-08-24 18:31:32
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

#dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/demand_planing/daily_order_projections')
#file_path = dbutils.widgets.get('file_path') + f'/{currdate}'

_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily','source_bucket')
complete_bucket = _bucket + 'daily_order_projections/'
key ="daily_order"

source_file = get_src_file(key, complete_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_Daily_Order_Projections1, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_Daily_Order_Projections1 = spark.read.option('header',False).option('skipRows',1).option('sep', ';').option('ignoreTrailingWhiteSpace', True).option('inferSchema',True).csv(source_file).withColumn("sys_row_id", monotonically_increasing_id())\
.withColumnRenamed('_c0' ,'Location_code')\
.withColumnRenamed('_c1' ,'Product_code')\
.withColumnRenamed('_c2' ,'Supplier_code')\
.withColumnRenamed('_c3' ,'Date')\
.withColumnRenamed('_c4' ,'Projected_order_proposals')\
.withColumnRenamed('_c5' ,'Projected_deliveries')


# COMMAND ----------

# Processing node FIL_UNKNOWN_RECORDS, type FILTER 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Daily_Order_Projections1_temp = SQ_Shortcut_to_Daily_Order_Projections1.toDF(*["SQ_Shortcut_to_Daily_Order_Projections1___" + col for col in SQ_Shortcut_to_Daily_Order_Projections1.columns])

FIL_UNKNOWN_RECORDS = SQ_Shortcut_to_Daily_Order_Projections1_temp.selectExpr(
	"SQ_Shortcut_to_Daily_Order_Projections1___Location_code as Location_code",
	"SQ_Shortcut_to_Daily_Order_Projections1___Product_code as Product_code",
	"SQ_Shortcut_to_Daily_Order_Projections1___Supplier_code as Supplier_code",
	"SQ_Shortcut_to_Daily_Order_Projections1___Date as Date",
	"SQ_Shortcut_to_Daily_Order_Projections1___Projected_order_proposals as Projected_order_proposals",
	"SQ_Shortcut_to_Daily_Order_Projections1___Projected_deliveries as Projected_deliveries").filter("Supplier_code != '<UNKNOWN>' AND Supplier_code IS NOT NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_DATE_CONV, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
FIL_UNKNOWN_RECORDS_temp = FIL_UNKNOWN_RECORDS.toDF(*["FIL_UNKNOWN_RECORDS___" + col for col in FIL_UNKNOWN_RECORDS.columns])

EXP_DATE_CONV = FIL_UNKNOWN_RECORDS_temp.selectExpr(
	"FIL_UNKNOWN_RECORDS___sys_row_id as sys_row_id",
	"FIL_UNKNOWN_RECORDS___Location_code as Location_code",
	"FIL_UNKNOWN_RECORDS___Product_code as Product_code",
	"IF (FIL_UNKNOWN_RECORDS___Supplier_code IS NULL, ' ', FIL_UNKNOWN_RECORDS___Supplier_code) as o_Supplier_Code",
	"TO_DATE ( FIL_UNKNOWN_RECORDS___Date , 'yyyy-MM-dd' ) as o_Date",
	"FIL_UNKNOWN_RECORDS___Projected_order_proposals as Projected_order_proposals",
	"FIL_UNKNOWN_RECORDS___Projected_deliveries as Projected_deliveries",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE = EXP_DATE_CONV.selectExpr(
	"CAST(o_Date AS DATE) as DAY_DATE",
	"CAST(Location_code AS INT) as LOCATION_CODE",
	"CAST(Product_code AS INT) as PRODUCT_CODE",
	"CAST(o_Supplier_Code AS STRING) as SUPPLIER_CODE",
	"CAST(Projected_order_proposals AS DECIMAL(12,4)) as PROJECTED_ORDER_PROPOSALS",
	"CAST(Projected_deliveries AS DECIMAL(12,4)) as PROJECTED_DELIVERIES",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)

Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE.write.saveAsTable(f'{raw}.RELEX_DAILY_ORDER_PROJECTION_PRE', mode = 'overwrite')
