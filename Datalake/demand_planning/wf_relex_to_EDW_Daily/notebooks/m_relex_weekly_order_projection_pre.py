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

#dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/demand_planing/weekly_order_projection')
#file_path = dbutils.widgets.get('file_path') + f'/{currdate}'

#_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily.M:m_relex_weekly_order_projection_pre','source_bucket')
#file_path = _bucket + f'/{currdate}'

_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily','source_bucket')
complete_bucket = _bucket + 'weekly_order_projection/'
key ="weekly_order_projection"

source_file = get_src_file(key, complete_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_weekly_order_projection, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_weekly_order_projection = spark.read.option('header',False).option('sep', ';').option('skipRows',1).option('ignoreTrailingWhiteSpace', True).option('inferSchema',True).csv(source_file).withColumn("sys_row_id", monotonically_increasing_id())\
.withColumnRenamed('_c0' ,'Location_code')\
.withColumnRenamed('_c1' ,'Product_code')\
.withColumnRenamed('_c2' ,'Supplier_code')\
.withColumnRenamed('_c3' ,'Date')\
.withColumnRenamed('_c4' ,'Weekly_Projected_Deliveries')\
.withColumnRenamed('_c5' ,'Weekly_Projected_Order_Proposals')

# COMMAND ----------

# Processing node FIL_UNKNOWN_SUPPLIER_CODE_RECS, type FILTER 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_weekly_order_projection_temp = SQ_Shortcut_to_weekly_order_projection.toDF(*["SQ_Shortcut_to_weekly_order_projection___" + col for col in SQ_Shortcut_to_weekly_order_projection.columns])

FIL_UNKNOWN_SUPPLIER_CODE_RECS = SQ_Shortcut_to_weekly_order_projection_temp.selectExpr(
	"SQ_Shortcut_to_weekly_order_projection___Location_code as Location_code",
	"SQ_Shortcut_to_weekly_order_projection___Product_code as Product_code",
	"SQ_Shortcut_to_weekly_order_projection___Supplier_code as Supplier_code",
	"SQ_Shortcut_to_weekly_order_projection___Date as Date",
	"SQ_Shortcut_to_weekly_order_projection___Weekly_Projected_Order_Proposals as Weekly_Projected_Order_Proposals",
	"SQ_Shortcut_to_weekly_order_projection___Weekly_Projected_Deliveries as Weekly_Projected_Deliveries").filter("Supplier_code IS NOT NULL AND Supplier_code != '<UNKNOWN>'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_DEFAULTS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
FIL_UNKNOWN_SUPPLIER_CODE_RECS_temp = FIL_UNKNOWN_SUPPLIER_CODE_RECS.toDF(*["FIL_UNKNOWN_SUPPLIER_CODE_RECS___" + col for col in FIL_UNKNOWN_SUPPLIER_CODE_RECS.columns])

# .selectExpr(
# 	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Location_code as Location_code",
# 	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Product_code as Product_code",
# 	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Supplier_code as Supplier_code",
# 	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Date as in_Date",
# 	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Weekly_Projected_Order_Proposals as Weekly_Projected_Order_Proposals",
# 	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Weekly_Projected_Deliveries as Weekly_Projected_Deliveries")
EXP_DEFAULTS = FIL_UNKNOWN_SUPPLIER_CODE_RECS_temp.selectExpr(
	# "FIL_UNKNOWN_SUPPLIER_CODE_RECS___sys_row_id as sys_row_id",
	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Location_code as Location_code",
	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Product_code as Product_code",
	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Supplier_code as Supplier_code",
	"TO_DATE ( FIL_UNKNOWN_SUPPLIER_CODE_RECS___Date , 'yyyy-MM-dd' ) as o_Date",
	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Weekly_Projected_Order_Proposals as Weekly_Projected_Order_Proposals",
	"FIL_UNKNOWN_SUPPLIER_CODE_RECS___Weekly_Projected_Deliveries as Weekly_Projected_Deliveries",
	"CURRENT_TIMESTAMP as o_LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_RELEX_WEEKLY_ORDER_PROJECTION_PRE, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_RELEX_WEEKLY_ORDER_PROJECTION_PRE = EXP_DEFAULTS.selectExpr(
	"CAST(o_Date AS DATE) as WEEK_DATE",
	"CAST(Location_code AS INT) as LOCATION_CODE",
	"CAST(Product_code AS INT) as PRODUCT_CODE",
	"CAST(Supplier_code AS STRING) as SUPPLIER_CODE",
	"CAST(Weekly_Projected_Order_Proposals AS DECIMAL(12,4)) as PROJECTED_ORDER_PROPOSALS",
	"CAST(Weekly_Projected_Deliveries AS DECIMAL(12,4)) as PROJECTED_DELIVERIES",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_RELEX_WEEKLY_ORDER_PROJECTION_PRE.write.saveAsTable(f'{raw}.RELEX_WEEKLY_ORDER_PROJECTION_PRE', mode = 'overwrite')
