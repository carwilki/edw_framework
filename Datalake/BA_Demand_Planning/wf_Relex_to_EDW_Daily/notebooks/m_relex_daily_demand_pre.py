# Databricks notebook source
#Code converted on 2023-08-25 11:19:45
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
#currdate = starttime.strftime('%Y%m%d')
#currdate = '20230901' #only for validation, after validation remove this line

#dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-qa-raw-p1-gcs-gbl/nas/demand_planing/daily_demand')
#file_path = dbutils.widgets.get('file_path') + f'/{currdate}'

_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily','source_bucket')
complete_bucket = _bucket + 'daily_demand/'
key ="daily"

source_file = get_src_file(key, complete_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_RELEX_Daily_Demand_FF, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_RELEX_Daily_Demand_FF = spark.read.option('header',False).option('sep', ';').option('skipRows',1).option('ignoreTrailingWhiteSpace', True).option('inferSchema',True).csv(source_file).withColumn("sys_row_id", monotonically_increasing_id())\
.withColumnRenamed('_c0' ,'Date')\
.withColumnRenamed('_c1' ,'Location_code')\
.withColumnRenamed('_c2' ,'Product_code')\
.withColumnRenamed('_c3' ,'Supplier_code')\
.withColumnRenamed('_c4' ,'Ordering_need')\
.withColumnRenamed('_c5' ,'Needed_quantity')\
.withColumnRenamed('_c6' ,'Proposed_quantity')\
.withColumnRenamed('_c7' ,'Effective_quantity')

# COMMAND ----------

# Processing node FIL_UNKNOWN_RECORDS, type FILTER 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RELEX_Daily_Demand_FF_temp = SQ_Shortcut_to_RELEX_Daily_Demand_FF.toDF(*["SQ_Shortcut_to_RELEX_Daily_Demand_FF___" + col for col in SQ_Shortcut_to_RELEX_Daily_Demand_FF.columns])

FIL_UNKNOWN_RECORDS = SQ_Shortcut_to_RELEX_Daily_Demand_FF_temp.selectExpr(
	"SQ_Shortcut_to_RELEX_Daily_Demand_FF___Date as Date",
	"SQ_Shortcut_to_RELEX_Daily_Demand_FF___Location_code as Location_code",
	"SQ_Shortcut_to_RELEX_Daily_Demand_FF___Product_code as Product_code",
	"SQ_Shortcut_to_RELEX_Daily_Demand_FF___Supplier_code as Supplier_code",
	"SQ_Shortcut_to_RELEX_Daily_Demand_FF___Ordering_need as Ordering_need",
	"SQ_Shortcut_to_RELEX_Daily_Demand_FF___Needed_quantity as Needed_quantity",
	"SQ_Shortcut_to_RELEX_Daily_Demand_FF___Proposed_quantity as Proposed_quantity",
	"SQ_Shortcut_to_RELEX_Daily_Demand_FF___Effective_quantity as Effective_quantity").filter("Supplier_code != '<UNKNOWN>' AND Supplier_code IS NOT NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_DATE_CONV, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
FIL_UNKNOWN_RECORDS_temp = FIL_UNKNOWN_RECORDS.toDF(*["FIL_UNKNOWN_RECORDS___" + col for col in FIL_UNKNOWN_RECORDS.columns])

EXP_DATE_CONV = FIL_UNKNOWN_RECORDS_temp.selectExpr(
	"FIL_UNKNOWN_RECORDS___sys_row_id as sys_row_id",
	"TO_DATE ( FIL_UNKNOWN_RECORDS___Date , 'YYYY-MM_DD' ) as o_Date",
	"FIL_UNKNOWN_RECORDS___Location_code as Location_code",
	"FIL_UNKNOWN_RECORDS___Product_code as Product_code",
	"IF (FIL_UNKNOWN_RECORDS___Supplier_code IS NULL, ' ', FIL_UNKNOWN_RECORDS___Supplier_code) as o_Supplier_code",
	"FIL_UNKNOWN_RECORDS___Ordering_need as Ordering_need",
	"FIL_UNKNOWN_RECORDS___Needed_quantity as Needed_quantity",
	"FIL_UNKNOWN_RECORDS___Effective_quantity as Effective_quantity",
	"FIL_UNKNOWN_RECORDS___Proposed_quantity as Proposed_quantity",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_RELEX_DAILY_DEMAND_PRE, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_RELEX_DAILY_DEMAND_PRE = EXP_DATE_CONV.selectExpr(
	"CAST(o_Date AS DATE) as DAY_DATE",
	"CAST(Location_code AS INT) as LOCATION_CODE",
	"CAST(Product_code AS INT) as PRODUCT_CODE",
	"CAST(o_Supplier_code AS STRING) as SUPPLIER_CODE",
	"CAST(Ordering_need AS DECIMAL(12,4)) as ORDERING_NEED",
	"CAST(Needed_quantity AS DECIMAL(12,4)) as NEEDED_QUANTITY",
	"CAST(Effective_quantity AS DECIMAL(12,4)) as EFFECTIVE_QUANTITY",
	"CAST(Proposed_quantity AS DECIMAL(12,4)) as PROPOSED_QUANTITY",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_RELEX_DAILY_DEMAND_PRE.write.saveAsTable(f'{raw}.RELEX_DAILY_DEMAND_PRE', mode = 'overwrite')

# COMMAND ----------


