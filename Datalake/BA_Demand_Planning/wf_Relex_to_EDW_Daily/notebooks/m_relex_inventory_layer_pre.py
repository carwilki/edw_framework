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
#currdate = '20230926' #only for validation, after validation please remove this line

#dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/demand_planing/inventory_layers')
#file_path = dbutils.widgets.get('file_path') + f'/{currdate}'

#_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily.M:m_relex_inventory_layer_pre','source_bucket')
#file_path = _bucket + f'/{currdate}'

_bucket=getParameterValue(raw,'BA_Demand_Planning_Parameter.prm','BA_Demand_Planning.WF:wf_relex_to_EDW_Daily','source_bucket')
complete_bucket = _bucket + 'inventory_layers/'
key ="inventory_layers"

source_file = get_src_file(key, complete_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_inventory_layers_FF, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_inventory_layers_FF = spark.read.option('header',False).option('sep', ';').option('skipRows',1).option('ignoreTrailingWhiteSpace', True).option('inferSchema',True).csv(source_file).withColumn("sys_row_id", monotonically_increasing_id())\
.withColumnRenamed('_c0' ,'Product_code')\
.withColumnRenamed('_c1' ,'Location_code')\
.withColumnRenamed('_c2' ,'End_balance')\
.withColumnRenamed('_c3' ,'Order_parameter__qty')\
.withColumnRenamed('_c4' ,'Min__fill')\
.withColumnRenamed('_c5' ,'Order_batch_size')\
.withColumnRenamed('_c6' ,'EOQ')\
.withColumnRenamed('_c7' ,'Min_delivery')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_inventory_layers_FF_temp = SQ_Shortcut_to_inventory_layers_FF.toDF(*["SQ_Shortcut_to_inventory_layers_FF___" + col for col in SQ_Shortcut_to_inventory_layers_FF.columns])

EXPTRANS = SQ_Shortcut_to_inventory_layers_FF_temp.selectExpr(
	"SQ_Shortcut_to_inventory_layers_FF___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_inventory_layers_FF___Product_code as Product_code",
	"SQ_Shortcut_to_inventory_layers_FF___Location_code as Location_code",
	"SQ_Shortcut_to_inventory_layers_FF___End_balance as End_balance",
	"SQ_Shortcut_to_inventory_layers_FF___Order_parameter__qty as Order_parameter__qty",
	"SQ_Shortcut_to_inventory_layers_FF___Min__fill as Min__fill",
	"SQ_Shortcut_to_inventory_layers_FF___Order_batch_size as Order_batch_size",
	"CURRENT_TIMESTAMP as o_LOAD_TSTMP",
	"SQ_Shortcut_to_inventory_layers_FF___EOQ as EOQ",
	"SQ_Shortcut_to_inventory_layers_FF___Min_delivery as Min_delivery"
)

# COMMAND ----------

# Processing node Shortcut_to_RELEX_INVENTORY_LAYER_PRE, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_RELEX_INVENTORY_LAYER_PRE = EXPTRANS.selectExpr(
	"CAST(Location_code AS INT) as LOCATION_CODE",
	"CAST(Product_code AS INT) as PRODUCT_CODE",
	"CAST(End_balance AS STRING) as END_BALANCE",
	"CAST(Order_parameter__qty AS DECIMAL(12,4)) as ORDER_PARAMETER_QTY",
	"CAST(Min__fill AS DECIMAL(12,4)) as MIN_FILL",
	"CAST(Order_batch_size AS DECIMAL(12,4)) as ORDER_BATCH_SIZE",
	"CAST(EOQ AS DECIMAL(12,4)) as ECONOMIC_ORDER_QTY",
	"CAST(Min_delivery AS DECIMAL(12,4)) as MIN_DELIVERY_QTY",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_RELEX_INVENTORY_LAYER_PRE.write.saveAsTable(f'{raw}.RELEX_INVENTORY_LAYER_PRE', mode = 'overwrite')
