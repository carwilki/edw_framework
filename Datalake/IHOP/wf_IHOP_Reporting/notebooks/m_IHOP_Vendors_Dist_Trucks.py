# Databricks notebook source
# Code converted on 2023-08-24 13:54:04
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


# COMMAND ----------

# Processing node SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE, type SOURCE 
# COLUMN COUNT: 13

SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE = spark.sql(f"""select event_id,

       v.vid,

       l.location_id,

       smoothing_type_id,

       p.product_id,

       primary_wave_id,

       delivery_date,

       distribution_center_id,

       vendor_nbr,

       sku,

       quantity,

       volume,

       truck_count

from {raw}.IHOP_VENDORS_DIST_TRUCKS_PRE a

JOIN (select vendor_nbr, max(vendor_id) vid from {legacy}.VENDOR_PROFILE group by vendor_nbr) v

     on CAST(A.VENDOR_ID AS VARCHAR(10)) = decode(v.VENDOR_NBR,'V0043','900043',v.VENDOR_NBR)

JOIN {legacy}.SKU_PROFILE p

     on a.sku = p.sku_nbr

JOIN {legacy}.SITE_PROFILE l

     on a.distribution_center_id = l.store_nbr""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE = SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[0],'EVENT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[1],'VENDOR_ID1') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[2],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[3],'SMOOTHING_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[4],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[5],'PRIMARY_WAVE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[6],'DELIVERY_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[7],'DISTRIBUTION_CENTER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[8],'VENDOR_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[9],'SKU') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[10],'QUANTITY') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[11],'VOLUME') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns[12],'TRUCK_COUNT')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 14
# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE_temp = SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.toDF(*["SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___" + col for col in SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.columns])

EXPTRANS = SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE_temp.selectExpr(
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___EVENT_ID as EVENT_ID",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___VENDOR_ID1 as VENDOR_ID1",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___SMOOTHING_TYPE_ID as SMOOTHING_TYPE_ID",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___PRIMARY_WAVE_ID as PRIMARY_WAVE_ID",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___DELIVERY_DATE as DELIVERY_DATE",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___DISTRIBUTION_CENTER_ID as DISTRIBUTION_CENTER_ID",
	"string(SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___VENDOR_ID) as o_VENDOR_NBR",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___SKU as SKU",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___QUANTITY as QUANTITY",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___VOLUME as VOLUME",
	"SQ_Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE___TRUCK_COUNT as TRUCK_COUNT",
	"CURRENT_TIMESTAMP as o_LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_IHOP_VENDORS_DIST_TRUCKS, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_IHOP_VENDORS_DIST_TRUCKS = EXPTRANS.selectExpr(
	"CAST(EVENT_ID AS INT) as EVENT_ID",
	"VENDOR_ID1 as VENDOR_ID",
	"CAST(LOCATION_ID AS INT) as FROM_LOCATION_ID",
	"CAST(SMOOTHING_TYPE_ID AS INT) as SMOOTHING_TYPE_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(PRIMARY_WAVE_ID AS INT) as PRIMARY_WAVE_ID",
	"CAST(DELIVERY_DATE AS TIMESTAMP) as DELIVERY_DATE",
	"CAST(DISTRIBUTION_CENTER_ID AS INT) as DISTRIBUTION_CENTER_ID",
	"CAST(o_VENDOR_NBR AS STRING) as VENDOR_NBR",
	"CAST(SKU AS INT) as SKU",
	"CAST(QUANTITY AS INT) as QUANTITY",
	"CAST(VOLUME AS DECIMAL(13,6)) as VOLUME",
	"CAST(TRUCK_COUNT AS DECIMAL(13,6)) as TRUCK_COUNT",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
# overwriteDeltaPartition(Shortcut_to_IHOP_VENDORS_DIST_TRUCKS,'DC_NBR',dcnbr,f'{raw}.IHOP_VENDORS_DIST_TRUCKS')
Shortcut_to_IHOP_VENDORS_DIST_TRUCKS.write.mode("overwrite").saveAsTable(f'{legacy}.IHOP_VENDORS_DIST_TRUCKS')

# COMMAND ----------


