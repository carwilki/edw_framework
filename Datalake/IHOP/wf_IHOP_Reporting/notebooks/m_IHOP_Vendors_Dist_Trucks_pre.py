# Databricks notebook source
# Code converted on 2023-08-24 13:54:00
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

(username,password,connection_string) = mtx_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_VendorsToDistributionCentersTrucks, type SOURCE 
# COLUMN COUNT: 10

SQ_Shortcut_to_VendorsToDistributionCentersTrucks = jdbcSqlServerConnection(f"""(SELECT
VendorsToDistributionCentersTrucks.EventId,
VendorsToDistributionCentersTrucks.VendorId,
VendorsToDistributionCentersTrucks.DistributionCenterId,
VendorsToDistributionCentersTrucks.VendorToDcSmoothingTypeId,
VendorsToDistributionCentersTrucks.SKU,
VendorsToDistributionCentersTrucks.PrimaryWaveId,
VendorsToDistributionCentersTrucks.DeliveryDate,
VendorsToDistributionCentersTrucks.Quantity,
VendorsToDistributionCentersTrucks.Volume,
VendorsToDistributionCentersTrucks.TruckCount
FROM HolidayPlanning.dbo.VendorsToDistributionCentersTrucks) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE = SQ_Shortcut_to_VendorsToDistributionCentersTrucks.selectExpr(
	"CAST(EventId AS INT) as EVENT_ID",
	"CAST(VendorId AS INT) as VENDOR_ID",
	"CAST(DistributionCenterId AS INT) as DISTRIBUTION_CENTER_ID",
	"CAST(VendorToDcSmoothingTypeId AS INT) as SMOOTHING_TYPE_ID",
	"CAST(SKU AS INT) as SKU",
	"CAST(PrimaryWaveId AS INT) as PRIMARY_WAVE_ID",
	"CAST(DeliveryDate AS TIMESTAMP) as DELIVERY_DATE",
	"CAST(Quantity AS INT) as QUANTITY",
	"CAST(Volume AS DECIMAL(13,6)) as VOLUME",
	"CAST(TruckCount AS DECIMAL(13,6)) as TRUCK_COUNT"
)
# overwriteDeltaPartition(Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE,'DC_NBR',dcnbr,f'{raw}.IHOP_VENDORS_DIST_TRUCKS_PRE')
Shortcut_to_IHOP_VENDORS_DIST_TRUCKS_PRE.write.mode("overwrite").saveAsTable(f'{raw}.IHOP_VENDORS_DIST_TRUCKS_PRE')

# COMMAND ----------


