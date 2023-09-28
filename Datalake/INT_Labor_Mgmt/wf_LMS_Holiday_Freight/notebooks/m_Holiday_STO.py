# Databricks notebook source
# Code converted on 2023-09-05 14:08:54
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

# Processing node SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1 = spark.sql(f""" SELECT ZTB_HOLIDAY_STO_PRE.PO_NBR, ZTB_HOLIDAY_STO_PRE.VENDOR_ID, ZTB_HOLIDAY_STO_PRE.PURCH_GROUP_ID, ZTB_HOLIDAY_STO_PRE.DELIVERY_DATE, ZTB_HOLIDAY_STO_PRE.EXE_SENT_DATE, ZTB_HOLIDAY_STO_PRE.TMS_SENT_DATE, ZTB_HOLIDAY_STO_PRE.TMS_PROCESSED, SITE_PROFILE.LOCATION_ID, STO_TYPE_LU.STO_TYPE_ID 
FROM  {raw}.ZTB_HOLIDAY_STO_PRE   
LEFT OUTER JOIN  {legacy}.STO_TYPE_LU ON  ZTB_HOLIDAY_STO_PRE.sto_type = STO_TYPE_LU.sto_type 
LEFT OUTER JOIN  {legacy}.SITE_PROFILE ON  ZTB_HOLIDAY_STO_PRE.store_nbr = SITE_PROFILE.store_nbr 
WHERE  STO_TYPE_LU.STR_END_DATE > CURRENT_DATE - 1 
""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
# changed to for validation STO_TYPE_LU.STR_END_DATE > to_date("2023-09-07", "yyyy-MM-dd") - 1 
SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1 = SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1 \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.columns[0],'PO_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.columns[1],'VENDOR_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.columns[2],'PURCH_GROUP_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.columns[3],'DELIVERY_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.columns[4],'EXE_SENT_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.columns[5],'TMS_SENT_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.columns[6],'TMS_PROCESSED') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.columns[7],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.columns[8],'STO_TYPE_ID')

# COMMAND ----------

# Processing node Shortcut_to_HOLIDAY_STO, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_HOLIDAY_STO = SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.selectExpr(
	"PO_NBR as PO_NBR",
	"VENDOR_ID as VENDOR_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(PURCH_GROUP_ID AS INT) as PURCH_GROUP_ID",
	"CAST(STO_TYPE_ID AS INT) as STO_TYPE_ID",
	"TO_TIMESTAMP(DELIVERY_DATE, 'MM/dd/yyyy HH:mm:ss') as DELIVERY_DATE",
	"TO_TIMESTAMP(EXE_SENT_DATE, 'MM/dd/yyyy HH:mm:ss') as EXE_SENT_DATE",
	"TO_TIMESTAMP(TMS_SENT_DATE, 'MM/dd/yyyy HH:mm:ss') as TMS_SENT_DATE",
	"TMS_PROCESSED as TMS_PROCESSED"
)
# Shortcut_to_HOLIDAY_STO = SQ_Shortcut_to_ZTB_HOLIDAY_STO_PRE1.selectExpr(
# 	"PO_NBR as PO_NBR",
# 	"VENDOR_ID as VENDOR_ID",
# 	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
# 	"CAST(PURCH_GROUP_ID AS INT) as PURCH_GROUP_ID",
# 	"CAST(STO_TYPE_ID AS INT) as STO_TYPE_ID",
# 	"CAST(DELIVERY_DATE AS TIMESTAMP) as DELIVERY_DATE",
# 	"CAST(EXE_SENT_DATE AS TIMESTAMP) as EXE_SENT_DATE",
# 	"CAST(TMS_SENT_DATE AS TIMESTAMP) as TMS_SENT_DATE",
# 	"TMS_PROCESSED as TMS_PROCESSED"
# )
Shortcut_to_HOLIDAY_STO.write.mode("overwrite").saveAsTable(f'{legacy}.HOLIDAY_STO')

# try:
#     primary_key = "source.PO_NBR = target.PO_NBR"
#     legacy_table = f"{legacy}.HOLIDAY_STO"
#     executeMerge(Shortcut_to_HOLIDAY_STO, legacy_table, primary_key)
#     logger.info("Merge with" + legacy_table + "completed")
#     logPrevRunDt("HOLIDAY_STO", "HOLIDAY_STO", "Completed", "N/A", f"{raw}.log_run_details")
# except Exception as e:
#     logPrevRunDt("HOLIDAY_STO", "HOLIDAY_STO", "Failed", str(e), f"{raw}.log_run_details")
#     raise e
# COMMAND ----------


