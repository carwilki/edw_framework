# Databricks notebook source
#Code converted on 2023-10-06 15:46:09
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'

# Set global variables
starttime = datetime.now() #start timestamp of the script

source_bucket=getParameterValue(raw,'bs_Demand_Planning_Day.prm','Demand_Planning.WF:bs_Demand_Planning_Day.M:m_dp_site_vend_pre','source_bucket')
key=getParameterValue(raw,'bs_Demand_Planning_Day.prm','Demand_Planning.WF:bs_Demand_Planning_Day.M:m_dp_site_vend_pre','key')

source_file=get_src_file(key,source_bucket)



# COMMAND ----------

print(source_file)

# COMMAND ----------

# Processing node SQ_Shortcut_to_ZTB_RELATE_MAST, type APPLICATION_SOURCE_QUALIFIER 
# COLUMN COUNT: 75

# ---------- No handler defined for type APPLICATION_SOURCE_QUALIFIER, node SQ_Shortcut_to_ZTB_RELATE_MAST, job m_dp_site_vend_pre ---------- #

SQ_Shortcut_to_ZTB_RELATE_MAST = spark.read.csv(source_file,sep='|',header=True, inferSchema=True)


# COMMAND ----------

# Processing node FLT_GOOD_VENDOR, type FILTER 
# COLUMN COUNT: 75

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ZTB_RELATE_MAST_temp = SQ_Shortcut_to_ZTB_RELATE_MAST.toDF(*["SQ_Shortcut_to_ZTB_RELATE_MAST___" + col for col in SQ_Shortcut_to_ZTB_RELATE_MAST.columns])

FLT_GOOD_VENDOR = SQ_Shortcut_to_ZTB_RELATE_MAST_temp.selectExpr(
	"SQ_Shortcut_to_ZTB_RELATE_MAST___SITE as SITE",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___VENDOR as VENDOR",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___VENDOR_SUBRANGE as VENDOR_SUBRANGE",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___EKGRP as EKGRP",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___TOTAL_LEADTIME as TOTAL_LEADTIME",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ADJUST_LEADTIME as ADJUST_LEADTIME",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___MANDT as MANDT",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DPR_ORDER_ARRAY as DPR_ORDER_ARRAY",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___PROD_LEADTIME as PROD_LEADTIME",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___TRANSIT_LEADTIME as TRANSIT_LEADTIME",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ADJUST_TIME_SUN as ADJUST_TIME_SUN",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___TRANSIT_TIME_SUN as TRANSIT_TIME_SUN",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ADJUST_TIME_MON as ADJUST_TIME_MON",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___TRANSIT_TIME_MON as TRANSIT_TIME_MON",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ADJUST_TIME_TUE as ADJUST_TIME_TUE",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___TRANSIT_TIME_TUE as TRANSIT_TIME_TUE",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ADJUST_TIME_WED as ADJUST_TIME_WED",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___TRANSIT_TIME_WED as TRANSIT_TIME_WED",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ADJUST_TIME_THU as ADJUST_TIME_THU",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___TRANSIT_TIME_THU as TRANSIT_TIME_THU",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ADJUST_TIME_FRI as ADJUST_TIME_FRI",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___TRANSIT_TIME_FRI as TRANSIT_TIME_FRI",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ADJUST_TIME_SAT as ADJUST_TIME_SAT",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___TRANSIT_TIME_SAT as TRANSIT_TIME_SAT",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ALT_ORIGIN as ALT_ORIGIN",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_START_TIME_SUN as DEL_START_TIME_SUN",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_END_TIME_SUN as DEL_END_TIME_SUN",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_START_TIME_MON as DEL_START_TIME_MON",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_END_TIME_MON as DEL_END_TIME_MON",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_START_TIME_TUE as DEL_START_TIME_TUE",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_END_TIME_TUE as DEL_END_TIME_TUE",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_START_TIME_WED as DEL_START_TIME_WED",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_END_TIME_WED as DEL_END_TIME_WED",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_START_TIME_THU as DEL_START_TIME_THU",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_END_TIME_THU as DEL_END_TIME_THU",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_START_TIME_FRI as DEL_START_TIME_FRI",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_END_TIME_FRI as DEL_END_TIME_FRI",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_START_TIME_SAT as DEL_START_TIME_SAT",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_END_TIME_SAT as DEL_END_TIME_SAT",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___PICK_DAY1 as PICK_DAY1",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___PICK_DAY2 as PICK_DAY2",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___PICK_DAY3 as PICK_DAY3",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___PICK_DAY4 as PICK_DAY4",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___PICK_DAY5 as PICK_DAY5",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___PICK_DAY6 as PICK_DAY6",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___PICK_DAY7 as PICK_DAY7",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_DAY1 as DEL_DAY1",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_DAY2 as DEL_DAY2",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_DAY3 as DEL_DAY3",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_DAY4 as DEL_DAY4",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_DAY5 as DEL_DAY5",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_DAY6 as DEL_DAY6",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___DEL_DAY7 as DEL_DAY7",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___BLUE_GREEN_FLAG as BLUE_GREEN_FLAG",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___PROTECTION_LEVEL as PROTECTION_LEVEL",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___OVERRIDE_ORDER_WEIGHT as OVERRIDE_ORDER_WEIGHT",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___OVERRIDE_ORDER_VOLUME as OVERRIDE_ORDER_VOLUME",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___CHANGE_DATE as CHANGE_DATE",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___CHANGE_TIME as CHANGE_TIME",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___NOTES as NOTES",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___OVERRIDE_FLAG as OVERRIDE_FLAG",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___CROSSDOCK_ID as CROSSDOCK_ID",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ALT_ORIGIN1 as ALT_ORIGIN1",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ALT_ORIGIN2 as ALT_ORIGIN2",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ALT_ORIGIN3 as ALT_ORIGIN3",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ALT_ORIGIN4 as ALT_ORIGIN4",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ALT_ORIGIN5 as ALT_ORIGIN5",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ALT_ORIGIN6 as ALT_ORIGIN6",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___ALT_ORIGIN7 as ALT_ORIGIN7",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___CROSSDOCK_ID2 as CROSSDOCK_ID2",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___CROSSDOCK_ID3 as CROSSDOCK_ID3",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___CROSSDOCK_ID4 as CROSSDOCK_ID4",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___CROSSDOCK_ID5 as CROSSDOCK_ID5",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___CROSSDOCK_ID6 as CROSSDOCK_ID6",
	"SQ_Shortcut_to_ZTB_RELATE_MAST___CROSSDOCK_ID7 as CROSSDOCK_ID7").filter("VENDOR != '*DEFAULT  ' AND VENDOR != '          ' AND cast(SITE as double) IS NOT NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_DP_SITE_VEND_PRE, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 79

# for each involved DataFrame, append the dataframe name to each column
FLT_GOOD_VENDOR_temp = FLT_GOOD_VENDOR.toDF(*["FLT_GOOD_VENDOR___" + col for col in FLT_GOOD_VENDOR.columns])

EXP_DP_SITE_VEND_PRE = FLT_GOOD_VENDOR_temp.selectExpr(
	"FLT_GOOD_VENDOR___SITE as SITE",
	"FLT_GOOD_VENDOR___VENDOR as VENDOR",
	"FLT_GOOD_VENDOR___VENDOR_SUBRANGE as VENDOR_SUBRANGE",
	"FLT_GOOD_VENDOR___EKGRP as EKGRP",
	"FLT_GOOD_VENDOR___TOTAL_LEADTIME as TOTAL_LEADTIME",
	"FLT_GOOD_VENDOR___ADJUST_LEADTIME as ADJUST_LEADTIME",
	"FLT_GOOD_VENDOR___MANDT as MANDT",
	"FLT_GOOD_VENDOR___DPR_ORDER_ARRAY as DPR_ORDER_ARRAY",
	"FLT_GOOD_VENDOR___PROD_LEADTIME as PROD_LEADTIME",
	"FLT_GOOD_VENDOR___TRANSIT_LEADTIME as TRANSIT_LEADTIME",
	"FLT_GOOD_VENDOR___ADJUST_TIME_SUN as ADJUST_TIME_SUN",
	"FLT_GOOD_VENDOR___TRANSIT_TIME_SUN as TRANSIT_TIME_SUN",
	"FLT_GOOD_VENDOR___ADJUST_TIME_MON as ADJUST_TIME_MON",
	"FLT_GOOD_VENDOR___TRANSIT_TIME_MON as TRANSIT_TIME_MON",
	"FLT_GOOD_VENDOR___ADJUST_TIME_TUE as ADJUST_TIME_TUE",
	"FLT_GOOD_VENDOR___TRANSIT_TIME_TUE as TRANSIT_TIME_TUE",
	"FLT_GOOD_VENDOR___ADJUST_TIME_WED as ADJUST_TIME_WED",
	"FLT_GOOD_VENDOR___TRANSIT_TIME_WED as TRANSIT_TIME_WED",
	"FLT_GOOD_VENDOR___ADJUST_TIME_THU as ADJUST_TIME_THU",
	"FLT_GOOD_VENDOR___TRANSIT_TIME_THU as TRANSIT_TIME_THU",
	"FLT_GOOD_VENDOR___ADJUST_TIME_FRI as ADJUST_TIME_FRI",
	"FLT_GOOD_VENDOR___TRANSIT_TIME_FRI as TRANSIT_TIME_FRI",
	"FLT_GOOD_VENDOR___ADJUST_TIME_SAT as ADJUST_TIME_SAT",
	"FLT_GOOD_VENDOR___TRANSIT_TIME_SAT as TRANSIT_TIME_SAT",
	"FLT_GOOD_VENDOR___ALT_ORIGIN as ALT_ORIGIN",
	"FLT_GOOD_VENDOR___DEL_START_TIME_SUN as DEL_START_TIME_SUN",
	"FLT_GOOD_VENDOR___DEL_END_TIME_SUN as DEL_END_TIME_SUN",
	"FLT_GOOD_VENDOR___DEL_START_TIME_MON as DEL_START_TIME_MON",
	"FLT_GOOD_VENDOR___DEL_END_TIME_MON as DEL_END_TIME_MON",
	"FLT_GOOD_VENDOR___DEL_START_TIME_TUE as DEL_START_TIME_TUE",
	"FLT_GOOD_VENDOR___DEL_END_TIME_TUE as DEL_END_TIME_TUE",
	"FLT_GOOD_VENDOR___DEL_START_TIME_WED as DEL_START_TIME_WED",
	"FLT_GOOD_VENDOR___DEL_END_TIME_WED as DEL_END_TIME_WED",
	"FLT_GOOD_VENDOR___DEL_START_TIME_THU as DEL_START_TIME_THU",
	"FLT_GOOD_VENDOR___DEL_END_TIME_THU as DEL_END_TIME_THU",
	"FLT_GOOD_VENDOR___DEL_START_TIME_FRI as DEL_START_TIME_FRI",
	"FLT_GOOD_VENDOR___DEL_END_TIME_FRI as DEL_END_TIME_FRI",
	"FLT_GOOD_VENDOR___DEL_START_TIME_SAT as DEL_START_TIME_SAT",
	"FLT_GOOD_VENDOR___DEL_END_TIME_SAT as DEL_END_TIME_SAT",
	"FLT_GOOD_VENDOR___PICK_DAY1 as PICK_DAY1",
	"FLT_GOOD_VENDOR___PICK_DAY2 as PICK_DAY2",
	"FLT_GOOD_VENDOR___PICK_DAY3 as PICK_DAY3",
	"FLT_GOOD_VENDOR___PICK_DAY4 as PICK_DAY4",
	"FLT_GOOD_VENDOR___PICK_DAY5 as PICK_DAY5",
	"FLT_GOOD_VENDOR___PICK_DAY6 as PICK_DAY6",
	"FLT_GOOD_VENDOR___PICK_DAY7 as PICK_DAY7",
	"FLT_GOOD_VENDOR___DEL_DAY1 as DEL_DAY1",
	"FLT_GOOD_VENDOR___DEL_DAY2 as DEL_DAY2",
	"FLT_GOOD_VENDOR___DEL_DAY3 as DEL_DAY3",
	"FLT_GOOD_VENDOR___DEL_DAY4 as DEL_DAY4",
	"FLT_GOOD_VENDOR___DEL_DAY5 as DEL_DAY5",
	"FLT_GOOD_VENDOR___DEL_DAY6 as DEL_DAY6",
	"FLT_GOOD_VENDOR___DEL_DAY7 as DEL_DAY7",
	"FLT_GOOD_VENDOR___BLUE_GREEN_FLAG as BLUE_GREEN_FLAG",
	"FLT_GOOD_VENDOR___PROTECTION_LEVEL as PROTECTION_LEVEL",
	"FLT_GOOD_VENDOR___OVERRIDE_ORDER_WEIGHT as OVERRIDE_ORDER_WEIGHT",
	"FLT_GOOD_VENDOR___OVERRIDE_ORDER_VOLUME as OVERRIDE_ORDER_VOLUME",
	"FLT_GOOD_VENDOR___CHANGE_DATE as CHANGE_DATE",
	"FLT_GOOD_VENDOR___CHANGE_TIME as CHANGE_TIME",
	"FLT_GOOD_VENDOR___NOTES as NOTES",
	"FLT_GOOD_VENDOR___OVERRIDE_FLAG as OVERRIDE_FLAG",
	"FLT_GOOD_VENDOR___CROSSDOCK_ID as CROSSDOCK_ID",
	"FLT_GOOD_VENDOR___ALT_ORIGIN1 as ALT_ORIGIN1",
	"FLT_GOOD_VENDOR___ALT_ORIGIN2 as ALT_ORIGIN2",
	"FLT_GOOD_VENDOR___ALT_ORIGIN3 as ALT_ORIGIN3",
	"FLT_GOOD_VENDOR___ALT_ORIGIN4 as ALT_ORIGIN4",
	"FLT_GOOD_VENDOR___ALT_ORIGIN5 as ALT_ORIGIN5",
	"FLT_GOOD_VENDOR___ALT_ORIGIN6 as ALT_ORIGIN6",
	"FLT_GOOD_VENDOR___ALT_ORIGIN7 as ALT_ORIGIN7",
	"FLT_GOOD_VENDOR___CROSSDOCK_ID2 as CROSSDOCK_ID2",
	"FLT_GOOD_VENDOR___CROSSDOCK_ID3 as CROSSDOCK_ID3",
	"FLT_GOOD_VENDOR___CROSSDOCK_ID4 as CROSSDOCK_ID4",
	"FLT_GOOD_VENDOR___CROSSDOCK_ID5 as CROSSDOCK_ID5",
	"FLT_GOOD_VENDOR___CROSSDOCK_ID6 as CROSSDOCK_ID6",
	"FLT_GOOD_VENDOR___CROSSDOCK_ID7 as CROSSDOCK_ID7") \
	.withColumn('NEWFIELD', lit(None)) \
	.withColumn('NEWFIELD1', lit(None)) \
	.selectExpr(
	# "FLT_GOOD_VENDOR___sys_row_id as sys_row_id",
	"IF (cast(SITE as double) IS NOT NULL, SITE, SITE::STRING) as STORE_NBR",
#  iif(VENDOR= '*DEFAULT  ',1802,
#   TO_INTEGER(iif(IS_NUMBER(VENDOR),VENDOR,
#     TO_CHAR(iif(substr(VENDOR,1,1)='V',
#       900000+TO_FLOAT(SUBSTR(VENDOR,2,4))
#     ))
#   ),1)
# )
	"IF (VENDOR = '*DEFAULT  ', '1802', cast(IF (cast(VENDOR as double) IS NOT NULL, VENDOR, IF (substr ( VENDOR , 1 , 1 ) = 'V', 900000 + FLOAT ( SUBSTR ( VENDOR , 2 , 4 ) ), NULL)) as int)) as VENDOR_ID",
	# "VENDOR_SUBRANGE as VENDOR_SUBRANGE1",
	"IF (VENDOR_SUBRANGE IS NULL , '' ,VENDOR_SUBRANGE)  as VENDOR_SUBRANGE1",
	"rtrim ( EKGRP ) as EKGRP1",
	"TOTAL_LEADTIME as TOTAL_LEADTIME",
	"ADJUST_LEADTIME as ADJUST_LEADTIME",
	"TOTAL_LEADTIME + ADJUST_LEADTIME as LT_DAY_CNT",
	"CURRENT_TIMESTAMP as LOAD_DT",
	"MANDT as MANDT",
	"DPR_ORDER_ARRAY as DPR_ORDER_ARRAY",
	"PROD_LEADTIME as PROD_LEADTIME",
	"TRANSIT_LEADTIME as TRANSIT_LEADTIME",
	"ADJUST_TIME_SUN as ADJUST_TIME_SUN",
	"TRANSIT_TIME_SUN as TRANSIT_TIME_SUN",
	"ADJUST_TIME_MON as ADJUST_TIME_MON",
	"TRANSIT_TIME_MON as TRANSIT_TIME_MON",
	"ADJUST_TIME_TUE as ADJUST_TIME_TUE",
	"TRANSIT_TIME_TUE as TRANSIT_TIME_TUE",
	"ADJUST_TIME_WED as ADJUST_TIME_WED",
	"TRANSIT_TIME_WED as TRANSIT_TIME_WED",
	"ADJUST_TIME_THU as ADJUST_TIME_THU",
	"TRANSIT_TIME_THU as TRANSIT_TIME_THU",
	"ADJUST_TIME_FRI as ADJUST_TIME_FRI",
	"TRANSIT_TIME_FRI as TRANSIT_TIME_FRI",
	"ADJUST_TIME_SAT as ADJUST_TIME_SAT",
	"TRANSIT_TIME_SAT as TRANSIT_TIME_SAT",
	"ALT_ORIGIN as ALT_ORIGIN",
	"DEL_START_TIME_SUN as DEL_START_TIME_SUN",
	"DEL_END_TIME_SUN as DEL_END_TIME_SUN",
	"DEL_START_TIME_MON as DEL_START_TIME_MON",
	"DEL_END_TIME_MON as DEL_END_TIME_MON",
	"DEL_START_TIME_TUE as DEL_START_TIME_TUE",
	"DEL_END_TIME_TUE as DEL_END_TIME_TUE",
	"DEL_START_TIME_WED as DEL_START_TIME_WED",
	"DEL_END_TIME_WED as DEL_END_TIME_WED",
	"DEL_START_TIME_THU as DEL_START_TIME_THU",
	"DEL_END_TIME_THU as DEL_END_TIME_THU",
	"DEL_START_TIME_FRI as DEL_START_TIME_FRI",
	"DEL_END_TIME_FRI as DEL_END_TIME_FRI",
	"DEL_START_TIME_SAT as DEL_START_TIME_SAT",
	"DEL_END_TIME_SAT as DEL_END_TIME_SAT",
	"PICK_DAY1 as PICK_DAY1",
	"PICK_DAY2 as PICK_DAY2",
	"PICK_DAY3 as PICK_DAY3",
	"PICK_DAY4 as PICK_DAY4",
	"PICK_DAY5 as PICK_DAY5",
	"PICK_DAY6 as PICK_DAY6",
	"PICK_DAY7 as PICK_DAY7",
	"DEL_DAY1 as DEL_DAY1",
	"DEL_DAY2 as DEL_DAY2",
	"DEL_DAY3 as DEL_DAY3",
	"DEL_DAY4 as DEL_DAY4",
	"DEL_DAY5 as DEL_DAY5",
	"DEL_DAY6 as DEL_DAY6",
	"DEL_DAY7 as DEL_DAY7",
	"BLUE_GREEN_FLAG as BLUE_GREEN_FLAG",
	"PROTECTION_LEVEL as PROTECTION_LEVEL",
	"OVERRIDE_ORDER_WEIGHT as OVERRIDE_ORDER_WEIGHT",
	"OVERRIDE_ORDER_VOLUME as OVERRIDE_ORDER_VOLUME",
	"CHANGE_DATE as CHANGE_DATE",
	"CHANGE_TIME as CHANGE_TIME",
	"NOTES as NOTES",
	"OVERRIDE_FLAG as OVERRIDE_FLAG",
	"CROSSDOCK_ID as CROSSDOCK_ID",
	"ALT_ORIGIN1 as ALT_ORIGIN1",
	"ALT_ORIGIN2 as ALT_ORIGIN2",
	"ALT_ORIGIN3 as ALT_ORIGIN3",
	"ALT_ORIGIN4 as ALT_ORIGIN4",
	"ALT_ORIGIN5 as ALT_ORIGIN5",
	"ALT_ORIGIN6 as ALT_ORIGIN6",
	"ALT_ORIGIN7 as ALT_ORIGIN7",
	"CROSSDOCK_ID2 as CROSSDOCK_ID2",
	"CROSSDOCK_ID3 as CROSSDOCK_ID3",
	"CROSSDOCK_ID4 as CROSSDOCK_ID4",
	"CROSSDOCK_ID5 as CROSSDOCK_ID5",
	"CROSSDOCK_ID6 as CROSSDOCK_ID6",
	"CROSSDOCK_ID7 as CROSSDOCK_ID7",
	"NEWFIELD as NEWFIELD",
	"NEWFIELD1 as NEWFIELD1"
)

# COMMAND ----------

# Processing node Shortcut_to_DP_SITE_VEND_PRE, type TARGET 
# COLUMN COUNT: 80


Shortcut_to_DP_SITE_VEND_PRE = EXP_DP_SITE_VEND_PRE.selectExpr(
	"CAST(STORE_NBR AS INT) as STORE_NBR",
	"CAST(VENDOR_ID as bigint) as VENDOR_ID",
	"CAST(NVL(VENDOR_SUBRANGE1,'') AS STRING) as VENDOR_SUBGROUP_ID",
	"CAST(NVL(EKGRP1,'') AS STRING) as DP_PURCH_GROUP_ID",
	"CAST(NULL AS CHAR(1)) as DUE_BASED_ON_CD",
	"CAST(NULL AS STRING) as ORDER_DAY_OF_WK_ARRAY",
	"CAST(NULL AS CHAR(1)) as WEEK_FREQ_CD",
	"CAST(TOTAL_LEADTIME as smallint) as QUOTED_LT_DAY_CNT",
	"CAST(LT_DAY_CNT AS DECIMAL(5,2)) as LT_DAY_CNT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"CAST(NVL(MANDT,'') AS STRING) as MANDT",
	"CAST(NVL(DPR_ORDER_ARRAY,'') AS STRING) as DPR_ORDER_ARRAY",
	"CAST(PROD_LEADTIME AS INT) as PROD_LEADTIME",
	"CAST(TRANSIT_LEADTIME AS INT) as TRANSIT_LEADTIME",
	"CAST(ADJUST_TIME_SUN AS DECIMAL(5,2)) as ADJUST_TIME_SUN",
	"CAST(TRANSIT_TIME_SUN AS INT) as TRANSIT_TIME_SUN",
	"CAST(ADJUST_TIME_MON AS DECIMAL(5,2)) as ADJUST_TIME_MON",
	"CAST(TRANSIT_TIME_MON AS INT) as TRANSIT_TIME_MON",
	"CAST(ADJUST_TIME_TUE AS DECIMAL(5,2)) as ADJUST_TIME_TUE",
	"CAST(TRANSIT_TIME_TUE AS INT) as TRANSIT_TIME_TUE",
	"CAST(ADJUST_TIME_WED AS DECIMAL(5,2)) as ADJUST_TIME_WED",
	"CAST(TRANSIT_TIME_WED AS INT) as TRANSIT_TIME_WED",
	"CAST(ADJUST_TIME_THU AS DECIMAL(5,2)) as ADJUST_TIME_THU",
	"CAST(TRANSIT_TIME_THU AS INT) as TRANSIT_TIME_THU",
	"CAST(ADJUST_TIME_FRI AS DECIMAL(5,2)) as ADJUST_TIME_FRI",
	"CAST(TRANSIT_TIME_FRI AS INT) as TRANSIT_TIME_FRI",
	"CAST(ADJUST_TIME_SAT AS DECIMAL(5,2)) as ADJUST_TIME_SAT",
	"CAST(TRANSIT_TIME_SAT AS INT) as TRANSIT_TIME_SAT",
	"CAST(NVL(ALT_ORIGIN,'') AS STRING) as ALT_ORIGIN",
	"date_format(to_timestamp(DEL_START_TIME_SUN, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_START_TIME_SUN",
	"date_format(to_timestamp(DEL_END_TIME_SUN, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_END_TIME_SUN",
	"date_format(to_timestamp(DEL_START_TIME_MON, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_START_TIME_MON",
	"date_format(to_timestamp(DEL_END_TIME_MON, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_END_TIME_MON",
	"date_format(to_timestamp(DEL_START_TIME_TUE, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_START_TIME_TUE",
	"date_format(to_timestamp(DEL_END_TIME_TUE, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_END_TIME_TUE",
	"date_format(to_timestamp(DEL_START_TIME_WED, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_START_TIME_WED",
	"date_format(to_timestamp(DEL_END_TIME_WED, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_END_TIME_WED",
	"date_format(to_timestamp(DEL_START_TIME_THU, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_START_TIME_THU",
	"date_format(to_timestamp(DEL_END_TIME_THU, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_END_TIME_THU",
	"date_format(to_timestamp(DEL_START_TIME_FRI, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_START_TIME_FRI",
	"date_format(to_timestamp(DEL_END_TIME_FRI, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_END_TIME_FRI",
	"date_format(to_timestamp(DEL_START_TIME_SAT, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_START_TIME_SAT",
	"date_format(to_timestamp(DEL_END_TIME_SAT, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as DEL_END_TIME_SAT",
	"CAST(NVL(PICK_DAY1,'') AS STRING) as PICK_DAY1",
	"CAST(NVL(PICK_DAY2,'') AS STRING) as PICK_DAY2",
	"CAST(NVL(PICK_DAY3,'') AS STRING) as PICK_DAY3",
	"CAST(NVL(PICK_DAY4,'') AS STRING) as PICK_DAY4",
	"CAST(NVL(PICK_DAY5,'') AS STRING) as PICK_DAY5",
	"CAST(NVL(PICK_DAY6,'') AS STRING) as PICK_DAY6",
	"CAST(NVL(PICK_DAY7,'') AS STRING) as PICK_DAY7",
	"CAST(NVL(DEL_DAY1,'') AS STRING) as DEL_DAY1",
	"CAST(NVL(DEL_DAY2,'') AS STRING) as DEL_DAY2",
	"CAST(NVL(DEL_DAY3,'') AS STRING) as DEL_DAY3",
	"CAST(NVL(DEL_DAY4,'') AS STRING) as DEL_DAY4",
	"CAST(NVL(DEL_DAY5,'') AS STRING) as DEL_DAY5",
	"CAST(NVL(DEL_DAY6,'') AS STRING) as DEL_DAY6",
	"CAST(NVL(DEL_DAY7,'') AS STRING) as DEL_DAY7",
	"CAST(NVL(BLUE_GREEN_FLAG,'') AS STRING) as BLUE_GREEN_FLAG",
	"CAST(NVL(PROTECTION_LEVEL,'') AS STRING) as PROTECTION_LEVEL",
	"CAST(OVERRIDE_ORDER_WEIGHT AS INT) as OVERRIDE_ORDER_WEIGHT",
	"CAST(OVERRIDE_ORDER_VOLUME AS INT) as OVERRIDE_ORDER_VOLUME",
	"to_timestamp(CHANGE_DATE, 'MM/dd/yyyy HH:mm:ss') as CHANGE_DATE",
	"date_format(to_timestamp(CHANGE_TIME, 'MM/dd/yyyy HH:mm:ss'),'HH:mm:ss') as CHANGE_TIME",
	"CAST(NVL(NOTES,'') AS STRING) as NOTES",
	"CAST(NVL(OVERRIDE_FLAG,'') AS STRING) as OVERRIDE_FLAG",
	"CAST(NVL(CROSSDOCK_ID,'') AS STRING) as CROSSDOCK_ID",
	"CAST(NVL(ALT_ORIGIN1,'') AS STRING) as ALT_ORIGIN1",
	"CAST(NVL(ALT_ORIGIN2,'') AS STRING) as ALT_ORIGIN2",
	"CAST(NVL(ALT_ORIGIN3,'') AS STRING) as ALT_ORIGIN3",
	"CAST(NVL(ALT_ORIGIN4,'') AS STRING) as ALT_ORIGIN4",
	"CAST(NVL(ALT_ORIGIN5,'') AS STRING) as ALT_ORIGIN5",
	"CAST(NVL(ALT_ORIGIN6,'') AS STRING) as ALT_ORIGIN6",
	"CAST(NVL(ALT_ORIGIN7,'') AS STRING) as ALT_ORIGIN7",
	"CAST(NVL(CROSSDOCK_ID2,'') AS STRING) as CROSSDOCK_ID2",
	"CAST(NVL(CROSSDOCK_ID3,'') AS STRING) as CROSSDOCK_ID3",
	"CAST(NVL(CROSSDOCK_ID4,'') AS STRING) as CROSSDOCK_ID4",
	"CAST(NVL(CROSSDOCK_ID5,'') AS STRING) as CROSSDOCK_ID5",
	"CAST(NVL(CROSSDOCK_ID6,'') AS STRING) as CROSSDOCK_ID6",
	"CAST(NVL(CROSSDOCK_ID7,'') AS STRING) as CROSSDOCK_ID7",
	"CAST(ADJUST_LEADTIME AS DECIMAL(5,2)) as ADJUST_LEADTIME"
)
# Shortcut_to_DP_SITE_VEND_PRE.show(truncate=False)
# try:
    # chk=DuplicateChecker()
    # chk.check_for_duplicate_primary_keys(spark,Shortcut_to_DP_SITE_VEND_PRE,key)
Shortcut_to_DP_SITE_VEND_PRE.write.saveAsTable(f'{raw}.DP_SITE_VEND_PRE', mode = 'overwrite')
# except Exception as e:
#     raise e
