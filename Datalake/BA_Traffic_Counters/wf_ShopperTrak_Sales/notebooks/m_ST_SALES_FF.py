# Databricks notebook source
#Code converted on 2023-11-29 15:21:52
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *

# COMMAND ----------

def renamePartFileNames(filePath, newFilename ,ext = ''):
    fileList = dbutils.fs.ls(filePath)

    for file in fileList:
        if file.name.startswith("part-0000"):
            print(file.name)
            partFileName = filePath.strip("/") + "/" + file.name
            print("part file name:", partFileName)
            print("new file name:", newFilename + ext)
            dbutils.fs.cp(partFileName, newFilename  + ext)
            dbutils.fs.rm(filePath,True)


def writeToFlatFiles(df, filePath, fileName, mode , header="True", ext=''):
    print(filePath)
    if mode == "overwrite":
        dbutils.fs.rm(filePath.strip("/") + "/" + fileName, True)

    df.repartition(1).write.mode(mode).option("header", header).option("ignoreTrailingWhiteSpace", "False").text(filePath.strip("/") + "/" + fileName)
    print("File added to GCS Path")
    removeTransactionFiles(filePath.strip("/") + "/" + fileName)
    newFilePath = filePath.strip("/") + "/" + fileName
    print(newFilePath)
    print(filePath)
    renamePartFileNames(newFilePath, newFilePath,ext)

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
target_bucket=getParameterValue(raw,'wf_ShopperTrak_Sales','m_ST_SALES_FF','target_bucket')
nas_target=getParameterValue(raw,'wf_ShopperTrak_Sales','m_ST_SALES_FF','nas_target')
# target_file=getParameterValue(raw,'wf_ShopperTrak_Sales','m_ST_SALES_FF','target_file')


# COMMAND ----------

# Processing node SQ_Shortcut_to_SALES_TRANS_SKU1, type SOURCE 
# COLUMN COUNT: 10

SQ_Shortcut_to_SALES_TRANS_SKU1 = spark.sql(f"""SELECT
DAY_DT,
SALES_INSTANCE_ID,
LOCATION_ID,
SALES_TYPE_ID,
VOID_TYPE_CD,
ORDER_CHANNEL,
TRANS_TSTMP,
NET_SALES_AMT,
NET_SALES_QTY,
EXCH_RATE_PCT
FROM {legacy}.SALES_TRANS_SKU""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SALES_TRANS_SKU1 = SQ_Shortcut_to_SALES_TRANS_SKU1 \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU1.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU1.columns[1],'SALES_INSTANCE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU1.columns[2],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU1.columns[3],'SALES_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU1.columns[4],'VOID_TYPE_CD') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU1.columns[5],'ORDER_CHANNEL') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU1.columns[6],'TRANS_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU1.columns[7],'NET_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU1.columns[8],'NET_SALES_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU1.columns[9],'EXCH_RATE_PCT')
 
# SQ_Shortcut_to_SALES_TRANS_SKU1.show(10)

# COMMAND ----------

# Processing node SQ_Shortcut_to_ST_FILES_CTRL, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_ST_FILES_CTRL = spark.sql(f"""SELECT
ST_DAY_DT,
ST_CREATE_FILE_FLAG
FROM {legacy}.ST_FILES_CTRL""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_ST_FILES_CTRL = SQ_Shortcut_to_ST_FILES_CTRL \
	.withColumnRenamed(SQ_Shortcut_to_ST_FILES_CTRL.columns[0],'ST_DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_ST_FILES_CTRL.columns[1],'ST_CREATE_FILE_FLAG')
 

# COMMAND ----------

# Processing node EXPTRANS1, type EXPRESSION 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ST_FILES_CTRL_temp = SQ_Shortcut_to_ST_FILES_CTRL.toDF(*["SQ_Shortcut_to_ST_FILES_CTRL___" + col for col in SQ_Shortcut_to_ST_FILES_CTRL.columns])

EXPTRANS1 = SQ_Shortcut_to_ST_FILES_CTRL_temp.selectExpr(
	"SQ_Shortcut_to_ST_FILES_CTRL___sys_row_id as sys_row_id",
	"DATE_ADD( SQ_Shortcut_to_ST_FILES_CTRL___ST_DAY_DT ,1) as ST_DAY_DT_PLUS1DY",
	"SQ_Shortcut_to_ST_FILES_CTRL___ST_CREATE_FILE_FLAG as ST_CREATE_FILE_FLAG"
)


# COMMAND ----------

# Processing node SQ_Shortcut_to_SALES_TRANS_ORDER, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_SALES_TRANS_ORDER = spark.sql(f"""SELECT
DAY_DT,
LOCATION_ID,
SALES_INSTANCE_ID,
ORDER_DT
FROM {legacy}.SALES_TRANS_ORDER""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SALES_TRANS_ORDER = SQ_Shortcut_to_SALES_TRANS_ORDER \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_ORDER.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_ORDER.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_ORDER.columns[2],'SALES_INSTANCE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_ORDER.columns[3],'ORDER_DT')


# COMMAND ----------

# Processing node SQ_Shortcut_to_SALES_TRANS_SKU, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_SALES_TRANS_SKU = spark.sql(f"""SELECT
DAY_DT,
SALES_INSTANCE_ID,
LOCATION_ID,
SALES_TYPE_ID,
VOID_TYPE_CD,
ORDER_CHANNEL,
ORDER_ASSIST_LOCATION_ID,
LOAD_TSTMP
FROM {legacy}.SALES_TRANS_SKU""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SALES_TRANS_SKU = SQ_Shortcut_to_SALES_TRANS_SKU \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU.columns[1],'SALES_INSTANCE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU.columns[2],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU.columns[3],'SALES_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU.columns[4],'VOID_TYPE_CD') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU.columns[5],'ORDER_CHANNEL') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU.columns[6],'ORDER_ASSIST_LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_TRANS_SKU.columns[7],'LOAD_TSTMP')


# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
LOCATION_ID,
LOCATION_TYPE_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[1],'LOCATION_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[2],'STORE_NBR')


# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SALES_TRANS_SKU_temp = SQ_Shortcut_to_SALES_TRANS_SKU.toDF(*["SQ_Shortcut_to_SALES_TRANS_SKU___" + col for col in SQ_Shortcut_to_SALES_TRANS_SKU.columns])

EXPTRANS = SQ_Shortcut_to_SALES_TRANS_SKU_temp.selectExpr(
	"SQ_Shortcut_to_SALES_TRANS_SKU___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_SALES_TRANS_SKU___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_SALES_TRANS_SKU___SALES_INSTANCE_ID as SALES_INSTANCE_ID",
	"SQ_Shortcut_to_SALES_TRANS_SKU___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SALES_TRANS_SKU___SALES_TYPE_ID as SALES_TYPE_ID",
	"SQ_Shortcut_to_SALES_TRANS_SKU___VOID_TYPE_CD as VOID_TYPE_CD",
	"SQ_Shortcut_to_SALES_TRANS_SKU___ORDER_CHANNEL as ORDER_CHANNEL",
	"SQ_Shortcut_to_SALES_TRANS_SKU___ORDER_ASSIST_LOCATION_ID as ORDER_ASSIST_LOCATION_ID",
	"to_date (SQ_Shortcut_to_SALES_TRANS_SKU___LOAD_TSTMP) as LOAD_DT"
)

# EXPTRANS.show(10) 

# COMMAND ----------

# Processing node Jnr_STS1_Ctrl, type JOINER 
# COLUMN COUNT: 10

Jnr_STS1_Ctrl = EXPTRANS1.join(EXPTRANS,[EXPTRANS1.ST_DAY_DT_PLUS1DY == EXPTRANS.LOAD_DT],'inner')
# .limit(1000)
# Jnr_STS1_Ctrl.show(10) 

# COMMAND ----------

# Processing node Jnr_STS_STO, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
Jnr_STS1_Ctrl_temp = Jnr_STS1_Ctrl.toDF(*["Jnr_STS1_Ctrl___" + col for col in Jnr_STS1_Ctrl.columns])
SQ_Shortcut_to_SALES_TRANS_ORDER_temp = SQ_Shortcut_to_SALES_TRANS_ORDER.toDF(*["SQ_Shortcut_to_SALES_TRANS_ORDER___" + col for col in SQ_Shortcut_to_SALES_TRANS_ORDER.columns])

Jnr_STS_STO = SQ_Shortcut_to_SALES_TRANS_ORDER_temp.join(Jnr_STS1_Ctrl_temp,[SQ_Shortcut_to_SALES_TRANS_ORDER_temp.SQ_Shortcut_to_SALES_TRANS_ORDER___DAY_DT == Jnr_STS1_Ctrl_temp.Jnr_STS1_Ctrl___DAY_DT, SQ_Shortcut_to_SALES_TRANS_ORDER_temp.SQ_Shortcut_to_SALES_TRANS_ORDER___LOCATION_ID == Jnr_STS1_Ctrl_temp.Jnr_STS1_Ctrl___LOCATION_ID, SQ_Shortcut_to_SALES_TRANS_ORDER_temp.SQ_Shortcut_to_SALES_TRANS_ORDER___SALES_INSTANCE_ID == Jnr_STS1_Ctrl_temp.Jnr_STS1_Ctrl___SALES_INSTANCE_ID],'right_outer').selectExpr(
	"Jnr_STS1_Ctrl___DAY_DT as DAY_DT",
	"Jnr_STS1_Ctrl___SALES_INSTANCE_ID as SALES_INSTANCE_ID",
	"Jnr_STS1_Ctrl___LOCATION_ID as LOCATION_ID",
	"Jnr_STS1_Ctrl___SALES_TYPE_ID as SALES_TYPE_ID",
	"Jnr_STS1_Ctrl___VOID_TYPE_CD as VOID_TYPE_CD",
	"Jnr_STS1_Ctrl___ORDER_CHANNEL as ORDER_CHANNEL",
	"Jnr_STS1_Ctrl___ORDER_ASSIST_LOCATION_ID as ORDER_ASSIST_LOCATION_ID",
	"Jnr_STS1_Ctrl___ST_CREATE_FILE_FLAG as ST_CREATE_FILE_FLAG",
	"SQ_Shortcut_to_SALES_TRANS_ORDER___DAY_DT as DAY_DT1",
	"SQ_Shortcut_to_SALES_TRANS_ORDER___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_to_SALES_TRANS_ORDER___SALES_INSTANCE_ID as SALES_INSTANCE_ID1",
	"SQ_Shortcut_to_SALES_TRANS_ORDER___ORDER_DT as ORDER_DT")
#  .limit(1000)


# COMMAND ----------

# Processing node Fil_STS1, type FILTER 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
Jnr_STS_STO_temp = Jnr_STS_STO.toDF(*["Jnr_STS_STO___" + col for col in Jnr_STS_STO.columns])

Fil_STS1 = Jnr_STS_STO_temp.selectExpr(
	"Jnr_STS_STO___DAY_DT as DAY_DT",
	"Jnr_STS_STO___LOCATION_ID as LOCATION_ID",
	"Jnr_STS_STO___SALES_TYPE_ID as SALES_TYPE_ID",
	"Jnr_STS_STO___VOID_TYPE_CD as VOID_TYPE_CD",
	"Jnr_STS_STO___ORDER_CHANNEL as ORDER_CHANNEL",
	"Jnr_STS_STO___ORDER_ASSIST_LOCATION_ID as ORDER_ASSIST_LOCATION_ID",
	"Jnr_STS_STO___ST_CREATE_FILE_FLAG as ST_CREATE_FILE_FLAG",
	"Jnr_STS_STO___ORDER_DT as ORDER_DT").filter("SALES_TYPE_ID in (1,2,3,4,5,10) AND VOID_TYPE_CD = 'N' AND ORDER_CHANNEL IN ( 'STR' ,'AOS','ISPU' ) AND ST_CREATE_FILE_FLAG = 'Y'").withColumn("sys_row_id", monotonically_increasing_id())
	
#  .limit(100)


# COMMAND ----------

# Processing node Exp_STS1, type EXPRESSION 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column

Exp_STS1 = Fil_STS1.selectExpr(
	"IF (ORDER_CHANNEL = 'AOS', to_date (ORDER_DT), DAY_DT) as SALES_DAY_DT",
	"IF (ORDER_CHANNEL = 'AOS', ORDER_ASSIST_LOCATION_ID, LOCATION_ID) as SALES_LOCATION_ID"
)
# .limit(1000)


# COMMAND ----------

# Processing node Srt_Distinct, type SORTER 
# COLUMN COUNT: 2

Srt_Distinct = Exp_STS1.select( '*'
).sort(col('SALES_DAY_DT').asc(), col('SALES_LOCATION_ID').asc()).distinct()

# COMMAND ----------

# Processing node Jnr_STS, type JOINER 
# COLUMN COUNT: 12

Jnr_STS = Srt_Distinct.join(SQ_Shortcut_to_SALES_TRANS_SKU1,[Srt_Distinct.SALES_DAY_DT == SQ_Shortcut_to_SALES_TRANS_SKU1.DAY_DT, Srt_Distinct.SALES_LOCATION_ID == SQ_Shortcut_to_SALES_TRANS_SKU1.LOCATION_ID],'inner')
# .limit(1000)


# COMMAND ----------

# Processing node Jnr_STS_SITE, type JOINER 
# COLUMN COUNT: 13

Jnr_STS_SITE = SQ_Shortcut_to_SITE_PROFILE.join(Jnr_STS,[SQ_Shortcut_to_SITE_PROFILE.LOCATION_ID == Jnr_STS.SALES_LOCATION_ID],'inner')

# COMMAND ----------

# Processing node Fil_STS, type FILTER 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
Jnr_STS_SITE_temp = Jnr_STS_SITE.toDF(*["Jnr_STS_SITE___" + col for col in Jnr_STS_SITE.columns])

Fil_STS = Jnr_STS_SITE_temp.selectExpr(
	"Jnr_STS_SITE___DAY_DT as DAY_DT",
	"Jnr_STS_SITE___SALES_INSTANCE_ID as SALES_INSTANCE_ID",
	"Jnr_STS_SITE___SALES_TYPE_ID as SALES_TYPE_ID",
	"Jnr_STS_SITE___VOID_TYPE_CD as VOID_TYPE_CD",
	"Jnr_STS_SITE___ORDER_CHANNEL as ORDER_CHANNEL",
	"Jnr_STS_SITE___TRANS_TSTMP as TRANS_TSTMP",
	"Jnr_STS_SITE___NET_SALES_AMT as NET_SALES_AMT",
	"Jnr_STS_SITE___NET_SALES_QTY as NET_SALES_QTY",
	"Jnr_STS_SITE___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
	"Jnr_STS_SITE___STORE_NBR as STORE_NBR",
	"Jnr_STS_SITE___EXCH_RATE_PCT as EXCH_RATE_PCT")\
    .filter("SALES_TYPE_ID in (1,2,3,4,5,10) AND VOID_TYPE_CD = 'N' AND ORDER_CHANNEL IN ( 'STR'  ,'AOS'  ,'ISPU' ) AND LOCATION_TYPE_ID = 8").withColumn("sys_row_id", monotonically_increasing_id())
#     .limit(1000)
# Fil_STS.show(10)
# (SALES_TYPE_ID = 1
#     OR SALES_TYPE_ID = 2
#     OR SALES_TYPE_ID = 3
#     OR SALES_TYPE_ID = 4
#     OR SALES_TYPE_ID = 5
#     OR SALES_TYPE_ID = 10)
#  AND VOID_TYPE_CD = 'N'
#  AND (ORDER_CHANNEL = 'STR'
#     OR ORDER_CHANNEL = 'AOS'
#     OR ORDER_CHANNEL = 'ISPU')
# AND LOCATION_TYPE_ID = 8

# COMMAND ----------

# Processing node EXPTRANS2, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
Fil_STS_temp = Fil_STS.toDF(*["Fil_STS___" + col for col in Fil_STS.columns])

EXPTRANS2 = Fil_STS_temp.selectExpr(
	"Fil_STS___STORE_NBR as STORE_NBR",
	"Fil_STS___DAY_DT as i_DAY_DT",
	"Fil_STS___SALES_INSTANCE_ID as SALES_INSTANCE_ID",
	"Fil_STS___TRANS_TSTMP as i_TRANS_TSTMP",
	"Fil_STS___NET_SALES_AMT as NET_SALES_AMT",
	"Fil_STS___NET_SALES_QTY as NET_SALES_QTY",
	"Fil_STS___EXCH_RATE_PCT as i_EXCH_RATE_PCT").selectExpr(
	# "Fil_STS___sys_row_id as sys_row_id",
	"STORE_NBR as STORE_NBR",
	"date_format(i_DAY_DT, 'yyyyMMdd') as TRANS_DT",
	"SALES_INSTANCE_ID as SALES_INSTANCE_ID",
	"date_format(i_TRANS_TSTMP, 'HHmmss') as TRANS_END_DT",
	"NET_SALES_AMT as NET_SALES_AMT",
	"NET_SALES_QTY as NET_SALES_QTY",
	"IF (i_EXCH_RATE_PCT = 1, 'USD', 'CAD') as CURRENCY_TYPE",
	"i_DAY_DT as DAY_DT"
)
#  .limit(1000)
# EXPTRANS2.show(10)
# EXPTRANS2.count()

# COMMAND ----------

# Processing node Agg_STS, type AGGREGATOR 
# COLUMN COUNT: 8

Agg_STS = EXPTRANS2 \
	.groupBy("STORE_NBR","TRANS_DT","SALES_INSTANCE_ID","TRANS_END_DT","CURRENCY_TYPE","DAY_DT") \
	.agg( \
	sum("NET_SALES_AMT").alias("NET_SALES"),
	sum("NET_SALES_QTY").alias("SALES_QTY")
	) \
	.withColumn("sys_row_id", monotonically_increasing_id())
# Agg_STS.show(10)

# COMMAND ----------

# Processing node Srt_STS, type SORTER 
# COLUMN COUNT: 8

Srt_STS = Agg_STS.select('*'
).sort(col('TRANS_DT').asc(), col('STORE_NBR').asc(), col('SALES_INSTANCE_ID').asc())

# COMMAND ----------

# Processing node Exp_STS, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
Srt_STS_temp = Srt_STS.toDF(*["Srt_STS___" + col for col in Srt_STS.columns])

Exp_STS = Srt_STS_temp.selectExpr(
	"Srt_STS___sys_row_id as sys_row_id",
	"Srt_STS___STORE_NBR as STORE_NBR",
	"Srt_STS___TRANS_DT as TRANS_DT",
	"Srt_STS___SALES_INSTANCE_ID as SALES_INSTANCE_ID",
	"Srt_STS___TRANS_END_DT as TRANS_END_DT",
	"Srt_STS___NET_SALES as NET_SALES",
	"Srt_STS___SALES_QTY as SALES_QTY",
	"Srt_STS___CURRENCY_TYPE as CURRENCY_TYPE",
	"'' as CATEGORY_ID",
	"Srt_STS___DAY_DT as DAY_DT"
)


# COMMAND ----------

# Processing node Rtr_ST_Sales_FF, type ROUTER 
# COLUMN COUNT: 27


# Creating output dataframe for Rtr_ST_Sales_FF, output group Sales_Greater_Than_30Days
Rtr_ST_Sales_FF_Sales_Greater_Than_30Days = Exp_STS.select(Exp_STS.sys_row_id.alias('sys_row_id'),
	Exp_STS.STORE_NBR.alias('STORE_NBR3'),
	Exp_STS.TRANS_DT.alias('TRANS_DT3'),
	Exp_STS.SALES_INSTANCE_ID.alias('SALES_INSTANCE_ID3'),
	Exp_STS.TRANS_END_DT.alias('TRANS_END_DT3'),
	Exp_STS.NET_SALES.alias('NET_SALES3'),
	Exp_STS.SALES_QTY.alias('SALES_QTY3'),
	Exp_STS.CURRENCY_TYPE.alias('CURRENCY_TYPE3'),
	Exp_STS.CATEGORY_ID.alias('CATEGORY_ID3'),
	Exp_STS.DAY_DT.alias('DAY_DT3'))\
	.filter("DAY_DT >= to_date('2023-12-20') - INTERVAL '30 DAYS'")

	# .filter("DAY_DT >= date_add(trunc(current_date(), 'D'),-30)")


# Creating output dataframe for Rtr_ST_Sales_FF, output group Sales_Less_Than_30Days
Rtr_ST_Sales_FF_Sales_Less_Than_30Days = Exp_STS.select(Exp_STS.sys_row_id.alias('sys_row_id'),
	Exp_STS.STORE_NBR.alias('STORE_NBR1'),
	Exp_STS.TRANS_DT.alias('TRANS_DT1'),
	Exp_STS.SALES_INSTANCE_ID.alias('SALES_INSTANCE_ID1'),
	Exp_STS.TRANS_END_DT.alias('TRANS_END_DT1'),
	Exp_STS.NET_SALES.alias('NET_SALES1'),
	Exp_STS.SALES_QTY.alias('SALES_QTY1'),
	Exp_STS.CURRENCY_TYPE.alias('CURRENCY_TYPE1'),
	Exp_STS.CATEGORY_ID.alias('CATEGORY_ID1'),
	Exp_STS.DAY_DT.alias('DAY_DT1'))\
    .filter("DAY_DT < to_date('2023-12-20') - INTERVAL '30 DAYS'")
    # .filter("DAY_DT < date_add(trunc(current_date(), 'D'),-30)")


# COMMAND ----------

# Processing node Exp_TransactionCtrl_FileName1, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
Rtr_ST_Sales_FF_Sales_Greater_Than_30Days_temp = Rtr_ST_Sales_FF_Sales_Greater_Than_30Days.toDF(*["Rtr_ST_Sales_FF_Sales_Greater_Than_30Days___" + col for col in Rtr_ST_Sales_FF_Sales_Greater_Than_30Days.columns])

# Exp_TransactionCtrl_FileName1 = Rtr_ST_Sales_FF_Sales_Greater_Than_30Days_temp.selectExpr(
# 	"Rtr_ST_Sales_FF_Sales_Greater_Than_30Days___DAY_DT3 as DAY_DT").selectExpr(
# 	"Rtr_ST_Sales_FF_Sales_Greater_Than_30Days___sys_row_id as sys_row_id",
# 	"concat('Sales_' , date_format(DATE_ADD(- 1, TRUNC ( CURRENT_TIMESTAMP , 'D' )), 'yyyyMMdd') , '.txt' ) as FileName"
# )

Exp_TransactionCtrl_FileName1 = 'Sales_' +  (datetime.now() - timedelta(1)).strftime("%Y%m%d") + '.txt'

# COMMAND ----------

# Processing node Exp_TransactionCtrl_FileName, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
Rtr_ST_Sales_FF_Sales_Less_Than_30Days_temp = Rtr_ST_Sales_FF_Sales_Less_Than_30Days.toDF(*["Rtr_ST_Sales_FF_Sales_Less_Than_30Days___" + col for col in Rtr_ST_Sales_FF_Sales_Less_Than_30Days.columns])

# Exp_TransactionCtrl_FileName = Rtr_ST_Sales_FF_Sales_Less_Than_30Days_temp.selectExpr(
# 	"Rtr_ST_Sales_FF_Sales_Less_Than_30Days___DAY_DT1 as DAY_DT").selectExpr(
# 	"Rtr_ST_Sales_FF_Sales_Less_Than_30Days___sys_row_id as sys_row_id",
# 	"concat('Sales_' , date_format(DATE_ADD(- 2, TRUNC ( CURRENT_TIMESTAMP , 'D' )), 'yyyyMMdd') , '.txt' ) as FileName"
# )

Exp_TransactionCtrl_FileName = 'Sales_' +  (datetime.now() - timedelta(2)).strftime("%Y%m%d") + '.txt'


# COMMAND ----------

# Processing node Shortcut_to_ST_Sales_FF_LessThan30, type TARGET 
# COLUMN COUNT: 11

# Joining dataframes Rtr_ST_Sales_FF_Sales_Less_Than_30Days, Exp_TransactionCtrl_FileName to form Shortcut_to_ST_Sales_FF_LessThan30
# Shortcut_to_ST_Sales_FF_LessThan30_joined = Rtr_ST_Sales_FF_Sales_Less_Than_30Days.join(Exp_TransactionCtrl_FileName, Rtr_ST_Sales_FF_Sales_Less_Than_30Days.sys_row_id == Exp_TransactionCtrl_FileName.sys_row_id, 'inner')

Shortcut_to_ST_Sales_FF_LessThan30 = Rtr_ST_Sales_FF_Sales_Less_Than_30Days.selectExpr(   
	"STORE_NBR1 as STORE_NBR",
	"CAST(TRANS_DT1 AS STRING) as TRANS_DT",
	"CAST(TRANS_END_DT1 AS STRING) as TRANS_END_TIME",
	"NET_SALES1 as NET_SALES",
	"SALES_INSTANCE_ID1 as SALES_INSTANCE_ID",
	"SALES_QTY1 as SALES_QTY",
	"CAST(CATEGORY_ID1 AS STRING) as CATEGORY_ID",
	"CAST(CURRENCY_TYPE1 AS STRING) as CURRENCY_TYPE",
	# "CAST(NULL as TIMESTAMP) as LOAD_DT",
	# "CAST(NULL AS STRING) as SOURCE_TYPE",
	# f"'{Exp_TransactionCtrl_FileName}' as FileName"
)
Shortcut_to_ST_Sales_FF_LessThan30.createOrReplaceTempView('Sales_FF_LessThan30Days')
ST_Sales_FF_LessThan30Days=spark.sql("""select coalesce(STORE_NBR,'') ||','|| coalesce(TRANS_DT,'') ||','||coalesce(TRANS_END_TIME,'')  ||','||coalesce( NET_SALES ,'')||','|| coalesce(SALES_INSTANCE_ID,'') ||','|| coalesce(SALES_QTY,'') ||','|| coalesce(CATEGORY_ID,'') ||','|| coalesce(CURRENCY_TYPE,'') from Sales_FF_LessThan30Days """)

# COMMAND ----------

less_than_30_days_target_bucket = target_bucket + (datetime.now() - timedelta(2)).strftime("%Y%m%d") + '/'
print(less_than_30_days_target_bucket)

writeToFlatFiles(ST_Sales_FF_LessThan30Days, less_than_30_days_target_bucket, Exp_TransactionCtrl_FileName[:-4], 'overwrite' , header="False" , ext='.txt' )

# COMMAND ----------

gs_source_path = less_than_30_days_target_bucket + "*.txt"
less_than_30_days_nas_target = nas_target + (datetime.now() - timedelta(2)).strftime("%Y%m%d") + '/'

copy_file_to_nas(gs_source_path, less_than_30_days_nas_target)

# COMMAND ----------

# Processing node Shortcut_to_ST_Sales_FF_GreaterThan30Days, type TARGET 
# COLUMN COUNT: 11

# Joining dataframes Rtr_ST_Sales_FF_Sales_Greater_Than_30Days, Exp_TransactionCtrl_FileName1 to form Shortcut_to_ST_Sales_FF_GreaterThan30Days
# Shortcut_to_ST_Sales_FF_GreaterThan30Days_joined = Rtr_ST_Sales_FF_Sales_Greater_Than_30Days.join(Exp_TransactionCtrl_FileName1, Rtr_ST_Sales_FF_Sales_Greater_Than_30Days.sys_row_id == Exp_TransactionCtrl_FileName1.sys_row_id, 'inner')

Shortcut_to_ST_Sales_FF_GreaterThan30Days = Rtr_ST_Sales_FF_Sales_Greater_Than_30Days.selectExpr(
	"CAST(STORE_NBR3 AS STRING) as STORE_NBR",
	"CAST(TRANS_DT3 AS STRING) as TRANS_DT",
	"CAST(TRANS_END_DT3 AS STRING) as TRANS_END_TIME",
	"CAST(NET_SALES3 AS STRING) as NET_SALES",
	"CAST(SALES_INSTANCE_ID3 AS STRING) as SALES_INSTANCE_ID",
	"CAST(SALES_QTY3 AS STRING) as SALES_QTY",
	"CAST(CATEGORY_ID3 AS STRING) as CATEGORY_ID",
	"CAST(CURRENCY_TYPE3 AS STRING) as CURRENCY_TYPE",
	# "CAST(NULL as TIMESTAMP) as LOAD_DT",
	# "CAST(NULL AS STRING) as SOURCE_TYPE",
	# f"'{Exp_TransactionCtrl_FileName1}' as FileName"
)
Shortcut_to_ST_Sales_FF_GreaterThan30Days.createOrReplaceTempView('Sales_FF_GreaterThan30Days')

# COMMAND ----------

ST_Sales_FF_GreaterThan30Days=spark.sql("""select coalesce(STORE_NBR,'') ||','|| coalesce(TRANS_DT,'') ||','||coalesce(TRANS_END_TIME,'')  ||','||coalesce( NET_SALES ,'')||','|| coalesce(SALES_INSTANCE_ID,'') ||','|| coalesce(SALES_QTY,'') ||','|| coalesce(CATEGORY_ID,'') ||','|| coalesce(CURRENCY_TYPE,'') from Sales_FF_GreaterThan30Days """)

# COMMAND ----------

greater_than_30_days_target_bucket = target_bucket + (datetime.now() - timedelta(1)).strftime("%Y%m%d") + '/'
print(greater_than_30_days_target_bucket)
writeToFlatFiles(ST_Sales_FF_GreaterThan30Days, greater_than_30_days_target_bucket, Exp_TransactionCtrl_FileName1[:-4], 'overwrite' , header="False" , ext = '.txt' )

# COMMAND ----------

greater_than_30_days_nas_target = nas_target + (datetime.now() - timedelta(1)).strftime("%Y%m%d") + '/'
gs_source_path = greater_than_30_days_target_bucket + "*.txt"
copy_file_to_nas(gs_source_path, greater_than_30_days_nas_target)
