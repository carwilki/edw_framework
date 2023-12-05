# Databricks notebook source
#Code converted on 2023-10-06 15:46:22
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

source_bucket=getParameterValue(raw,'bs_Demand_Planning_Day.prm','Demand_Planning.WF:bs_Demand_Planning_Day.M:m_dp_sku_link_pre','source_bucket')
key=getParameterValue(raw,'bs_Demand_Planning_Day.prm','Demand_Planning.WF:bs_Demand_Planning_Day.M:m_dp_sku_link_pre','key')

source_file=get_src_file(key,source_bucket)
print(source_file)

# COMMAND ----------

from pyspark.sql.types import *
file_schema = StructType([StructField("MANDT", StringType(), True), 
                    StructField("ORIGINAL_ART_NR", StringType(), True), 
                    StructField("FOLUP_ART_NR", StringType(), True),
                    StructField("ASORT", StringType(), True),  
                    StructField("FOLLOWUP_TYP_NR", StringType(), True),    
                    StructField("DATE_FROM", StringType(), True), 
                    StructField("DATE_TO", StringType(), True),   
                    StructField("PRIORITY_A", StringType(), True),
                    StructField("FOLLOWUP_ACTION", StringType(), True),   
                    StructField("TIMESTAMP", LongType(), True),   
                    StructField("CREATE_TIMESTAMP", LongType(), True),   
                    StructField("ERNAM", StringType(), True),   
                    StructField("AENAM", StringType(), True),   
                    StructField("ORG_ART_FACTOR", LongType(), True),   
                    StructField("SUBST_ART_FACTOR", LongType(), True) ]) 

# COMMAND ----------

# Processing node SQ_Shortcut_to_WRF_FOLUP_TYP_A, type APPLICATION_SOURCE_QUALIFIER 
# COLUMN COUNT: 6

# ---------- No handler defined for type APPLICATION_SOURCE_QUALIFIER, node SQ_Shortcut_to_WRF_FOLUP_TYP_A, job m_dp_sku_link_pre ---------- #

SQ_Shortcut_to_WRF_FOLUP_TYP_A = spark.read.csv(source_file,sep='|',header=True , schema=file_schema).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# SQ_Shortcut_to_WRF_FOLUP_TYP_A.filter("DATE_FROM IS NULL").count()

# COMMAND ----------

# Processing node EXP_INSERT, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WRF_FOLUP_TYP_A_temp = SQ_Shortcut_to_WRF_FOLUP_TYP_A.toDF(*["SQ_Shortcut_to_WRF_FOLUP_TYP_A___" + col for col in SQ_Shortcut_to_WRF_FOLUP_TYP_A.columns])

EXP_INSERT = SQ_Shortcut_to_WRF_FOLUP_TYP_A_temp.selectExpr(
	"SQ_Shortcut_to_WRF_FOLUP_TYP_A___sys_row_id as sys_row_id",
	"cast(SQ_Shortcut_to_WRF_FOLUP_TYP_A___ORIGINAL_ART_NR as int) as o_ORIGINAL_ART_NR",
	"cast(SQ_Shortcut_to_WRF_FOLUP_TYP_A___FOLUP_ART_NR as int) as o_FOLUP_ART_NR",
	"cast(SQ_Shortcut_to_WRF_FOLUP_TYP_A___ASORT as int) as o_ASORT",
	"SQ_Shortcut_to_WRF_FOLUP_TYP_A___FOLLOWUP_TYP_NR as FOLLOWUP_TYP_NR",
	"TO_TIMESTAMP(SQ_Shortcut_to_WRF_FOLUP_TYP_A___DATE_FROM , 'M/d/yyyy HH:mm:ss') as DATE_FROM",
	"TO_TIMESTAMP(SQ_Shortcut_to_WRF_FOLUP_TYP_A___DATE_TO , 'M/d/yyyy HH:mm:ss')  as DATE_TO",
	"CURRENT_TIMESTAMP as o_LOAD_DT"
)

# COMMAND ----------

# EXP_INSERT.filter("DATE_FROM IS  NULL").show()

# COMMAND ----------

# Processing node Shortcut_To_DP_SKU_LINK_PRE, type TARGET 
# COLUMN COUNT: 8


Shortcut_To_DP_SKU_LINK_PRE = EXP_INSERT.selectExpr(
	"CAST(FOLLOWUP_TYP_NR AS STRING) as SKU_LINK_TYPE_CD",
	"CAST(o_ASORT AS INT) as FROM_STORE_NBR",
	"CAST(o_ORIGINAL_ART_NR AS INT) as FROM_SKU_NBR",
	"CAST(o_ASORT AS INT) as TO_STORE_NBR",
	"CAST(o_FOLUP_ART_NR AS INT) as TO_SKU_NBR",
	"CAST(DATE_FROM AS TIMESTAMP) as SKU_LINK_EFF_DT",
	"CAST(DATE_TO AS TIMESTAMP) as SKU_LINK_END_DT",
	"CAST(o_LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
# Shortcut_To_DP_SKU_LINK_PRE.filter("SKU_LINK_EFF_DT IS NULL").show()
Shortcut_To_DP_SKU_LINK_PRE.write.saveAsTable(f'{raw}.DP_SKU_LINK_PRE', mode = 'overwrite')


# COMMAND ----------


