# Databricks notebook source
#Code converted on 2023-10-24 21:17:06
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

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

# Processing node LKP_GET_PRODUCT_ID_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_GET_PRODUCT_ID_SRC = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE""")


# COMMAND ----------

# Processing node LKP_GET_TIME_DIMENSIONS_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_GET_TIME_DIMENSIONS_SRC = spark.sql(f"""SELECT
DAY_DT,
FISCAL_WK,
FISCAL_MO,
FISCAL_YR,
WEEK_DT
FROM {enterprise}.DAYS""")

# COMMAND ----------

# Processing node LKP_GET_LOCATION_ID_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_GET_LOCATION_ID_SRC = spark.sql(f"""SELECT
LOCATION_ID,
LOCATION_TYPE_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE""")


# COMMAND ----------

# Processing node SQ_Shortcut_to_LABEL_DAY_STORE_SKU, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_LABEL_DAY_STORE_SKU = spark.sql(f"""SELECT
LABEL_CHANGE_DT,
LOCATION_ID,
PRODUCT_ID,
ACTUAL_FLAG,
LABEL_POG_TYPE_CD,
LABEL_SIZE_ID,
LABEL_TYPE_ID,
EXPIRATION_FLAG,
SKU_NBR,
STORE_NBR,
WEEK_DT,
FISCAL_WK,
FISCAL_MO,
FISCAL_YR,
SUPPRESSED_FLAG,
LABEL_CNT,
LOAD_TSTMP
FROM {legacy}.LABEL_DAY_STORE_SKU
WHERE ACTUAL_FLAG = 0 
  AND LOAD_TSTMP > CURRENT_DATE - INTERVAL 15 DAYS""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE, type SOURCE 
# COLUMN COUNT: 10

SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE = spark.sql(f"""SELECT
MANDT,
EFFECTIVE_DATE,
ARTICLE,
SITE,
POG_TYPE,
LABEL_SIZE,
LABEL_TYPE,
EXP_LABEL_TYPE,
SUPPRESS_IND,
NUM_LABELS,
ENH_LBL_ID
FROM {raw}.ZTB_ADV_LBL_CHGS_PRE""").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node EXP_LBL_FIELDS_CONVERSION, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE_temp = SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE.toDF(*["SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___" + col for col in SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE.columns])

EXP_LBL_FIELDS_CONVERSION = SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE_temp.selectExpr(
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___sys_row_id as sys_row_id",
    "SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___EFFECTIVE_DATE as in_EFFECTIVE_DATE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___SITE as in_SITE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___ARTICLE as in_ARTICLE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___POG_TYPE as POG_TYPE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___LABEL_SIZE as LABEL_SIZE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___LABEL_TYPE as in_LABEL_TYPE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___ENH_LBL_ID as ENH_LBL_ID",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___EXP_LABEL_TYPE as in_EXP_LABEL_TYPE",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___SUPPRESS_IND as in_SUPPRESS_IND",
	"SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE___NUM_LABELS as in_NUM_LABELS").selectExpr(
	"sys_row_id as sys_row_id",
	"to_date(in_EFFECTIVE_DATE , 'yyyyMMdd') as LABEL_CHANGE_DT",
	"cast(in_SITE as int) as STORE_NBR",
	"cast(in_ARTICLE as int) as SKU_NBR",
	"0 as ACTUAL_FLAG",
	"POG_TYPE as POG_TYPE",
	"LABEL_SIZE as LABEL_SIZE",
	"IF (cast(LTRIM ( RTRIM ( ENH_LBL_ID ) ) as double) IS NOT NULL, cast(LTRIM ( RTRIM ( ENH_LBL_ID ) ) as int), cast(in_LABEL_TYPE as int)) as LABEL_TYPE",
	"IF (in_EXP_LABEL_TYPE = 'X', '1', '0') as EXPIRATION_FLAG",
	"IF (in_SUPPRESS_IND = 'X', '1', '0') as SUPPRESSED_FLAG",
	"cast(in_NUM_LABELS as int) as LABEL_CNT"
)


# COMMAND ----------

# Processing node LKP_GET_PRODUCT_ID, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

LKP_GET_PRODUCT_ID_lookup_result = EXP_LBL_FIELDS_CONVERSION.selectExpr(
	"SKU_NBR as SKU_NBR1", "sys_row_id").join(LKP_GET_PRODUCT_ID_SRC, (col('SKU_NBR') == col('SKU_NBR1')), 'left') \
.withColumn('row_num_PRODUCT_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("PRODUCT_ID")))

LKP_GET_PRODUCT_ID = LKP_GET_PRODUCT_ID_lookup_result.filter(col("row_num_PRODUCT_ID") == 1).select(
	LKP_GET_PRODUCT_ID_lookup_result.sys_row_id,
	col('PRODUCT_ID')
)

# COMMAND ----------

# Processing node LKP_GET_TIME_DIMENSIONS, type LOOKUP_FROM_PRECACHED_DATASET 
# COLUMN COUNT: 6


LKP_GET_TIME_DIMENSIONS_lookup_result = EXP_LBL_FIELDS_CONVERSION.join(LKP_GET_TIME_DIMENSIONS_SRC, (col('DAY_DT') == col('LABEL_CHANGE_DT')), 'left') \
.withColumn('row_num_DAY_DT', row_number().over(Window.partitionBy("sys_row_id").orderBy("DAY_DT")))
LKP_GET_TIME_DIMENSIONS = LKP_GET_TIME_DIMENSIONS_lookup_result.filter(col("row_num_DAY_DT") == 1).select(
	LKP_GET_TIME_DIMENSIONS_lookup_result.sys_row_id,
	col('FISCAL_WK'),
	col('FISCAL_MO'),
	col('FISCAL_YR'),
	col('WEEK_DT')
)

# COMMAND ----------

# Processing node LKP_GET_LOCATION_ID, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4


LKP_GET_LOCATION_ID_lookup_result = EXP_LBL_FIELDS_CONVERSION.selectExpr(
	"STORE_NBR as STORE_NBR1", "sys_row_id").join(LKP_GET_LOCATION_ID_SRC, (col('STORE_NBR') == col('STORE_NBR1')), 'left') \
.withColumn('row_num_LOCATION_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("LOCATION_ID")))

LKP_GET_LOCATION_ID = LKP_GET_LOCATION_ID_lookup_result.filter(col("row_num_LOCATION_ID") == 1).select(
	LKP_GET_LOCATION_ID_lookup_result.sys_row_id,
	col('LOCATION_ID')
)

# COMMAND ----------

# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
EXP_LBL_FIELDS_CONVERSION_temp = EXP_LBL_FIELDS_CONVERSION.toDF(*["EXP_LBL_FIELDS_CONVERSION___" + col for col in EXP_LBL_FIELDS_CONVERSION.columns])
LKP_GET_PRODUCT_ID_temp = LKP_GET_PRODUCT_ID.toDF(*["LKP_GET_PRODUCT_ID___" + col for col in LKP_GET_PRODUCT_ID.columns])
LKP_GET_LOCATION_ID_temp = LKP_GET_LOCATION_ID.toDF(*["LKP_GET_LOCATION_ID___" + col for col in LKP_GET_LOCATION_ID.columns])
LKP_GET_TIME_DIMENSIONS_temp = LKP_GET_TIME_DIMENSIONS.toDF(*["LKP_GET_TIME_DIMENSIONS___" + col for col in LKP_GET_TIME_DIMENSIONS.columns])

# Joining dataframes EXP_LBL_FIELDS_CONVERSION, LKP_GET_PRODUCT_ID, LKP_GET_TIME_DIMENSIONS, LKP_GET_LOCATION_ID to form EXP_LOAD_TSTMP
EXP_LOAD_TSTMP_joined = EXP_LBL_FIELDS_CONVERSION_temp.join(LKP_GET_PRODUCT_ID_temp, col("EXP_LBL_FIELDS_CONVERSION___sys_row_id") == col("LKP_GET_PRODUCT_ID___sys_row_id"), 'inner') \
 .join(LKP_GET_TIME_DIMENSIONS_temp, col("LKP_GET_PRODUCT_ID___sys_row_id") == col("LKP_GET_TIME_DIMENSIONS___sys_row_id"), 'inner') \
  .join(LKP_GET_LOCATION_ID_temp, col("LKP_GET_TIME_DIMENSIONS___sys_row_id") == col("LKP_GET_LOCATION_ID___sys_row_id"), 'inner')

# COMMAND ----------

#display(EXP_LOAD_TSTMP_joined.groupby("EXP_LBL_FIELDS_CONVERSION___LABEL_CHANGE_DT", "LKP_GET_LOCATION_ID___LOCATION_ID", "LKP_GET_PRODUCT_ID___PRODUCT_ID", "EXP_LBL_FIELDS_CONVERSION___ACTUAL_FLAG","EXP_LBL_FIELDS_CONVERSION___POG_TYPE", "EXP_LBL_FIELDS_CONVERSION___LABEL_SIZE", "EXP_LBL_FIELDS_CONVERSION___LABEL_TYPE", "EXP_LBL_FIELDS_CONVERSION___EXPIRATION_FLAG").count().orderBy(col("count").desc()).limit(10))

# COMMAND ----------


  
  
EXP_LOAD_TSTMP = EXP_LOAD_TSTMP_joined.selectExpr(
	"EXP_LBL_FIELDS_CONVERSION___sys_row_id as sys_row_id",
	"EXP_LBL_FIELDS_CONVERSION___LABEL_CHANGE_DT as LABEL_CHANGE_DT",
	"LKP_GET_LOCATION_ID___LOCATION_ID as LOCATION_ID",
	"LKP_GET_PRODUCT_ID___PRODUCT_ID as PRODUCT_ID",
	"EXP_LBL_FIELDS_CONVERSION___ACTUAL_FLAG as ACTUAL_FLAG",
	"EXP_LBL_FIELDS_CONVERSION___POG_TYPE as POG_TYPE",
	"EXP_LBL_FIELDS_CONVERSION___LABEL_SIZE as LABEL_SIZE",
	"EXP_LBL_FIELDS_CONVERSION___LABEL_TYPE as LABEL_TYPE",
	"EXP_LBL_FIELDS_CONVERSION___EXPIRATION_FLAG as EXPIRATION_FLAG",
	"EXP_LBL_FIELDS_CONVERSION___SKU_NBR as SKU_NBR",
	"EXP_LBL_FIELDS_CONVERSION___STORE_NBR as STORE_NBR",
	"LKP_GET_TIME_DIMENSIONS___WEEK_DT as WEEK_DT",
	"LKP_GET_TIME_DIMENSIONS___FISCAL_WK as FISCAL_WK",
	"LKP_GET_TIME_DIMENSIONS___FISCAL_MO as FISCAL_MO",
	"LKP_GET_TIME_DIMENSIONS___FISCAL_YR as FISCAL_YR",
	"EXP_LBL_FIELDS_CONVERSION___SUPPRESSED_FLAG as SUPPRESSED_FLAG",
	"EXP_LBL_FIELDS_CONVERSION___LABEL_CNT as LABEL_CNT",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP"
)

# COMMAND ----------

# Processing node JNR_LABEL_DAY_STORE_SKU__ZTB, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 34

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp = SQ_Shortcut_to_LABEL_DAY_STORE_SKU.toDF(*["SQ_Shortcut_to_LABEL_DAY_STORE_SKU___" + col for col in SQ_Shortcut_to_LABEL_DAY_STORE_SKU.columns])
EXP_LOAD_TSTMP_temp = EXP_LOAD_TSTMP.toDF(*["EXP_LOAD_TSTMP___" + col for col in EXP_LOAD_TSTMP.columns])

JNR_LABEL_DAY_STORE_SKU__ZTB = EXP_LOAD_TSTMP_temp.join(SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp,[EXP_LOAD_TSTMP_temp.EXP_LOAD_TSTMP___LABEL_CHANGE_DT == SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp.SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_CHANGE_DT, EXP_LOAD_TSTMP_temp.EXP_LOAD_TSTMP___LOCATION_ID == SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp.SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LOCATION_ID, EXP_LOAD_TSTMP_temp.EXP_LOAD_TSTMP___PRODUCT_ID == SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp.SQ_Shortcut_to_LABEL_DAY_STORE_SKU___PRODUCT_ID, EXP_LOAD_TSTMP_temp.EXP_LOAD_TSTMP___ACTUAL_FLAG == SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp.SQ_Shortcut_to_LABEL_DAY_STORE_SKU___ACTUAL_FLAG, EXP_LOAD_TSTMP_temp.EXP_LOAD_TSTMP___POG_TYPE == SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp.SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_POG_TYPE_CD, EXP_LOAD_TSTMP_temp.EXP_LOAD_TSTMP___LABEL_SIZE == SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp.SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_SIZE_ID, EXP_LOAD_TSTMP_temp.EXP_LOAD_TSTMP___LABEL_TYPE == SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp.SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_TYPE_ID, EXP_LOAD_TSTMP_temp.EXP_LOAD_TSTMP___EXPIRATION_FLAG == SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp.SQ_Shortcut_to_LABEL_DAY_STORE_SKU___EXPIRATION_FLAG],'left_outer').selectExpr(
	"EXP_LOAD_TSTMP___LABEL_CHANGE_DT as LABEL_CHANGE_DT",
	"EXP_LOAD_TSTMP___LOCATION_ID as LOCATION_ID",
	"EXP_LOAD_TSTMP___PRODUCT_ID as PRODUCT_ID",
	"EXP_LOAD_TSTMP___ACTUAL_FLAG as ACTUAL_FLAG",
	"EXP_LOAD_TSTMP___POG_TYPE as POG_TYPE",
	"EXP_LOAD_TSTMP___LABEL_SIZE as LABEL_SIZE",
	"EXP_LOAD_TSTMP___LABEL_TYPE as LABEL_TYPE",
	"EXP_LOAD_TSTMP___EXPIRATION_FLAG as EXPIRATION_FLAG",
	"EXP_LOAD_TSTMP___SKU_NBR as SKU_NBR",
	"EXP_LOAD_TSTMP___STORE_NBR as STORE_NBR",
	"EXP_LOAD_TSTMP___WEEK_DT as WEEK_DT",
	"EXP_LOAD_TSTMP___FISCAL_WK as FISCAL_WK",
	"EXP_LOAD_TSTMP___FISCAL_MO as FISCAL_MO",
	"EXP_LOAD_TSTMP___FISCAL_YR as FISCAL_YR",
	"EXP_LOAD_TSTMP___SUPPRESSED_FLAG as SUPPRESSED_FLAG",
	"EXP_LOAD_TSTMP___LABEL_CNT as LABEL_CNT",
	"EXP_LOAD_TSTMP___UPDATE_TSTMP as UPDATE_TSTMP",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_CHANGE_DT as LABEL_CHANGE_DT_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LOCATION_ID as LOCATION_ID_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___PRODUCT_ID as PRODUCT_ID_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___ACTUAL_FLAG as ACTUAL_FLAG_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_POG_TYPE_CD as LABEL_POG_TYPE_CD_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_SIZE_ID as LABEL_SIZE_ID_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_TYPE_ID as LABEL_TYPE_ID_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___EXPIRATION_FLAG as EXPIRATION_FLAG_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___SKU_NBR as SKU_NBR_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___STORE_NBR as STORE_NBR_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___WEEK_DT as WEEK_DT_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___FISCAL_WK as FISCAL_WK_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___FISCAL_MO as FISCAL_MO_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___FISCAL_YR as FISCAL_YR_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___SUPPRESSED_FLAG as SUPPRESSED_FLAG_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_CNT as LABEL_CNT_OLD",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LOAD_TSTMP as LOAD_TSTMP_OLD")

# COMMAND ----------

# Processing node EXP_MD5, type EXPRESSION 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
JNR_LABEL_DAY_STORE_SKU__ZTB_temp = JNR_LABEL_DAY_STORE_SKU__ZTB.toDF(*["JNR_LABEL_DAY_STORE_SKU__ZTB___" + col for col in JNR_LABEL_DAY_STORE_SKU__ZTB.columns])

EXP_MD5 = JNR_LABEL_DAY_STORE_SKU__ZTB_temp.selectExpr(
	# "JNR_LABEL_DAY_STORE_SKU__ZTB___sys_row_id as sys_row_id",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___LABEL_CHANGE_DT as LABEL_CHANGE_DT",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___LOCATION_ID as LOCATION_ID",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___PRODUCT_ID as PRODUCT_ID",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___ACTUAL_FLAG as ACTUAL_FLAG",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___POG_TYPE as POG_TYPE",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___LABEL_SIZE as LABEL_SIZE",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___LABEL_TYPE as LABEL_TYPE",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___EXPIRATION_FLAG as EXPIRATION_FLAG",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___SKU_NBR as SKU_NBR",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___STORE_NBR as STORE_NBR",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___WEEK_DT as WEEK_DT",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___FISCAL_WK as FISCAL_WK",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___FISCAL_MO as FISCAL_MO",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___FISCAL_YR as FISCAL_YR",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___SUPPRESSED_FLAG as SUPPRESSED_FLAG",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___LABEL_CNT as LABEL_CNT",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___UPDATE_TSTMP as UPDATE_TSTMP",
	"JNR_LABEL_DAY_STORE_SKU__ZTB___SKU_NBR_OLD          AS SKU_NBR_OLD",  
	"JNR_LABEL_DAY_STORE_SKU__ZTB___STORE_NBR_OLD        AS STORE_NBR_OLD",  
	"JNR_LABEL_DAY_STORE_SKU__ZTB___WEEK_DT_OLD          AS WEEK_DT_OLD",  
	"JNR_LABEL_DAY_STORE_SKU__ZTB___FISCAL_WK_OLD        AS FISCAL_WK_OLD",  
	"JNR_LABEL_DAY_STORE_SKU__ZTB___FISCAL_MO_OLD        AS FISCAL_MO_OLD",  
	"JNR_LABEL_DAY_STORE_SKU__ZTB___FISCAL_YR_OLD        AS FISCAL_YR_OLD",  
	"JNR_LABEL_DAY_STORE_SKU__ZTB___SUPPRESSED_FLAG_OLD  AS SUPPRESSED_FLAG_OLD",            
	"JNR_LABEL_DAY_STORE_SKU__ZTB___LABEL_CNT_OLD        AS LABEL_CNT_OLD",  
    "JNR_LABEL_DAY_STORE_SKU__ZTB___LABEL_CHANGE_DT_OLD AS LABEL_CHANGE_DT_OLD",
	"IF (JNR_LABEL_DAY_STORE_SKU__ZTB___LABEL_CHANGE_DT_OLD IS NULL, CURRENT_TIMESTAMP, JNR_LABEL_DAY_STORE_SKU__ZTB___LOAD_TSTMP_OLD) as LOAD_TSTMP"
) \
.withColumn(
    "MD5_PRE",
    md5(
        concat(
            col("SKU_NBR").cast(StringType()),
            col("STORE_NBR").cast(StringType()),
            col("WEEK_DT").cast(StringType()),
            col("FISCAL_WK").cast(StringType()),
            col("FISCAL_MO").cast(StringType()),
            col("FISCAL_YR").cast(StringType()),
            col("SUPPRESSED_FLAG").cast(StringType()),
            col("LABEL_CNT").cast(StringType())
        )
    )
) \
.withColumn(
    "MD5_OLD",
    md5(
        concat(
            col("SKU_NBR_OLD").cast(StringType()),
            col("STORE_NBR_OLD").cast(StringType()),
            col("WEEK_DT_OLD").cast(StringType()),
            col("FISCAL_WK_OLD").cast(StringType()),
            col("FISCAL_MO_OLD").cast(StringType()),
            col("FISCAL_YR_OLD").cast(StringType()),
            col("SUPPRESSED_FLAG_OLD").cast(StringType()),
            col("LABEL_CNT_OLD").cast(StringType())
        )
    )
) \
.withColumn(
    "UPDATE_STRATEGY",
    expr("IF (LABEL_CHANGE_DT_OLD IS NULL, 0, IF (MD5_PRE <> MD5_OLD, 1, 3))")
)

# COMMAND ----------

# Processing node FIL_INS_UPD, type FILTER 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
EXP_MD5_temp = EXP_MD5.toDF(*["EXP_MD5___" + col for col in EXP_MD5.columns])

FIL_INS_UPD = EXP_MD5_temp.selectExpr(
	"EXP_MD5___LABEL_CHANGE_DT as LABEL_CHANGE_DT",
	"EXP_MD5___LOCATION_ID as LOCATION_ID",
	"EXP_MD5___PRODUCT_ID as PRODUCT_ID",
	"EXP_MD5___ACTUAL_FLAG as ACTUAL_FLAG",
	"EXP_MD5___POG_TYPE as POG_TYPE",
	"EXP_MD5___LABEL_SIZE as LABEL_SIZE",
	"EXP_MD5___LABEL_TYPE as LABEL_TYPE",
	"EXP_MD5___EXPIRATION_FLAG as EXPIRATION_FLAG",
	"EXP_MD5___SKU_NBR as SKU_NBR",
	"EXP_MD5___STORE_NBR as STORE_NBR",
	"EXP_MD5___WEEK_DT as WEEK_DT",
	"EXP_MD5___FISCAL_WK as FISCAL_WK",
	"EXP_MD5___FISCAL_MO as FISCAL_MO",
	"EXP_MD5___FISCAL_YR as FISCAL_YR",
	"EXP_MD5___SUPPRESSED_FLAG as SUPPRESSED_FLAG",
	"EXP_MD5___LABEL_CNT as LABEL_CNT",
	"EXP_MD5___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_MD5___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_MD5___UPDATE_STRATEGY as UPDATE_STRATEGY").filter("UPDATE_STRATEGY != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node UPS_LABEL_DAY_STORE_SKU, type UPDATE_STRATEGY 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
FIL_INS_UPD_temp = FIL_INS_UPD.toDF(*["FIL_INS_UPD___" + col for col in FIL_INS_UPD.columns])

UPS_LABEL_DAY_STORE_SKU = FIL_INS_UPD_temp.selectExpr(
	"FIL_INS_UPD___LABEL_CHANGE_DT as LABEL_CHANGE_DT",
	"FIL_INS_UPD___LOCATION_ID as LOCATION_ID",
	"FIL_INS_UPD___PRODUCT_ID as PRODUCT_ID",
	"FIL_INS_UPD___ACTUAL_FLAG as ACTUAL_FLAG",
	"FIL_INS_UPD___POG_TYPE as POG_TYPE",
	"FIL_INS_UPD___LABEL_SIZE as LABEL_SIZE",
	"FIL_INS_UPD___LABEL_TYPE as LABEL_TYPE",
	"FIL_INS_UPD___EXPIRATION_FLAG as EXPIRATION_FLAG",
	"FIL_INS_UPD___SKU_NBR as SKU_NBR",
	"FIL_INS_UPD___STORE_NBR as STORE_NBR",
	"FIL_INS_UPD___WEEK_DT as WEEK_DT",
	"FIL_INS_UPD___FISCAL_WK as FISCAL_WK",
	"FIL_INS_UPD___FISCAL_MO as FISCAL_MO",
	"FIL_INS_UPD___FISCAL_YR as FISCAL_YR",
	"FIL_INS_UPD___SUPPRESSED_FLAG as SUPPRESSED_FLAG",
	"FIL_INS_UPD___LABEL_CNT as LABEL_CNT",
	"FIL_INS_UPD___UPDATE_TSTMP as UPDATE_TSTMP",
	"FIL_INS_UPD___LOAD_TSTMP as LOAD_TSTMP",
	"FIL_INS_UPD___UPDATE_STRATEGY as UPDATE_STRATEGY") \
	.withColumn('pyspark_data_action', col("UPDATE_STRATEGY"))

# COMMAND ----------

def has_duplicates(df: DataFrame, key_columns: list) -> bool:
	"""
	Check if a DataFrame has duplicate rows based on the specified key columns.

	:param df: The DataFrame to check for duplicates.
	:param key_columns: A list of column names to use as the key for checking duplicates.
	:return: True if duplicates are found, False otherwise.
	"""
	if df.count() == 0:
		return False
	# Group by the key columns and count occurrences
	grouped = df.groupBy(key_columns).count()

	# Find the maximum count
	max_count = grouped.agg({"count": "max"}).collect()[0][0]

	# Return True if duplicates found, False otherwise
	return max_count > 1

# COMMAND ----------

# Processing node Shortcut_to_LABEL_DAY_STORE_SKU, type TARGET 
# COLUMN COUNT: 18


Shortcut_to_LABEL_DAY_STORE_SKU = UPS_LABEL_DAY_STORE_SKU.selectExpr(
	"CAST(LABEL_CHANGE_DT AS DATE) as LABEL_CHANGE_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(ACTUAL_FLAG AS TINYINT) as  ACTUAL_FLAG",
	"CAST(POG_TYPE AS STRING) as LABEL_POG_TYPE_CD",
	"CAST(LABEL_SIZE AS STRING) as LABEL_SIZE_ID",
	"CAST(LABEL_TYPE AS TINYINT) as  LABEL_TYPE_ID",
	"CAST(EXPIRATION_FLAG AS TINYINT) as  EXPIRATION_FLAG",
	"CAST(SKU_NBR AS INT) as SKU_NBR",
	"CAST(STORE_NBR AS INT) as STORE_NBR",
	"CAST(WEEK_DT AS DATE) as WEEK_DT",
	"CAST(FISCAL_WK AS INT) as FISCAL_WK",
	"CAST(FISCAL_MO AS INT) as FISCAL_MO",
	"CAST(FISCAL_YR AS INT) as FISCAL_YR",
	"CAST(SUPPRESSED_FLAG AS TINYINT) as  SUPPRESSED_FLAG",
	"CAST(LABEL_CNT AS INT) as LABEL_CNT",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.LABEL_CHANGE_DT = target.LABEL_CHANGE_DT AND 
                     source.LOCATION_ID = target.LOCATION_ID AND 
                     source.PRODUCT_ID = target.PRODUCT_ID AND 
                     source.ACTUAL_FLAG = target.ACTUAL_FLAG AND 
                     source.LABEL_POG_TYPE_CD = target.LABEL_POG_TYPE_CD AND 
                     source.LABEL_SIZE_ID = target.LABEL_SIZE_ID AND 
                     source.LABEL_TYPE_ID = target.LABEL_TYPE_ID AND 
                     source.EXPIRATION_FLAG = target.EXPIRATION_FLAG"""
	refined_perf_table = f"{legacy}.LABEL_DAY_STORE_SKU"
	
	if has_duplicates(Shortcut_to_LABEL_DAY_STORE_SKU, ["LABEL_CHANGE_DT", "LOCATION_ID", "PRODUCT_ID", "ACTUAL_FLAG","LABEL_POG_TYPE_CD", "LABEL_SIZE_ID", "LABEL_TYPE_ID", "EXPIRATION_FLAG"]):
		raise Exception("Duplicates found in the dataset")
    
	executeMerge(Shortcut_to_LABEL_DAY_STORE_SKU, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("LABEL_DAY_STORE_SKU", "LABEL_DAY_STORE_SKU", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("LABEL_DAY_STORE_SKU", "LABEL_DAY_STORE_SKU","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		
