# Databricks notebook source
# Code converted on 2023-11-02 10:06:31
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
from Datalake.utils.pk import *

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
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

# Processing node SQ_Shortcut_to_USER_RANKING_HIERARCHY, type SOURCE 
# COLUMN COUNT: 10

SQ_Shortcut_to_USER_RANKING_HIERARCHY = spark.sql(f"""SELECT TT.PRODUCT_ID,

       TT.SKU_NBR,

       HIERARCHY_SUB_LVL,

       HIERARCHY_NAME,

       RANK_NAME,

       RANK_CRITERIA,

       RANK_VALUE_TYPE,

       RANK_MIN_VALUE,

       RANK_MAX_VALUE,

       CURRENT_DATE AS LOAD_DT

  FROM {legacy}.USER_RANKING_HIERARCHY U,

       (SELECT PRODUCT_ID,

               MAX(SKU_NBR) AS SKU_NBR,

               MAX(CONSUM_DESC) AS CONSUM_DESC,

               ROUND(SUM(NQTY) / COUNT(*),4) AS UNITS_STR_WK_VALUE

          FROM (SELECT S.WEEK_DT,

                       S.LOCATION_ID,

                       S.PRODUCT_ID,

                       MAX(K.SKU_NBR)            AS SKU_NBR,

                       MAX(D.CONSUM_ID)          AS CONSUM_ID,

                       MAX(D.CONSUM_DESC)        AS CONSUM_DESC,

                       SUM(S.SALES_QTY - S.RETURN_QTY - S.SPECIAL_SALES_QTY + S.SPECIAL_RETURN_QTY) AS NQTY

                  FROM {legacy}.SALES_DAY_SKU_STORE S,

                       {legacy}.SKU_PROFILE K,

                       {legacy}.DM_DEPT_SEGMENTS D,

                       {legacy}.SITE_PROFILE P

                 WHERE S.WEEK_DT          >= ((CURRENT_DATE + 1 - DATE_PART('DOW', CURRENT_DATE)) - 7*7)

                   AND S.WEEK_DT          <= (CURRENT_DATE + 1 - DATE_PART('DOW', CURRENT_DATE))

                   AND S.PRODUCT_ID        = K.PRODUCT_ID

                   AND K.SAP_DEPT_ID       = D.SAP_DEPT_ID

                   AND S.LOCATION_ID       = P.LOCATION_ID

                   AND P.LOCATION_TYPE_ID  = 8

                   AND P.SITE_SALES_FLAG   = 1

                   AND D.CONSUM_ID IN (2,3)

                 GROUP BY S.WEEK_DT,

                          S.LOCATION_ID,

                          S.PRODUCT_ID)T

           GROUP BY T.PRODUCT_ID) TT

 WHERE U.HIERARCHY_SUB_LVL = TT.CONSUM_DESC

   AND U.HIERARCHY_NAME    = 'SKU Rank 1'

   AND TT.UNITS_STR_WK_VALUE BETWEEN NVL(RANK_MIN_VALUE,-1000000) AND NVL(RANK_MAX_VALUE,1000000)""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_USER_RANKING_HIERARCHY = SQ_Shortcut_to_USER_RANKING_HIERARCHY \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[1],'SKU_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[2],'HIERARCHY_SUB_LVL') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[3],'HIERARCHY_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[4],'RANK_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[5],'RANK_CRITERIA') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[6],'RANK_VALUE_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[7],'RANK_MIN_VALUE') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[8],'RANK_MAX_VALUE') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[9],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_to_SKU_RANK, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_SKU_RANK = SQ_Shortcut_to_USER_RANKING_HIERARCHY.selectExpr(
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(SKU_NBR AS INT) as SKU_NBR",
	"CAST(HIERARCHY_SUB_LVL AS STRING) as HIERARCHY_SUB_LVL",
	"CAST(HIERARCHY_NAME AS STRING) as HIERARCHY_NAME",
	"CAST(RANK_NAME AS STRING) as RANK_NAME",
	"CAST(RANK_CRITERIA AS STRING) as RANK_CRITERIA",
	"CAST(RANK_VALUE_TYPE AS STRING) as RANK_VALUE_TYPE",
	"CAST(RANK_MIN_VALUE AS DECIMAL(20,6)) as RANK_MIN_VALUE",
	"CAST(RANK_MAX_VALUE AS DECIMAL(20,6)) as RANK_MAX_VALUE",
	"CAST(LOAD_DT AS DATE) as LOAD_DT"
)
try:
	chk=DuplicateChecker()
	chk.check_for_duplicate_primary_keys(Shortcut_to_SKU_RANK,["PRODUCT_ID",
                    "SKU_NBR",
                    "HIERARCHY_SUB_LVL",
                    "HIERARCHY_NAME",
                    "RANK_NAME"])
	Shortcut_to_SKU_RANK.write.mode("overwrite").saveAsTable(f'{legacy}.SKU_RANK')
except Exception as e:
	raise e
