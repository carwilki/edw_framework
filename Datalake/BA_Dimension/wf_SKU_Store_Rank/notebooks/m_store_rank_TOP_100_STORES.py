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

SQ_Shortcut_to_USER_RANKING_HIERARCHY = spark.sql(f"""SELECT TT.LOCATION_ID,

       TT.STORE_NBR,

       U.HIERARCHY_SUB_LVL,

       U.HIERARCHY_NAME,

       U.RANK_NAME,

       U.RANK_CRITERIA,

       U.RANK_VALUE_TYPE,

       U.RANK_MIN_VALUE,

       U.RANK_MAX_VALUE ,

       CURRENT_DATE AS LOAD_DT

  FROM {legacy}.USER_RANKING_HIERARCHY U,

       (SELECT T1.LOCATION_ID,

               T1.STORE_NBR,

               T1.CONSUM_ID,

               T1.CONSUM_DESC,

               T1.NSAMT,

               RANK () OVER (PARTITION BY CONSUM_ID ORDER BY NSAMT DESC) AS RANKING

         FROM (SELECT W.LOCATION_ID,

                      MAX(S.STORE_NBR)         AS STORE_NBR,

                      D.CONSUM_ID,

                      MAX(D.CONSUM_DESC)       AS CONSUM_DESC,

                      SUM(SALES_AMT)           AS SAMT,

                      SUM(RETURN_AMT)          AS RAMT,

                      SUM(DISCOUNT_AMT)        AS DAMT,

                      SUM(DISCOUNT_RETURN_AMT) AS DRAMT,

                      SUM(POS_COUPON_AMT)      AS CAMT,

                      SUM(SPECIAL_SALES_AMT)   AS SSAMT,

                      SUM(SPECIAL_RETURN_AMT)  AS SRAMT,

                      SUM(SPECIAL_SRVC_AMT)    AS SSRVAMT,

                      SUM(SALES_AMT - RETURN_AMT - DISCOUNT_AMT + DISCOUNT_RETURN_AMT - POS_COUPON_AMT

                          - SPECIAL_SALES_AMT + SPECIAL_RETURN_AMT + SPECIAL_SRVC_AMT) AS NSAMT

                 FROM {legacy}.SALES_DAY_SKU_STORE W,

                      {legacy}.SITE_PROFILE S,

                      {legacy}.SKU_PROFILE P,

                      {legacy}.DM_DEPT_SEGMENTS D

                WHERE W.WEEK_DT          >= ((CURRENT_DATE + 1 - DATE_PART('DOW', CURRENT_DATE)) - 51*7)

                  AND W.WEEK_DT          <= (CURRENT_DATE + 1 - DATE_PART('DOW', CURRENT_DATE))

                  AND W.LOCATION_ID       = S.LOCATION_ID

                  AND S.LOCATION_TYPE_ID  = 8

                  AND S.SITE_SALES_FLAG   = 1

                  AND W.PRODUCT_ID        = P.PRODUCT_ID

                  AND P.SAP_DEPT_ID       = D.SAP_DEPT_ID

                  AND D.CONSUM_ID IN (2,3)

               GROUP BY W.LOCATION_ID,

                        D.CONSUM_ID) T1 ) TT

  WHERE U.HIERARCHY_NAME    = 'Top 100 Stores'

    AND U.HIERARCHY_SUB_LVL = TT.CONSUM_DESC

    AND TT.RANKING         <= 100""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_USER_RANKING_HIERARCHY = SQ_Shortcut_to_USER_RANKING_HIERARCHY \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[1],'STORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[2],'HIERARCHY_SUB_LVL') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[3],'HIERARCHY_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[4],'RANK_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[5],'RANK_CRITERIA') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[6],'RANK_VALUE_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[7],'RANK_MIN_VALUE') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[8],'RANK_MAX_VALUE') \
	.withColumnRenamed(SQ_Shortcut_to_USER_RANKING_HIERARCHY.columns[9],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_to_STORE_RANK, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_STORE_RANK = SQ_Shortcut_to_USER_RANKING_HIERARCHY.selectExpr(
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(STORE_NBR AS INT) as STORE_NBR",
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
	chk.check_for_duplicate_primary_keys(Shortcut_to_STORE_RANK,["LOCATION_ID",
                    "STORE_NBR",
                    "HIERARCHY_SUB_LVL",
                    "HIERARCHY_NAME",
                    "RANK_NAME"])
	Shortcut_to_STORE_RANK.write.mode("overwrite").saveAsTable(f'{legacy}.STORE_RANK')
except Exception as e:
	raise e
