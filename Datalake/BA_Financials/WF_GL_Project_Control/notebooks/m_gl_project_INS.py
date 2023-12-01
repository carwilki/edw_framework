# Databricks notebook source
#Code converted on 2023-09-08 09:28:23
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


# COMMAND ----------

# Processing node SQ_Shortcut_to_GL_PRPS_PRE, type SOURCE 
# COLUMN COUNT: 16

_sql = f"""
SELECT  pre.posid
       ,pre.objnr
       ,pre.pspnr
       ,pre.post1
       ,pre.pwpos
       ,CAST (pre.vernr as INTEGER) as responsible_id
       ,pre.verna
       ,sp.location_id
       ,CASE WHEN werks = ' '
             THEN NULL
             ELSE TRIM(LTRIM( '0',werks))
       END as store_nbr
       ,pc.gl_profit_center_gid as profit_center
       ,rpc.gl_profit_center_gid as resp_profit_center
       ,CAST (pre.astnr as INTEGER) as applicant_id
       ,pre.astna
       ,pre.zzmatkl
       ,CASE WHEN TRIM(erdat) = '00000000'
             THEN NULL
             ELSE TO_DATE (TRIM(erdat),'yyyyMMdd')
        END as create_dt
       ,CASE WHEN TRIM(aedat) = '00000000'
            THEN NULL
            ELSE TO_DATE (TRIM(aedat),'yyyyMMdd')
        END as change_dt
FROM {raw}.gl_prps_pre pre
  LEFT OUTER JOIN {legacy}.site_profile sp
  ON (CASE WHEN pre.werks = ' '
          THEN '99999999'
          ELSE pre.werks END   = sp.store_nbr)
  LEFT OUTER JOIN {legacy}.gl_profit_center_profile pc
  ON (pre.pbukr = pc.gl_company_cd
  AND pre.prctr = pc.gl_profit_center_cd)
  LEFT OUTER JOIN {legacy}.gl_profit_center_profile rpc
  ON (pre.pbukr = rpc.gl_company_cd
  AND pre.fkstl = rpc.gl_profit_center_cd)
  LEFT OUTER JOIN {legacy}.GL_PROJECT gp
  ON ( pre.objnr = gp.wbs_object_cd )
WHERE gp.wbs_object_cd IS NULL
"""

SQ_Shortcut_to_GL_PRPS_PRE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_Shortcut_to_GL_PRPS_PRE = SQ_Shortcut_to_GL_PRPS_PRE \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[0],'POSID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[1],'OBJNR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[2],'PSPNR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[3],'POST1') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[4],'PWPOS') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[5],'VERNR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[6],'VERNA') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[7],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[8],'STORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[9],'GL_PROFIT_CENTER_GID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[10],'GL_PROFIT_CENTER_GID1') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[11],'ASTNR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[12],'ASTNA') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[13],'ZZMATKL') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[14],'ERDAT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[15],'AEDAT')

# COMMAND ----------

# Processing node EXP_TRANS22, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# Query to find the starting value for monotonically_increasing_id()
starting_value_df = spark.sql(f"SELECT MAX(GL_PROJECT_GID) + 1 AS starting_value FROM {legacy}.GL_PROJECT")

# Get the starting value from the result DataFrame
starting_value = starting_value_df.first()["starting_value"]

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GL_PRPS_PRE_temp = SQ_Shortcut_to_GL_PRPS_PRE.toDF(*["SQ_Shortcut_to_GL_PRPS_PRE___" + col for col in SQ_Shortcut_to_GL_PRPS_PRE.columns])

EXP_TRANS22 = SQ_Shortcut_to_GL_PRPS_PRE_temp \
	.withColumn("NEXTVAL", starting_value + monotonically_increasing_id()) \
	.selectExpr(
	"NEXTVAL AS GL_PROJECT_GID",
	"SQ_Shortcut_to_GL_PRPS_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_GL_PRPS_PRE___POSID as POSID",
	"SQ_Shortcut_to_GL_PRPS_PRE___OBJNR as OBJNR",
	"SQ_Shortcut_to_GL_PRPS_PRE___PSPNR as PSPNR",
	"SQ_Shortcut_to_GL_PRPS_PRE___POST1 as POST1",
	"SQ_Shortcut_to_GL_PRPS_PRE___PWPOS as PWPOS",
	"SQ_Shortcut_to_GL_PRPS_PRE___VERNR as VERNR",
	"SQ_Shortcut_to_GL_PRPS_PRE___VERNA as VERNA",
	"SQ_Shortcut_to_GL_PRPS_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_GL_PRPS_PRE___STORE_NBR as STORE_NBR",
	"SQ_Shortcut_to_GL_PRPS_PRE___GL_PROFIT_CENTER_GID as GL_PROFIT_CENTER_GID",
	"SQ_Shortcut_to_GL_PRPS_PRE___GL_PROFIT_CENTER_GID1 as GL_PROFIT_CENTER_GID1",
	"SQ_Shortcut_to_GL_PRPS_PRE___ASTNR as ASTNR",
	"SQ_Shortcut_to_GL_PRPS_PRE___ASTNA as ASTNA",
	"SQ_Shortcut_to_GL_PRPS_PRE___ZZMATKL as ZZMATKL",
	"SQ_Shortcut_to_GL_PRPS_PRE___ERDAT as ERDAT",
	"SQ_Shortcut_to_GL_PRPS_PRE___AEDAT as AEDAT"
)

# COMMAND ----------

# Processing node Shortcut_to_GL_PROJECT, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_GL_PROJECT = EXP_TRANS22.selectExpr(
	"CAST(GL_PROJECT_GID AS INT) as GL_PROJECT_GID",
	"CAST(POSID AS STRING) as WBS_ELEMENT_CD",
	"CAST(OBJNR AS STRING) as WBS_OBJECT_CD",
	"CAST(PSPNR AS STRING) as WBS_ELEMENT_ID",
	"CAST(POST1 AS STRING) as GL_PROJ_DESC",
	"CAST(PWPOS AS STRING) as GL_PROJ_CURRENCY_CD",
	"CAST(VERNR AS INT) as GL_PROJ_RESPONSIBLE_ID",
	"CAST(VERNA AS STRING) as GL_PROJ_RESPONSIBLE_NAME",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(STORE_NBR AS INT) as STORE_NBR",
	"CAST(GL_PROFIT_CENTER_GID AS INT) as GL_PROFIT_CENTER_GID",
	"CAST(GL_PROFIT_CENTER_GID1 AS INT) as RESP_GL_PROFIT_CENTER_GID",
	"CAST(ASTNR AS INT) as GL_PROJ_APPLICANT_ID",
	"CAST(ASTNA AS STRING) as GL_PROJ_APPLICANT_NAME",
	"CAST(ZZMATKL AS STRING) as GL_PROJ_MERCH_CATEGORY",
	"CAST(ERDAT AS TIMESTAMP) as CREATE_DT",
	"CAST(AEDAT AS TIMESTAMP) as CHANGE_DT"
)
Shortcut_to_GL_PROJECT.write.mode("append").saveAsTable(f'{legacy}.GL_PROJECT')

# COMMAND ----------


