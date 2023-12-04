# Databricks notebook source
#Code converted on 2023-09-08 09:28:24
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
# COLUMN COUNT: 17

_sql = f"""
SELECT gp.gl_project_gid
       ,pre.posid
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
  JOIN {legacy}.GL_PROJECT gp ON pre.objnr = gp.wbs_object_cd
"""


SQ_Shortcut_to_GL_PRPS_PRE = spark.sql(_sql)

# Conforming fields names to the component layout
SQ_Shortcut_to_GL_PRPS_PRE = SQ_Shortcut_to_GL_PRPS_PRE \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[0],'GL_PROJECT_GID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[1],'POSID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[2],'OBJNR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[3],'PSPNR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[4],'POST1') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[5],'PWPOS') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[6],'VERNR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[7],'VERNA') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[8],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[9],'STORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[10],'GL_PROFIT_CENTER_GID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[11],'GL_PROFIT_CENTER_GID1') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[12],'ASTNR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[13],'ASTNA') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[14],'ZZMATKL') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[15],'ERDAT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PRPS_PRE.columns[16],'AEDAT')

# COMMAND ----------

# Processing node Shortcut_to_GL_PROJECT, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_GL_PROJECT = SQ_Shortcut_to_GL_PRPS_PRE.selectExpr(
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
	"CAST(AEDAT AS TIMESTAMP) as CHANGE_DT", 
  "1 AS pyspark_data_action"
)

try:
	primary_key = """source.GL_PROJECT_GID = target.GL_PROJECT_GID"""
	refined_perf_table = f"{legacy}.GL_PROJECT"
	executeMerge(Shortcut_to_GL_PROJECT, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("GL_PROJECT", "GL_PROJECT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("GL_PROJECT", "GL_PROJECT","Failed",str(e), f"{raw}.log_run_details", )
	raise e

# Shortcut_to_GL_PROJECT.write.mode("overwrite").saveAsTable(f'{raw}.GL_PROJECT')

# COMMAND ----------


