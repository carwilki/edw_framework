# Databricks notebook source
#Code converted on 2023-09-08 09:28:25
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
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window

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

# Processing node SQ_Shortcut_to_GL_COSP_PRE, type SOURCE 
# COLUMN COUNT: 20

_sql = f"""
SELECT distinct pre.gl_project_gid
      ,pre.gjahr
      ,pre.wrttp
      ,pre.kstar
      ,pre.twaer
      ,pre.p1
      ,pre.p2
      ,pre.p3
      ,pre.p4
      ,pre.p5
      ,pre.p6
      ,pre.p7
      ,pre.p8
      ,pre.p9
      ,pre.p10
      ,pre.p11
      ,pre.p12
      ,CURRENT_DATE as update_dt
      ,NVL(gpcd.LOAD_DT,CURRENT_DATE) as load_dt
      ,CASE WHEN gpcd.gl_project_gid IS NULL
            THEN 1
            ELSE 2
       END AS load_flag
  FROM (SELECT   p.gl_project_gid
                ,c.gjahr
                ,c.wrttp
                ,c.kstar
                ,c.twaer
                ,SUM (c.wtg001) AS p1
                ,SUM (c.wtg002) AS p2
                ,SUM (c.wtg003) AS p3
                ,SUM (c.wtg004) AS p4
                ,SUM (c.wtg005) AS p5
                ,SUM (c.wtg006) AS p6
                ,SUM (c.wtg007) AS p7
                ,SUM (c.wtg008) AS p8
                ,SUM (c.wtg009) AS p9
                ,SUM (c.wtg010) AS p10
                ,SUM (c.wtg011) AS p11
                ,SUM (c.wtg012) AS p12
            FROM {raw}.gl_cosp_pre c, {legacy}.gl_project p
           WHERE c.objnr = p.wbs_object_cd
        GROUP BY p.gl_project_gid,
                 c.gjahr,
                 c.wrttp,
                 c.kstar,
                 c.twaer) pre
       LEFT OUTER JOIN {legacy}.gl_project_cost_detail gpcd
       ON (    pre.gl_project_gid = gpcd.gl_project_gid
           AND LTRIM ( '0',pre.kstar) = gpcd.gl_cost_element_cd
          )
"""

SQ_Shortcut_to_GL_COSP_PRE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_Shortcut_to_GL_COSP_PRE = SQ_Shortcut_to_GL_COSP_PRE \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[0],'GL_PROJECT_GID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[1],'GJAHR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[2],'WRTTP') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[3],'KSTAR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[4],'TWAER') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[5],'WTG001') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[6],'WTG002') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[7],'WTG003') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[8],'WTG004') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[9],'WTG005') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[10],'WTG006') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[11],'WTG007') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[12],'WTG008') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[13],'WTG009') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[14],'WTG010') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[15],'WTG011') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[16],'WTG012') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[17],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[18],'LOAD_DT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_COSP_PRE.columns[19],'LOAD_FLAG')

# COMMAND ----------

# Processing node NRM_TRANS, type NORMALIZER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 31

unpivotExpr = '''stack(12,
PERIOD_in1, PERIOD_in2, PERIOD_in3, PERIOD_in4, PERIOD_in5, PERIOD_in6, PERIOD_in7, PERIOD_in8, PERIOD_in9, PERIOD_in10, PERIOD_in11,
PERIOD_in12 ) as (PERIOD)'''

NRM_TRANS = SQ_Shortcut_to_GL_COSP_PRE.selectExpr(
	"GL_PROJECT_GID as GL_PROJECT_GID_in",
	"GJAHR as GJAHR_in",
	"WRTTP as WRTTP_in",
	"KSTAR as KSTAR_in",
	"TWAER as TWAER_in",
	"WTG001 as PERIOD_in1",
	"WTG002 as PERIOD_in2",
	"WTG003 as PERIOD_in3",
	"WTG004 as PERIOD_in4",
	"WTG005 as PERIOD_in5",
	"WTG006 as PERIOD_in6",
	"WTG007 as PERIOD_in7",
	"WTG008 as PERIOD_in8",
	"WTG009 as PERIOD_in9",
	"WTG010 as PERIOD_in10",
	"WTG011 as PERIOD_in11",
	"WTG012 as PERIOD_in12",
	"UPDATE_DT as UPDATE_DT_in",
	"LOAD_DT as LOAD_DT_in",
	"LOAD_FLAG as LOAD_FLAG_in").select(
	col('GL_PROJECT_GID_in').alias('GL_PROJECT_GID'),
	col('GJAHR_in').alias('GJAHR'),
	col('WRTTP_in').alias('WRTTP'),
	col('KSTAR_in').alias('KSTAR'),
	col('TWAER_in').alias('TWAER'),
	col('UPDATE_DT_in').alias('UPDATE_DT'),
	col('LOAD_DT_in').alias('LOAD_DT'),
	col('LOAD_FLAG_in').alias('LOAD_FLAG'),
	expr(unpivotExpr)) \
	.withColumn('GK_PERIOD', monotonically_increasing_id()+1) \
	.withColumn('GCID_PERIOD', monotonically_increasing_id()+1) \
	.withColumn("sys_row_id", monotonically_increasing_id())
 
NRM_TRANS=NRM_TRANS.withColumn("GCID_PERIOD",F.row_number().over(Window.partitionBy(["GL_PROJECT_GID","GJAHR","WRTTP","KSTAR","TWAER"]).orderBy("GCID_PERIOD")))

# COMMAND ----------

# Processing node EXP_TRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
NRM_TRANS_temp = NRM_TRANS.toDF(*["NRM_TRANS___" + col for col in NRM_TRANS.columns])

EXP_TRANS = NRM_TRANS_temp.selectExpr(
	"NRM_TRANS___sys_row_id as sys_row_id",
	"NRM_TRANS___GL_PROJECT_GID as GL_PROJECT_GID",
	"NRM_TRANS___GJAHR as GJAHR",
	"NRM_TRANS___GCID_PERIOD as GCID_PERIOD",
	"NRM_TRANS___GJAHR||(LPAD(CAST(NRM_TRANS___GCID_PERIOD AS string) , 2 , '0')) as FISCAL_MO",
	"LTRIM('0', NRM_TRANS___KSTAR) as KSTAR",
	"NRM_TRANS___TWAER as TWAER",
	"IF (NRM_TRANS___WRTTP = '01', NRM_TRANS___PERIOD, 0) as PLAN_AMT",
	"IF (NRM_TRANS___WRTTP = '21' OR NRM_TRANS___WRTTP = '22', NRM_TRANS___PERIOD, 0) as COMM_AMT",
	"IF (NRM_TRANS___WRTTP = '04', NRM_TRANS___PERIOD, 0) as ACTUAL_AMT",
	"NRM_TRANS___LOAD_FLAG as LOAD_FLAG",
	"TO_DATE (NRM_TRANS___UPDATE_DT ) as UPDATE_DT",
	"TO_DATE (NRM_TRANS___LOAD_DT ) as LOAD_DT"
)

# COMMAND ----------

# Processing node AGGTRANS, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

AGGTRANS = EXP_TRANS.selectExpr(
	"GL_PROJECT_GID as GL_PROJECT_GID",
	"FISCAL_MO as FISCAL_MO",
	"KSTAR as KSTAR",
	"TWAER as TWAER",
	"PLAN_AMT as in_PLAN_AMT",
	"COMM_AMT as in_COMM_AMT",
	"ACTUAL_AMT as in_ACTUAL_AMT",
	"UPDATE_DT as in_UPDATE_DT",
	"LOAD_DT as in_LOAD_DT",
	"LOAD_FLAG as in_LOAD_FLAG") \
	.groupBy("GL_PROJECT_GID","FISCAL_MO","KSTAR","TWAER") \
	.agg( 
        sum(col('in_PLAN_AMT')).alias("PLAN_AMT"),
        sum(col('in_COMM_AMT')).alias("COMM_AMT"),
        sum(col('in_ACTUAL_AMT')).alias("ACTUAL_AMT"),
        max(col('in_UPDATE_DT')).alias("UPDATE_DT"),
        max(col('in_LOAD_DT')).alias("LOAD_DT"),
        max(col('in_LOAD_FLAG')).alias("LOAD_FLAG")
	).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node UPD_TRANS, type UPDATE_STRATEGY 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
AGGTRANS_temp = AGGTRANS.toDF(*["AGGTRANS___" + col for col in AGGTRANS.columns])

UPD_TRANS = AGGTRANS_temp.selectExpr(
	"AGGTRANS___GL_PROJECT_GID as GL_PROJECT_GID",
	"AGGTRANS___FISCAL_MO as FISCAL_MO",
	"AGGTRANS___KSTAR as KSTAR",
	"AGGTRANS___TWAER as TWAER",
	"AGGTRANS___PLAN_AMT as PLAN_AMT",
	"AGGTRANS___COMM_AMT as COMM_AMT",
	"AGGTRANS___ACTUAL_AMT as ACTUAL_AMT",
	"AGGTRANS___UPDATE_DT as UPDATE_DT",
	"AGGTRANS___LOAD_DT as LOAD_DT",
	"AGGTRANS___LOAD_FLAG as LOAD_FLAG") \
	.withColumn('pyspark_data_action', when((col("LOAD_FLAG") == lit(1)), (lit(0))).otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_GL_PROJECT_COST_DETAIL, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_GL_PROJECT_COST_DETAIL = UPD_TRANS.selectExpr(
	"CAST(GL_PROJECT_GID AS BIGINT) as GL_PROJECT_GID",
	"CAST(FISCAL_MO AS INT) as FISCAL_MO",
	"CAST(KSTAR AS STRING) as GL_COST_ELEMENT_CD",
	"CAST(TWAER AS STRING) as CURRENCY_CD",
	"CAST(PLAN_AMT AS DECIMAL(15,2)) as GL_PROJ_PLAN_AMT",
	"CAST(COMM_AMT AS DECIMAL(15,2)) as GL_PROJ_COMMITMENT_AMT",
	"CAST(ACTUAL_AMT AS DECIMAL(15,2)) as GL_PROJ_ACTUAL_AMT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)

Shortcut_to_GL_PROJECT_COST_DETAIL.createOrReplaceTempView('source')

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

upsertQuery=f"""MERGE INTO {legacy}.GL_PROJECT_COST_DETAIL target
USING source
ON source.GL_PROJECT_GID = target.GL_PROJECT_GID AND source.FISCAL_MO = target.FISCAL_MO AND source.GL_COST_ELEMENT_CD = target.GL_COST_ELEMENT_CD AND source.CURRENCY_CD = target.CURRENCY_CD
WHEN MATCHED THEN
  UPDATE SET
    GL_PROJECT_GID=source.GL_PROJECT_GID,
  FISCAL_MO=source.FISCAL_MO,
  GL_COST_ELEMENT_CD=source.GL_COST_ELEMENT_CD,
  CURRENCY_CD=source.CURRENCY_CD,
  GL_PROJ_PLAN_AMT=source.GL_PROJ_PLAN_AMT,
  GL_PROJ_COMMITMENT_AMT=source.GL_PROJ_COMMITMENT_AMT,
  GL_PROJ_ACTUAL_AMT=source.GL_PROJ_ACTUAL_AMT,
  UPDATE_DT=source.UPDATE_DT,
  LOAD_DT=source.LOAD_DT
WHEN NOT MATCHED
  THEN INSERT (
 
   GL_PROJECT_GID,
  FISCAL_MO,
  GL_COST_ELEMENT_CD,
  CURRENCY_CD,
  GL_PROJ_PLAN_AMT,
  GL_PROJ_COMMITMENT_AMT,
  GL_PROJ_ACTUAL_AMT,
  UPDATE_DT,
  LOAD_DT
  )
  VALUES (
  source.GL_PROJECT_GID,
  source.FISCAL_MO,
  source.GL_COST_ELEMENT_CD,
  source.CURRENCY_CD,
  source.GL_PROJ_PLAN_AMT,
  source.GL_PROJ_COMMITMENT_AMT,
  source.GL_PROJ_ACTUAL_AMT,
  source.UPDATE_DT,
  source.LOAD_DT
  )"""
 
try:
    spark.sql(upsertQuery)
    logger.info(f"Merge with {legacy}.GL_PROJECT_COST_DETAIL completed]")
    logPrevRunDt("GL_PROJECT_COST_DETAIL", "GL_PROJECT_COST_DETAIL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("GL_PROJECT_COST_DETAIL", "GL_PROJECT_COST_DETAIL","Failed",str(e), f"{raw}.log_run_details", )
  raise e
		
