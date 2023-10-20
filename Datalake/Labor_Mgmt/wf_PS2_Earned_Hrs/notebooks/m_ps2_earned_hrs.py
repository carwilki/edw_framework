# Databricks notebook source
# Code converted on 2023-09-13 14:38:35
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

# Processing node SQ_Shortcut_to_PS2_EARNED_HRS, type SOURCE 
# COLUMN COUNT: 13

SQ_Shortcut_to_PS2_EARNED_HRS = spark.sql(f"""SELECT pre.day_dt

      ,pre.location_id

      ,pre.wfa_busn_area_id

      ,pre.wfa_dept_id

      ,pre.wfa_task_id

      ,pre.store_nbr

      ,pre.wfa_busn_area_desc

      ,pre.wfa_dept_desc

      ,pre.wfa_task_desc

      ,pre.earned_hrs

      ,CASE WHEN peh.day_dt IS NULL

            THEN 'I'

            ELSE 'U'

       END load_flag

      ,CURRENT_DATE AS update_dt

	  ,NVL(peh.load_dt,CURRENT_DATE) as load_dt

  FROM (SELECT ehp.day_dt

              ,sp.location_id

              ,wba.wfa_busn_area_id

              ,wd.wfa_dept_id

              ,wt.wfa_task_id

              ,sp.store_nbr

              ,wba.wfa_busn_area_desc

              ,wd.wfa_dept_desc

              ,wt.wfa_task_desc

              ,ehp.earned_hrs

          FROM {raw}.ps2_earned_hrs_pre ehp

              ,{legacy}.wfa_org wo

              ,{legacy}.wfa_business_area wba

              ,{legacy}.wfa_department wd

              ,{legacy}.wfa_task wt

              ,{legacy}.site_profile sp

         WHERE ehp.org_ids_id = wo.org_ids_id

           AND wo.org_lvl06_nam = sp.store_nbr

           AND wo.org_lvl07_nam = wba.wfa_busn_area_desc

           AND wo.org_lvl08_nam = wd.wfa_dept_desc

           AND wo.org_lvl09_nam = wt.wfa_task_desc

           AND ehp.day_dt BETWEEN wo.org_eff_dat AND date_add(org_exp_dat, - 1)

           AND to_date(wo.rec_exp_dtm) = '3000-01-01'

           AND wo.org_lvl_nbr > 6

       ) pre

       LEFT OUTER JOIN {legacy}.ps2_earned_hrs peh

       ON (    pre.day_dt = peh.day_dt

           AND pre.location_id = peh.location_id

           AND pre.wfa_busn_area_id = peh.wfa_busn_area_id

           AND pre.wfa_dept_id = peh.wfa_dept_id

           AND pre.wfa_task_id = peh.wfa_task_id

          )""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_PS2_EARNED_HRS = SQ_Shortcut_to_PS2_EARNED_HRS \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[2],'WFA_BUSN_AREA_ID') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[3],'WFA_DEPT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[4],'WFA_TASK_ID') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[5],'STORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[6],'WFA_BUSN_AREA_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[7],'WFA_DEPT_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[8],'WFA_TASK_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[9],'EARNED_HRS') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[10],'LOAD_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[11],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_EARNED_HRS.columns[12],'LOAD_DT')

# COMMAND ----------

# Processing node UPD_ins_upd, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_PS2_EARNED_HRS_temp = SQ_Shortcut_to_PS2_EARNED_HRS.toDF(*["SQ_Shortcut_to_PS2_EARNED_HRS___" + col for col in SQ_Shortcut_to_PS2_EARNED_HRS.columns])

UPD_ins_upd = SQ_Shortcut_to_PS2_EARNED_HRS_temp.selectExpr(
	"SQ_Shortcut_to_PS2_EARNED_HRS___DAY_DT as DAY_DT1",
	"SQ_Shortcut_to_PS2_EARNED_HRS___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_to_PS2_EARNED_HRS___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID1",
	"SQ_Shortcut_to_PS2_EARNED_HRS___WFA_DEPT_ID as WFA_DEPT_ID1",
	"SQ_Shortcut_to_PS2_EARNED_HRS___WFA_TASK_ID as WFA_TASK_ID1",
	"SQ_Shortcut_to_PS2_EARNED_HRS___STORE_NBR as STORE_NBR1",
	"SQ_Shortcut_to_PS2_EARNED_HRS___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC1",
	"SQ_Shortcut_to_PS2_EARNED_HRS___WFA_DEPT_DESC as WFA_DEPT_DESC1",
	"SQ_Shortcut_to_PS2_EARNED_HRS___WFA_TASK_DESC as WFA_TASK_DESC1",
	"SQ_Shortcut_to_PS2_EARNED_HRS___EARNED_HRS as EARNED_HRS1",
	"SQ_Shortcut_to_PS2_EARNED_HRS___LOAD_FLAG as LOAD_FLAG",
	"SQ_Shortcut_to_PS2_EARNED_HRS___UPDATE_DT as UPDATE_DT",
	"SQ_Shortcut_to_PS2_EARNED_HRS___LOAD_DT as LOAD_DT1",
	"if(SQ_Shortcut_to_PS2_EARNED_HRS___LOAD_FLAG == 'I', 0, 1) as pyspark_data_action")
 

# COMMAND ----------

# Processing node Shortcut_to_PS2_EARNED_HRS_ins_upd, type TARGET 
# COLUMN COUNT: 12


Shortcut_to_PS2_EARNED_HRS_ins_upd = UPD_ins_upd.selectExpr(
	"CAST(DAY_DT1 AS TIMESTAMP) as DAY_DT",
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"WFA_BUSN_AREA_ID1 as WFA_BUSN_AREA_ID",
	"WFA_DEPT_ID1 as WFA_DEPT_ID",
	"WFA_TASK_ID1 as WFA_TASK_ID",
	"CAST(STORE_NBR1 AS BIGINT) as STORE_NBR",
	"CAST(WFA_BUSN_AREA_DESC1 AS STRING) as WFA_BUSN_AREA_DESC",
	"CAST(WFA_DEPT_DESC1 AS STRING) as WFA_DEPT_DESC",
	"CAST(WFA_TASK_DESC1 AS STRING) as WFA_TASK_DESC",
	"CAST(EARNED_HRS1 AS DECIMAL(16,6)) as EARNED_HRS",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT1 AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.DAY_DT = target.DAY_DT AND source.LOCATION_ID = target.LOCATION_ID AND source.WFA_BUSN_AREA_ID = target.WFA_BUSN_AREA_ID AND source.WFA_DEPT_ID = target.WFA_DEPT_ID AND source.WFA_TASK_ID = target.WFA_TASK_ID"""
	refined_perf_table = f"{legacy}.PS2_EARNED_HRS"
	executeMerge(Shortcut_to_PS2_EARNED_HRS_ins_upd, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("PS2_EARNED_HRS", "PS2_EARNED_HRS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("PS2_EARNED_HRS", "PS2_EARNED_HRS","Failed",str(e), f"{raw}.log_run_details")
	raise e
		

# COMMAND ----------


