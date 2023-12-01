# Databricks notebook source
# Code converted on 2023-10-24 09:45:12
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
enterprise = getEnvPrefix(env) + 'enterprise'
empl_protected = getEnvPrefix(env) + "empl_protected"


# COMMAND ----------

# Processing node LKP_DAYS_FOR_OPEN_DATE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_DAYS_FOR_OPEN_DATE_SRC = spark.sql(f"""SELECT
FISCAL_WK_NBR,
FISCAL_YR,
WEEK_DT,
DAY_DT
FROM {enterprise}.DAYS""")
# Conforming fields names to the component layout
LKP_DAYS_FOR_OPEN_DATE_SRC = LKP_DAYS_FOR_OPEN_DATE_SRC \
	.withColumnRenamed(LKP_DAYS_FOR_OPEN_DATE_SRC.columns[0],'FISCAL_WK_NBR')

# COMMAND ----------

# Processing node LKP_USR_PETSAFETY_TO_PLAN_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_USR_PETSAFETY_TO_PLAN_SRC = spark.sql(f"""SELECT
FISCAL_YR,
LOCATION_ID,
PS_PLAN_RATE
FROM {legacy}.USR_PETSAFETY_TO_PLAN""")
# Conforming fields names to the component layout
LKP_USR_PETSAFETY_TO_PLAN_SRC = LKP_USR_PETSAFETY_TO_PLAN_SRC \
	.withColumnRenamed(LKP_USR_PETSAFETY_TO_PLAN_SRC.columns[0],'FISCAL_YR')

# COMMAND ----------

# Processing node LKP_USR_90DAY_TO_PLAN_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_USR_90DAY_TO_PLAN_SRC = spark.sql(f"""SELECT
AVG_EMPL_CNT,
FISCAL_YR,
LOCATION_ID,
PLAN_TERM_CNT
FROM {legacy}.USR_90DAY_TO_PLAN""")
# Conforming fields names to the component layout
LKP_USR_90DAY_TO_PLAN_SRC = LKP_USR_90DAY_TO_PLAN_SRC \
	.withColumnRenamed(LKP_USR_90DAY_TO_PLAN_SRC.columns[0],'AVG_EMPL_CNT')

# COMMAND ----------

# Processing node LKP_DAYS_FOR_ADJ_OPEN_DATE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_DAYS_FOR_ADJ_OPEN_DATE_SRC = spark.sql(f"""SELECT
FISCAL_WK_NBR,
FISCAL_YR,
WEEK_DT,
DAY_DT
FROM {enterprise}.DAYS""")
# Conforming fields names to the component layout
LKP_DAYS_FOR_ADJ_OPEN_DATE_SRC = LKP_DAYS_FOR_ADJ_OPEN_DATE_SRC \
	.withColumnRenamed(LKP_DAYS_FOR_ADJ_OPEN_DATE_SRC.columns[0],'FISCAL_WK_NBR')

# COMMAND ----------

# Processing node LKP_IC_WC_PLAN_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_IC_WC_PLAN_SRC = spark.sql(f"""SELECT
FISCAL_YR,
LOCATION_ID,
PLAN_CLAIM_CNT,
PLAN_HOURS_WORKED,
LYR_CLAIM_CNT,
LYR_HOURS_WORKED,
LYR_ANIMAL_CLAIM_CNT,
LYR_SALON_CLAIM_CNT,
LYR_PETSHOTEL_CLAIM_CNT,
UPDATE_DT,
LOAD_DT
FROM {legacy}.IC_WC_PLAN""")
# Conforming fields names to the component layout
LKP_IC_WC_PLAN_SRC = LKP_IC_WC_PLAN_SRC \
	.withColumnRenamed(LKP_IC_WC_PLAN_SRC.columns[0],'FISCAL_YR')

# COMMAND ----------

# Processing node SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE = spark.sql(f"""SELECT
PLAN_ACT_TO_DATE_PRE.WEEK_DT,
PLAN_ACT_TO_DATE_PRE.FISCAL_WK,
PLAN_ACT_TO_DATE_PRE.FISCAL_WK_NBR,
PLAN_ACT_TO_DATE_PRE.FISCAL_YR,
PLAN_ACT_TO_DATE_PRE.MAX_FISCAL_WK_NBR,
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR,
SITE_PROFILE.STORE_NAME,
SITE_PROFILE.OPEN_DT
--DAYS.FISCAL_WK_NBR,
--DAYS.FISCAL_YR
--DAYS.WEEK_DT
FROM {raw}.PLAN_ACT_TO_DATE_PRE, {legacy}.SITE_PROFILE
left JOIN {enterprise}.DAYS on SITE_PROFILE.OPEN_DT = DAYS.DAY_DT
WHERE SITE_PROFILE.SITE_SALES_FLAG = 1 OR (SITE_PROFILE.REGION_ID IN (100,8000))""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node LKP_DAYS_FOR_OPEN_DATE, type LOOKUP_FROM_PRECACHED_DATASET 
# COLUMN COUNT: 48


LKP_DAYS_FOR_OPEN_DATE_lookup_result = SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE.select("open_dt","sys_row_id").join(LKP_DAYS_FOR_OPEN_DATE_SRC, (col('DAY_DT') == col('OPEN_DT')), 'left') \
.withColumn('row_num_FISCAL_WK_NBR', row_number().over(Window.partitionBy("sys_row_id").orderBy("FISCAL_WK_NBR")))

LKP_DAYS_FOR_OPEN_DATE = LKP_DAYS_FOR_OPEN_DATE_lookup_result.filter(col("row_num_FISCAL_WK_NBR") == 1).select(
	LKP_DAYS_FOR_OPEN_DATE_lookup_result.sys_row_id,
	col('FISCAL_WK_NBR'),
	col('FISCAL_YR'),
	col('WEEK_DT'),
    col("OPEN_DT")
)


# LKP_DAYS_FOR_OPEN_DATE = SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE

# COMMAND ----------

# Processing node SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED, type SOURCE 
# COLUMN COUNT: 5

# SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED = spark.sql(f"""SELECT
# EMPL_TERM_DAYS_WORKED.EMPL_TERM_WEEK_DT,
# EMPL_TERM_DAYS_WORKED.LOCATION_ID,
# EMPL_TERM_DAYS_WORKED.NBR_DAYS_WORKED,
# EMPL_TERM_DAYS_WORKED.COUNT_FLAG,
# EMPLOYEE_PROFILE_WK.TERM_REASON_CD
# FROM {legacy}.EMPL_TERM_DAYS_WORKED INNER JOIN {legacy}.EMPLOYEE_PROFILE_WK ON EMPL_TERM_DAYS_WORKED.EMPLOYEE_ID = EMPLOYEE_PROFILE_WK.EMPLOYEE_ID AND EMPL_TERM_DAYS_WORKED.EMPL_TERM_WEEK_DT = EMPLOYEE_PROFILE_WK.WEEK_DT
# AND EMPLOYEE_PROFILE_WK.JOB_CODE NOT IN (
#               --Exclude PHO
#               561
#               ,688
#               ,849
#               ,839
#               ,1680
#               --Exclude Seasonal Store
#               ,1680
#               ,2651
#               --Exclude Seasonal DC
#               ,8110
#               ,8210
#               ,8310
#               ,8410
#               ,8510
#               )

#        AND EMPLOYEE_PROFILE_WK.PS_TAX_COMPANY_CD !='LJO' -- Exclude ONP DC 52
# ORDER BY 1,2""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED = spark.sql(f"""SELECT
EMPL_TERM_DAYS_WORKED.EMPL_TERM_WEEK_DT,
EMPL_TERM_DAYS_WORKED.LOCATION_ID,
EMPL_TERM_DAYS_WORKED.NBR_DAYS_WORKED,
EMPL_TERM_DAYS_WORKED.COUNT_FLAG,
EMPLOYEE_PROFILE_WK.TERM_REASON_CD
FROM {legacy}.EMPL_TERM_DAYS_WORKED INNER JOIN {empl_protected}.legacy_EMPLOYEE_PROFILE_WK as EMPLOYEE_PROFILE_WK  ON EMPL_TERM_DAYS_WORKED.EMPLOYEE_ID = EMPLOYEE_PROFILE_WK.EMPLOYEE_ID AND EMPL_TERM_DAYS_WORKED.EMPL_TERM_WEEK_DT = EMPLOYEE_PROFILE_WK.WEEK_DT
AND EMPLOYEE_PROFILE_WK.JOB_CODE NOT IN (
              --Exclude PHO
              561
              ,688
              ,849
              ,839
              ,1680
              --Exclude Seasonal Store
              ,1680
              ,2651
              --Exclude Seasonal DC
              ,8110
              ,8210
              ,8310
              ,8410
              ,8510
              )

       AND EMPLOYEE_PROFILE_WK.PS_TAX_COMPANY_CD !='LJO' -- Exclude ONP DC 52
ORDER BY 1,2""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node LKP_USR_PETSAFETY_TO_PLAN, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

LKP_USR_PETSAFETY_TO_PLAN_lookup_result = SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE.selectExpr(
	"LOCATION_ID as i_LOCATION_ID",
	"FISCAL_YR as i_FISCAL_YR",
	"sys_row_id").join(LKP_USR_PETSAFETY_TO_PLAN_SRC, (col('LOCATION_ID') == col('i_LOCATION_ID')) & (col('FISCAL_YR') == col('i_FISCAL_YR')), 'left') \
.withColumn('row_num_FISCAL_YR', row_number().over(Window.partitionBy("sys_row_id").orderBy("i_FISCAL_YR")))

LKP_USR_PETSAFETY_TO_PLAN = LKP_USR_PETSAFETY_TO_PLAN_lookup_result.filter(col("row_num_FISCAL_YR") == 1).select(
	LKP_USR_PETSAFETY_TO_PLAN_lookup_result.sys_row_id,
	col('PS_PLAN_RATE')
)

# COMMAND ----------

# Processing node LKP_USR_90DAY_TO_PLAN, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6


LKP_USR_90DAY_TO_PLAN_lookup_result = SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE.selectExpr(
	"LOCATION_ID as i_LOCATION_ID",
	"FISCAL_YR as i_FISCAL_YR",
	"sys_row_id").join(LKP_USR_90DAY_TO_PLAN_SRC, (col('LOCATION_ID') == col('i_LOCATION_ID')) & (col('FISCAL_YR') == col('i_FISCAL_YR')), 'left') \
.withColumn('row_num_AVG_EMPL_CNT', row_number().over(Window.partitionBy("sys_row_id").orderBy("AVG_EMPL_CNT")))
LKP_USR_90DAY_TO_PLAN = LKP_USR_90DAY_TO_PLAN_lookup_result.filter(col("row_num_AVG_EMPL_CNT") == 1).select(
	LKP_USR_90DAY_TO_PLAN_lookup_result.sys_row_id,
	col('AVG_EMPL_CNT'),
	col('PLAN_TERM_CNT')
)

# COMMAND ----------

# Processing node EXP_Chg_Data_Type_and_Adj_Open_Dt, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE_temp = SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE.toDF(*["SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___" + col for col in SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE.columns])

EXP_Chg_Data_Type_and_Adj_Open_Dt = SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE_temp.selectExpr(
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___sys_row_id as sys_row_id2",
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___FISCAL_YR as FISCAL_YR_dec",
	"DATE_ADD(SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___OPEN_DT, 90) as ADJ_OPEN_DT"
)

# COMMAND ----------

# Processing node EXP_TERM_CNT, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED_temp = SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED.toDF(*["SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___" + col for col in SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED.columns])

EXP_TERM_CNT = SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED_temp.selectExpr(
	"SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___EMPL_TERM_WEEK_DT as EMPL_TERM_WEEK_DT",
	"SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___LOCATION_ID as LOCATION_ID",
	"IF (SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___NBR_DAYS_WORKED <= 90 AND ( IF (SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___TERM_REASON_CD IS NULL, '', SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___TERM_REASON_CD) <> 'UR' AND IF (SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___TERM_REASON_CD IS NULL, '', SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___TERM_REASON_CD) <> 'DEA' ), 1, 0) as TERM_CNT_90DAY",
	"IF (SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___NBR_DAYS_WORKED <= 365 AND ( IF (SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___TERM_REASON_CD IS NULL, '', SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___TERM_REASON_CD) <> 'UR' AND IF (SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___TERM_REASON_CD IS NULL, '', SQ_Shortcut_to_EMPL_TERM_DAYS_WORKED___TERM_REASON_CD) <> 'DEA' ), 1, 0) as TERM_CNT_365DAY"
)

# COMMAND ----------

# Processing node LKP_DAYS_FOR_ADJ_OPEN_DATE, type LOOKUP_FROM_PRECACHED_DATASET 
# COLUMN COUNT: 48


LKP_DAYS_FOR_ADJ_OPEN_DATE_lookup_result = EXP_Chg_Data_Type_and_Adj_Open_Dt.join(LKP_DAYS_FOR_ADJ_OPEN_DATE_SRC, (col('DAY_DT') == col('ADJ_OPEN_DT')), 'left') \
.withColumn('row_num_FISCAL_WK_NBR', row_number().over(Window.partitionBy("sys_row_id2").orderBy("FISCAL_WK_NBR")))
LKP_DAYS_FOR_ADJ_OPEN_DATE = LKP_DAYS_FOR_ADJ_OPEN_DATE_lookup_result.filter(col("row_num_FISCAL_WK_NBR") == 1).select(
	LKP_DAYS_FOR_ADJ_OPEN_DATE_lookup_result.sys_row_id2,
	col('FISCAL_WK_NBR'),
	col('FISCAL_YR'),
	col('WEEK_DT'),
	col('ADJ_OPEN_DT')
)

# COMMAND ----------

# Processing node AGG_WEEK_STORE, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

AGG_WEEK_STORE = EXP_TERM_CNT.selectExpr(
	"EMPL_TERM_WEEK_DT as EMPL_TERM_WEEK_DT",
	"LOCATION_ID as LOCATION_ID",
	"TERM_CNT_90DAY as i_TERM_CNT_90DAY",
	"TERM_CNT_365DAY as i_TERM_CNT_365DAY") \
	.groupBy("EMPL_TERM_WEEK_DT","LOCATION_ID") \
	.agg( \
	sum(col('i_TERM_CNT_90DAY')).alias("TERM_CNT_90DAY"),
	sum(col('i_TERM_CNT_365DAY')).alias("TERM_CNT_365DAY")
	) \
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node LKP_IC_WC_PLAN, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# Joining dataframes SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE, EXP_Chg_Data_Type_and_Adj_Open_Dt to form LKP_IC_WC_PLAN
LKP_IC_WC_PLAN_joined = SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE.join(EXP_Chg_Data_Type_and_Adj_Open_Dt, SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE.sys_row_id == EXP_Chg_Data_Type_and_Adj_Open_Dt.sys_row_id2, 'inner')

LKP_IC_WC_PLAN_lookup_result = LKP_IC_WC_PLAN_joined.selectExpr(
	"LOCATION_ID as i_LOCATION_ID",
	"FISCAL_YR_dec as i_FISCAL_YR_dec",
 	"sys_row_id").join(LKP_IC_WC_PLAN_SRC, (col('LOCATION_ID') == col('i_LOCATION_ID')) & (col('FISCAL_YR') == col('i_FISCAL_YR_dec')), 'left') \
.withColumn('row_num_FISCAL_YR', row_number().over(Window.partitionBy("sys_row_id").orderBy("FISCAL_YR")))
LKP_IC_WC_PLAN = LKP_IC_WC_PLAN_lookup_result.filter(col("row_num_FISCAL_YR") == 1).select(
	LKP_IC_WC_PLAN_lookup_result.sys_row_id,
	col('PLAN_CLAIM_CNT')
)

# COMMAND ----------

# Processing node EXP_Calculate, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
LKP_USR_90DAY_TO_PLAN_temp = LKP_USR_90DAY_TO_PLAN.toDF(*["LKP_USR_90DAY_TO_PLAN___" + col for col in LKP_USR_90DAY_TO_PLAN.columns])
SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE_temp = SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE.toDF(*["SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___" + col for col in SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE.columns])
LKP_USR_PETSAFETY_TO_PLAN_temp = LKP_USR_PETSAFETY_TO_PLAN.toDF(*["LKP_USR_PETSAFETY_TO_PLAN___" + col for col in LKP_USR_PETSAFETY_TO_PLAN.columns])
LKP_DAYS_FOR_OPEN_DATE_temp = LKP_DAYS_FOR_OPEN_DATE.toDF(*["LKP_DAYS_FOR_OPEN_DATE___" + col for col in LKP_DAYS_FOR_OPEN_DATE.columns])
LKP_IC_WC_PLAN_temp = LKP_IC_WC_PLAN.toDF(*["LKP_IC_WC_PLAN___" + col for col in LKP_IC_WC_PLAN.columns])
LKP_DAYS_FOR_ADJ_OPEN_DATE_temp = LKP_DAYS_FOR_ADJ_OPEN_DATE.toDF(*["LKP_DAYS_FOR_ADJ_OPEN_DATE___" + col for col in LKP_DAYS_FOR_ADJ_OPEN_DATE.columns])

# Joining dataframes SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE, LKP_DAYS_FOR_OPEN_DATE, LKP_USR_PETSAFETY_TO_PLAN, LKP_USR_90DAY_TO_PLAN, LKP_DAYS_FOR_ADJ_OPEN_DATE, LKP_IC_WC_PLAN to form EXP_Calculate
EXP_Calculate_joined = SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE_temp.join(LKP_DAYS_FOR_OPEN_DATE_temp, SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE_temp.SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___sys_row_id == LKP_DAYS_FOR_OPEN_DATE_temp.LKP_DAYS_FOR_OPEN_DATE___sys_row_id, 'inner')\
 .join(LKP_USR_PETSAFETY_TO_PLAN_temp, LKP_DAYS_FOR_OPEN_DATE_temp.LKP_DAYS_FOR_OPEN_DATE___sys_row_id == LKP_USR_PETSAFETY_TO_PLAN_temp.LKP_USR_PETSAFETY_TO_PLAN___sys_row_id, 'inner')\
  .join(LKP_USR_90DAY_TO_PLAN_temp, LKP_USR_PETSAFETY_TO_PLAN_temp.LKP_USR_PETSAFETY_TO_PLAN___sys_row_id == LKP_USR_90DAY_TO_PLAN_temp.LKP_USR_90DAY_TO_PLAN___sys_row_id, 'inner')\
   .join(LKP_DAYS_FOR_ADJ_OPEN_DATE_temp, LKP_USR_90DAY_TO_PLAN_temp.LKP_USR_90DAY_TO_PLAN___sys_row_id == LKP_DAYS_FOR_ADJ_OPEN_DATE_temp.LKP_DAYS_FOR_ADJ_OPEN_DATE___sys_row_id2, 'inner')\
    .join(LKP_IC_WC_PLAN_temp, LKP_DAYS_FOR_ADJ_OPEN_DATE_temp.LKP_DAYS_FOR_ADJ_OPEN_DATE___sys_row_id2 == LKP_IC_WC_PLAN_temp.LKP_IC_WC_PLAN___sys_row_id, 'inner')

EXP_Calculate2 = EXP_Calculate_joined.selectExpr(
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___WEEK_DT as WEEK_DT",
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___FISCAL_WK as FISCAL_WK",
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___FISCAL_WK_NBR as FISCAL_WK_NBR",
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___FISCAL_YR as FISCAL_YR",
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___MAX_FISCAL_WK_NBR as MAX_FISCAL_WK_NBR",
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___STORE_NBR as STORE_NBR",
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___STORE_NAME as STORE_NAME",
	"SQ_Shortcut_to_PLAN_ACT_TO_DATE_PRE___OPEN_DT as OPEN_DT",
	"LKP_DAYS_FOR_OPEN_DATE___FISCAL_WK_NBR as OPEN_FISCAL_WK_NBR",
	"LKP_DAYS_FOR_OPEN_DATE___FISCAL_YR as OPEN_FISCAL_YR",
	"LKP_DAYS_FOR_OPEN_DATE___WEEK_DT as OPEN_WEEK_DT",
	"LKP_USR_90DAY_TO_PLAN___PLAN_TERM_CNT as i_PLAN_TERM_CNT",
	"LKP_USR_90DAY_TO_PLAN___AVG_EMPL_CNT as i_AVG_EMPL_CNT",
	"LKP_IC_WC_PLAN___PLAN_CLAIM_CNT as i_PLAN_CLAIM_CNT",
	"LKP_USR_PETSAFETY_TO_PLAN___PS_PLAN_RATE as i_PS_PLAN_RATE",
	"LKP_DAYS_FOR_ADJ_OPEN_DATE___ADJ_OPEN_DT as ADJ_OPEN_DT",
	"LKP_DAYS_FOR_ADJ_OPEN_DATE___FISCAL_WK_NBR as ADJ_OPEN_FISCAL_WK_NBR",
	"LKP_DAYS_FOR_ADJ_OPEN_DATE___FISCAL_YR as ADJ_OPEN_FISCAL_YR",
	"LKP_DAYS_FOR_ADJ_OPEN_DATE___WEEK_DT as ADJ_OPEN_WEEK_DT")\
    .withColumn("v_PLAN_TERM_CNT", expr("""DECODE ( true , OPEN_WEEK_DT IS NOT NULL AND (  WEEK_DT < OPEN_WEEK_DT  ) , 0 , i_PLAN_TERM_CNT IS NULL , 0 , i_PLAN_TERM_CNT )""")) \
	.withColumn("v_WC_PLAN_CLAIMS", expr("""DECODE ( true , OPEN_WEEK_DT IS NOT NULL AND (  WEEK_DT < OPEN_WEEK_DT  ) , 0 , i_PLAN_CLAIM_CNT IS NULL , 0 , i_PLAN_CLAIM_CNT )""")) \
	.withColumn("v_PS_PLAN_RATE", expr("""DECODE ( true , OPEN_WEEK_DT IS NOT NULL AND (  WEEK_DT < OPEN_WEEK_DT  ) , 0 , i_PS_PLAN_RATE IS NULL , 0 , i_PS_PLAN_RATE )"""))
	
EXP_Calculate_joined_temp = EXP_Calculate2.toDF(*["EXP_Calculate_joined___" + col for col in EXP_Calculate2.columns])
 
EXP_Calculate = EXP_Calculate_joined_temp\
    .selectExpr(
	# "EXP_Calculate_joined___sys_row_id as sys_row_id",
	"EXP_Calculate_joined___WEEK_DT as WEEK_DT",
	"EXP_Calculate_joined___FISCAL_WK as FISCAL_WK",
	"EXP_Calculate_joined___FISCAL_WK_NBR as FISCAL_WK_NBR",
	"EXP_Calculate_joined___FISCAL_YR as FISCAL_YR",
	"EXP_Calculate_joined___MAX_FISCAL_WK_NBR as MAX_FISCAL_WK_NBR",
	"EXP_Calculate_joined___LOCATION_ID as LOCATION_ID",
	"EXP_Calculate_joined___STORE_NBR as STORE_NBR",
	"EXP_Calculate_joined___STORE_NAME as STORE_NAME",
	"EXP_Calculate_joined___OPEN_DT as OPEN_DT",
	"EXP_Calculate_joined___OPEN_FISCAL_WK_NBR as OPEN_FISCAL_WK_NBR",
	"EXP_Calculate_joined___OPEN_FISCAL_YR as OPEN_FISCAL_YR",
	"EXP_Calculate_joined___OPEN_WEEK_DT as OPEN_WEEK_DT",
	"EXP_Calculate_joined___v_PLAN_TERM_CNT as PLAN_TERM_CNT",
	"DECODE ( TRUE , EXP_Calculate_joined___v_PLAN_TERM_CNT = 0 , 0 , EXP_Calculate_joined___WEEK_DT < EXP_Calculate_joined___ADJ_OPEN_WEEK_DT , 0 , EXP_Calculate_joined___v_PLAN_TERM_CNT / ( IF (EXP_Calculate_joined___ADJ_OPEN_FISCAL_YR < EXP_Calculate_joined___FISCAL_YR, EXP_Calculate_joined___MAX_FISCAL_WK_NBR, EXP_Calculate_joined___MAX_FISCAL_WK_NBR - ( EXP_Calculate_joined___ADJ_OPEN_FISCAL_WK_NBR - 1 )) ) ) as DEANNL_PLAN",
	"DECODE ( TRUE , EXP_Calculate_joined___OPEN_WEEK_DT IS NOT NULL AND EXP_Calculate_joined___WEEK_DT < EXP_Calculate_joined___OPEN_WEEK_DT , 0 , EXP_Calculate_joined___i_AVG_EMPL_CNT IS NULL , 0 , EXP_Calculate_joined___i_AVG_EMPL_CNT ) as AVG_EMPL_CNT",
	"EXP_Calculate_joined___v_WC_PLAN_CLAIMS as WC_PLAN_CLAIMS",
	"DECODE ( TRUE , EXP_Calculate_joined___v_WC_PLAN_CLAIMS = 0 , 0 , EXP_Calculate_joined___v_WC_PLAN_CLAIMS / ( IF (EXP_Calculate_joined___OPEN_FISCAL_YR < EXP_Calculate_joined___FISCAL_YR, EXP_Calculate_joined___MAX_FISCAL_WK_NBR, EXP_Calculate_joined___MAX_FISCAL_WK_NBR - ( EXP_Calculate_joined___OPEN_FISCAL_WK_NBR - 1 )) ) ) as WC_DEANNL_PLAN",
	"EXP_Calculate_joined___v_PS_PLAN_RATE as PS_PLAN_RATE",
	"DECODE ( TRUE , EXP_Calculate_joined___v_PS_PLAN_RATE = 0 , 0 , EXP_Calculate_joined___v_PS_PLAN_RATE / ( IF (EXP_Calculate_joined___OPEN_FISCAL_YR < EXP_Calculate_joined___FISCAL_YR, EXP_Calculate_joined___MAX_FISCAL_WK_NBR, EXP_Calculate_joined___MAX_FISCAL_WK_NBR - ( EXP_Calculate_joined___OPEN_FISCAL_WK_NBR - 1 )) ) ) as PS_DEANNL_PLAN",
	"EXP_Calculate_joined___ADJ_OPEN_WEEK_DT as ADJ_OPEN_WEEK_DT"
)

# COMMAND ----------

# Processing node JNR_Master_Outer_Join, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
AGG_WEEK_STORE_temp = AGG_WEEK_STORE.toDF(*["AGG_WEEK_STORE___" + col for col in AGG_WEEK_STORE.columns])
EXP_Calculate_temp = EXP_Calculate.toDF(*["EXP_Calculate___" + col for col in EXP_Calculate.columns])

JNR_Master_Outer_Join = AGG_WEEK_STORE_temp.join(EXP_Calculate_temp,[AGG_WEEK_STORE_temp.AGG_WEEK_STORE___EMPL_TERM_WEEK_DT == EXP_Calculate_temp.EXP_Calculate___WEEK_DT, AGG_WEEK_STORE_temp.AGG_WEEK_STORE___LOCATION_ID == EXP_Calculate_temp.EXP_Calculate___LOCATION_ID],'right_outer').selectExpr(
	"EXP_Calculate___WEEK_DT as WEEK_DT",
	"EXP_Calculate___FISCAL_WK as FISCAL_WK",
	"EXP_Calculate___FISCAL_WK_NBR as FISCAL_WK_NBR",
	"EXP_Calculate___FISCAL_YR as FISCAL_YR",
	"EXP_Calculate___LOCATION_ID as LOCATION_ID",
	"EXP_Calculate___STORE_NBR as STORE_NBR",
	"EXP_Calculate___STORE_NAME as STORE_NAME",
	"EXP_Calculate___OPEN_DT as OPEN_DT",
	"EXP_Calculate___OPEN_FISCAL_WK_NBR as OPEN_FISCAL_WK_NBR",
	"EXP_Calculate___OPEN_FISCAL_YR as OPEN_FISCAL_YR",
	"EXP_Calculate___OPEN_WEEK_DT as OPEN_WEEK_DT",
	"EXP_Calculate___PLAN_TERM_CNT as PLAN_TERM_CNT",
	"EXP_Calculate___DEANNL_PLAN as DEANNL_PLAN",
	"EXP_Calculate___AVG_EMPL_CNT as AVG_EMPL_CNT",
	"EXP_Calculate___WC_PLAN_CLAIMS as WC_PLAN_CLAIMS",
	"EXP_Calculate___WC_DEANNL_PLAN as WC_DEANNL_PLAN",
	"EXP_Calculate___PS_PLAN_RATE as PS_PLAN_RATE",
	"EXP_Calculate___PS_DEANNL_PLAN as PS_DEANNL_PLAN",
	"EXP_Calculate___ADJ_OPEN_WEEK_DT as ADJ_OPEN_WEEK_DT",
	"AGG_WEEK_STORE___EMPL_TERM_WEEK_DT as EMPL_TERM_WEEK_DT",
	"AGG_WEEK_STORE___LOCATION_ID as LOCATION_ID1",
	"AGG_WEEK_STORE___TERM_CNT_90DAY as TERM_CNT_90DAY",
	"AGG_WEEK_STORE___TERM_CNT_365DAY as TERM_CNT_365DAY")

# COMMAND ----------

# Processing node EXP_Default, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
JNR_Master_Outer_Join_temp = JNR_Master_Outer_Join.toDF(*["JNR_Master_Outer_Join___" + col for col in JNR_Master_Outer_Join.columns])

EXP_Default = JNR_Master_Outer_Join_temp.selectExpr(
	"JNR_Master_Outer_Join___WEEK_DT as WEEK_DT",
	"JNR_Master_Outer_Join___LOCATION_ID as LOCATION_ID",
	"JNR_Master_Outer_Join___FISCAL_YR as FISCAL_YR",
	"JNR_Master_Outer_Join___FISCAL_WK_NBR as FISCAL_WK_NBR",
	"JNR_Master_Outer_Join___STORE_NBR as STORE_NBR",
	"JNR_Master_Outer_Join___STORE_NAME as STORE_NAME",
	"JNR_Master_Outer_Join___OPEN_DT as OPEN_DT",
	"JNR_Master_Outer_Join___PLAN_TERM_CNT as TO_PLAN_TERM_CNT",
	"JNR_Master_Outer_Join___AVG_EMPL_CNT as TO_AVG_EMPL_CNT",
	"JNR_Master_Outer_Join___DEANNL_PLAN as TO_DEANNL_PLAN",
	"DECODE ( TRUE , JNR_Master_Outer_Join___TERM_CNT_90DAY IS NULL , 0 , JNR_Master_Outer_Join___WEEK_DT < JNR_Master_Outer_Join___ADJ_OPEN_WEEK_DT , 0 , JNR_Master_Outer_Join___TERM_CNT_90DAY ) as TO_EMPL_90DAY_CNT",
	"DECODE ( TRUE , JNR_Master_Outer_Join___TERM_CNT_365DAY IS NULL , 0 , JNR_Master_Outer_Join___WEEK_DT < JNR_Master_Outer_Join___ADJ_OPEN_WEEK_DT , 0 , JNR_Master_Outer_Join___TERM_CNT_365DAY ) as TO_EMPL_365DAY_CNT",
	"JNR_Master_Outer_Join___WC_PLAN_CLAIMS as WC_PLAN_CLAIMS",
	"JNR_Master_Outer_Join___WC_DEANNL_PLAN as WC_DEANNL_PLAN",
	"JNR_Master_Outer_Join___PS_PLAN_RATE as PET_SAFETY_PLAN_INC_CNT",
	"JNR_Master_Outer_Join___PS_DEANNL_PLAN as PET_SAFETY_DEANNL_PLAN_CNT"
)

# COMMAND ----------

# Processing node Shortcut_to_PLAN_ACT_TURNOVER, type TARGET 
# COLUMN COUNT: 17

Shortcut_to_PLAN_ACT_TURNOVER = EXP_Default.selectExpr(
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(FISCAL_YR AS SMALLINT) as FISCAL_YR",
	"CAST(FISCAL_WK_NBR AS TINYINT) as FISCAL_WK_NBR",
	"CAST(STORE_NBR AS INT) as STORE_NBR",
	"CAST(STORE_NAME AS STRING) as STORE_NAME",
	"CAST(OPEN_DT AS TIMESTAMP) as OPEN_DT",
	"CAST(TO_PLAN_TERM_CNT AS INT) as TO_PLAN_TERM_CNT",
	"CAST(TO_AVG_EMPL_CNT AS DECIMAL(18,2)) as TO_AVG_EMPL_CNT",
	"CAST(TO_DEANNL_PLAN AS DECIMAL(9,6)) as TO_DEANNL_PLAN",
	"CAST(TO_EMPL_90DAY_CNT AS INT) as TO_EMPL_90DAY_CNT",
	"CAST(TO_EMPL_365DAY_CNT AS INT) as TO_EMPL_365DAY_CNT",
	"CAST(WC_PLAN_CLAIMS AS INT) as WC_PLAN_CLAIMS",
	"CAST(WC_DEANNL_PLAN AS DECIMAL(9,6)) as WC_DEANNL_PLAN",
	"CAST(PET_SAFETY_PLAN_INC_CNT AS INT) as PET_SAFETY_PLAN_INC_CNT",
	"CAST(PET_SAFETY_DEANNL_PLAN_CNT AS DECIMAL(9,6)) as PET_SAFETY_DEANNL_PLAN_CNT",
	"CAST(current_date AS TIMESTAMP) as LOAD_TSTMP"
)
try:
	# chk=DuplicateChecker()
	# chk.check_for_duplicate_primary_keys(spark,f'{legacy}.PLAN_ACT_TURNOVER',Shortcut_to_PLAN_ACT_TURNOVER,["KEY1","KEY1"])
	Shortcut_to_PLAN_ACT_TURNOVER.write.mode("overwrite").saveAsTable(f'{legacy}.PLAN_ACT_TURNOVER')
except Exception as e:
	raise e
