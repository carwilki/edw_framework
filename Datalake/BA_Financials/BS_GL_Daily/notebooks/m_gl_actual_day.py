# Databricks notebook source
#Code converted on 2023-08-09 13:02:36
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

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")




if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node SQ_Shortcut_to_GL_ACTUAL_DAY, type SOURCE 
# COLUMN COUNT: 17

_sql = f"""
SELECT 
    T.day_dt, 
    T.company_cd, 
    T.gl_acct_nbr, 
    T.gl_category_cd, 
    T.profit_ctr_cd, 
    T.doc_type_cd, 
    T.vendor_id, 
    T.loc_curr, 
    T.fiscal_mo, 
    T.location_id, 
    T.gl_doc_amt + COALESCE(D.gl_doc_amt, 0) AS GL_DOC_AMT, 
    T.gl_loc_amt + COALESCE(D.gl_loc_amt, 0) AS GL_LOC_AMT, 
    T.gl_group_amt + COALESCE(D.gl_group_amt, 0) AS GL_GROUP_AMT, 
    T.exch_rate_pct, 
    CASE 
        WHEN D.day_dt IS NULL THEN 0 
        ELSE 1 
    END AS UPDATE_FLAG, 
    CURRENT_DATE AS UPDATE_DT, 
    COALESCE(D.load_dt, CURRENT_DATE) AS LOAD_DT 
FROM (
    SELECT 
        day_dt, 
        company_cd, 
        gl_acct_nbr, 
        gl_category_cd, 
        profit_ctr_cd, 
        doc_type_cd, 
        vendor_id, 
        loc_curr, 
        MAX(fiscal_mo) AS FISCAL_MO, 
        MAX(location_id) AS LOCATION_ID, 
        SUM(cr_doc_loc_amt) - SUM(dr_doc_loc_amt) AS GL_DOC_AMT, 
        SUM(cr_loc_curr_amt) - SUM(dr_loc_curr_amt) AS GL_LOC_AMT, 
        SUM(cr_loc2_curr_amt) - SUM(dr_loc2_curr_amt) AS GL_GROUP_AMT, 
        MAX(exch_rate_pct) AS EXCH_RATE_PCT 
    FROM (
        SELECT 
            B.posting_dt AS DAY_DT, 
            COALESCE(company_cd, 0) AS COMPANY_CD, 
            gl_acct_nbr, 
            CASE 
                WHEN SUBSTR(profit_ctr, 5, 1) = '-' THEN 
                    SUBSTR(profit_ctr, 6, 2) 
                ELSE '00' 
            END AS GL_CATEGORY_CD, 
            COALESCE(TRIM(B.profit_ctr), '90000') AS PROFIT_CTR_CD, 
            doc_type_cd, 
            CASE 
                WHEN COALESCE(vendor_id, '0') = '0' THEN 0 
                WHEN SUBSTR(vendor_id, 1, 1) = 'V' THEN 
                    CAST(SUBSTR(vendor_id, 2, 7) AS NUMERIC) + 90000 
                WHEN SUBSTR(vendor_id, 1, 2) = 'IV' THEN 
                    CAST(SUBSTR(vendor_id, 3, 7) AS NUMERIC) + 90000
                WHEN COALESCE(vendor_id, '0') > '0' THEN 
                    CAST(vendor_id AS NUMERIC) 
                ELSE 0 
            END AS VENDOR_ID, 
            TRIM(loc_curr) AS LOC_CURR, 
            fiscal_yr * 100 + fiscal_period AS FISCAL_MO, 
            COALESCE(P.location_id, 90000) AS LOCATION_ID, 
            CASE 
                WHEN db_cr_ind = 'S' AND db_cr_ind IS NOT NULL THEN 
                    COALESCE(doc_curr_amt, 0) 
                ELSE 0 
            END AS CR_DOC_LOC_AMT, 
            CASE 
                WHEN db_cr_ind = 'H' AND db_cr_ind IS NOT NULL THEN 
                    COALESCE(doc_curr_amt, 0) 
                ELSE 0 
            END AS DR_DOC_LOC_AMT, 
            CASE 
                WHEN db_cr_ind = 'S' AND db_cr_ind IS NOT NULL THEN 
                    COALESCE(loc2_curr_amt, 0) 
                ELSE 0 
            END AS CR_LOC2_CURR_AMT, 
            CASE 
                WHEN db_cr_ind = 'H' AND db_cr_ind IS NOT NULL THEN 
                    COALESCE(loc2_curr_amt, 0) 
                ELSE 0 
            END AS DR_LOC2_CURR_AMT, 
            CASE 
                WHEN db_cr_ind = 'S' AND db_cr_ind IS NOT NULL THEN 
                    COALESCE(loc_curr_amt, 0) 
                ELSE 0 
            END AS CR_LOC_CURR_AMT, 
            CASE 
                WHEN db_cr_ind = 'H' AND db_cr_ind IS NOT NULL THEN 
                    COALESCE(loc_curr_amt, 0) 
                ELSE 0 
            END AS DR_LOC_CURR_AMT, 
            CASE 
                WHEN TRIM(B.loc_curr) = 'CAD' THEN 
                    CD.exchange_rate_pcnt 
                ELSE 1 
            END AS EXCH_RATE_PCT 
        FROM 
            {raw}.gl_bseg_pre B 
            LEFT OUTER JOIN {legacy}.gl_profit_center P 
                ON TRIM(B.profit_ctr) = P.gl_profit_ctr_cd
            CROSS JOIN {legacy}.currency_day CD 
        WHERE  
            B.posting_dt = CD.day_dt
    ) ALIAS_NZ 
    GROUP BY 
        day_dt, 
        company_cd, 
        gl_acct_nbr, 
        gl_category_cd, 
        profit_ctr_cd, 
        doc_type_cd, 
        vendor_id, 
        loc_curr
) T 
LEFT OUTER JOIN {legacy}.gl_actual_day D 
    ON T.day_dt = D.day_dt 
    AND T.company_cd = D.company_cd 
    AND T.gl_acct_nbr = D.gl_acct_nbr 
    AND T.gl_category_cd = D.gl_category_cd 
    AND T.profit_ctr_cd = D.gl_profit_ctr_cd 
    AND T.doc_type_cd = D.gl_doc_type_cd 
    AND T.vendor_id = D.vendor_id 
    AND T.loc_curr = D.loc_currency_id
"""

SQ_Shortcut_to_GL_ACTUAL_DAY = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GL_ACTUAL_DAY = SQ_Shortcut_to_GL_ACTUAL_DAY \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[1],'COMPANY_CD') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[2],'GL_ACCT_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[3],'GL_CATEGORY_CD') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[4],'GL_PROFIT_CTR_CD') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[5],'GL_DOC_TYPE_CD') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[6],'VENDOR_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[7],'LOC_CURRENCY_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[8],'FISCAL_MO') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[9],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[10],'GL_DOC_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[11],'GL_LOC_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[12],'GL_GROUP_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[13],'EXCH_RATE_PCT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[14],'UPDATE_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[15],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_ACTUAL_DAY.columns[16],'LOAD_DT')

# COMMAND ----------

# Processing node UPD_Ins_Upd, type UPDATE_STRATEGY 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GL_ACTUAL_DAY_temp = SQ_Shortcut_to_GL_ACTUAL_DAY.toDF(*["SQ_Shortcut_to_GL_ACTUAL_DAY___" + col for col in SQ_Shortcut_to_GL_ACTUAL_DAY.columns])

UPD_Ins_Upd = SQ_Shortcut_to_GL_ACTUAL_DAY_temp.selectExpr(
	"SQ_Shortcut_to_GL_ACTUAL_DAY___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___COMPANY_CD as COMPANY_CD",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___GL_ACCT_NBR as GL_ACCT_NBR",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___GL_CATEGORY_CD as GL_CATEGORY_CD",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___GL_PROFIT_CTR_CD as GL_PROFIT_CTR_CD",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___GL_DOC_TYPE_CD as GL_DOC_TYPE_CD",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___VENDOR_ID as VENDOR_ID",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___LOC_CURRENCY_ID as LOC_CURRENCY_ID",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___FISCAL_MO as FISCAL_MO",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___GL_DOC_AMT as GL_DOC_AMT",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___GL_LOC_AMT as GL_LOC_AMT",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___GL_GROUP_AMT as GL_GROUP_AMT",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___EXCH_RATE_PCT as EXCH_RATE_PCT",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___UPDATE_FLAG as UPDATE_FLAG",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___UPDATE_DT as UPDATE_DT",
	"SQ_Shortcut_to_GL_ACTUAL_DAY___LOAD_DT as LOAD_DT") \
	.withColumn('pyspark_data_action', when((col("UPDATE_FLAG") == lit(0)) ,(lit(0))).otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_GL_ACTUAL_DAY_ins_upd, type TARGET 
# COLUMN COUNT: 16


Shortcut_to_GL_ACTUAL_DAY_ins_upd = UPD_Ins_Upd.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(COMPANY_CD AS SMALLINT) as COMPANY_CD",
	"CAST(GL_ACCT_NBR AS BIGINT) as GL_ACCT_NBR",
	"CAST(GL_CATEGORY_CD AS STRING) as GL_CATEGORY_CD",
	"CAST(GL_PROFIT_CTR_CD AS STRING) as GL_PROFIT_CTR_CD",
	"CAST(GL_DOC_TYPE_CD AS STRING) as GL_DOC_TYPE_CD",
	"CAST(VENDOR_ID AS BIGINT) as VENDOR_ID",
	"CAST(LOC_CURRENCY_ID AS STRING) as LOC_CURRENCY_ID",
	"CAST(FISCAL_MO AS BIGINT) as FISCAL_MO",
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(GL_DOC_AMT AS DECIMAL(15,2)) as GL_DOC_AMT",
	"CAST(GL_LOC_AMT AS DECIMAL(15,2)) as GL_LOC_AMT",
	"CAST(GL_GROUP_AMT AS DECIMAL(15,2)) as GL_GROUP_AMT",
	"CAST(EXCH_RATE_PCT AS DECIMAL(9,4)) as EXCH_RATE_PCT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.DAY_DT = target.DAY_DT AND source.COMPANY_CD = target.COMPANY_CD AND source.GL_ACCT_NBR = target.GL_ACCT_NBR AND source.GL_CATEGORY_CD = target.GL_CATEGORY_CD AND source.GL_PROFIT_CTR_CD = target.GL_PROFIT_CTR_CD AND source.GL_DOC_TYPE_CD = target.GL_DOC_TYPE_CD AND source.VENDOR_ID = target.VENDOR_ID AND source.LOC_CURRENCY_ID = target.LOC_CURRENCY_ID"""
	refined_perf_table = f"{legacy}.GL_ACTUAL_DAY"
	executeMerge(Shortcut_to_GL_ACTUAL_DAY_ins_upd, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("GL_ACTUAL_DAY", "GL_ACTUAL_DAY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("GL_ACTUAL_DAY", "GL_ACTUAL_DAY","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		

# COMMAND ----------


