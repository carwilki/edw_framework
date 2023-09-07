# Databricks notebook source
#Code converted on 2023-08-09 13:02:39
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

# Processing node ASQ_Shortcut_To_GL_BSEG_PRE, type SOURCE 
# COLUMN COUNT: 13

_sql = f"""
SELECT 
    gl_new.fiscal_mo, 
    gl_new.company_cd, 
    gl_new.gl_acct_nbr, 
    gl_new.gl_category_cd, 
    COALESCE(gl_new.profit_ctr, '90000') AS GL_PROFIT_CTR_CD, 
    gl_new.doc_type_cd AS GL_DOC_TYPE_CD, 
    gl_new.vendor_id, 
    gl_new.loc_curr AS LOC_CURRENCY_ID, 
    COALESCE(pr.location_id, 90000) AS LOCATION_ID, 
    biwamt_doc_amt + COALESCE(gl_old.gl_doc_amt, 0) AS GL_DOC_AMT, 
    biwamt_loc_amt + COALESCE(gl_old.gl_loc_amt, 0) AS GL_LOC_AMT, 
    biwamt_loc2_amt + COALESCE(gl_old.gl_group_amt, 0) AS GL_GROUP_AMT, 
    CASE 
        WHEN COALESCE(gl_old.fiscal_mo, 0) = 0 THEN 0 
        ELSE 1 
    END AS gl_old_fiscal_mo 
FROM (
    SELECT fiscal_mo, 
           gl_acct_nbr, 
           COALESCE(TRIM(profit_ctr), '90000') AS profit_ctr, 
           gl_category_cd, 
           doc_type_cd, 
           COALESCE(Nvl(vendor_id, 0)) AS vendor_id, 
           COALESCE(TRIM(loc_curr)) AS loc_curr, 
           COALESCE(Nvl(company_cd, 0)) AS company_cd, 
           SUM(cr_doc_loc_amt) - SUM(dr_doc_loc_amt) AS biwamt_doc_amt, 
           SUM(cr_loc2_curr_amt) - SUM(dr_loc2_curr_amt) AS biwamt_loc2_amt, 
           SUM(cr_loc_curr_amt) - SUM(dr_loc_curr_amt) AS biwamt_loc_amt 
    FROM (
        SELECT fiscal_yr * 100 + fiscal_period AS FISCAL_MO, 
               gl_acct_nbr, 
               profit_ctr, 
               CASE 
                 WHEN SUBSTR(profit_ctr, 5, 1) = '-' THEN SUBSTR(profit_ctr, 6, 2) 
                 ELSE '00' 
               END AS GL_CATEGORY_CD, 
               doc_type_cd, 
               CASE 
                 WHEN Nvl(vendor_id, '0') = '0' THEN 0 
                 WHEN SUBSTR(vendor_id, 1, 1) = 'V' THEN 
                     CAST(SUBSTR(vendor_id, 2, 7) AS NUMERIC) + 90000 
                 WHEN SUBSTR(vendor_id, 1, 2) = 'IV' THEN 
                     CAST(SUBSTR(vendor_id, 3, 7) AS NUMERIC) + 90000
                 WHEN Nvl(vendor_id, '0') > '0' THEN CAST(vendor_id AS NUMERIC) 
                 ELSE 0 
               END AS VENDOR_ID, 
               xrate, 
               xrate_l2, 
               company_cd, 
               loc_curr, 
               CASE 
                 WHEN db_cr_ind = 'S' AND db_cr_ind IS NOT NULL THEN COALESCE(doc_curr_amt, 0) 
                 ELSE 0 
               END AS cr_doc_loc_amt, 
               CASE 
                 WHEN db_cr_ind = 'H' AND db_cr_ind IS NOT NULL THEN COALESCE(doc_curr_amt, 0) 
                 ELSE 0 
               END AS dr_doc_loc_amt, 
               CASE 
                 WHEN db_cr_ind = 'S' AND db_cr_ind IS NOT NULL THEN COALESCE(loc2_curr_amt, 0) 
                 ELSE 0 
               END AS cr_loc2_curr_amt, 
               CASE 
                 WHEN db_cr_ind = 'H' AND db_cr_ind IS NOT NULL THEN COALESCE(loc2_curr_amt, 0) 
                 ELSE 0 
               END AS dr_loc2_curr_amt, 
               CASE 
                 WHEN db_cr_ind = 'S' AND db_cr_ind IS NOT NULL THEN COALESCE(loc_curr_amt, 0) 
                 ELSE 0 
               END AS cr_loc_curr_amt, 
               CASE 
                 WHEN db_cr_ind = 'H' AND db_cr_ind IS NOT NULL THEN COALESCE(loc_curr_amt, 0) 
                 ELSE 0 
               END AS dr_loc_curr_amt 
        FROM {raw}.gl_bseg_pre
    ) ALIAS_NZZ 
    GROUP BY fiscal_mo, gl_acct_nbr, profit_ctr, gl_category_cd, doc_type_cd, vendor_id, company_cd, loc_curr
) gl_new 
LEFT OUTER JOIN {legacy}.gl_actual_month gl_old 
    ON gl_new.fiscal_mo = gl_old.fiscal_mo 
       AND gl_new.company_cd = gl_old.company_cd 
       AND gl_new.gl_acct_nbr = gl_old.gl_acct_nbr 
       AND gl_new.gl_category_cd = gl_old.gl_category_cd 
       AND gl_new.profit_ctr = gl_old.gl_profit_ctr_cd 
       AND gl_new.doc_type_cd = gl_old.gl_doc_type_cd 
       AND gl_new.vendor_id = gl_old.vendor_id 
       AND gl_new.loc_curr = gl_old.loc_currency_id 
LEFT OUTER JOIN {legacy}.gl_profit_center pr 
    ON gl_new.profit_ctr = pr.gl_profit_ctr_cd
"""

ASQ_Shortcut_To_GL_BSEG_PRE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_To_GL_BSEG_PRE = ASQ_Shortcut_To_GL_BSEG_PRE \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[0],'FISCAL_MO') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[1],'company_cd') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[2],'GL_ACCT_NBR') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[3],'GL_CATEGORY_CD') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[4],'PROFIT_CTR') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[5],'DOC_TYPE') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[6],'VENDOR_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[7],'CURR_KEY') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[8],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[9],'GL_AMT_DOC') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[10],'GL_AMT_LOC') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[11],'GL_AMT_GRP') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_BSEG_PRE.columns[12],'GL_OLD_FISCAL_MO')

# COMMAND ----------

# Processing node UPDTRANS, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
ASQ_Shortcut_To_GL_BSEG_PRE_temp = ASQ_Shortcut_To_GL_BSEG_PRE.toDF(*["ASQ_Shortcut_To_GL_BSEG_PRE___" + col for col in ASQ_Shortcut_To_GL_BSEG_PRE.columns])

# COMMAND ----------



UPDTRANS = ASQ_Shortcut_To_GL_BSEG_PRE_temp.selectExpr(
	"ASQ_Shortcut_To_GL_BSEG_PRE___FISCAL_MO as FISCAL_MO",
	"ASQ_Shortcut_To_GL_BSEG_PRE___company_cd as COMPANY_CD",
	"ASQ_Shortcut_To_GL_BSEG_PRE___GL_ACCT_NBR as GL_ACCT_NBR",
	"ASQ_Shortcut_To_GL_BSEG_PRE___GL_CATEGORY_CD as GL_CATEGORY_CD",
	"ASQ_Shortcut_To_GL_BSEG_PRE___PROFIT_CTR as PROFIT_CTR",
	"ASQ_Shortcut_To_GL_BSEG_PRE___DOC_TYPE as DOC_TYPE",
	"ASQ_Shortcut_To_GL_BSEG_PRE___VENDOR_ID as VENDOR_ID",
	"ASQ_Shortcut_To_GL_BSEG_PRE___LOCATION_ID as LOCATION_ID",
	"ASQ_Shortcut_To_GL_BSEG_PRE___CURR_KEY as CURR_KEY",
	"ASQ_Shortcut_To_GL_BSEG_PRE___GL_AMT_DOC as GL_AMT_DOC",
	"ASQ_Shortcut_To_GL_BSEG_PRE___GL_AMT_LOC as GL_AMT_LOC",
	"ASQ_Shortcut_To_GL_BSEG_PRE___GL_AMT_GRP as GL_AMT_GRP",
	"ASQ_Shortcut_To_GL_BSEG_PRE___GL_OLD_FISCAL_MO as GL_OLD_FISCAL_MO").withColumn('pyspark_data_action', when((col("GL_OLD_FISCAL_MO") == lit(0)), (lit(0))).otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_To_GL_ACTUAL_MONTH, type TARGET 
# COLUMN COUNT: 12


Shortcut_To_GL_ACTUAL_MONTH = UPDTRANS.selectExpr(
	"CAST(FISCAL_MO AS BIGINT) as FISCAL_MO",
	"CAST(COMPANY_CD AS SMALLINT) as COMPANY_CD",
	"CAST(GL_ACCT_NBR AS BIGINT) as GL_ACCT_NBR",
	"CAST(GL_CATEGORY_CD AS STRING) as GL_CATEGORY_CD",
	"CAST(PROFIT_CTR AS STRING) as GL_PROFIT_CTR_CD",
	"CAST(DOC_TYPE AS STRING) as GL_DOC_TYPE_CD",
	"CAST(VENDOR_ID AS BIGINT) as VENDOR_ID",
	"CAST(CURR_KEY AS STRING) as LOC_CURRENCY_ID",
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(GL_AMT_DOC AS DECIMAL(15,2)) as GL_DOC_AMT",
	"CAST(GL_AMT_LOC AS DECIMAL(15,2)) as GL_LOC_AMT",
	"CAST(GL_AMT_GRP AS DECIMAL(15,2)) as GL_GROUP_AMT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.FISCAL_MO = target.FISCAL_MO AND source.COMPANY_CD = target.COMPANY_CD AND source.GL_ACCT_NBR = target.GL_ACCT_NBR AND source.GL_CATEGORY_CD = target.GL_CATEGORY_CD AND source.GL_PROFIT_CTR_CD = target.GL_PROFIT_CTR_CD AND source.GL_DOC_TYPE_CD = target.GL_DOC_TYPE_CD AND source.VENDOR_ID = target.VENDOR_ID AND source.LOC_CURRENCY_ID = target.LOC_CURRENCY_ID"""
	refined_perf_table = f"{legacy}.GL_ACTUAL_MONTH"
	executeMerge(Shortcut_To_GL_ACTUAL_MONTH, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("GL_ACTUAL_MONTH", "GL_ACTUAL_MONTH", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("GL_ACTUAL_MONTH", "GL_ACTUAL_MONTH","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		

# COMMAND ----------


