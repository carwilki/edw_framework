# Databricks notebook source
#Code converted on 2023-08-09 13:02:38
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

# Processing node SQ_Shortcut_To_GL_BSEG_PRE, type SOURCE 
# COLUMN COUNT: 22

_sql = f"""
SELECT 
    B.posting_dt AS DAY_DT, 
    COALESCE(B.company_cd, 0), 
    B.gl_acct_nbr, 
    CASE 
        WHEN SUBSTR(B.profit_ctr, 5, 1) = '-' THEN SUBSTR(B.profit_ctr, 6, 2) 
        ELSE '00' 
    END AS GL_CATEGORY_CD, 
    COALESCE(TRIM(B.profit_ctr), '90000') AS PROFIT_CTR, 
    B.doc_type_cd, 
    B.acct_doc_nbr, 
    B.line_nbr, 
    CASE 
        WHEN COALESCE(B.vendor_id, '0') = '0' THEN 0 
        WHEN SUBSTR(B.vendor_id, 1, 1) = 'V' THEN 
            CAST(SUBSTR(B.vendor_id, 2, 7) AS NUMERIC) + 90000 
        WHEN SUBSTR(B.vendor_id, 1, 2) = 'IV' THEN 
            CAST(SUBSTR(B.vendor_id, 3, 7) AS NUMERIC) + 90000
        WHEN COALESCE(B.vendor_id, '0') > '0' THEN 
            CAST(B.vendor_id AS NUMERIC) 
        ELSE 0 
    END AS VENDOR_ID, 
    B.fiscal_yr * 100 + B.fiscal_period AS FISCAL_MO, 
    COALESCE(P.location_id, 90000) AS LOCATION_ID, 
    trim(B.loc_curr) as loc_curr, 
    B.doc_dt, 
    B.purch_doc_nbr, 
    CASE 
        WHEN db_cr_ind = 'S' AND db_cr_ind IS NOT NULL THEN COALESCE(doc_curr_amt, 0) 
        WHEN db_cr_ind = 'H' AND db_cr_ind IS NOT NULL THEN COALESCE(doc_curr_amt, 0) * -1 
        ELSE 0 
    END AS GL_DOC_AMT, 
    CASE 
        WHEN db_cr_ind = 'S' AND db_cr_ind IS NOT NULL THEN COALESCE(loc_curr_amt, 0) 
        WHEN db_cr_ind = 'H' AND db_cr_ind IS NOT NULL THEN COALESCE(loc_curr_amt, 0) * -1 
        ELSE 0 
    END AS GL_LOC_AMT, 
    CASE 
        WHEN db_cr_ind = 'S' AND db_cr_ind IS NOT NULL THEN COALESCE(loc2_curr_amt, 0) 
        WHEN db_cr_ind = 'H' AND db_cr_ind IS NOT NULL THEN COALESCE(loc2_curr_amt, 0) * -1 
        ELSE 0 
    END AS GL_GROUP_AMT, 
    CASE 
        WHEN TRIM(B.bal_sheet_ind) = '' THEN 0 
        ELSE 1 
    END AS BAL_SHEET_FLAG, 
    CASE 
        WHEN TRIM(B.p_l_ind) = '' THEN 0 
        ELSE 1 
    END AS P_L_FLAG, 
    TRIM(B.control_area), 
    CASE 
        WHEN TRIM(B.loc_curr) = 'CAD' THEN CD.exchange_rate_pcnt 
        ELSE 1 
    END AS EXCH_RATE_PCT, 
    DATE_TRUNC('DAY', CURRENT_DATE) AS LOAD_DT 
FROM   
    {raw}.gl_bseg_pre B 
    LEFT OUTER JOIN {legacy}.gl_profit_center P 
        ON TRIM(B.profit_ctr) = P.gl_profit_ctr_cd
    CROSS JOIN {legacy}.currency_day CD 
WHERE  
    B.posting_dt = CD.day_dt
"""

SQ_Shortcut_To_GL_BSEG_PRE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_GL_BSEG_PRE = SQ_Shortcut_To_GL_BSEG_PRE \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[0],'POSTING_DT') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[1],'COMPANY_CD') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[2],'GL_ACCT_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[3],'GL_CATEGORY_CD') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[4],'PROFIT_CTR') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[5],'DOC_TYPE_CD') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[6],'ACCT_DOC_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[7],'LINE_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[8],'VENDOR_ID') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[9],'FISCAL_YR') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[10],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[11],'LOC_CURR') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[12],'DOC_DT') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[13],'PURCH_DOC_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[14],'DOC_CURR_AMT') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[15],'LOC_CURR_AMT') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[16],'LOC2_CURR_AMT') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[17],'BAL_SHEET_IND') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[18],'P_L_IND') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[19],'CONTROL_AREA') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[20],'EXCHANGE_RATE_PCNT') \
	.withColumnRenamed(SQ_Shortcut_To_GL_BSEG_PRE.columns[21],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_to_GL_ACTUAL_DAY_DETAIL, type TARGET 
# COLUMN COUNT: 22


Shortcut_to_GL_ACTUAL_DAY_DETAIL = SQ_Shortcut_To_GL_BSEG_PRE.selectExpr(
	"CAST(POSTING_DT AS TIMESTAMP) as DAY_DT",
	"CAST(COMPANY_CD AS SMALLINT) as COMPANY_CD",
	"CAST(GL_ACCT_NBR AS INT) as GL_ACCT_NBR",
	"CAST(GL_CATEGORY_CD AS STRING) as GL_CATEGORY_CD",
	"CAST(PROFIT_CTR AS STRING) as GL_PROFIT_CTR_CD",
	"CAST(DOC_TYPE_CD AS STRING) as GL_DOC_TYPE_CD",
	"CAST(ACCT_DOC_NBR AS BIGINT) as GL_ACCT_DOC_NBR",
	"CAST(LINE_NBR AS SMALLINT) as GL_ACCT_LINE_NBR",
	"CAST(VENDOR_ID AS BIGINT) as VENDOR_ID",
	"CAST(FISCAL_YR AS INT) as FISCAL_MO",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(LOC_CURR AS STRING) as LOC_CURRENCY_ID",
	"CAST(DOC_DT AS TIMESTAMP) as GL_DOC_DT",
	"CAST(PURCH_DOC_NBR AS STRING) as GL_PURCH_DOC_NBR",
	"CAST(DOC_CURR_AMT AS DECIMAL(15,2)) as GL_DOC_AMT",
	"CAST(LOC_CURR_AMT AS DECIMAL(15,2)) as GL_LOC_AMT",
	"CAST(LOC2_CURR_AMT AS DECIMAL(15,2)) as GL_GROUP_AMT",
	"CAST(BAL_SHEET_IND AS TINYINT) as BAL_SHEET_FLAG",
	"CAST(P_L_IND AS TINYINT) as P_L_FLAG",
	"CAST(CONTROL_AREA AS STRING) as CONTROL_AREA",
	"CAST(EXCHANGE_RATE_PCNT AS DECIMAL(9,4)) as EXCH_RATE_PCT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
Shortcut_to_GL_ACTUAL_DAY_DETAIL.write.mode('append').saveAsTable(f'{legacy}.GL_ACTUAL_DAY_DETAIL')
