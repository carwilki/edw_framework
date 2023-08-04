#Code converted on 2023-07-13 13:11:41
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

#env = 'qa'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
enterprise = getEnvPrefix(env) + 'enterprise'
cust_sensitive = getCustSensitivePrefix(env) + 'cust_sensitive'

schemaName = refine
tableName = 'PETSHOTEL_ACCRUAL'
target_table_name = schemaName + "." + tableName

# Set parameter lookup values
parameter_filename = 'Merch_Parameter.prm'
parameter_section = 'BA_TouchPoint.WF:wf_Petshotel_Accrual.ST:s_petshotel_accrual_SQL'
parameter_key = 'p_accrual_sql'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Variable_declaration_comment
p_accrual_sql = getParameterValue(raw, 'Merch_Parameter.prm', 'BA_TouchPoint.WF:wf_Petshotel_Accrual.ST:s_petshotel_accrual_SQL', 'p_accrual_sql' )

if p_accrual_sql is None:
    raise ValueError('p_accrual_sql parameter is not set')

# SQL query:

_trunc_sql = f"""TRUNCATE TABLE {refine}.PETSHOTEL_ACCRUAL;"""

truncdf = spark.sql(_trunc_sql)

_sql = f"""
INSERT INTO {refine}.PETSHOTEL_ACCRUAL
SELECT
    b.day_dt,
    b.day_dt AS accrual,
    a.location_id,
    e.store_nbr,
    a.tp_invoice_nbr,
    a.service_start_dt,
    a.service_end_dt,
    a.length_of_stay,
    SUM(c.tp_item_price * c.tp_item_qty) AS tp_extended_price,
    COUNT(DISTINCT a.tp_pet_nbr) AS petcount,
    SUM(c.tp_item_price * c.tp_item_qty) / (SUM(a.length_of_stay) / COUNT(*)) AS accrual_amt,
    f.exch_rate_pct,
    b.week_dt,
    b.fiscal_yr,
    b.fiscal_mo,
    b.fiscal_wk,
    CURRENT_DATE AS load_dt
FROM
    {refine}.tp_invoice_service c,
    {refine}.tp_invoice a,
    {enterprise}.days b,
    {legacy}.sku_profile d,
    {legacy}.site_profile e,
    {raw}.petshotel_exch_rate_pre f
WHERE
    c.tp_invoice_nbr = a.tp_invoice_nbr
    AND (
        (b.day_dt BETWEEN a.service_start_dt AND a.service_end_dt - INTERVAL 1 DAY)
        OR (a.service_start_dt = a.service_end_dt AND b.day_dt = a.service_start_dt)
    )
    AND c.product_id = d.product_id
    AND a.location_id = e.location_id
    AND trim(e.country_cd) = trim(f.country_cd)
    --AND b.day_dt = f.day_dt
    AND CAST(b.day_dt AS DATE) = CAST(f.day_dt AS DATE)
    AND a.tp_day_dt BETWEEN TO_DATE('01-01-2006', 'mm-dd-yyyy') AND CURRENT_DATE
    AND a.tp_invoice_nbr NOT IN (
        SELECT tp_invoice_nbr FROM {cust_sensitive}.refine_tp_event WHERE void_txn_flag = 1
    )
    AND {p_accrual_sql}
    AND c.invoice_state_id NOT IN (100, 4)
    AND a.tp_pet_nbr > 0
    AND a.tp_extended_price > 0
    AND a.cancel_flag <> 'Y'
GROUP BY
    b.day_dt,
    a.location_id,
    e.store_nbr,
    a.tp_invoice_nbr,
    a.service_start_dt,
    a.service_end_dt,
    a.length_of_stay,
    f.exch_rate_pct,
    b.fiscal_yr,
    b.fiscal_mo,
    b.fiscal_wk,
    b.week_dt
ORDER BY
    day_dt
"""

print(_sql)
# df = spark.sql(_sql)

# _rows_affected = df.collect()[0][0]

try:
  df = spark.sql(_sql)
  _rows_affected = df.collect()[0][0]
  logPrevRunDt("PETSHOTEL_ACCRUAL", "PETSHOTEL_ACCRUAL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("PETSHOTEL_ACCRUAL", "PETSHOTEL_ACCRUAL","Failed",str(e), f"{raw}.log_run_details", )
  raise e
