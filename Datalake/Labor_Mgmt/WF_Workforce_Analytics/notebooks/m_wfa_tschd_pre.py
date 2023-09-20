#Code converted on 2023-08-08 15:42:15
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

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')

args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
empl_protected = getEnvPrefix(env) + 'empl_protected'

(username,password,connection_string) = or_kro_read_krap1(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_FCT_TSCHD, type SOURCE 
# COLUMN COUNT: 18

SQ_Shortcut_to_FCT_TSCHD = jdbcOracleConnection(f"""SELECT
FCT_TSCHD.TSCHD_ID,
FCT_TSCHD.TSCHD_DAT,
FCT_TSCHD.EMP_SKEY,
FCT_TSCHD.STRT_DTM,
FCT_TSCHD.END_DTM,
FCT_TSCHD.PAYCD_SKEY,
FCT_TSCHD.CORE_HRS_SWT,
FCT_TSCHD.PRI_ORG_SKEY,
FCT_TSCHD.ORG_SKEY,
FCT_TSCHD.SHIFT_ID,
FCT_TSCHD.MONEY_AMT,
FCT_TSCHD.DRTN_AMT,
FCT_TSCHD.CORE_AMT,
FCT_TSCHD.NON_CORE_AMT,
FCT_TSCHD.DRTN_HRS,
FCT_TSCHD.CORE_HRS,
FCT_TSCHD.NON_CORE_HRS,
FCT_TSCHD.UPDT_DTM
FROM ia.FCT_TSCHD
WHERE tschd_dat > CURRENT_DATE - 36""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_WFA_TSCHD_PRE, type TARGET 
# COLUMN COUNT: 18


Shortcut_to_WFA_TSCHD_PRE = SQ_Shortcut_to_FCT_TSCHD.selectExpr(
	"CAST(TSCHD_ID AS BIGINT) as TSCHD_ID",
	"CAST(TSCHD_DAT AS TIMESTAMP) as TSCHD_DAT",
	"CAST(EMP_SKEY AS BIGINT) as EMP_SKEY",
	"CAST(STRT_DTM AS TIMESTAMP) as STRT_DTM",
	"CAST(END_DTM AS TIMESTAMP) as END_DTM",
	"CAST(PAYCD_SKEY AS BIGINT) as PAYCD_SKEY",
	"CAST(CORE_HRS_SWT AS INT) as CORE_HRS_SWT",
	"CAST(PRI_ORG_SKEY AS BIGINT) as PRI_ORG_SKEY",
	"CAST(ORG_SKEY AS BIGINT) as ORG_SKEY",
	"CAST(SHIFT_ID AS BIGINT) as SHIFT_ID",
	"CAST(MONEY_AMT AS DECIMAL(16,6)) as MONEY_AMT",
	"CAST(DRTN_AMT AS DECIMAL(16,6)) as DRTN_AMT",
	"CAST(CORE_AMT AS DECIMAL(16,6)) as CORE_AMT",
	"CAST(NON_CORE_AMT AS DECIMAL(16,6)) as NON_CORE_AMT",
	"CAST(DRTN_HRS AS DECIMAL(16,6)) as DRTN_HRS",
	"CAST(CORE_HRS AS DECIMAL(16,6)) as CORE_HRS",
	"CAST(NON_CORE_HRS AS DECIMAL(16,6)) as NON_CORE_HRS",
	"CAST(UPDT_DTM AS TIMESTAMP) as UPDT_DTM"
)
# overwriteDeltaPartition(Shortcut_to_WFA_TSCHD_PRE,'DC_NBR',dcnbr,f'{raw}.WFA_TSCHD_PRE')
Shortcut_to_WFA_TSCHD_PRE.write.mode("overwrite").saveAsTable(f'{empl_protected}.raw_WFA_TSCHD_PRE')