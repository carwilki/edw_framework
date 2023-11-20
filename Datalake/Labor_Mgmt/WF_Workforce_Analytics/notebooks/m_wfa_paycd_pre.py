#Code converted on 2023-08-08 15:41:59
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

(username,password,connection_string) = or_kro_read_krap1(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_DIM_PAYCD, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_DIM_PAYCD = jdbcOracleConnection(f"""SELECT
DIM_PAYCD.PAYCD_ID,
DIM_PAYCD.PAYCD_SKEY,
DIM_PAYCD.PAYCD_NAM,
DIM_PAYCD.PAYCD_TYP_COD,
DIM_PAYCD.PAYCD_WAGE_ADD_AMT,
DIM_PAYCD.PAYCD_WAGE_MLT_AMT,
DIM_PAYCD.PAYCD_MONEY_SWT,
DIM_PAYCD.PAYCD_TOT_SWT,
DIM_PAYCD.PAYCD_EXCUSED_SWT,
DIM_PAYCD.PAYCD_OT_SWT,
DIM_PAYCD.PAYCD_CONSEC_OT_SWT,
DIM_PAYCD.PAYCD_VISIBLE_SWT,
DIM_PAYCD.PAYCAT_NAM,
DIM_PAYCD.CORE_HRS_SWT,
DIM_PAYCD.UPDT_DTM
FROM ia.DIM_PAYCD""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_WFA_PAYCD_PRE, type TARGET 
# COLUMN COUNT: 15


Shortcut_to_WFA_PAYCD_PRE = SQ_Shortcut_to_DIM_PAYCD.selectExpr(
	"CAST(PAYCD_ID AS BIGINT) as PAYCD_ID",
	"CAST(PAYCD_SKEY AS BIGINT) as PAYCD_SKEY",
	"PAYCD_NAM as PAYCD_NAM",
	"PAYCD_TYP_COD as PAYCD_TYP_COD",
	"CAST(PAYCD_WAGE_ADD_AMT AS DECIMAL(16,6)) as PAYCD_WAGE_ADD_AMT",
	"CAST(PAYCD_WAGE_MLT_AMT AS DECIMAL(16,6)) as PAYCD_WAGE_MLT_AMT",
	"CAST(PAYCD_MONEY_SWT AS INT) as PAYCD_MONEY_SWT",
	"CAST(PAYCD_TOT_SWT AS INT) as PAYCD_TOT_SWT",
	"CAST(PAYCD_EXCUSED_SWT AS INT) as PAYCD_EXCUSED_SWT",
	"CAST(PAYCD_OT_SWT AS INT) as PAYCD_OT_SWT",
	"CAST(PAYCD_CONSEC_OT_SWT AS INT) as PAYCD_CONSEC_OT_SWT",
	"CAST(PAYCD_VISIBLE_SWT AS INT) as PAYCD_VISIBLE_SWT",
	"PAYCAT_NAM as PAYCAT_NAM",
	"CAST(CORE_HRS_SWT AS INT) as CORE_HRS_SWT",
	"CAST(UPDT_DTM AS TIMESTAMP) as UPDT_DTM"
)
# overwriteDeltaPartition(Shortcut_to_WFA_PAYCD_PRE,'DC_NBR',dcnbr,f'{raw}.WFA_PAYCD_PRE')
Shortcut_to_WFA_PAYCD_PRE.write.mode("overwrite").saveAsTable(f'{raw}.WFA_PAYCD_PRE')