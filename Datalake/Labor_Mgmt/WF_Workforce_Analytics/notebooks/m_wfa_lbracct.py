#Code converted on 2023-08-08 15:41:41
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


# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_LBRACCT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_LBRACCT = spark.sql(f"""SELECT
WFA_LBRACCT.LBRACCT_ID,
WFA_LBRACCT.LOAD_DT
FROM {legacy}.WFA_LBRACCT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_LBRACCT_PRE, type SOURCE 
# COLUMN COUNT: 36

SQ_Shortcut_to_WFA_LBRACCT_PRE = spark.sql(f"""SELECT
WFA_LBRACCT_PRE.LBRACCT_ID,
WFA_LBRACCT_PRE.LBRACCT_SKEY,
WFA_LBRACCT_PRE.LBRACCT_FULL_ID,
WFA_LBRACCT_PRE.LBRACCT_FULL_NAM,
WFA_LBRACCT_PRE.LBRLVL1_NAM,
WFA_LBRACCT_PRE.LBRACCT1_ID,
WFA_LBRACCT_PRE.LBRACCT1_NAM,
WFA_LBRACCT_PRE.LBRACCT1_DES,
WFA_LBRACCT_PRE.LBRLVL2_NAM,
WFA_LBRACCT_PRE.LBRACCT2_ID,
WFA_LBRACCT_PRE.LBRACCT2_NAM,
WFA_LBRACCT_PRE.LBRACCT2_DES,
WFA_LBRACCT_PRE.LBRLVL3_NAM,
WFA_LBRACCT_PRE.LBRACCT3_ID,
WFA_LBRACCT_PRE.LBRACCT3_NAM,
WFA_LBRACCT_PRE.LBRACCT3_DES,
WFA_LBRACCT_PRE.LBRLVL4_NAM,
WFA_LBRACCT_PRE.LBRACCT4_ID,
WFA_LBRACCT_PRE.LBRACCT4_NAM,
WFA_LBRACCT_PRE.LBRACCT4_DES,
WFA_LBRACCT_PRE.LBRLVL5_NAM,
WFA_LBRACCT_PRE.LBRACCT5_ID,
WFA_LBRACCT_PRE.LBRACCT5_NAM,
WFA_LBRACCT_PRE.LBRACCT5_DES,
WFA_LBRACCT_PRE.LBRLVL6_NAM,
WFA_LBRACCT_PRE.LBRACCT6_ID,
WFA_LBRACCT_PRE.LBRACCT6_NAM,
WFA_LBRACCT_PRE.LBRACCT6_DES,
WFA_LBRACCT_PRE.LBRLVL7_NAM,
WFA_LBRACCT_PRE.LBRACCT7_ID,
WFA_LBRACCT_PRE.LBRACCT7_NAM,
WFA_LBRACCT_PRE.LBRACCT7_DES,
WFA_LBRACCT_PRE.UPDT_DTM,
WFA_LBRACCT_PRE.SRC_COD,
WFA_LBRACCT_PRE.SRC_SKEY,
WFA_LBRACCT_PRE.TENANT_SKEY
FROM {raw}.WFA_LBRACCT_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Jnr_Wfa_Lbracct, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 38

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_LBRACCT_temp = SQ_Shortcut_to_WFA_LBRACCT.toDF(*["SQ_Shortcut_to_WFA_LBRACCT___" + col for col in SQ_Shortcut_to_WFA_LBRACCT.columns])
SQ_Shortcut_to_WFA_LBRACCT_PRE_temp = SQ_Shortcut_to_WFA_LBRACCT_PRE.toDF(*["SQ_Shortcut_to_WFA_LBRACCT_PRE___" + col for col in SQ_Shortcut_to_WFA_LBRACCT_PRE.columns])

Jnr_Wfa_Lbracct = SQ_Shortcut_to_WFA_LBRACCT_temp.join(SQ_Shortcut_to_WFA_LBRACCT_PRE_temp,[SQ_Shortcut_to_WFA_LBRACCT_temp.SQ_Shortcut_to_WFA_LBRACCT___LBRACCT_ID == SQ_Shortcut_to_WFA_LBRACCT_PRE_temp.SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT_ID as LBRACCT_ID",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT_SKEY as LBRACCT_SKEY",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT_FULL_ID as LBRACCT_FULL_ID",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT_FULL_NAM as LBRACCT_FULL_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRLVL1_NAM as LBRLVL1_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT1_ID as LBRACCT1_ID",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT1_NAM as LBRACCT1_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT1_DES as LBRACCT1_DES",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRLVL2_NAM as LBRLVL2_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT2_ID as LBRACCT2_ID",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT2_NAM as LBRACCT2_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT2_DES as LBRACCT2_DES",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRLVL3_NAM as LBRLVL3_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT3_ID as LBRACCT3_ID",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT3_NAM as LBRACCT3_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT3_DES as LBRACCT3_DES",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRLVL4_NAM as LBRLVL4_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT4_ID as LBRACCT4_ID",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT4_NAM as LBRACCT4_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT4_DES as LBRACCT4_DES",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRLVL5_NAM as LBRLVL5_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT5_ID as LBRACCT5_ID",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT5_NAM as LBRACCT5_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT5_DES as LBRACCT5_DES",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRLVL6_NAM as LBRLVL6_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT6_ID as LBRACCT6_ID",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT6_NAM as LBRACCT6_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT6_DES as LBRACCT6_DES",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRLVL7_NAM as LBRLVL7_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT7_ID as LBRACCT7_ID",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT7_NAM as LBRACCT7_NAM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___LBRACCT7_DES as LBRACCT7_DES",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___UPDT_DTM as UPDT_DTM",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___SRC_COD as SRC_COD",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___SRC_SKEY as SRC_SKEY",
	"SQ_Shortcut_to_WFA_LBRACCT_PRE___TENANT_SKEY as TENANT_SKEY",
	"SQ_Shortcut_to_WFA_LBRACCT___LBRACCT_ID as LBRACCT_ID1",
	"SQ_Shortcut_to_WFA_LBRACCT___LOAD_DT as LOAD_DT")

# COMMAND ----------
# Processing node Exp_Load_Strategy, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 39

# for each involved DataFrame, append the dataframe name to each column
Jnr_Wfa_Lbracct_temp = Jnr_Wfa_Lbracct.toDF(*["Jnr_Wfa_Lbracct___" + col for col in Jnr_Wfa_Lbracct.columns])

Exp_Load_Strategy = Jnr_Wfa_Lbracct_temp.selectExpr(
	"Jnr_Wfa_Lbracct___sys_row_id as sys_row_id",
	"Jnr_Wfa_Lbracct___LBRACCT_ID as LBRACCT_ID",
	"Jnr_Wfa_Lbracct___LBRACCT_SKEY as LBRACCT_SKEY",
	"Jnr_Wfa_Lbracct___LBRACCT_FULL_ID as LBRACCT_FULL_ID",
	"Jnr_Wfa_Lbracct___LBRACCT_FULL_NAM as LBRACCT_FULL_NAM",
	"Jnr_Wfa_Lbracct___LBRLVL1_NAM as LBRLVL1_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT1_ID as LBRACCT1_ID",
	"Jnr_Wfa_Lbracct___LBRACCT1_NAM as LBRACCT1_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT1_DES as LBRACCT1_DES",
	"Jnr_Wfa_Lbracct___LBRLVL2_NAM as LBRLVL2_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT2_ID as LBRACCT2_ID",
	"Jnr_Wfa_Lbracct___LBRACCT2_NAM as LBRACCT2_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT2_DES as LBRACCT2_DES",
	"Jnr_Wfa_Lbracct___LBRLVL3_NAM as LBRLVL3_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT3_ID as LBRACCT3_ID",
	"Jnr_Wfa_Lbracct___LBRACCT3_NAM as LBRACCT3_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT3_DES as LBRACCT3_DES",
	"Jnr_Wfa_Lbracct___LBRLVL4_NAM as LBRLVL4_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT4_ID as LBRACCT4_ID",
	"Jnr_Wfa_Lbracct___LBRACCT4_NAM as LBRACCT4_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT4_DES as LBRACCT4_DES",
	"Jnr_Wfa_Lbracct___LBRLVL5_NAM as LBRLVL5_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT5_ID as LBRACCT5_ID",
	"Jnr_Wfa_Lbracct___LBRACCT5_NAM as LBRACCT5_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT5_DES as LBRACCT5_DES",
	"Jnr_Wfa_Lbracct___LBRLVL6_NAM as LBRLVL6_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT6_ID as LBRACCT6_ID",
	"Jnr_Wfa_Lbracct___LBRACCT6_NAM as LBRACCT6_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT6_DES as LBRACCT6_DES",
	"Jnr_Wfa_Lbracct___LBRLVL7_NAM as LBRLVL7_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT7_ID as LBRACCT7_ID",
	"Jnr_Wfa_Lbracct___LBRACCT7_NAM as LBRACCT7_NAM",
	"Jnr_Wfa_Lbracct___LBRACCT7_DES as LBRACCT7_DES",
	"Jnr_Wfa_Lbracct___UPDT_DTM as UPDT_DTM",
	"Jnr_Wfa_Lbracct___SRC_COD as SRC_COD",
	"Jnr_Wfa_Lbracct___SRC_SKEY as SRC_SKEY",
	"Jnr_Wfa_Lbracct___TENANT_SKEY as TENANT_SKEY",
	"IF (Jnr_Wfa_Lbracct___LBRACCT_ID IS NULL, 0, IF (Jnr_Wfa_Lbracct___LBRACCT_ID IS NOT NULL AND Jnr_Wfa_Lbracct___LBRACCT_ID IS NOT NULL, 1, 3)) as LOAD_STRATEGY",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"IF (Jnr_Wfa_Lbracct___LBRACCT_ID IS NULL, CURRENT_TIMESTAMP, Jnr_Wfa_Lbracct___LOAD_DT) as LOAD_DT"
)

# COMMAND ----------
# Processing node Upd_Wfa_Lbracct, type UPDATE_STRATEGY 
# COLUMN COUNT: 39

# for each involved DataFrame, append the dataframe name to each column
Exp_Load_Strategy_temp = Exp_Load_Strategy.toDF(*["Exp_Load_Strategy___" + col for col in Exp_Load_Strategy.columns])

Upd_Wfa_Lbracct = Exp_Load_Strategy_temp.selectExpr(
	"Exp_Load_Strategy___LBRACCT_ID as LBRACCT_ID",
	"Exp_Load_Strategy___LBRACCT_SKEY as LBRACCT_SKEY",
	"Exp_Load_Strategy___LBRACCT_FULL_ID as LBRACCT_FULL_ID",
	"Exp_Load_Strategy___LBRACCT_FULL_NAM as LBRACCT_FULL_NAM",
	"Exp_Load_Strategy___LBRLVL1_NAM as LBRLVL1_NAM",
	"Exp_Load_Strategy___LBRACCT1_ID as LBRACCT1_ID",
	"Exp_Load_Strategy___LBRACCT1_NAM as LBRACCT1_NAM",
	"Exp_Load_Strategy___LBRACCT1_DES as LBRACCT1_DES",
	"Exp_Load_Strategy___LBRLVL2_NAM as LBRLVL2_NAM",
	"Exp_Load_Strategy___LBRACCT2_ID as LBRACCT2_ID",
	"Exp_Load_Strategy___LBRACCT2_NAM as LBRACCT2_NAM",
	"Exp_Load_Strategy___LBRACCT2_DES as LBRACCT2_DES",
	"Exp_Load_Strategy___LBRLVL3_NAM as LBRLVL3_NAM",
	"Exp_Load_Strategy___LBRACCT3_ID as LBRACCT3_ID",
	"Exp_Load_Strategy___LBRACCT3_NAM as LBRACCT3_NAM",
	"Exp_Load_Strategy___LBRACCT3_DES as LBRACCT3_DES",
	"Exp_Load_Strategy___LBRLVL4_NAM as LBRLVL4_NAM",
	"Exp_Load_Strategy___LBRACCT4_ID as LBRACCT4_ID",
	"Exp_Load_Strategy___LBRACCT4_NAM as LBRACCT4_NAM",
	"Exp_Load_Strategy___LBRACCT4_DES as LBRACCT4_DES",
	"Exp_Load_Strategy___LBRLVL5_NAM as LBRLVL5_NAM",
	"Exp_Load_Strategy___LBRACCT5_ID as LBRACCT5_ID",
	"Exp_Load_Strategy___LBRACCT5_NAM as LBRACCT5_NAM",
	"Exp_Load_Strategy___LBRACCT5_DES as LBRACCT5_DES",
	"Exp_Load_Strategy___LBRLVL6_NAM as LBRLVL6_NAM",
	"Exp_Load_Strategy___LBRACCT6_ID as LBRACCT6_ID",
	"Exp_Load_Strategy___LBRACCT6_NAM as LBRACCT6_NAM",
	"Exp_Load_Strategy___LBRACCT6_DES as LBRACCT6_DES",
	"Exp_Load_Strategy___LBRLVL7_NAM as LBRLVL7_NAM",
	"Exp_Load_Strategy___LBRACCT7_ID as LBRACCT7_ID",
	"Exp_Load_Strategy___LBRACCT7_NAM as LBRACCT7_NAM",
	"Exp_Load_Strategy___LBRACCT7_DES as LBRACCT7_DES",
	"Exp_Load_Strategy___UPDT_DTM as UPDT_DTM",
	"Exp_Load_Strategy___SRC_COD as SRC_COD",
	"Exp_Load_Strategy___SRC_SKEY as SRC_SKEY",
	"Exp_Load_Strategy___TENANT_SKEY as TENANT_SKEY",
	"Exp_Load_Strategy___LOAD_STRATEGY as LOAD_STRATEGY",
	"Exp_Load_Strategy___UPDATE_DT as UPDATE_DT",
	"Exp_Load_Strategy___LOAD_DT as LOAD_DT",
	"Exp_Load_Strategy___LOAD_STRATEGY as pyspark_data_action")

# COMMAND ----------
# Processing node Shortcut_to_WFA_LBRACCT, type TARGET 
# COLUMN COUNT: 38


Shortcut_to_WFA_LBRACCT = Upd_Wfa_Lbracct.selectExpr(
	"CAST(LBRACCT_ID AS INT) as LBRACCT_ID",
	"CAST(LBRACCT_SKEY AS INT) as LBRACCT_SKEY",
	"CAST(LBRACCT_FULL_ID AS STRING) as LBRACCT_FULL_ID",
	"CAST(LBRACCT_FULL_NAM AS STRING) as LBRACCT_FULL_NAM",
	"CAST(LBRLVL1_NAM AS STRING) as LBRLVL1_NAM",
	"CAST(LBRACCT1_ID AS INT) as LBRACCT1_ID",
	"CAST(LBRACCT1_NAM AS STRING) as LBRACCT1_NAM",
	"CAST(LBRACCT1_DES AS STRING) as LBRACCT1_DES",
	"CAST(LBRLVL2_NAM AS STRING) as LBRLVL2_NAM",
	"CAST(LBRACCT2_ID AS INT) as LBRACCT2_ID",
	"CAST(LBRACCT2_NAM AS STRING) as LBRACCT2_NAM",
	"CAST(LBRACCT2_DES AS STRING) as LBRACCT2_DES",
	"CAST(LBRLVL3_NAM AS STRING) as LBRLVL3_NAM",
	"CAST(LBRACCT3_ID AS INT) as LBRACCT3_ID",
	"CAST(LBRACCT3_NAM AS STRING) as LBRACCT3_NAM",
	"CAST(LBRACCT3_DES AS STRING) as LBRACCT3_DES",
	"CAST(LBRLVL4_NAM AS STRING) as LBRLVL4_NAM",
	"CAST(LBRACCT4_ID AS INT) as LBRACCT4_ID",
	"CAST(LBRACCT4_NAM AS STRING) as LBRACCT4_NAM",
	"CAST(LBRACCT4_DES AS STRING) as LBRACCT4_DES",
	"CAST(LBRLVL5_NAM AS STRING) as LBRLVL5_NAM",
	"CAST(LBRACCT5_ID AS INT) as LBRACCT5_ID",
	"CAST(LBRACCT5_NAM AS STRING) as LBRACCT5_NAM",
	"CAST(LBRACCT5_DES AS STRING) as LBRACCT5_DES",
	"CAST(LBRLVL6_NAM AS STRING) as LBRLVL6_NAM",
	"CAST(LBRACCT6_ID AS INT) as LBRACCT6_ID",
	"CAST(LBRACCT6_NAM AS STRING) as LBRACCT6_NAM",
	"CAST(LBRACCT6_DES AS STRING) as LBRACCT6_DES",
	"CAST(LBRLVL7_NAM AS STRING) as LBRLVL7_NAM",
	"CAST(LBRACCT7_ID AS INT) as LBRACCT7_ID",
	"CAST(LBRACCT7_NAM AS STRING) as LBRACCT7_NAM",
	"CAST(LBRACCT7_DES AS STRING) as LBRACCT7_DES",
	"CAST(UPDT_DTM AS TIMESTAMP) as UPDT_DTM",
	"CAST(SRC_COD AS STRING) as SRC_COD",
	"CAST(SRC_SKEY AS INT) as SRC_SKEY",
	"CAST(TENANT_SKEY AS INT) as TENANT_SKEY",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

Shortcut_to_WFA_LBRACCT.createOrReplaceTempView("source")

try:
	# primary_key = """source.LBRACCT_ID = target.LBRACCT_ID"""
	refined_perf_table = f"{legacy}.WFA_LBRACCT"
	# executeMerge(Shortcut_to_WFA_LBRACCT, refined_perf_table, primary_key)
	spark.sql(f"""merge into {legacy}.WFA_LBRACCT target 
           using  source on source.LBRACCT_ID = target.LBRACCT_ID
           when matched then update set *
           when not matched then insert *""")
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("WFA_LBRACCT", "WFA_LBRACCT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("WFA_LBRACCT", "WFA_LBRACCT","Failed",str(e), f"{raw}.log_run_details")
	raise e
		