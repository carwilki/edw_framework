#Code converted on 2023-08-03 13:28:36
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

# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = mtx_prd_sqlServer(env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_StoreBlackouts, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_StoreBlackouts = jdbcSqlServerConnection(f"""(SELECT
StoreBlackouts.StoreNumber,
StoreBlackouts.BlackoutStartDate,
StoreBlackouts.BlackoutEndDate
FROM Training.dbo.StoreBlackouts) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_StoreBlackouts_temp = SQ_Shortcut_to_StoreBlackouts.toDF(*["SQ_Shortcut_to_StoreBlackouts___" + col for col in SQ_Shortcut_to_StoreBlackouts.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_StoreBlackouts_temp.selectExpr( \
	"SQ_Shortcut_to_StoreBlackouts___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_StoreBlackouts___StoreNumber as StoreNumber", \
	"SQ_Shortcut_to_StoreBlackouts___BlackoutStartDate as BlackoutStartDate", \
	"SQ_Shortcut_to_StoreBlackouts___BlackoutEndDate as BlackoutEndDate", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(StoreNumber AS INT) as STORE_NUMBER", \
	"CAST(BlackoutStartDate AS DATE) as BLACK_OUT_START_DATE", \
	"CAST(BlackoutEndDate AS DATE) as BLACK_OUT_END_DATE", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

# try:
Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE.write.saveAsTable(f'{raw}.TRAINING_STORE_BLACKOUTS_PRE', mode = 'overwrite')
# 	logPrevRunDt("TRAINING_STORE_BLACKOUTS_PRE", "TRAINING_STORE_BLACKOUTS_PRE", "Completed", "N/A", f"{raw}.log_run_details")
# except Exception as e:
# 	logPrevRunDt("TRAINING_STORE_BLACKOUTS_PRE", "TRAINING_STORE_BLACKOUTS_PRE", "Failed", "N/A", f"{raw}.log_run_details")
# 	raise e

