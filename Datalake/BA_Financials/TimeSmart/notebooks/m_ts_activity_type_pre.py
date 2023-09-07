#Code converted on 2023-08-07 16:26:06
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

(username,password,connection_string) = timesmart_prd_sqlServer(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_Activity_Type, type SOURCE 
# COLUMN COUNT: 2

query = f"""(SELECT
Activity_Type.ActTypeID,
Activity_Type.ActivityType
FROM Time_Tracking.dbo.Activity_Type) as src"""

SQ_Shortcut_to_Activity_Type = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_TS_ACTIVITY_TYPE_PRE, type TARGET 
# COLUMN COUNT: 2


Shortcut_to_TS_ACTIVITY_TYPE_PRE = SQ_Shortcut_to_Activity_Type.selectExpr(
	"CAST(ActTypeID AS INT) as ACTTYPEID",
	"CAST(ActivityType AS STRING) as ACTIVITYTYPE"
)
# overwriteDeltaPartition(Shortcut_to_TS_ACTIVITY_TYPE_PRE,'DC_NBR',dcnbr,f'{raw}.TS_ACTIVITY_TYPE_PRE')
Shortcut_to_TS_ACTIVITY_TYPE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.TS_ACTIVITY_TYPE_PRE')