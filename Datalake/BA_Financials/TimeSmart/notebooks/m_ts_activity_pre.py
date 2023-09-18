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

(username,password,connection_string,linked_server) = timesmart_prd_sqlServer(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_Activity, type SOURCE 
# COLUMN COUNT: 4

query = f"""(SELECT
Activity.ActivityID,
Activity.ActivityName,
Activity.ActivityDesc,
Activity.CreatorID
FROM {linked_server}.Time_Tracking.dbo.Activity) as src"""

SQ_Shortcut_to_Activity = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_TS_ACTIVITY_PRE, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_TS_ACTIVITY_PRE = SQ_Shortcut_to_Activity.selectExpr(
	"CAST(ActivityID AS INT) as ACTIVITYID",
	"CAST(ActivityName AS STRING) as ACTIVITYNAME",
	"CAST(ActivityDesc AS STRING) as ACTIVITYDESC",
	"CAST(CreatorID AS INT) as CREATORID"
)
# overwriteDeltaPartition(Shortcut_to_TS_ACTIVITY_PRE,'DC_NBR',dcnbr,f'{raw}.TS_ACTIVITY_PRE')
Shortcut_to_TS_ACTIVITY_PRE.write.mode("overwrite").saveAsTable(f'{raw}.TS_ACTIVITY_PRE')