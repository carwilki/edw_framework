#Code converted on 2023-08-07 16:26:09
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

(username,password,connection_string) = mtx_prd_sqlServer(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_Activity_XRef, type SOURCE 
# COLUMN COUNT: 6

query = f"""(SELECT
Activity_XRef.ActXRefID,
Activity_XRef.ActivityID,
Activity_XRef.ActTypeID,
Activity_XRef.ActCategoryID,
Activity_XRef.ActStatusID,
Activity_XRef.RFCNBR
FROM Time_Tracking.dbo.Activity_XRef) as src"""

SQ_Shortcut_to_Activity_XRef = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_TS_ACTIVITY_XREF_PRE, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_TS_ACTIVITY_XREF_PRE = SQ_Shortcut_to_Activity_XRef.selectExpr(
	"CAST(ActXRefID AS INT) as ACTXREFID",
	"CAST(ActivityID AS INT) as ACTIVITYID",
	"CAST(ActTypeID AS INT) as ACTTYPEID",
	"CAST(ActCategoryID AS INT) as ACTCATEGORYID",
	"CAST(ActStatusID AS INT) as ACTSTATUSID",
	"CAST(RFCNBR AS STRING) as RFCNBR"
)
# overwriteDeltaPartition(Shortcut_to_TS_ACTIVITY_XREF_PRE,'DC_NBR',dcnbr,f'{raw}.TS_ACTIVITY_XREF_PRE')
Shortcut_to_TS_ACTIVITY_XREF_PRE.write.mode("overwrite").saveAsTable(f'{raw}.TS_ACTIVITY_XREF_PRE')