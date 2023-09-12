#Code converted on 2023-08-03 13:28:37
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
# Processing node SQ_Shortcut_to_StoreClassDetail, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_StoreClassDetail = jdbcSqlServerConnection(f"""(SELECT
StoreClassDetail.StoreClassDetailId,
StoreClassDetail.StoreClassId,
StoreClassDetail.ClassDateTime,
StoreClassDetail.ClassTypeId,
StoreClassDetail.LastModified,
StoreClassDetail.StoreNumber
FROM Training.dbo.StoreClassDetail) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_StoreClassDetail_temp = SQ_Shortcut_to_StoreClassDetail.toDF(*["SQ_Shortcut_to_StoreClassDetail___" + col for col in SQ_Shortcut_to_StoreClassDetail.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_StoreClassDetail_temp.selectExpr( \
	"SQ_Shortcut_to_StoreClassDetail___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_StoreClassDetail___StoreClassDetailId as StoreClassDetailId", \
	"SQ_Shortcut_to_StoreClassDetail___StoreClassId as StoreClassId", \
	"SQ_Shortcut_to_StoreClassDetail___ClassDateTime as ClassDateTime", \
	"SQ_Shortcut_to_StoreClassDetail___ClassTypeId as ClassTypeId", \
	"SQ_Shortcut_to_StoreClassDetail___LastModified as LastModified", \
	"SQ_Shortcut_to_StoreClassDetail___StoreNumber as StoreNumber", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(StoreClassDetailId AS INT) as STORE_CLASS_DETAIL_ID", \
	"CAST(StoreClassId AS INT) as STORE_CLASS_ID", \
	"CAST(ClassDateTime AS TIMESTAMP) as CLASS_DATE_TIME", \
	"CAST(ClassTypeId AS INT) as CLASS_TYPE_ID", \
	"CAST(LastModified AS TIMESTAMP) as LAST_MODIFIED", \
	"CAST(StoreNumber AS INT) as STORE_NUMBER", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE.write.saveAsTable(f'{raw}.TRAINING_STORE_CLASS_DETAIL_PRE', mode = 'overwrite')