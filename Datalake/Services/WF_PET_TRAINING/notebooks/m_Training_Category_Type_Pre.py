#Code converted on 2023-08-03 13:28:13
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
(username, password, connection_string) = pettraining_prd_sqlServer_training(env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_CategoryType, type SOURCE 
# COLUMN COUNT: 2
# print(username,password,connection_string)
SQ_Shortcut_to_CategoryType = jdbcSqlServerConnection("""
(SELECT
CategoryID,
CategoryName
FROM Training.dbo.CategoryType) as src  """,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_CategoryType_temp = SQ_Shortcut_to_CategoryType.toDF(*["SQ_Shortcut_to_CategoryType___" + col for col in SQ_Shortcut_to_CategoryType.columns])

EXPTRANS = SQ_Shortcut_to_CategoryType_temp.selectExpr( \
	"SQ_Shortcut_to_CategoryType___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_CategoryType___CategoryID as CategoryID", \
	"SQ_Shortcut_to_CategoryType___CategoryName as CategoryName", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_CATEGORY_TYPE_PRE, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_TRAINING_CATEGORY_TYPE_PRE = EXPTRANS.selectExpr( \
	"CAST(CategoryID AS INT) as CATEGORY_ID", \
	"CAST(CategoryName AS STRING) as CATEGORY_NAME", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_TRAINING_CATEGORY_TYPE_PRE.write.saveAsTable(f'{raw}.TRAINING_CATEGORY_TYPE_PRE', mode = 'overwrite')