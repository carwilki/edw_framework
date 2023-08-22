#Code converted on 2023-07-28 07:59:25
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
# Processing node SQ_Shortcut_to_GS_Training, type SOURCE 
# COLUMN COUNT: 16

query = f"""(SELECT
GS_Training.ID,
GS_Training.TravelID,
GS_Training.HomeStoreID,
GS_Training.AssociateID,
GS_Training.AssociateFirstName,
GS_Training.AssociateLastName,
GS_Training.DM,
GS_Training.TrainingStoreID,
GS_Training.DateTrainingStart,
GS_Training.DateTrainingEnd,
GS_Training.TrainingStatus,
GS_Training.TrainingType,
GS_Training.DateCreated,
GS_Training.CreatedBy,
GS_Training.ModifiedBy,
GS_Training.DateModified
FROM PetTraining.dbo.GS_Training) as src"""

SQ_Shortcut_to_GS_Training = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_Training_temp = SQ_Shortcut_to_GS_Training.toDF(*["SQ_Shortcut_to_GS_Training___" + col for col in SQ_Shortcut_to_GS_Training.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_GS_Training_temp.selectExpr(
	"SQ_Shortcut_to_GS_Training___sys_row_id as sys_row_id",
	"CURRENT_TIMESTAMP as LOAD_TSTMP",
	"SQ_Shortcut_to_GS_Training___ID as ID",
	"SQ_Shortcut_to_GS_Training___TravelID as TravelID",
	"SQ_Shortcut_to_GS_Training___HomeStoreID as HomeStoreID",
	"SQ_Shortcut_to_GS_Training___AssociateID as AssociateID",
	"SQ_Shortcut_to_GS_Training___AssociateFirstName as AssociateFirstName",
	"SQ_Shortcut_to_GS_Training___AssociateLastName as AssociateLastName",
	"SQ_Shortcut_to_GS_Training___DM as DM",
	"SQ_Shortcut_to_GS_Training___TrainingStoreID as TrainingStoreID",
	"SQ_Shortcut_to_GS_Training___DateTrainingStart as DateTrainingStart",
	"SQ_Shortcut_to_GS_Training___DateTrainingEnd as DateTrainingEnd",
	"SQ_Shortcut_to_GS_Training___TrainingStatus as TrainingStatus",
	"SQ_Shortcut_to_GS_Training___TrainingType as TrainingType",
	"SQ_Shortcut_to_GS_Training___DateCreated as DateCreated",
	"SQ_Shortcut_to_GS_Training___CreatedBy as CreatedBy",
	"SQ_Shortcut_to_GS_Training___ModifiedBy as ModifiedBy",
	"SQ_Shortcut_to_GS_Training___DateModified as DateModified"
)

# COMMAND ----------
# Processing node Shortcut_to_GS_PT_TRAINING_PRE, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_GS_PT_TRAINING_PRE = EXP_LOAD_TSTMP.selectExpr(
	"CAST(ID as INT) as GS_PT_TRAINING_ID",
	"CAST(TravelID as INT) as GS_PT_TRAVEL_ID",
	"CAST(HomeStoreID as INT) as HOME_STORE_ID",
	"CAST(AssociateID AS STRING) as ASSOCIATE_ID",
	"CAST(AssociateFirstName AS STRING) as ASSOCIATE_FIRST_NAME",
	"CAST(AssociateLastName AS STRING) as ASSOCIATE_LAST_NAME",
	"CAST(DM AS STRING) as DM",
	"CAST(TrainingStoreID as INT) as TRAINING_STORE_ID",
	"CAST(DateTrainingStart AS TIMESTAMP) as TRAINING_START_DT",
	"CAST(DateTrainingEnd AS TIMESTAMP) as TRAINING_END_DT",
	"CAST(TrainingStatus AS BIGINT) as TRAINING_STATUS",
	"CAST(TrainingType AS BIGINT) as TRAINING_TYPE",
	"CAST(DateCreated AS TIMESTAMP) as CREATED_DT",
	"CAST(CreatedBy AS STRING) as CREATED_BY",
	"CAST(ModifiedBy AS STRING) as MODIFIED_BY",
	"CAST(DateModified AS TIMESTAMP) as MODIFIED_DT",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_GS_PT_TRAINING_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GS_PT_TRAINING_PRE')
