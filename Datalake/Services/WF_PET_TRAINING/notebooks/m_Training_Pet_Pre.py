#Code converted on 2023-08-03 13:28:25
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
sensitive = getEnvPrefix(env) + 'cust_sensitive'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = pettraining_prd_sqlServer_training(env)

# COMMAND ----------
# Variable_declaration_comment
MAX_LOAD_DATE=genPrevRunDt('TRAINING_PET',legacy,raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_Pet, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_Pet = jdbcSqlServerConnection(f"""(SELECT
Pet.PetId,
Pet.ProviderId,
Pet.Name,
Pet.Breed,
Pet.BirthDate,
Pet.Notes,
Pet.CreateDateTime,
Pet.ExternalPetId
FROM Training.dbo.Pet
WHERE CreateDateTime > '{MAX_LOAD_DATE}') as src """,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Pet_temp = SQ_Shortcut_to_Pet.toDF(*["SQ_Shortcut_to_Pet___" + col for col in SQ_Shortcut_to_Pet.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_Pet_temp.selectExpr( \
	"SQ_Shortcut_to_Pet___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_Pet___PetId as PetId", \
	"SQ_Shortcut_to_Pet___ProviderId as ProviderId", \
	"SQ_Shortcut_to_Pet___Name as Name", \
	"SQ_Shortcut_to_Pet___Breed as Breed", \
	"SQ_Shortcut_to_Pet___BirthDate as BirthDate", \
	"SQ_Shortcut_to_Pet___Notes as Notes", \
	"SQ_Shortcut_to_Pet___CreateDateTime as CreateDateTime", \
	"SQ_Shortcut_to_Pet___ExternalPetId as ExternalPetId", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)
# .withColumn("v_MAX_LOAD_DATE", SET {MAX_LOAD_DATE} = GREATEST({MAX_LOAD_DATE}, CreateDateTime))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_PET_PRE, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_TRAINING_PET_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(PetId AS INT) as PET_ID", \
	"CAST(ProviderId AS STRING) as PROVIDER_ID", \
	"CAST(Name AS STRING) as NAME", \
	"CAST(Breed AS STRING) as BREED", \
	"CAST(BirthDate AS DATE) as BIRTH_DATE", \
	"CAST(Notes AS STRING) as NOTES", \
	"CAST(CreateDateTime AS TIMESTAMP) as CREATE_DATE_TIME", \
	"CAST(ExternalPetId as BIGINT) as EXTERNAL_PET_ID", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_TRAINING_PET_PRE.write.saveAsTable(f'{sensitive}.raw_TRAINING_PET_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_PET_PRE", "TRAINING_PET_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_PET_PRE", "TRAINING_PET_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e


