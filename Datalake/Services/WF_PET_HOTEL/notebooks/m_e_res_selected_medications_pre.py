#Code converted on 2023-07-28 11:37:33
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime,timedelta
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')

# uncomment before checking in
args = parser.parse_args()
env = args.env

# remove before checking in
# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Variable_declaration_comment
# CREATE_DATE=args.CREATE_DATE
# UPDATE_DATE=args.UPDATE_DATE


refined_perf_table = "E_RES_SELECTED_MEDICATIONS"
prev_run_dt = genPrevRunDt(refined_perf_table,legacy,raw)
print("The prev run date is " + prev_run_dt)
CREATE_DATE = (datetime.strptime(prev_run_dt, '%Y-%m-%d') - timedelta(days=1)).strftime("%Y-%m-%d")
UPDATE_DATE = (datetime.strptime(prev_run_dt, '%Y-%m-%d') - timedelta(days=1)).strftime("%Y-%m-%d")

(username, password, connection_string) = mtx_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SelectedMedications, type SOURCE 
# COLUMN COUNT: 10

_sql = f"""
SELECT
    SelectedMedications.SelectedMedicationId,
    SelectedMedications.PetId,
    SelectedMedications.Name,
    SelectedMedications.Dose,
    SelectedMedications.TimeOfDay,
    SelectedMedications.Ailments,
    SelectedMedications.Instructions,
    SelectedMedications.Frequency,
    SelectedMedications.UnitPrice,
    SelectedMedications.TotalPrice
FROM eReservations.dbo.SelectedMedications
"""

SQ_Shortcut_to_SelectedMedications = SQ_Shortcut_to_AddonCategories = jdbcSqlServerConnection(f"({_sql}) as src",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_Requests, type SOURCE 
# COLUMN COUNT: 3

_sql = f"""
SELECT
    Requests.RequestId,
    Requests.CreatedAt,
    Requests.UpdatedAt
FROM eReservations.dbo.Requests
WHERE CreatedAt >= '{CREATE_DATE}' OR UpdatedAt >=  '{UPDATE_DATE}'
"""

SQ_Shortcut_to_Requests = SQ_Shortcut_to_AddonCategories = jdbcSqlServerConnection(f"({_sql}) as src",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_Pets, type SOURCE 
# COLUMN COUNT: 2

_sql = f"""
SELECT
    Pets.PetId,
    Pets.RequestId
FROM eReservations.dbo.Pets
"""

SQ_Shortcut_to_Pets = SQ_Shortcut_to_AddonCategories = jdbcSqlServerConnection(f"({_sql}) as src", username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node JNR_REQUESTS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Pets_temp = SQ_Shortcut_to_Pets.toDF(*["SQ_Shortcut_to_Pets___" + col for col in SQ_Shortcut_to_Pets.columns])
SQ_Shortcut_to_Requests_temp = SQ_Shortcut_to_Requests.toDF(*["SQ_Shortcut_to_Requests___" + col for col in SQ_Shortcut_to_Requests.columns])

JNR_REQUESTS = SQ_Shortcut_to_Requests_temp.join(SQ_Shortcut_to_Pets_temp,[SQ_Shortcut_to_Requests_temp.SQ_Shortcut_to_Requests___RequestId == SQ_Shortcut_to_Pets_temp.SQ_Shortcut_to_Pets___RequestId],'inner').selectExpr(
	"SQ_Shortcut_to_Pets___PetId as PetId",
	"SQ_Shortcut_to_Pets___RequestId as RequestId",
	"SQ_Shortcut_to_Requests___RequestId as i_RequestId",
	"SQ_Shortcut_to_Requests___CreatedAt as CreatedAt",
	"SQ_Shortcut_to_Requests___UpdatedAt as UpdatedAt")

# COMMAND ----------

# Processing node JNR_PETS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SelectedMedications_temp = SQ_Shortcut_to_SelectedMedications.toDF(*["SQ_Shortcut_to_SelectedMedications___" + col for col in SQ_Shortcut_to_SelectedMedications.columns])
JNR_REQUESTS_temp = JNR_REQUESTS.toDF(*["JNR_REQUESTS___" + col for col in JNR_REQUESTS.columns])

JNR_PETS = JNR_REQUESTS_temp.join(SQ_Shortcut_to_SelectedMedications_temp,[JNR_REQUESTS_temp.JNR_REQUESTS___PetId == SQ_Shortcut_to_SelectedMedications_temp.SQ_Shortcut_to_SelectedMedications___PetId],'inner').selectExpr(
	"SQ_Shortcut_to_SelectedMedications___SelectedMedicationId as SelectedMedicationId",
	"SQ_Shortcut_to_SelectedMedications___PetId as PetId",
	"SQ_Shortcut_to_SelectedMedications___Name as Name",
	"SQ_Shortcut_to_SelectedMedications___Dose as Dose",
	"SQ_Shortcut_to_SelectedMedications___TimeOfDay as TimeOfDay",
	"SQ_Shortcut_to_SelectedMedications___Ailments as Ailments",
	"SQ_Shortcut_to_SelectedMedications___Instructions as Instructions",
	"SQ_Shortcut_to_SelectedMedications___Frequency as Frequency",
	"SQ_Shortcut_to_SelectedMedications___UnitPrice as UnitPrice",
	"SQ_Shortcut_to_SelectedMedications___TotalPrice as TotalPrice",
	"JNR_REQUESTS___PetId as i_PetId",
	"JNR_REQUESTS___CreatedAt as CreatedAt",
	"JNR_REQUESTS___UpdatedAt as UpdatedAt")

# COMMAND ----------

# Processing node EXP_SELECTED_MEDICATIONS_PRE, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_PETS_temp = JNR_PETS.toDF(*["JNR_PETS___" + col for col in JNR_PETS.columns])

EXP_SELECTED_MEDICATIONS_PRE = JNR_PETS_temp.selectExpr(
	# "JNR_PETS___sys_row_id as sys_row_id",
	"JNR_PETS___SelectedMedicationId as SelectedMedicationId",
	"JNR_PETS___PetId as PetId",
	"JNR_PETS___Name as Name",
	"JNR_PETS___Dose as Dose",
	"JNR_PETS___TimeOfDay as TimeOfDay",
	"JNR_PETS___Ailments as Ailments",
	"JNR_PETS___Instructions as Instructions",
	"JNR_PETS___Frequency as Frequency",
	"JNR_PETS___UnitPrice as UnitPrice",
	"JNR_PETS___TotalPrice as TotalPrice",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE, type TARGET 
# COLUMN COUNT: 11


Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE = EXP_SELECTED_MEDICATIONS_PRE.selectExpr(
	"CAST(SelectedMedicationId AS INT) as SELECTED_MEDICATION_ID",
	"CAST(PetId AS INT) as PET_ID",
	"CAST(Name AS STRING) as NAME",
	"CAST(Dose AS STRING) as DOSE",
	"CAST(TimeOfDay AS STRING) as TIME_OF_DAY",
	"CAST(Ailments AS STRING) as AILMENTS",
	"CAST(Instructions AS STRING) as INSTRUCTIONS",
	"CAST(Frequency AS STRING) as FREQUENCY",
	"CAST(UnitPrice AS DECIMAL(15,2)) as UNIT_PRICE",
	"CAST(TotalPrice AS DECIMAL(15,2)) as TOTAL_PRICE",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)

Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE.write.mode("overwrite").saveAsTable(f'{raw}.E_RES_SELECTED_MEDICATIONS_PRE')