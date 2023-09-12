#Code converted on 2023-08-03 13:28:27
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
(username, password, connection_string) = mtx_prd_sqlServer(env)

# COMMAND ----------
# Variable_declaration_comment
MAX_LOAD_DATE=genPrevRunDt('TRAINING_RESERVATION',legacy,raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_Reservation, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_Reservation = jdbcSqlServerConnection(f"""(SELECT
Reservation.ReservationId,
Reservation.CustomerId,
Reservation.PetId,
Reservation.StoreClassId,
Reservation.StoreNumber,
Reservation.EnrollmentDateTime,
Reservation.CreateDateTime,
Reservation.PackageOptionId
FROM Training.dbo.Reservation
WHERE CreateDateTime > '{MAX_LOAD_DATE}') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Reservation_temp = SQ_Shortcut_to_Reservation.toDF(*["SQ_Shortcut_to_Reservation___" + col for col in SQ_Shortcut_to_Reservation.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_Reservation_temp.selectExpr( \
	"SQ_Shortcut_to_Reservation___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_Reservation___ReservationId as ReservationId", \
	"SQ_Shortcut_to_Reservation___CustomerId as CustomerId", \
	"SQ_Shortcut_to_Reservation___PetId as PetId", \
	"SQ_Shortcut_to_Reservation___StoreClassId as StoreClassId", \
	"SQ_Shortcut_to_Reservation___StoreNumber as StoreNumber", \
	"SQ_Shortcut_to_Reservation___EnrollmentDateTime as EnrollmentDateTime", \
	"SQ_Shortcut_to_Reservation___CreateDateTime as CreateDateTime", \
	"SQ_Shortcut_to_Reservation___PackageOptionId as PackageOptionId", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)
# .withColumn("v_MAX_LOAD_DATE", SET {MAX_LOAD_DATE} = GREATEST({MAX_LOAD_DATE}, CreateDateTime))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_RESERVATION_PRE, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_TRAINING_RESERVATION_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(ReservationId AS INT) as RESERVATION_ID", \
	"CAST(CustomerId AS INT) as CUSTOMER_ID", \
	"CAST(PetId AS INT) as PET_ID", \
	"CAST(StoreClassId AS INT) as STORE_CLASS_ID", \
	"CAST(StoreNumber AS INT) as STORE_NUMBER", \
	"CAST(EnrollmentDateTime AS TIMESTAMP) as ENROLLMENT_DATE_TIME", \
	"CAST(CreateDateTime AS TIMESTAMP) as CREATE_DATE_TIME", \
	"CAST(PackageOptionId AS INT) as PACKAGE_OPTION_ID", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_TRAINING_RESERVATION_PRE.write.saveAsTable(f'{raw}.TRAINING_RESERVATION_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_RESERVATION_PRE", "TRAINING_RESERVATION_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_RESERVATION_PRE", "TRAINING_RESERVATION_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e


