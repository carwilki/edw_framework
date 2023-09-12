#Code converted on 2023-08-03 13:28:17
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

# Set parameter lookup values
#parameter_filename = 'TBD'
#parameter_section = 'TBD'
parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

# env = 'dev'


if env is None or env == '':
    raise ValueError('env is not set')

sensitive = getEnvPrefix(env) + 'cust_sensitive'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = pettraining_prd_sqlServer_training(env)

# COMMAND ----------
# Variable_declaration_comment
MAX_LOAD_DATE=genPrevRunDt('legacy_TRAINING_CUSTOMER',sensitive,raw)
# MAX_LOAD_DATE='2023-08-06'
# COMMAND ----------
# Processing node SQ_Shortcut_to_Customer, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_Customer = jdbcSqlServerConnection(f"""(SELECT
Customer.CustomerId,
Customer.ProviderId,
Customer.FirstName,
Customer.LastName,
Customer.EmailAddress,
Customer.PrimaryPhone,
Customer.PrimaryPhoneType,
Customer.AlternatePhone,
Customer.AlternatePhoneType,
Customer.CreateDateTime,
Customer.ExternalCustomerId
FROM Training.dbo.Customer
WHERE CreateDateTime > '{MAX_LOAD_DATE}') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Customer_temp = SQ_Shortcut_to_Customer.toDF(*["SQ_Shortcut_to_Customer___" + col for col in SQ_Shortcut_to_Customer.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_Customer_temp.selectExpr( \
	"SQ_Shortcut_to_Customer___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_Customer___CustomerId as CustomerId", \
	"SQ_Shortcut_to_Customer___ProviderId as ProviderId", \
	"SQ_Shortcut_to_Customer___FirstName as FirstName", \
	"SQ_Shortcut_to_Customer___LastName as LastName", \
	"SQ_Shortcut_to_Customer___EmailAddress as EmailAddress", \
	"SQ_Shortcut_to_Customer___PrimaryPhone as PrimaryPhone", \
	"SQ_Shortcut_to_Customer___PrimaryPhoneType as PrimaryPhoneType", \
	"SQ_Shortcut_to_Customer___AlternatePhone as AlternatePhone", \
	"SQ_Shortcut_to_Customer___AlternatePhoneType as AlternatePhoneType", \
	"SQ_Shortcut_to_Customer___CreateDateTime as CreateDateTime", \
	"SQ_Shortcut_to_Customer___ExternalCustomerId as ExternalCustomerId", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)
# .withColumn("v_MAX_LOAD_DATE", SET {MAX_LOAD_DATE} = GREATEST({MAX_LOAD_DATE}, CreateDateTime))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_CUSTOMER_PRE, type TARGET 
# COLUMN COUNT: 12


Shortcut_to_TRAINING_CUSTOMER_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(CustomerId AS INT) as CUSTOMER_ID", \
	"CAST(ProviderId AS STRING) as PROVIDER_ID", \
	"CAST(FirstName AS STRING) as FIRST_NAME", \
	"CAST(LastName AS STRING) as LAST_NAME", \
	"CAST(EmailAddress AS STRING) as EMAIL_ADDRESS", \
	"CAST(PrimaryPhone AS STRING) as PRIMARY_PHONE", \
	"CAST(PrimaryPhoneType AS STRING) as PRIMARY_PHONE_TYPE", \
	"CAST(AlternatePhone AS STRING) as ALTERNATE_PHONE", \
	"CAST(AlternatePhoneType AS STRING) as ALTERNATE_PHONE_TYPE", \
	"CAST(CreateDateTime AS TIMESTAMP) as CREATE_DATE_TIME", \
	"CAST(ExternalCustomerId as BIGINT) as EXTERNAL_CUSTOMER_ID", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_TRAINING_CUSTOMER_PRE.write.saveAsTable(f'{sensitive}.raw_TRAINING_CUSTOMER_PRE', mode = 'overwrite')
	logPrevRunDt("legacy_TRAINING_CUSTOMER_PRE", "legacy_TRAINING_CUSTOMER_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("legacy_TRAINING_CUSTOMER_PRE", "legacy_TRAINING_CUSTOMER_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e

