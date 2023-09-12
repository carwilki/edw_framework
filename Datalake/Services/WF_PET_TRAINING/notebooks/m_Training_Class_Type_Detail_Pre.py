#Code converted on 2023-08-03 13:28:15
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
# Variable_declaration_comment
MAX_LOAD_DATE=genPrevRunDt('TRAINING_CLASS_TYPE_DETAIL', legacy, raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_ClassTypeDetail, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_ClassTypeDetail = jdbcSqlServerConnection(f"""(SELECT
ClassTypeDetail.ClassTypeDetailId,
ClassTypeDetail.ClassTypeId,
ClassTypeDetail.Name,
ClassTypeDetail.ShortDescription,
ClassTypeDetail.Duration,
ClassTypeDetail.Price,
ClassTypeDetail.UPC,
ClassTypeDetail.InfoUrl,
ClassTypeDetail.LastModified,
ClassTypeDetail.SKU,
ClassTypeDetail.CountryAbbreviation
FROM Training.dbo.ClassTypeDetail
WHERE LastModified > '{MAX_LOAD_DATE}') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ClassTypeDetail_temp = SQ_Shortcut_to_ClassTypeDetail.toDF(*["SQ_Shortcut_to_ClassTypeDetail___" + col for col in SQ_Shortcut_to_ClassTypeDetail.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_ClassTypeDetail_temp.selectExpr( \
	"SQ_Shortcut_to_ClassTypeDetail___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_ClassTypeDetail___ClassTypeDetailId as ClassTypeDetailId", \
	"SQ_Shortcut_to_ClassTypeDetail___ClassTypeId as ClassTypeId", \
	"SQ_Shortcut_to_ClassTypeDetail___Name as Name", \
	"SQ_Shortcut_to_ClassTypeDetail___ShortDescription as ShortDescription", \
	"SQ_Shortcut_to_ClassTypeDetail___Duration as Duration", \
	"SQ_Shortcut_to_ClassTypeDetail___Price as Price", \
	"SQ_Shortcut_to_ClassTypeDetail___UPC as UPC", \
	"SQ_Shortcut_to_ClassTypeDetail___InfoUrl as InfoUrl", \
	"SQ_Shortcut_to_ClassTypeDetail___LastModified as LastModified", \
	"SQ_Shortcut_to_ClassTypeDetail___SKU as SKU", \
	"SQ_Shortcut_to_ClassTypeDetail___CountryAbbreviation as CountryAbbreviation", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_CLASS_TYPE_DETAIL_PRE, type TARGET 
# COLUMN COUNT: 12


Shortcut_to_TRAINING_CLASS_TYPE_DETAIL_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(ClassTypeDetailId AS INT) as CLASS_TYPE_DETAIL_ID", \
	"CAST(ClassTypeId AS INT) as CLASS_TYPE_ID", \
	"CAST(Name AS STRING) as NAME", \
	"CAST(ShortDescription AS STRING) as SHORT_DESCRIPTION", \
	"CAST(Duration AS INT) as DURATION", \
	"CAST(Price AS DECIMAL(19,4)) as PRICE", \
	"CAST(UPC AS STRING) as UPC", \
	"CAST(InfoUrl AS STRING) as INFO_URL", \
	"CAST(LastModified AS TIMESTAMP) as LAST_MODIFIED", \
	"CAST(SKU AS STRING) as SKU", \
	"CAST(CountryAbbreviation AS STRING) as COUNTRY_ABBREVIATION", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)
try:
	Shortcut_to_TRAINING_CLASS_TYPE_DETAIL_PRE.write.saveAsTable(f'{raw}.TRAINING_CLASS_TYPE_DETAIL_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_CLASS_TYPE_DETAIL_PRE", "TRAINING_CLASS_TYPE_DETAIL_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_CLASS_TYPE_DETAIL_PRE", "TRAINING_CLASS_TYPE_DETAIL_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e

