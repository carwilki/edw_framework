#Code converted on 2023-08-03 13:28:21
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
MAX_LOAD_DATE=genPrevRunDt('TRAINING_PACKAGE_OPTION_DETAIL',legacy,raw)

# COMMAND ----------
# Processing node SQ_Shortcut_to_PackageOptionDetail, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_PackageOptionDetail = jdbcSqlServerConnection(f"""(SELECT
PackageOptionDetail.PackageOptionDetailId,
PackageOptionDetail.PackageOptionId,
PackageOptionDetail.PackageId,
PackageOptionDetail.Name,
PackageOptionDetail.DisplayName,
PackageOptionDetail.Description,
PackageOptionDetail.UPC,
PackageOptionDetail.Price,
PackageOptionDetail.LastModified,
PackageOptionDetail.SKU,
PackageOptionDetail.CountryAbbreviation
FROM Training.dbo.PackageOptionDetail
WHERE LastModified > '{MAX_LOAD_DATE}') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_PackageOptionDetail_temp = SQ_Shortcut_to_PackageOptionDetail.toDF(*["SQ_Shortcut_to_PackageOptionDetail___" + col for col in SQ_Shortcut_to_PackageOptionDetail.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_PackageOptionDetail_temp.selectExpr( \
	"SQ_Shortcut_to_PackageOptionDetail___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_PackageOptionDetail___PackageOptionDetailId as PackageOptionDetailId", \
	"SQ_Shortcut_to_PackageOptionDetail___PackageOptionId as PackageOptionId", \
	"SQ_Shortcut_to_PackageOptionDetail___PackageId as PackageId", \
	"SQ_Shortcut_to_PackageOptionDetail___Name as Name", \
	"SQ_Shortcut_to_PackageOptionDetail___DisplayName as DisplayName", \
	"SQ_Shortcut_to_PackageOptionDetail___Description as Description", \
	"SQ_Shortcut_to_PackageOptionDetail___UPC as UPC", \
	"SQ_Shortcut_to_PackageOptionDetail___Price as Price", \
	"SQ_Shortcut_to_PackageOptionDetail___LastModified as LastModified", \
	"SQ_Shortcut_to_PackageOptionDetail___SKU as SKU", \
	"SQ_Shortcut_to_PackageOptionDetail___CountryAbbreviation as CountryAbbreviation", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_PACKAGE_OPTION_DETAIL_PRE, type TARGET 
# COLUMN COUNT: 12


Shortcut_to_TRAINING_PACKAGE_OPTION_DETAIL_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(PackageOptionDetailId AS INT) as PACKAGE_OPTION_DETAIL_ID", \
	"CAST(PackageOptionId AS INT) as PACKAGE_OPTION_ID", \
	"CAST(PackageId AS INT) as PACKAGE_ID", \
	"CAST(Name AS STRING) as NAME", \
	"CAST(DisplayName AS STRING) as DISPLAY_NAME", \
	"CAST(Description AS STRING) as DESCRIPTION", \
	"CAST(UPC AS STRING) as UPC", \
	"CAST(Price AS DECIMAL(19,4)) as PRICE", \
	"CAST(LastModified AS TIMESTAMP) as LAST_MODIFIED", \
	"CAST(SKU AS STRING) as SKU", \
	"CAST(CountryAbbreviation AS STRING) as COUNTRY_ABBREVIATION", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_TRAINING_PACKAGE_OPTION_DETAIL_PRE.write.saveAsTable(f'{raw}.TRAINING_PACKAGE_OPTION_DETAIL_PRE', mode = 'overwrite')
	logPrevRunDt("TRAINING_PACKAGE_OPTION_DETAIL_PRE", "TRAINING_PACKAGE_OPTION_DETAIL_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TRAINING_PACKAGE_OPTION_DETAIL_PRE", "TRAINING_PACKAGE_OPTION_DETAIL_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e


