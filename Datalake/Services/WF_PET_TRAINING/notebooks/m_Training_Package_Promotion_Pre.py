#Code converted on 2023-08-03 13:28:24
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

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = pettraining_prd_sqlServer_training(env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_PackagePromotion, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_PackagePromotion = jdbcSqlServerConnection(f"""(SELECT
PackagePromotion.PackagePromotionId,
PackagePromotion.Name,
PackagePromotion.Description,
PackagePromotion.DiscountAmount,
PackagePromotion.StartDate,
PackagePromotion.EndDate,
PackagePromotion.PackageId,
PackagePromotion.CountryAbbreviation
FROM Training.dbo.PackagePromotion) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_PackagePromotion_temp = SQ_Shortcut_to_PackagePromotion.toDF(*["SQ_Shortcut_to_PackagePromotion___" + col for col in SQ_Shortcut_to_PackagePromotion.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_PackagePromotion_temp.selectExpr( \
	"SQ_Shortcut_to_PackagePromotion___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_PackagePromotion___PackagePromotionId as PackagePromotionId", \
	"SQ_Shortcut_to_PackagePromotion___Name as Name", \
	"SQ_Shortcut_to_PackagePromotion___Description as Description", \
	"SQ_Shortcut_to_PackagePromotion___DiscountAmount as DiscountAmount", \
	"SQ_Shortcut_to_PackagePromotion___StartDate as StartDate", \
	"SQ_Shortcut_to_PackagePromotion___EndDate as EndDate", \
	"SQ_Shortcut_to_PackagePromotion___PackageId as PackageId", \
	"SQ_Shortcut_to_PackagePromotion___CountryAbbreviation as CountryAbbreviation", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE = EXP_LOAD_TSTMP.selectExpr( \
	"CAST(PackagePromotionId AS INT) as PACKAGE_PROMOTION_ID", \
	"CAST(Name AS STRING) as NAME", \
	"CAST(Description AS STRING) as DESCRIPTION", \
	"CAST(DiscountAmount AS DECIMAL(19,4)) as DISCOUNT_AMOUNT", \
	"CAST(StartDate AS DATE) as START_DATE", \
	"CAST(EndDate AS DATE) as END_DATE", \
	"CAST(PackageId AS INT) as PACKAGE_ID", \
	"CAST(CountryAbbreviation AS STRING) as COUNTRY_ABBREVIATION", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE.write.saveAsTable(f'{raw}.TRAINING_PACKAGE_PROMOTION_PRE', mode = 'overwrite')