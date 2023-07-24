#Code converted on 2023-07-21 14:57:53
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


# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_PSVC_ORDER_PRODUCT_SPECIAL_C_PRE, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_SDS_PSVC_ORDER_PRODUCT_SPECIAL_C_PRE = spark.sql(f"""SELECT
ID
FROM {raw}.SDS_PSVC_ORDER_PRODUCT_SPECIAL_C_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL = spark.sql(f"""SELECT
SDS_ORDER_PRODUCT_SPECIAL_ID
FROM {legacy}.SDS_ORDER_PRODUCT_SPECIAL
WHERE HARD_DELETED_FLAG=1""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_ORDER_PRODUCT_SPECIAL, type JOINER 
# COLUMN COUNT: 2

JNR_ORDER_PRODUCT_SPECIAL = SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL.join(SQ_Shortcut_to_SDS_PSVC_ORDER_PRODUCT_SPECIAL_C_PRE,[SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL.SDS_ORDER_PRODUCT_SPECIAL_ID == SQ_Shortcut_to_SDS_PSVC_ORDER_PRODUCT_SPECIAL_C_PRE.ID],'inner')

# COMMAND ----------
# Processing node SRTTRANS, type SORTER 
# COLUMN COUNT: 1

SRTTRANS = JNR_ORDER_PRODUCT_SPECIAL.select(
).sort(col('SDS_ORDER_PRODUCT_SPECIAL_ID').asc())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SRTTRANS_temp = SRTTRANS.toDF(*["SRTTRANS___" + col for col in SRTTRANS.columns])

EXPTRANS = SRTTRANS_temp.selectExpr(
	"SRTTRANS___sys_row_id as sys_row_id",
	"SRTTRANS___SDS_ORDER_PRODUCT_SPECIAL_ID as SDS_ORDER_PRODUCT_SPECIAL_ID",
	"0 as HARD_DELETE_FLAG",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP"
)

# COMMAND ----------
# Processing node UPD_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])

UPD_UPDATE = EXPTRANS_temp.selectExpr(
	"EXPTRANS___SDS_ORDER_PRODUCT_SPECIAL_ID as SDS_ORDER_PRODUCT_SPECIAL_ID",
	"EXPTRANS___HARD_DELETE_FLAG as HARD_DELETE_FLAG",
	"EXPTRANS___UPDATE_TSTMP as UPDATE_TSTMP")\
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL2, type TARGET 
# COLUMN COUNT: 28


Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL2 = UPD_UPDATE.selectExpr(
	"CAST(SDS_ORDER_PRODUCT_SPECIAL_ID AS STRING) as SDS_ORDER_PRODUCT_SPECIAL_ID",
	"CAST(NULL AS STRING) as SDS_ORDER_PRODUCT_SPECIAL_NAME",
	"CAST(NULL AS STRING) as SDS_ORDER_ITEM_ID",
	"CAST(NULL AS STRING) as SDS_SPECIAL_ID",
	"CAST(NULL AS STRING) as SDS_SPECIAL_CD",
	"CAST(NULL AS STRING) as MANUALLY_ENTERED_CD",
	"CAST(NULL AS STRING) as PREMIUM_DISCOUNT",
	"CAST(NULL AS DECIMAL(18,3)) as ADJUSTMENT_AMT",
	"CAST(NULL AS DECIMAL(18,3)) as ADJUSTMENT_PCT",
	"CAST(NULL AS DECIMAL(18,3)) as APPLIED_PCT_DISCOUNT_AMT",
	"CAST(NULL AS TINYINT) as EVALUATION_ORDER_NBR",
	"CAST(NULL AS DECIMAL(18,2)) as AUTO_CALCULATED_PRICE",
	"CAST(NULL AS TINYINT) as MANUAL_ADJUSTED_FLAG",
	"CAST(NULL AS TINYINT) as HIDE_FROM_CUSTOMER_ONLINE_INVOICE_FLAG",
	"CAST(NULL AS TINYINT) as APPLY_TO_ALL_PRODUCTS_FLAG",
	"CAST(NULL AS STRING) as DUPLICATION",
	"CAST(NULL AS TINYINT) as FOR_QUOTE_FLAG",
	"CAST(NULL AS STRING) as SDS_OWNER_ID",
	"CAST(NULL AS STRING) as CURRENCY_ISO_CD",
	"CAST(NULL AS TINYINT) as DELETED_FLAG",
	"CAST(HARD_DELETE_FLAG AS TINYINT) as HARD_DELETED_FLAG",
	"CAST(NULL AS TIMESTAMP) as SDS_SYSTEM_MODIFIED_TSTMP",
	"CAST(NULL AS TIMESTAMP) as SDS_LAST_MODIFIED_TSTMP",
	"CAST(NULL AS STRING) as SDS_LAST_MODIFIED_BY_ID",
	"CAST(NULL AS TIMESTAMP) as SDS_CREATED_TSTMP",
	"CAST(NULL AS STRING) as SDS_CREATED_BY_ID",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(NULL AS TIMESTAMP) as LOAD_TSTMP",
	"UPD_UPDATE.pyspark_data_action as pyspark_data_action"
)
Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL2.write.saveAsTable(f'{legacy}.SDS_ORDER_PRODUCT_SPECIAL')