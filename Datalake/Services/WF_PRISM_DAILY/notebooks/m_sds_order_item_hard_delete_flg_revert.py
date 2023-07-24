#Code converted on 2023-07-24 08:24:32
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
# Processing node SQ_Shortcut_to_SDS_ORDER_ITEM_PRE, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_SDS_ORDER_ITEM_PRE = spark.sql(f"""SELECT
ID
FROM {raw}.SDS_ORDER_ITEM_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_ORDER_ITEM, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SDS_ORDER_ITEM = spark.sql(f"""SELECT
SDS_ORDER_ITEM_ID,
HARD_DELETED_FLAG
FROM {legacy}.SDS_ORDER_ITEM
WHERE HARD_DELETED_FLAG=1""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SDS_ORDER_ITEM, type JOINER 
# COLUMN COUNT: 3

JNR_SDS_ORDER_ITEM = SQ_Shortcut_to_SDS_ORDER_ITEM.join(SQ_Shortcut_to_SDS_ORDER_ITEM_PRE,[SQ_Shortcut_to_SDS_ORDER_ITEM.SDS_ORDER_ITEM_ID == SQ_Shortcut_to_SDS_ORDER_ITEM_PRE.ID],'inner')

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
JNR_SDS_ORDER_ITEM_temp = JNR_SDS_ORDER_ITEM.toDF(*["JNR_SDS_ORDER_ITEM___" + col for col in JNR_SDS_ORDER_ITEM.columns])

EXPTRANS = JNR_SDS_ORDER_ITEM_temp.selectExpr(
	"JNR_SDS_ORDER_ITEM___sys_row_id as sys_row_id",
	"JNR_SDS_ORDER_ITEM___ID as ID",
	"JNR_SDS_ORDER_ITEM___SDS_ORDER_ITEM_ID as SDS_ORDER_ITEM_ID",
	"0 as O_HARD_DELETED_FLAG",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP"
)

# COMMAND ----------
# Processing node UPD_UPDATE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])

UPD_UPDATE = EXPTRANS_temp.selectExpr(
	"EXPTRANS___SDS_ORDER_ITEM_ID as SDS_ORDER_ITEM_ID",
	"EXPTRANS___O_HARD_DELETED_FLAG as HARD_DELETED_FLAG",
	"EXPTRANS___UPDATE_TSTMP as UPDATE_TSTMP")\
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_SDS_ORDER_ITEM1, type TARGET 
# COLUMN COUNT: 49

# TODO review 
Shortcut_to_SDS_ORDER_ITEM1 = UPD_UPDATE.selectExpr(
	"CAST(SDS_ORDER_ITEM_ID AS STRING) as SDS_ORDER_ITEM_ID",
	"CAST(lit(None) AS STRING) as SDS_ORDER_ITEM_DESC",
	"CAST(lit(None) AS STRING) as SDS_ORDER_ID",
	"CAST(lit(None) AS STRING) as SDS_WORK_ORDER_ID",
	"CAST(lit(None) AS STRING) as SDS_PRODUCT_ID",
	"CAST(lit(None) AS STRING) as SDS_ORDER_ITEM_NBR",
	"CAST(lit(None) AS STRING) as SDS_ORDER_PRODUCT_NBR",
	"CAST(lit(None) AS STRING) as SDS_ORIG_ORDER_ITEM_ID",
	"CAST(lit(None) AS STRING) as SDS_QUOTE_LINE_ITEM_ID",
	"CAST(lit(None) AS STRING) as SDS_PRICEBOOK_ENTRY_ID",
	"CAST(lit(None) AS TINYINT) as PREPAID_FLAG",
	"CAST(lit(None) AS DECIMAL(18,2)) as ORDER_ITEM_AVAILABLE_QTY",
	"CAST(lit(None) AS DECIMAL(18,2)) as ORDER_ITEM_QTY",
	"CAST(lit(None) AS DECIMAL(18,2)) as UNIT_PRICE",
	"CAST(lit(None) AS DECIMAL(18,2)) as POS_UNIT_PRICE",
	"CAST(lit(None) AS DECIMAL(18,2)) as LIST_PRICE",
	"CAST(lit(None) AS DECIMAL(18,2)) as ORIG_UNIT_PRICE",
	"CAST(lit(None) AS DECIMAL(18,2)) as MANUAL_ADJUSTED_PRICE",
	"CAST(lit(None) AS STRING) as PRICE_MANUAL_ADJ_REASON",
	"CAST(lit(None) AS DECIMAL(18,2)) as AUTO_CALCULATED_PRICE",
	"CAST(lit(None) AS DECIMAL(9,5)) as LINE_ITEM_TOTAL_TAX",
	"CAST(lit(None) AS DECIMAL(18,2)) as TOTAL_PRICE",
	"CAST(lit(None) AS DECIMAL(18,2)) as POS_TOTAL_PRICE",
	"CAST(lit(None) AS DECIMAL(18,2)) as TOTAL_GROOM_PAY_ELIGIBLE_PRICE",
	"CAST(lit(None) AS DECIMAL(18,2)) as COMMISSION_AMT",
	"CAST(lit(None) AS STRING) as CHANGED_COMMISSION_TO_SERVICE_RESOURCE_ID",
	"CAST(lit(None) AS STRING) as COMMISSION_RECEIVER_ASSOCIATE_NAME",
	"CAST(lit(None) AS DECIMAL(6,2)) as CHANGED_TO_COMMISSION_PCT",
	"CAST(lit(None) AS DECIMAL(18,2)) as RESOURCE_COMMISSION_AMT",
	"CAST(lit(None) AS STRING) as DUPLICATION",
	"CAST(lit(None) AS TIMESTAMP) as APPOINTMENT_TSTMP",
	"CAST(lit(None) AS STRING) as ASSOCIATE_NAME",
	"CAST(lit(None) AS DECIMAL(18,2)) as AUTO_CALCULATED_PRICE_FORMULA",
	"CAST(lit(None) AS TINYINT) as EXPRESS_FLAG",
	"CAST(lit(None) AS TINYINT) as HIDDEN_FLAG",
	"CAST(lit(None) AS TINYINT) as FOR_QUOTE_FLAG",
	"CAST(lit(None) AS TIMESTAMP) as SERVICE_TSTMP",
	"CAST(lit(None) AS TIMESTAMP) as END_TSTMP",
	"CAST(lit(None) AS STRING) as SMS_ORDER_ITEM_ID",
	"CAST(lit(None) AS STRING) as CURRENCY_ISO_CD",
	"CAST(lit(None) AS TINYINT) as DELETED_FLAG",
	"CAST(HARD_DELETED_FLAG AS TINYINT) as HARD_DELETED_FLAG",
	"CAST(lit(None) AS TIMESTAMP) as SDS_SYSTEM_MODIFIED_TSTMP",
	"CAST(lit(None) AS TIMESTAMP) as SDS_LAST_MODIFIED_TSTMP",
	"CAST(lit(None) AS STRING) as SDS_LAST_MODIFIED_BY_ID",
	"CAST(lit(None) AS TIMESTAMP) as SDS_CREATED_TSTMP",
	"CAST(lit(None) AS STRING) as SDS_CREATED_BY_ID",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(lit(None) AS TIMESTAMP) as LOAD_TSTMP",
	"UPD_UPDATE.pyspark_data_action as pyspark_data_action"
)
Shortcut_to_SDS_ORDER_ITEM1.write.saveAsTable(f'{raw}.SDS_ORDER_ITEM')