#Code converted on 2023-07-19 17:01:52
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
#from pyspark.dbutils import DBUtils
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
cust_sensitive = getCustSensitivePrefix(env) + 'cust_sensitive'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# COMMAND ----------

# Processing node SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE = spark.sql(f"""SELECT
CREATE_DT,
TP_CUSTOMER_NBR,
TP_INVOICE_NBR,
SKU_DESC,
SAP_CLASS_DESC,
LOCATION_ID,
STORE_NBR,
COUNTRY_CD,
CUST_FIRST_NAME,
CUST_LAST_NAME,
CUST_EMAIL_ADDRESS,
SRC_CC_ID,
COMMUNICATION_CHANNEL,
EMAIL_OPT_OUT_FLAG,
LOAD_TSTMP
FROM {cust_sensitive}.raw_SRC_SERVICES_RESERVATION_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SRC_SERVICES_RESERVATION, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SRC_SERVICES_RESERVATION = spark.sql(f"""SELECT
CREATE_DT,
TP_CUSTOMER_NBR
FROM {cust_sensitive}.legacy_SRC_SERVICES_RESERVATION""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_NEW_RECORDS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SRC_SERVICES_RESERVATION_temp = SQ_Shortcut_to_SRC_SERVICES_RESERVATION.toDF(*["SQ_Shortcut_to_SRC_SERVICES_RESERVATION___" + col for col in SQ_Shortcut_to_SRC_SERVICES_RESERVATION.columns])
SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE_temp = SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE.toDF(*["SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___" + col for col in SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE.columns])

JNR_NEW_RECORDS = SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE_temp.join(SQ_Shortcut_to_SRC_SERVICES_RESERVATION_temp,[SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE_temp.SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___CREATE_DT == SQ_Shortcut_to_SRC_SERVICES_RESERVATION_temp.SQ_Shortcut_to_SRC_SERVICES_RESERVATION___CREATE_DT, SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE_temp.SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___TP_CUSTOMER_NBR == SQ_Shortcut_to_SRC_SERVICES_RESERVATION_temp.SQ_Shortcut_to_SRC_SERVICES_RESERVATION___TP_CUSTOMER_NBR],'left_outer').selectExpr(
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___CREATE_DT as CREATE_DT",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___SKU_DESC as SKU_DESC",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___STORE_NBR as STORE_NBR",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___COUNTRY_CD as COUNTRY_CD",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___CUST_LAST_NAME as CUST_LAST_NAME",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___SRC_CC_ID as SRC_CC_ID",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___COMMUNICATION_CHANNEL as COMMUNICATION_CHANNEL",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___EMAIL_OPT_OUT_FLAG as EMAIL_OPT_OUT_FLAG",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION_PRE___LOAD_TSTMP as LOAD_TSTMP",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION___CREATE_DT as CREATE_DT1",
	"SQ_Shortcut_to_SRC_SERVICES_RESERVATION___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR1")

# COMMAND ----------

# Processing node FIL_NEW_RECORDS, type FILTER 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
JNR_NEW_RECORDS_temp = JNR_NEW_RECORDS.toDF(*["JNR_NEW_RECORDS___" + col for col in JNR_NEW_RECORDS.columns])

FIL_NEW_RECORDS = JNR_NEW_RECORDS_temp.selectExpr(
	"JNR_NEW_RECORDS___CREATE_DT as CREATE_DT",
	"JNR_NEW_RECORDS___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"JNR_NEW_RECORDS___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"JNR_NEW_RECORDS___SKU_DESC as SKU_DESC",
	"JNR_NEW_RECORDS___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"JNR_NEW_RECORDS___LOCATION_ID as LOCATION_ID",
	"JNR_NEW_RECORDS___STORE_NBR as STORE_NBR",
	"JNR_NEW_RECORDS___COUNTRY_CD as COUNTRY_CD",
	"JNR_NEW_RECORDS___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"JNR_NEW_RECORDS___CUST_LAST_NAME as CUST_LAST_NAME",
	"JNR_NEW_RECORDS___CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"JNR_NEW_RECORDS___SRC_CC_ID as SRC_CC_ID",
	"JNR_NEW_RECORDS___COMMUNICATION_CHANNEL as COMMUNICATION_CHANNEL",
	"JNR_NEW_RECORDS___EMAIL_OPT_OUT_FLAG as EMAIL_OPT_OUT_FLAG",
	"JNR_NEW_RECORDS___LOAD_TSTMP as LOAD_TSTMP",
	"JNR_NEW_RECORDS___CREATE_DT1 as CREATE_DT1",
	"JNR_NEW_RECORDS___TP_CUSTOMER_NBR1 as TP_CUSTOMER_NBR1").filter("CREATE_DT1 is Null").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_SENT_FLAG, type EXPRESSION 
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
FIL_NEW_RECORDS_temp = FIL_NEW_RECORDS.toDF(*["FIL_NEW_RECORDS___" + col for col in FIL_NEW_RECORDS.columns])

EXP_SENT_FLAG = FIL_NEW_RECORDS_temp.selectExpr(
	"FIL_NEW_RECORDS___sys_row_id as sys_row_id",
	"FIL_NEW_RECORDS___CREATE_DT as CREATE_DT",
	"FIL_NEW_RECORDS___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"FIL_NEW_RECORDS___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"FIL_NEW_RECORDS___SKU_DESC as SKU_DESC",
	"FIL_NEW_RECORDS___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"FIL_NEW_RECORDS___LOCATION_ID as LOCATION_ID",
	"FIL_NEW_RECORDS___STORE_NBR as STORE_NBR",
	"FIL_NEW_RECORDS___COUNTRY_CD as COUNTRY_CD",
	"FIL_NEW_RECORDS___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"FIL_NEW_RECORDS___CUST_LAST_NAME as CUST_LAST_NAME",
	"FIL_NEW_RECORDS___CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"FIL_NEW_RECORDS___SRC_CC_ID as SRC_CC_ID",
	"FIL_NEW_RECORDS___COMMUNICATION_CHANNEL as COMMUNICATION_CHANNEL",
	"FIL_NEW_RECORDS___EMAIL_OPT_OUT_FLAG as EMAIL_OPT_OUT_FLAG",
	"0 as SENT_FLAG",
	"FIL_NEW_RECORDS___LOAD_TSTMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_SRC_SERVICES_RESERVATION1, type TARGET 
# COLUMN COUNT: 16


Shortcut_to_SRC_SERVICES_RESERVATION1 = EXP_SENT_FLAG.selectExpr(
	"CAST(CREATE_DT AS DATE) as CREATE_DT",
	"TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"TP_INVOICE_NBR as TP_INVOICE_NBR",
	"CAST(SKU_DESC AS STRING) as SKU_DESC",
	"CAST(SAP_CLASS_DESC AS STRING) as SAP_CLASS_DESC",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(STORE_NBR AS INT) as STORE_NBR",
	"CAST(COUNTRY_CD AS STRING) as COUNTRY_CD",
	"CAST(CUST_FIRST_NAME AS STRING) as CUST_FIRST_NAME",
	"CAST(CUST_LAST_NAME AS STRING) as CUST_LAST_NAME",
	"CAST(CUST_EMAIL_ADDRESS AS STRING) as CUST_EMAIL_ADDRESS",
	"CAST(SRC_CC_ID AS STRING) as SRC_CC_ID",
	"CAST(COMMUNICATION_CHANNEL AS STRING) as COMMUNICATION_CHANNEL",
	"CAST(EMAIL_OPT_OUT_FLAG AS INT) as EMAIL_OPT_OUT_FLAG",
	"CAST(SENT_FLAG AS INT) as SENT_FLAG",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)


Shortcut_to_SRC_SERVICES_RESERVATION1.write.mode("append").saveAsTable(f'{cust_sensitive}.legacy_SRC_SERVICES_RESERVATION')