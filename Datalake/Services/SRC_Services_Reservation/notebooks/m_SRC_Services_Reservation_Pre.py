#Code converted on 2023-07-19 17:01:54
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
cust_sensitive = getCustSensitivePrefix(env) + 'cust_sensitive'
empl_sensitive = getCustSensitivePrefix(env) + 'empl_sensitive'
enterprise = getEnvPrefix(env) + 'enterprise'

# Set global variables
starttime = datetime.now() #start timestamp of the script


# COMMAND ----------

# Processing node SQ_Shortcut_to_TP_CUSTOMER, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_TP_CUSTOMER = spark.sql(f"""SELECT
TP_CUSTOMER_NBR,
CUST_EMAIL_ADDRESS
FROM {cust_sensitive}.REFINE_TP_CUSTOMER""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_CUSTOMER_XREF, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_CUSTOMER_XREF = spark.sql(f"""SELECT
CUSTOMER_EID,
CUSTOMER_SRC_ID,
CUSTOMER_SRC_VALUE,
ACTIVE_FLG
FROM {legacy}.CUSTOMER_XREF""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT = spark.sql(f"""SELECT
TP_INVOICE_NBR,
PRODUCT_ID,
LOCATION_ID,
TP_CUSTOMER_NBR,
CUST_FIRST_NAME,
CUST_LAST_NAME
FROM {cust_sensitive}.REFINE_TP_INVOICE_SERVICE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_To_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_To_SITE_PROFILE = spark.sql(f"SELECT LOCATION_ID, STORE_NBR, COUNTRY_CD FROM {legacy}.site_profile").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node SQ_Shortcut_to_CUSTOMER_LOYALTY, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_CUSTOMER_LOYALTY = spark.sql(f"""SELECT
CUSTOMER_EID,
FIRST_NAME,
LAST_NAME,
LOYALTY_EMAIL,
EMAIL_OPT_OUT_FLAG
FROM {legacy}.CUSTOMER_LOYALTY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_SKU_PROFILE_RPT = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_DESC,
SAP_CATEGORY_ID,
SAP_CLASS_ID,
SAP_CLASS_DESC
FROM {legacy}.SKU_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_ACTIVE_FLAG, type FILTER 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_CUSTOMER_XREF_temp = SQ_Shortcut_to_CUSTOMER_XREF.toDF(*["SQ_Shortcut_to_CUSTOMER_XREF___" + col for col in SQ_Shortcut_to_CUSTOMER_XREF.columns])

FIL_ACTIVE_FLAG = SQ_Shortcut_to_CUSTOMER_XREF_temp.selectExpr(
	"SQ_Shortcut_to_CUSTOMER_XREF___CUSTOMER_EID as CUSTOMER_EID",
	"SQ_Shortcut_to_CUSTOMER_XREF___CUSTOMER_SRC_ID as CUSTOMER_SRC_ID",
	"SQ_Shortcut_to_CUSTOMER_XREF___CUSTOMER_SRC_VALUE as CUSTOMER_SRC_VALUE",
	"SQ_Shortcut_to_CUSTOMER_XREF___ACTIVE_FLG as ACTIVE_FLG").filter("ACTIVE_FLG = 1 and CUSTOMER_SRC_ID = 100006").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_BOARDING_SKUS, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SKU_PROFILE_RPT_temp = SQ_Shortcut_to_SKU_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT.columns])

FIL_BOARDING_SKUS = SQ_Shortcut_to_SKU_PROFILE_RPT_temp.selectExpr(
	"SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SKU_DESC as SKU_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_CATEGORY_ID as SAP_CATEGORY_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_CLASS_ID as SAP_CLASS_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_CLASS_DESC as SAP_CLASS_DESC").filter("SAP_CLASS_ID = 824").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_CUST_SRC_ID, type EXPRESSION 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_CUSTOMER_LOYALTY_temp = SQ_Shortcut_to_CUSTOMER_LOYALTY.toDF(*["SQ_Shortcut_to_CUSTOMER_LOYALTY___" + col for col in SQ_Shortcut_to_CUSTOMER_LOYALTY.columns])

EXP_CUST_SRC_ID = SQ_Shortcut_to_CUSTOMER_LOYALTY_temp.selectExpr(
	"SQ_Shortcut_to_CUSTOMER_LOYALTY___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_CUSTOMER_LOYALTY___CUSTOMER_EID as CUSTOMER_EID",
	"SQ_Shortcut_to_CUSTOMER_LOYALTY___FIRST_NAME as FIRST_NAME",
	"SQ_Shortcut_to_CUSTOMER_LOYALTY___LAST_NAME as LAST_NAME",
	"SQ_Shortcut_to_CUSTOMER_LOYALTY___LOYALTY_EMAIL as LOYALTY_EMAIL",
	"SQ_Shortcut_to_CUSTOMER_LOYALTY___EMAIL_OPT_OUT_FLAG as EMAIL_OPT_OUT_FLAG",
	"100006 as CUSTOMER_SRC_ID"
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_TP_HISTORY, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_TP_HISTORY = spark.sql(f"""SELECT
TP_HISTORY.HISTORY_DT,
TP_HISTORY.TP_INVOICE_NBR,
TP_HISTORY.USER_NAME,
TP_HISTORY.CHANGE_DT,
TP_HISTORY.RESERVATION_ORIGIN_IND,
TP_HISTORY.TP_HIST_ACTION
FROM {refine}.TP_HISTORY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_CREATE, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TP_HISTORY_temp = SQ_Shortcut_to_TP_HISTORY.toDF(*["SQ_Shortcut_to_TP_HISTORY___" + col for col in SQ_Shortcut_to_TP_HISTORY.columns])

FIL_CREATE = SQ_Shortcut_to_TP_HISTORY_temp.selectExpr(
	"SQ_Shortcut_to_TP_HISTORY___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"SQ_Shortcut_to_TP_HISTORY___USER_NAME as USER_NAME",
	"SQ_Shortcut_to_TP_HISTORY___CHANGE_DT as CREATE_DT",
	"SQ_Shortcut_to_TP_HISTORY___RESERVATION_ORIGIN_IND as RESERVATION_ORIGIN_IND",
	"SQ_Shortcut_to_TP_HISTORY___TP_HIST_ACTION as TP_HIST_ACTION",
	"SQ_Shortcut_to_TP_HISTORY___HISTORY_DT as HISTORY_DT").filter("LOWER(TP_HIST_ACTION) = 'create' AND RESERVATION_ORIGIN_IND = 1 AND CREATE_DT >= (CURRENT_TIMESTAMP() - INTERVAL 10 DAYS)").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_INTEGER_TP, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
FIL_ACTIVE_FLAG_temp = FIL_ACTIVE_FLAG.toDF(*["FIL_ACTIVE_FLAG___" + col for col in FIL_ACTIVE_FLAG.columns])

EXP_INTEGER_TP = FIL_ACTIVE_FLAG_temp.selectExpr(
	"FIL_ACTIVE_FLAG___CUSTOMER_EID as CUSTOMER_EID",
	"FIL_ACTIVE_FLAG___CUSTOMER_SRC_ID as CUSTOMER_SRC_ID",
	"CAST(FIL_ACTIVE_FLAG___CUSTOMER_SRC_VALUE AS BIGINT) as TP_CUSTOMER_NBR",
	"FIL_ACTIVE_FLAG___sys_row_id as sys_row_id"
)

# COMMAND ----------

# Processing node JNR_CUST_LOYALTY, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
EXP_CUST_SRC_ID_temp = EXP_CUST_SRC_ID.toDF(*["EXP_CUST_SRC_ID___" + col for col in EXP_CUST_SRC_ID.columns])
EXP_INTEGER_TP_temp = EXP_INTEGER_TP.toDF(*["EXP_INTEGER_TP___" + col for col in EXP_INTEGER_TP.columns])

JNR_CUST_LOYALTY = EXP_INTEGER_TP_temp.join(EXP_CUST_SRC_ID_temp,[EXP_INTEGER_TP_temp.EXP_INTEGER_TP___CUSTOMER_EID == EXP_CUST_SRC_ID_temp.EXP_CUST_SRC_ID___CUSTOMER_EID, EXP_INTEGER_TP_temp.EXP_INTEGER_TP___CUSTOMER_SRC_ID == EXP_CUST_SRC_ID_temp.EXP_CUST_SRC_ID___CUSTOMER_SRC_ID],'inner').selectExpr(
	"EXP_CUST_SRC_ID___CUSTOMER_EID as CUSTOMER_EID",
	"EXP_CUST_SRC_ID___FIRST_NAME as FIRST_NAME",
	"EXP_CUST_SRC_ID___LAST_NAME as LAST_NAME",
	"EXP_CUST_SRC_ID___LOYALTY_EMAIL as LOYALTY_EMAIL",
	"EXP_CUST_SRC_ID___EMAIL_OPT_OUT_FLAG as EMAIL_OPT_OUT_FLAG",
	"EXP_CUST_SRC_ID___CUSTOMER_SRC_ID as CUSTOMER_SRC_ID",
	"EXP_INTEGER_TP___CUSTOMER_EID as CUSTOMER_EID1",
	"EXP_INTEGER_TP___CUSTOMER_SRC_ID as CUSTOMER_SRC_ID1",
	"EXP_INTEGER_TP___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR")

# COMMAND ----------

# Processing node JNR_INVOICE_CREATE_RES, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
FIL_CREATE_temp = FIL_CREATE.toDF(*["FIL_CREATE___" + col for col in FIL_CREATE.columns])
SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT_temp = SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT.toDF(*["SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT___" + col for col in SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT.columns])

JNR_INVOICE_CREATE_RES = SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT_temp.join(FIL_CREATE_temp,[SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT_temp.SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT___TP_INVOICE_NBR == FIL_CREATE_temp.FIL_CREATE___TP_INVOICE_NBR],'inner').selectExpr(
	"FIL_CREATE___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"FIL_CREATE___USER_NAME as USER_NAME",
	"FIL_CREATE___CREATE_DT as CREATE_DT",
	"SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT___TP_INVOICE_NBR as TP_INVOICE_NBR1",
	"SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"SQ_Shortcut_to_TP_INVOICE_SERVICE_RPT___CUST_LAST_NAME as CUST_LAST_NAME",
	"FIL_CREATE___HISTORY_DT as HISTORY_DT")

# COMMAND ----------

# Processing node JNR_SKU_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
FIL_BOARDING_SKUS_temp = FIL_BOARDING_SKUS.toDF(*["FIL_BOARDING_SKUS___" + col for col in FIL_BOARDING_SKUS.columns])
JNR_INVOICE_CREATE_RES_temp = JNR_INVOICE_CREATE_RES.toDF(*["JNR_INVOICE_CREATE_RES___" + col for col in JNR_INVOICE_CREATE_RES.columns])

JNR_SKU_PROFILE = FIL_BOARDING_SKUS_temp.join(JNR_INVOICE_CREATE_RES_temp,[FIL_BOARDING_SKUS_temp.FIL_BOARDING_SKUS___PRODUCT_ID == JNR_INVOICE_CREATE_RES_temp.JNR_INVOICE_CREATE_RES___PRODUCT_ID],'inner').selectExpr(
	"JNR_INVOICE_CREATE_RES___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"JNR_INVOICE_CREATE_RES___USER_NAME as USER_NAME",
	"JNR_INVOICE_CREATE_RES___CREATE_DT as CREATE_DT",
	"JNR_INVOICE_CREATE_RES___PRODUCT_ID as PRODUCT_ID",
	"JNR_INVOICE_CREATE_RES___LOCATION_ID as LOCATION_ID",
	"JNR_INVOICE_CREATE_RES___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"JNR_INVOICE_CREATE_RES___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"JNR_INVOICE_CREATE_RES___CUST_LAST_NAME as CUST_LAST_NAME",
	"JNR_INVOICE_CREATE_RES___HISTORY_DT as HISTORY_DT",
	"FIL_BOARDING_SKUS___PRODUCT_ID as PRODUCT_ID1",
	"FIL_BOARDING_SKUS___SKU_DESC as SKU_DESC",
	"FIL_BOARDING_SKUS___SAP_CLASS_DESC as SAP_CLASS_DESC")

# COMMAND ----------

# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
JNR_SKU_PROFILE_temp = JNR_SKU_PROFILE.toDF(*["JNR_SKU_PROFILE___" + col for col in JNR_SKU_PROFILE.columns])
SQ_Shortcut_To_SITE_PROFILE_temp = SQ_Shortcut_To_SITE_PROFILE.toDF(*["SQ_Shortcut_To_SITE_PROFILE___" + col for col in SQ_Shortcut_To_SITE_PROFILE.columns])

JNR_SITE_PROFILE = SQ_Shortcut_To_SITE_PROFILE_temp.join(JNR_SKU_PROFILE_temp,[SQ_Shortcut_To_SITE_PROFILE_temp.SQ_Shortcut_To_SITE_PROFILE___LOCATION_ID == JNR_SKU_PROFILE_temp.JNR_SKU_PROFILE___LOCATION_ID],'inner').selectExpr(
	"JNR_SKU_PROFILE___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"JNR_SKU_PROFILE___USER_NAME as USER_NAME",
	"JNR_SKU_PROFILE___CREATE_DT as CREATE_DT",
	"JNR_SKU_PROFILE___PRODUCT_ID as PRODUCT_ID",
	"JNR_SKU_PROFILE___LOCATION_ID as LOCATION_ID",
	"JNR_SKU_PROFILE___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"JNR_SKU_PROFILE___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"JNR_SKU_PROFILE___CUST_LAST_NAME as CUST_LAST_NAME",
	"JNR_SKU_PROFILE___SKU_DESC as SKU_DESC",
	"JNR_SKU_PROFILE___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"JNR_SKU_PROFILE___HISTORY_DT as HISTORY_DT",
	"SQ_Shortcut_To_SITE_PROFILE___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_To_SITE_PROFILE___STORE_NBR as STORE_NBR",
	"SQ_Shortcut_To_SITE_PROFILE___COUNTRY_CD as COUNTRY_CD")

# COMMAND ----------

# Processing node JNR_TP_CUSTOMER, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_TP_CUSTOMER_temp = SQ_Shortcut_to_TP_CUSTOMER.toDF(*["SQ_Shortcut_to_TP_CUSTOMER___" + col for col in SQ_Shortcut_to_TP_CUSTOMER.columns])

JNR_TP_CUSTOMER = SQ_Shortcut_to_TP_CUSTOMER_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_TP_CUSTOMER_temp.SQ_Shortcut_to_TP_CUSTOMER___TP_CUSTOMER_NBR == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___TP_CUSTOMER_NBR],'inner').selectExpr(
	"JNR_SITE_PROFILE___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"JNR_SITE_PROFILE___USER_NAME as USER_NAME",
	"JNR_SITE_PROFILE___CREATE_DT as CREATE_DT",
	"JNR_SITE_PROFILE___PRODUCT_ID as PRODUCT_ID",
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID",
	"JNR_SITE_PROFILE___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"JNR_SITE_PROFILE___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"JNR_SITE_PROFILE___CUST_LAST_NAME as CUST_LAST_NAME",
	"JNR_SITE_PROFILE___SKU_DESC as SKU_DESC",
	"JNR_SITE_PROFILE___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"JNR_SITE_PROFILE___STORE_NBR as STORE_NBR",
	"JNR_SITE_PROFILE___COUNTRY_CD as COUNTRY_CD",
	"JNR_SITE_PROFILE___HISTORY_DT as HISTORY_DT",
	"SQ_Shortcut_to_TP_CUSTOMER___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR1",
	"SQ_Shortcut_to_TP_CUSTOMER___CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS")

# COMMAND ----------

# Processing node JNR_LOYALTY, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
JNR_CUST_LOYALTY_temp = JNR_CUST_LOYALTY.toDF(*["JNR_CUST_LOYALTY___" + col for col in JNR_CUST_LOYALTY.columns])
JNR_TP_CUSTOMER_temp = JNR_TP_CUSTOMER.toDF(*["JNR_TP_CUSTOMER___" + col for col in JNR_TP_CUSTOMER.columns])

JNR_LOYALTY = JNR_CUST_LOYALTY_temp.join(JNR_TP_CUSTOMER_temp,[JNR_CUST_LOYALTY_temp.JNR_CUST_LOYALTY___TP_CUSTOMER_NBR == JNR_TP_CUSTOMER_temp.JNR_TP_CUSTOMER___TP_CUSTOMER_NBR],'right_outer').selectExpr(
	"JNR_TP_CUSTOMER___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"JNR_TP_CUSTOMER___USER_NAME as USER_NAME",
	"JNR_TP_CUSTOMER___CREATE_DT as CREATE_DT",
	"JNR_TP_CUSTOMER___PRODUCT_ID as PRODUCT_ID",
	"JNR_TP_CUSTOMER___LOCATION_ID as LOCATION_ID",
	"JNR_TP_CUSTOMER___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"JNR_TP_CUSTOMER___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"JNR_TP_CUSTOMER___CUST_LAST_NAME as CUST_LAST_NAME",
	"JNR_TP_CUSTOMER___SKU_DESC as SKU_DESC",
	"JNR_TP_CUSTOMER___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"JNR_TP_CUSTOMER___STORE_NBR as STORE_NBR",
	"JNR_TP_CUSTOMER___COUNTRY_CD as COUNTRY_CD",
	"JNR_TP_CUSTOMER___CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"JNR_TP_CUSTOMER___HISTORY_DT as HISTORY_DT",
	"JNR_CUST_LOYALTY___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR1",
	"JNR_CUST_LOYALTY___CUSTOMER_EID as CUSTOMER_EID",
	"JNR_CUST_LOYALTY___FIRST_NAME as FIRST_NAME",
	"JNR_CUST_LOYALTY___LAST_NAME as LAST_NAME",
	"JNR_CUST_LOYALTY___LOYALTY_EMAIL as LOYALTY_EMAIL",
	"JNR_CUST_LOYALTY___EMAIL_OPT_OUT_FLAG as EMAIL_OPT_OUT_FLAG").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_CUST_DATA, type EXPRESSION 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
JNR_LOYALTY_temp = JNR_LOYALTY.toDF(*["JNR_LOYALTY___" + col for col in JNR_LOYALTY.columns])

EXP_CUST_DATA = JNR_LOYALTY_temp.selectExpr(
	"JNR_LOYALTY___sys_row_id as sys_row_id",
	"JNR_LOYALTY___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"JNR_LOYALTY___USER_NAME as USER_NAME",
	"JNR_LOYALTY___CREATE_DT as CREATE_DT",
	"JNR_LOYALTY___LOCATION_ID as LOCATION_ID",
	"JNR_LOYALTY___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"IF(JNR_LOYALTY___FIRST_NAME IS NULL, JNR_LOYALTY___CUST_FIRST_NAME, JNR_LOYALTY___FIRST_NAME) as o_CUST_FIRST_NAME",
	"IF(JNR_LOYALTY___LAST_NAME IS NULL, JNR_LOYALTY___CUST_LAST_NAME, JNR_LOYALTY___LAST_NAME) as o_CUST_LAST_NAME",
	"JNR_LOYALTY___SKU_DESC as SKU_DESC",
	"JNR_LOYALTY___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"JNR_LOYALTY___STORE_NBR as STORE_NBR",
	"JNR_LOYALTY___COUNTRY_CD as COUNTRY_CD",
	"IF(JNR_LOYALTY___LOYALTY_EMAIL IS NULL, JNR_LOYALTY___CUST_EMAIL_ADDRESS, JNR_LOYALTY___LOYALTY_EMAIL) as o_CUST_EMAIL_ADDRESS",
	"IF(JNR_LOYALTY___EMAIL_OPT_OUT_FLAG IS NULL, 0, JNR_LOYALTY___EMAIL_OPT_OUT_FLAG) as o_EMAIL_OPT_OUT_FLAG",
	"'phone' as COMMUNICATION_CHANNEL",
	"CURRENT_TIMESTAMP() as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node FIL_NULL_EMAILS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
EXP_CUST_DATA_temp = EXP_CUST_DATA.toDF(*["EXP_CUST_DATA___" + col for col in EXP_CUST_DATA.columns])

FIL_NULL_EMAILS = EXP_CUST_DATA_temp.selectExpr(
	"EXP_CUST_DATA___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"EXP_CUST_DATA___USER_NAME as USER_NAME",
	"EXP_CUST_DATA___CREATE_DT as CREATE_DT",
	"EXP_CUST_DATA___LOCATION_ID as LOCATION_ID",
	"EXP_CUST_DATA___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"EXP_CUST_DATA___o_CUST_FIRST_NAME as CUST_FIRST_NAME",
	"EXP_CUST_DATA___o_CUST_LAST_NAME as CUST_LAST_NAME",
	"EXP_CUST_DATA___SKU_DESC as SKU_DESC",
	"EXP_CUST_DATA___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"EXP_CUST_DATA___STORE_NBR as STORE_NBR",
	"EXP_CUST_DATA___COUNTRY_CD as COUNTRY_CD",
	"EXP_CUST_DATA___o_CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"EXP_CUST_DATA___o_EMAIL_OPT_OUT_FLAG as EMAIL_OPT_OUT_FLAG",
	"EXP_CUST_DATA___COMMUNICATION_CHANNEL as COMMUNICATION_CHANNEL",
	"EXP_CUST_DATA___LOAD_TSTMP as LOAD_TSTMP").filter("CUST_EMAIL_ADDRESS IS NOT NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node RNK_SINGLE_RECORD_PER_CUSTOMER, type RANK 
# COLUMN COUNT: 32

RNK_SINGLE_RECORD_PER_CUSTOMER = FIL_NULL_EMAILS.withColumn('RANKINDEX', row_number().over(Window.partitionBy(col('TP_CUSTOMER_NBR'), col('CREATE_DT')).orderBy(col('TP_INVOICE_NBR'), col('SKU_DESC').desc()))).filter('RANKINDEX <= 1')

# COMMAND ----------

# Processing node Shortcut_to_SRC_SERVICES_RESERVATION_PRE, type TARGET 
# COLUMN COUNT: 15


Shortcut_to_SRC_SERVICES_RESERVATION_PRE = RNK_SINGLE_RECORD_PER_CUSTOMER.selectExpr(
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
	"CAST(USER_NAME AS STRING) as SRC_CC_ID",
	"CAST(COMMUNICATION_CHANNEL AS STRING) as COMMUNICATION_CHANNEL",
	"CAST(EMAIL_OPT_OUT_FLAG AS INT) as EMAIL_OPT_OUT_FLAG",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)

																																 

Shortcut_to_SRC_SERVICES_RESERVATION_PRE.write.mode("overwrite").saveAsTable(f'{cust_sensitive}.raw_SRC_SERVICES_RESERVATION_PRE')
