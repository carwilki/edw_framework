#Code converted on 2023-07-28 11:37:27
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
cust_sensitive = getEnvPrefix(env) + 'cust_sensitive'


# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_REQUESTS, type SOURCE 
# COLUMN COUNT: 33

SQ_Shortcut_to_E_RES_REQUESTS = spark.sql(f"""SELECT
E_RES_REQUEST_ID,
E_RES_CART_ID,
E_RES_REQUEST_EXT_ID,
STORE_NBR,
CUST_FIRST_NAME,
CUST_LAST_NAME,
PRIMARY_PHONE,
SECONDARY_PHONE,
EMAIL,
ADDR1,
ADDR2,
CITY,
STATE_PROV,
POSTAL_CD,
CHECK_IN_TSTMP,
CHECK_OUT_TSTMP,
NOTES,
TOTAL_AMT,
CUST_CALLED_FLAG,
CALL_REASON,
CALL_COMMENT,
CLOSING_COMMENT,
E_RES_STATUS_ID,
DELETED_FLAG,
E_RES_SOURCE,
E_RES_LOCKED_BY,
E_RES_LOCKED_TSTMP,
E_RES_CREATED_BY,
E_RES_CREATED_TSTMP,
E_RES_UPDATED_BY,
E_RES_UPDATED_TSTMP,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {cust_sensitive}.legacy_e_res_requests""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_REQUESTS_PRE, type SOURCE 
# COLUMN COUNT: 31

SQ_Shortcut_to_E_RES_REQUESTS_PRE = spark.sql(f"""SELECT
REQUEST_ID,
CART_ID,
EXTERNAL_ID,
STORE_NUMBER,
FIRST_NAME,
LAST_NAME,
PRIMARY_PHONE,
SECONDARY_PHONE,
EMAIL,
ADDRESS1,
ADDRESS2,
CITY,
STATE_PROVINCE,
POSTAL,
CHECK_IN,
CHECK_OUT,
NOTES,
TOTAL_AMOUNT,
CUSTOMER_CALLED,
CALL_REASON,
CALL_COMMENT,
CLOSING_COMMENT,
STATUS,
LOCKED_BY,
LOCKED_DT,
CREATED_BY,
CREATED_DT,
UPDATED_BY,
UPDATED_DT,
IS_DELETED,
SOURCE
FROM {cust_sensitive}.legacy_e_res_requests_pre""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_E_RES_REQUESTS_PRE, type EXPRESSION 
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_E_RES_REQUESTS_PRE_temp = SQ_Shortcut_to_E_RES_REQUESTS_PRE.toDF(*["SQ_Shortcut_to_E_RES_REQUESTS_PRE___" + col for col in SQ_Shortcut_to_E_RES_REQUESTS_PRE.columns])

EXP_E_RES_REQUESTS_PRE = SQ_Shortcut_to_E_RES_REQUESTS_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___sys_row_id as sys_row_id", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___REQUEST_ID as REQUEST_ID", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___CART_ID as CART_ID", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___EXTERNAL_ID as EXTERNAL_ID", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___STORE_NUMBER as STORE_NUMBER", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___FIRST_NAME as FIRST_NAME", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___LAST_NAME as LAST_NAME", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___PRIMARY_PHONE as PRIMARY_PHONE", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___SECONDARY_PHONE as SECONDARY_PHONE", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___EMAIL as EMAIL", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___ADDRESS1 as ADDRESS1", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___ADDRESS2 as ADDRESS2", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___CITY as CITY", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___STATE_PROVINCE as STATE_PROVINCE", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___POSTAL as POSTAL", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___CHECK_IN as CHECK_IN", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___CHECK_OUT as CHECK_OUT", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___NOTES as NOTES", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___TOTAL_AMOUNT as TOTAL_AMOUNT", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___CUSTOMER_CALLED as CUSTOMER_CALLED", 
	"regexp_replace(SQ_Shortcut_to_E_RES_REQUESTS_PRE___CALL_REASON, '\\((\\\\r|\\\\n)\\)', '$1') as o_CALL_REASON", 
    "regexp_replace(SQ_Shortcut_to_E_RES_REQUESTS_PRE___CALL_COMMENT, '\\((\\\\r|\\\\n)\\)', '$1') as o_CALL_COMMENT", 
    "regexp_replace(SQ_Shortcut_to_E_RES_REQUESTS_PRE___CLOSING_COMMENT, '\\((\\\\r|\\\\n)\\)', '$1') as o_CLOSING_COMMENT", 
    "SQ_Shortcut_to_E_RES_REQUESTS_PRE___STATUS as STATUS", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___LOCKED_BY as LOCKED_BY", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___LOCKED_DT as LOCKED_DT", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___CREATED_BY as CREATED_BY", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___CREATED_DT as CREATED_DT", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___UPDATED_BY as UPDATED_BY", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___UPDATED_DT as UPDATED_DT", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___IS_DELETED as IS_DELETED", 
	"SQ_Shortcut_to_E_RES_REQUESTS_PRE___SOURCE as SOURCE" 
)

# COMMAND ----------

# Processing node JNR_E_RES_REQUESTS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 64

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_E_RES_REQUESTS_temp = SQ_Shortcut_to_E_RES_REQUESTS.toDF(*["SQ_Shortcut_to_E_RES_REQUESTS___" + col for col in SQ_Shortcut_to_E_RES_REQUESTS.columns])
EXP_E_RES_REQUESTS_PRE_temp = EXP_E_RES_REQUESTS_PRE.toDF(*["EXP_E_RES_REQUESTS_PRE___" + col for col in EXP_E_RES_REQUESTS_PRE.columns])

JNR_E_RES_REQUESTS = SQ_Shortcut_to_E_RES_REQUESTS_temp.join(EXP_E_RES_REQUESTS_PRE_temp,[SQ_Shortcut_to_E_RES_REQUESTS_temp.SQ_Shortcut_to_E_RES_REQUESTS___E_RES_REQUEST_ID == EXP_E_RES_REQUESTS_PRE_temp.EXP_E_RES_REQUESTS_PRE___REQUEST_ID],'right_outer').selectExpr(
	"EXP_E_RES_REQUESTS_PRE___REQUEST_ID as REQUEST_ID",
	"EXP_E_RES_REQUESTS_PRE___CART_ID as CART_ID",
	"EXP_E_RES_REQUESTS_PRE___EXTERNAL_ID as EXTERNAL_ID",
	"EXP_E_RES_REQUESTS_PRE___STORE_NUMBER as STORE_NUMBER",
	"EXP_E_RES_REQUESTS_PRE___FIRST_NAME as FIRST_NAME",
	"EXP_E_RES_REQUESTS_PRE___LAST_NAME as LAST_NAME",
	"EXP_E_RES_REQUESTS_PRE___PRIMARY_PHONE as PRIMARY_PHONE",
	"EXP_E_RES_REQUESTS_PRE___SECONDARY_PHONE as SECONDARY_PHONE",
	"EXP_E_RES_REQUESTS_PRE___EMAIL as EMAIL",
	"EXP_E_RES_REQUESTS_PRE___ADDRESS1 as ADDRESS1",
	"EXP_E_RES_REQUESTS_PRE___ADDRESS2 as ADDRESS2",
	"EXP_E_RES_REQUESTS_PRE___CITY as CITY",
	"EXP_E_RES_REQUESTS_PRE___STATE_PROVINCE as STATE_PROVINCE",
	"EXP_E_RES_REQUESTS_PRE___POSTAL as POSTAL",
	"EXP_E_RES_REQUESTS_PRE___CHECK_IN as CHECK_IN",
	"EXP_E_RES_REQUESTS_PRE___CHECK_OUT as CHECK_OUT",
	"EXP_E_RES_REQUESTS_PRE___NOTES as NOTES",
	"EXP_E_RES_REQUESTS_PRE___TOTAL_AMOUNT as TOTAL_AMOUNT",
	"EXP_E_RES_REQUESTS_PRE___CUSTOMER_CALLED as CUSTOMER_CALLED",
	"EXP_E_RES_REQUESTS_PRE___o_CALL_REASON as CALL_REASON",
	"EXP_E_RES_REQUESTS_PRE___o_CALL_COMMENT as CALL_COMMENT",
	"EXP_E_RES_REQUESTS_PRE___o_CLOSING_COMMENT as CLOSING_COMMENT",
	"EXP_E_RES_REQUESTS_PRE___STATUS as STATUS",
	"EXP_E_RES_REQUESTS_PRE___LOCKED_BY as LOCKED_BY",
	"EXP_E_RES_REQUESTS_PRE___LOCKED_DT as LOCKED_DT",
	"EXP_E_RES_REQUESTS_PRE___CREATED_BY as CREATED_BY",
	"EXP_E_RES_REQUESTS_PRE___CREATED_DT as CREATED_DT",
	"EXP_E_RES_REQUESTS_PRE___UPDATED_BY as UPDATED_BY",
	"EXP_E_RES_REQUESTS_PRE___UPDATED_DT as UPDATED_DT",
	"EXP_E_RES_REQUESTS_PRE___IS_DELETED as IS_DELETED",
	"EXP_E_RES_REQUESTS_PRE___SOURCE as SOURCE",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_REQUEST_ID as i_E_RES_REQUEST_ID",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_CART_ID as i_E_RES_CART_ID",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_REQUEST_EXT_ID as i_E_RES_REQUEST_EXT_ID",
	"SQ_Shortcut_to_E_RES_REQUESTS___STORE_NBR as i_STORE_NBR",
	"SQ_Shortcut_to_E_RES_REQUESTS___CUST_FIRST_NAME as i_CUST_FIRST_NAME",
	"SQ_Shortcut_to_E_RES_REQUESTS___CUST_LAST_NAME as i_CUST_LAST_NAME",
	"SQ_Shortcut_to_E_RES_REQUESTS___PRIMARY_PHONE as i_PRIMARY_PHONE",
	"SQ_Shortcut_to_E_RES_REQUESTS___SECONDARY_PHONE as i_SECONDARY_PHONE",
	"SQ_Shortcut_to_E_RES_REQUESTS___EMAIL as i_EMAIL",
	"SQ_Shortcut_to_E_RES_REQUESTS___ADDR1 as i_ADDR",
	"SQ_Shortcut_to_E_RES_REQUESTS___ADDR2 as i_ADDR2",
	"SQ_Shortcut_to_E_RES_REQUESTS___CITY as i_CITY",
	"SQ_Shortcut_to_E_RES_REQUESTS___STATE_PROV as i_STATE_PROV",
	"SQ_Shortcut_to_E_RES_REQUESTS___POSTAL_CD as i_POSTAL_CD",
	"SQ_Shortcut_to_E_RES_REQUESTS___CHECK_IN_TSTMP as i_CHECK_IN_TSTMP",
	"SQ_Shortcut_to_E_RES_REQUESTS___CHECK_OUT_TSTMP as i_CHECK_OUT_TSTMP",
	"SQ_Shortcut_to_E_RES_REQUESTS___NOTES as i_NOTES",
	"SQ_Shortcut_to_E_RES_REQUESTS___TOTAL_AMT as i_TOTAL_AMT",
	"SQ_Shortcut_to_E_RES_REQUESTS___CUST_CALLED_FLAG as i_CUST_CALLED_FLAG",
	"SQ_Shortcut_to_E_RES_REQUESTS___CALL_REASON as i_CALL_REASON",
	"SQ_Shortcut_to_E_RES_REQUESTS___CALL_COMMENT as i_CALL_COMMENT",
	"SQ_Shortcut_to_E_RES_REQUESTS___CLOSING_COMMENT as i_CLOSING_COMMENT",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_STATUS_ID as i_E_RES_STATUS_ID",
	"SQ_Shortcut_to_E_RES_REQUESTS___DELETED_FLAG as i_DELETED_FLAG",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_SOURCE as i_E_RES_SOURCE",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_LOCKED_BY as i_E_RES_LOCKED_BY",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_LOCKED_TSTMP as i_E_RES_LOCKED_TSTMP",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_CREATED_BY as i_E_RES_CREATED_BY",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_CREATED_TSTMP as i_E_RES_CREATED_TSTMP",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_UPDATED_BY as i_E_RES_UPDATED_BY",
	"SQ_Shortcut_to_E_RES_REQUESTS___E_RES_UPDATED_TSTMP as i_E_RES_UPDATED_TSTMP",
	"SQ_Shortcut_to_E_RES_REQUESTS___UPDATE_TSTMP as i_UPDATE_TSTMP",
	"SQ_Shortcut_to_E_RES_REQUESTS___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------

# Processing node FIL_E_RES_REQUESTS, type FILTER 
# COLUMN COUNT: 64

# for each involved DataFrame, append the dataframe name to each column
JNR_E_RES_REQUESTS_temp = JNR_E_RES_REQUESTS.toDF(*["JNR_E_RES_REQUESTS___" + col for col in JNR_E_RES_REQUESTS.columns])

FIL_E_RES_REQUESTS = JNR_E_RES_REQUESTS_temp.selectExpr(
	"JNR_E_RES_REQUESTS___REQUEST_ID as REQUEST_ID",
	"JNR_E_RES_REQUESTS___CART_ID as CART_ID",
	"JNR_E_RES_REQUESTS___EXTERNAL_ID as EXTERNAL_ID",
	"JNR_E_RES_REQUESTS___STORE_NUMBER as STORE_NUMBER",
	"JNR_E_RES_REQUESTS___FIRST_NAME as FIRST_NAME",
	"JNR_E_RES_REQUESTS___LAST_NAME as LAST_NAME",
	"JNR_E_RES_REQUESTS___PRIMARY_PHONE as PRIMARY_PHONE",
	"JNR_E_RES_REQUESTS___SECONDARY_PHONE as SECONDARY_PHONE",
	"JNR_E_RES_REQUESTS___EMAIL as EMAIL",
	"JNR_E_RES_REQUESTS___ADDRESS1 as ADDRESS1",
	"JNR_E_RES_REQUESTS___ADDRESS2 as ADDRESS2",
	"JNR_E_RES_REQUESTS___CITY as CITY",
	"JNR_E_RES_REQUESTS___STATE_PROVINCE as STATE_PROVINCE",
	"JNR_E_RES_REQUESTS___POSTAL as POSTAL",
	"JNR_E_RES_REQUESTS___CHECK_IN as CHECK_IN",
	"JNR_E_RES_REQUESTS___CHECK_OUT as CHECK_OUT",
	"JNR_E_RES_REQUESTS___NOTES as NOTES",
	"JNR_E_RES_REQUESTS___TOTAL_AMOUNT as TOTAL_AMOUNT",
	"JNR_E_RES_REQUESTS___CUSTOMER_CALLED as CUSTOMER_CALLED",
	"JNR_E_RES_REQUESTS___CALL_REASON as CALL_REASON",
	"JNR_E_RES_REQUESTS___CALL_COMMENT as CALL_COMMENT",
	"JNR_E_RES_REQUESTS___CLOSING_COMMENT as CLOSING_COMMENT",
	"JNR_E_RES_REQUESTS___STATUS as STATUS",
	"JNR_E_RES_REQUESTS___LOCKED_BY as LOCKED_BY",
	"JNR_E_RES_REQUESTS___LOCKED_DT as LOCKED_DT",
	"JNR_E_RES_REQUESTS___CREATED_BY as CREATED_BY",
	"JNR_E_RES_REQUESTS___CREATED_DT as CREATED_DT",
	"JNR_E_RES_REQUESTS___UPDATED_BY as UPDATED_BY",
	"JNR_E_RES_REQUESTS___UPDATED_DT as UPDATED_DT",
	"JNR_E_RES_REQUESTS___IS_DELETED as IS_DELETED",
	"JNR_E_RES_REQUESTS___SOURCE as SOURCE",
	"JNR_E_RES_REQUESTS___i_E_RES_REQUEST_ID as i_E_RES_REQUEST_ID",
	"JNR_E_RES_REQUESTS___i_E_RES_CART_ID as i_E_RES_CART_ID",
	"JNR_E_RES_REQUESTS___i_E_RES_REQUEST_EXT_ID as i_E_RES_REQUEST_EXT_ID",
	"JNR_E_RES_REQUESTS___i_STORE_NBR as i_STORE_NBR",
	"JNR_E_RES_REQUESTS___i_CUST_FIRST_NAME as i_CUST_FIRST_NAME",
	"JNR_E_RES_REQUESTS___i_CUST_LAST_NAME as i_CUST_LAST_NAME",
	"JNR_E_RES_REQUESTS___i_PRIMARY_PHONE as i_PRIMARY_PHONE",
	"JNR_E_RES_REQUESTS___i_SECONDARY_PHONE as i_SECONDARY_PHONE",
	"JNR_E_RES_REQUESTS___i_EMAIL as i_EMAIL",
	"JNR_E_RES_REQUESTS___i_ADDR as i_ADDR",
	"JNR_E_RES_REQUESTS___i_ADDR2 as i_ADDR2",
	"JNR_E_RES_REQUESTS___i_CITY as i_CITY",
	"JNR_E_RES_REQUESTS___i_STATE_PROV as i_STATE_PROV",
	"JNR_E_RES_REQUESTS___i_POSTAL_CD as i_POSTAL_CD",
	"JNR_E_RES_REQUESTS___i_CHECK_IN_TSTMP as i_CHECK_IN_TSTMP",
	"JNR_E_RES_REQUESTS___i_CHECK_OUT_TSTMP as i_CHECK_OUT_TSTMP",
	"JNR_E_RES_REQUESTS___i_NOTES as i_NOTES",
	"JNR_E_RES_REQUESTS___i_TOTAL_AMT as i_TOTAL_AMT",
	"JNR_E_RES_REQUESTS___i_CUST_CALLED_FLAG as i_CUST_CALLED_FLAG",
	"JNR_E_RES_REQUESTS___i_CALL_REASON as i_CALL_REASON",
	"JNR_E_RES_REQUESTS___i_CALL_COMMENT as i_CALL_COMMENT",
	"JNR_E_RES_REQUESTS___i_CLOSING_COMMENT as i_CLOSING_COMMENT",
	"JNR_E_RES_REQUESTS___i_E_RES_STATUS_ID as i_E_RES_STATUS_ID",
	"JNR_E_RES_REQUESTS___i_DELETED_FLAG as i_DELETED_FLAG",
	"JNR_E_RES_REQUESTS___i_E_RES_SOURCE as i_E_RES_SOURCE",
	"JNR_E_RES_REQUESTS___i_E_RES_LOCKED_BY as i_E_RES_LOCKED_BY",
	"JNR_E_RES_REQUESTS___i_E_RES_LOCKED_TSTMP as i_E_RES_LOCKED_TSTMP",
	"JNR_E_RES_REQUESTS___i_E_RES_CREATED_BY as i_E_RES_CREATED_BY",
	"JNR_E_RES_REQUESTS___i_E_RES_CREATED_TSTMP as i_E_RES_CREATED_TSTMP",
	"JNR_E_RES_REQUESTS___i_E_RES_UPDATED_BY as i_E_RES_UPDATED_BY",
	"JNR_E_RES_REQUESTS___i_E_RES_UPDATED_TSTMP as i_E_RES_UPDATED_TSTMP",
	"JNR_E_RES_REQUESTS___i_UPDATE_TSTMP as i_UPDATE_TSTMP",
	"JNR_E_RES_REQUESTS___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("i_E_RES_REQUEST_ID IS NULL OR ( i_E_RES_REQUEST_ID IS NOT NULL AND ( IF(CART_ID IS NULL, - 99999, CART_ID) != IF(i_E_RES_CART_ID IS NULL, - 99999, i_E_RES_CART_ID) OR IF(EXTERNAL_ID IS NULL, '', EXTERNAL_ID) != IF(i_E_RES_REQUEST_EXT_ID IS NULL, '', i_E_RES_REQUEST_EXT_ID) OR IF(STORE_NUMBER IS NULL, - 99999, STORE_NUMBER) != IF(i_STORE_NBR IS NULL, - 99999, i_STORE_NBR) OR IF(FIRST_NAME IS NULL, '', FIRST_NAME) != IF(i_CUST_FIRST_NAME IS NULL, '', i_CUST_FIRST_NAME) OR IF(LAST_NAME IS NULL, '', LAST_NAME) != IF(i_CUST_LAST_NAME IS NULL, '', i_CUST_LAST_NAME) OR IF(PRIMARY_PHONE IS NULL, '', PRIMARY_PHONE) != IF(i_PRIMARY_PHONE IS NULL, '', i_PRIMARY_PHONE) OR IF(SECONDARY_PHONE IS NULL, '', SECONDARY_PHONE) != IF(i_SECONDARY_PHONE IS NULL, '', i_SECONDARY_PHONE) OR IF(EMAIL IS NULL, '', EMAIL) != IF(i_EMAIL IS NULL, '', i_EMAIL) OR IF(ADDRESS1 IS NULL, '', ADDRESS1) != IF(i_ADDR IS NULL, '', i_ADDR) OR IF(ADDRESS2 IS NULL, '', ADDRESS2) != IF(i_ADDR2 IS NULL, '', i_ADDR2) OR IF(CITY IS NULL, '', CITY) != IF(i_CITY IS NULL, '', i_CITY) OR IF(STATE_PROVINCE IS NULL, '', STATE_PROVINCE) != IF(i_STATE_PROV IS NULL, '', i_STATE_PROV) OR IF(POSTAL IS NULL, '', POSTAL) != IF(i_POSTAL_CD IS NULL, '', i_POSTAL_CD) OR IF(CHECK_IN IS NULL, date'1900-01-01', CHECK_IN) != IF(i_CHECK_IN_TSTMP IS NULL, date'1900-01-01', i_CHECK_IN_TSTMP) OR IF(CHECK_OUT IS NULL, date'1900-01-01', CHECK_OUT) != IF(i_CHECK_OUT_TSTMP IS NULL, date'1900-01-01', i_CHECK_OUT_TSTMP) OR IF(NOTES IS NULL, '', NOTES) != IF(i_NOTES IS NULL, '', i_NOTES) OR IF(TOTAL_AMOUNT IS NULL, - 99999, TOTAL_AMOUNT) != IF(i_TOTAL_AMT IS NULL, - 99999, i_TOTAL_AMT) OR IF(CUSTOMER_CALLED IS NULL, 0, CUSTOMER_CALLED) != IF(i_CUST_CALLED_FLAG IS NULL, 0, i_CUST_CALLED_FLAG) OR IF(CALL_REASON IS NULL, '', CALL_REASON) != IF(i_CALL_REASON IS NULL, '', i_CALL_REASON) OR IF(CALL_COMMENT IS NULL, '', CALL_COMMENT) != IF(i_CALL_COMMENT IS NULL, '', i_CALL_COMMENT) OR IF(CLOSING_COMMENT IS NULL, '', CLOSING_COMMENT) != IF(i_CLOSING_COMMENT IS NULL, '', i_CLOSING_COMMENT) OR IF(STATUS IS NULL, - 99999, STATUS) != IF(i_E_RES_STATUS_ID IS NULL, - 99999, i_E_RES_STATUS_ID) OR IF(LOCKED_BY IS NULL, '', LOCKED_BY) != IF(i_E_RES_LOCKED_BY IS NULL, '', i_E_RES_LOCKED_BY) OR IF(LOCKED_DT IS NULL, date'1900-01-01', LOCKED_DT) != IF(i_E_RES_LOCKED_TSTMP IS NULL, date'1900-01-01', i_E_RES_LOCKED_TSTMP) OR IF(CREATED_BY IS NULL, '', CREATED_BY) != IF(i_E_RES_CREATED_BY IS NULL, '', i_E_RES_CREATED_BY) OR IF(CREATED_DT IS NULL, date'1900-01-01', CREATED_DT) != IF(i_E_RES_CREATED_TSTMP IS NULL, date'1900-01-01', i_E_RES_CREATED_TSTMP) OR IF(IS_DELETED IS NULL, 0, IS_DELETED) != IF(i_DELETED_FLAG IS NULL, 0, i_DELETED_FLAG) OR IF(SOURCE IS NULL, '', SOURCE) != IF(i_E_RES_SOURCE IS NULL, '', i_E_RES_SOURCE) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_E_RES_REQUESTS, type EXPRESSION 
# COLUMN COUNT: 34

# for each involved DataFrame, append the dataframe name to each column
FIL_E_RES_REQUESTS_temp = FIL_E_RES_REQUESTS.toDF(*["FIL_E_RES_REQUESTS___" + col for col in FIL_E_RES_REQUESTS.columns])

EXP_E_RES_REQUESTS = FIL_E_RES_REQUESTS_temp.selectExpr(
	"FIL_E_RES_REQUESTS___sys_row_id as sys_row_id",
	"FIL_E_RES_REQUESTS___REQUEST_ID as REQUEST_ID",
	"FIL_E_RES_REQUESTS___CART_ID as CART_ID",
	"FIL_E_RES_REQUESTS___EXTERNAL_ID as EXTERNAL_ID",
	"FIL_E_RES_REQUESTS___STORE_NUMBER as STORE_NUMBER",
	"FIL_E_RES_REQUESTS___FIRST_NAME as FIRST_NAME",
	"FIL_E_RES_REQUESTS___LAST_NAME as LAST_NAME",
	"FIL_E_RES_REQUESTS___PRIMARY_PHONE as PRIMARY_PHONE",
	"FIL_E_RES_REQUESTS___SECONDARY_PHONE as SECONDARY_PHONE",
	"FIL_E_RES_REQUESTS___EMAIL as EMAIL",
	"FIL_E_RES_REQUESTS___ADDRESS1 as ADDRESS1",
	"FIL_E_RES_REQUESTS___ADDRESS2 as ADDRESS2",
	"FIL_E_RES_REQUESTS___CITY as CITY",
	"FIL_E_RES_REQUESTS___STATE_PROVINCE as STATE_PROVINCE",
	"FIL_E_RES_REQUESTS___POSTAL as POSTAL",
	"FIL_E_RES_REQUESTS___CHECK_IN as CHECK_IN",
	"FIL_E_RES_REQUESTS___CHECK_OUT as CHECK_OUT",
	"FIL_E_RES_REQUESTS___NOTES as NOTES",
	"FIL_E_RES_REQUESTS___TOTAL_AMOUNT as TOTAL_AMOUNT",
	"FIL_E_RES_REQUESTS___CUSTOMER_CALLED as CUSTOMER_CALLED",
	"FIL_E_RES_REQUESTS___CALL_REASON as CALL_REASON",
	"FIL_E_RES_REQUESTS___CALL_COMMENT as CALL_COMMENT",
	"FIL_E_RES_REQUESTS___CLOSING_COMMENT as CLOSING_COMMENT",
	"FIL_E_RES_REQUESTS___STATUS as STATUS",
	"FIL_E_RES_REQUESTS___LOCKED_BY as LOCKED_BY",
	"FIL_E_RES_REQUESTS___LOCKED_DT as LOCKED_DT",
	"FIL_E_RES_REQUESTS___CREATED_BY as CREATED_BY",
	"FIL_E_RES_REQUESTS___CREATED_DT as CREATED_DT",
	"FIL_E_RES_REQUESTS___UPDATED_BY as UPDATED_BY",
	"FIL_E_RES_REQUESTS___UPDATED_DT as UPDATED_DT",
	"FIL_E_RES_REQUESTS___IS_DELETED as IS_DELETED",
	"FIL_E_RES_REQUESTS___SOURCE as SOURCE",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF(FIL_E_RES_REQUESTS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_E_RES_REQUESTS___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF(FIL_E_RES_REQUESTS___i_E_RES_REQUEST_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR"
)

# COMMAND ----------

# Processing node UPD_E_RES_REQUESTS, type UPDATE_STRATEGY 
# COLUMN COUNT: 34

# for each involved DataFrame, append the dataframe name to each column
EXP_E_RES_REQUESTS_temp = EXP_E_RES_REQUESTS.toDF(*["EXP_E_RES_REQUESTS___" + col for col in EXP_E_RES_REQUESTS.columns])

UPD_E_RES_REQUESTS = EXP_E_RES_REQUESTS_temp.selectExpr(
	"EXP_E_RES_REQUESTS___REQUEST_ID as REQUEST_ID",
	"EXP_E_RES_REQUESTS___CART_ID as CART_ID",
	"EXP_E_RES_REQUESTS___EXTERNAL_ID as EXTERNAL_ID",
	"EXP_E_RES_REQUESTS___STORE_NUMBER as STORE_NUMBER",
	"EXP_E_RES_REQUESTS___FIRST_NAME as FIRST_NAME",
	"EXP_E_RES_REQUESTS___LAST_NAME as LAST_NAME",
	"EXP_E_RES_REQUESTS___PRIMARY_PHONE as PRIMARY_PHONE",
	"EXP_E_RES_REQUESTS___SECONDARY_PHONE as SECONDARY_PHONE",
	"EXP_E_RES_REQUESTS___EMAIL as EMAIL",
	"EXP_E_RES_REQUESTS___ADDRESS1 as ADDRESS1",
	"EXP_E_RES_REQUESTS___ADDRESS2 as ADDRESS2",
	"EXP_E_RES_REQUESTS___CITY as CITY",
	"EXP_E_RES_REQUESTS___STATE_PROVINCE as STATE_PROVINCE",
	"EXP_E_RES_REQUESTS___POSTAL as POSTAL",
	"EXP_E_RES_REQUESTS___CHECK_IN as CHECK_IN",
	"EXP_E_RES_REQUESTS___CHECK_OUT as CHECK_OUT",
	"EXP_E_RES_REQUESTS___NOTES as NOTES",
	"EXP_E_RES_REQUESTS___TOTAL_AMOUNT as TOTAL_AMOUNT",
	"EXP_E_RES_REQUESTS___CUSTOMER_CALLED as CUSTOMER_CALLED",
	"EXP_E_RES_REQUESTS___CALL_REASON as CALL_REASON",
	"EXP_E_RES_REQUESTS___CALL_COMMENT as CALL_COMMENT",
	"EXP_E_RES_REQUESTS___CLOSING_COMMENT as CLOSING_COMMENT",
	"EXP_E_RES_REQUESTS___STATUS as STATUS",
	"EXP_E_RES_REQUESTS___LOCKED_BY as LOCKED_BY",
	"EXP_E_RES_REQUESTS___LOCKED_DT as LOCKED_DT",
	"EXP_E_RES_REQUESTS___CREATED_BY as CREATED_BY",
	"EXP_E_RES_REQUESTS___CREATED_DT as CREATED_DT",
	"EXP_E_RES_REQUESTS___UPDATED_BY as UPDATED_BY",
	"EXP_E_RES_REQUESTS___UPDATED_DT as UPDATED_DT",
	"EXP_E_RES_REQUESTS___IS_DELETED as IS_DELETED",
	"EXP_E_RES_REQUESTS___SOURCE as SOURCE",
	"EXP_E_RES_REQUESTS___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_E_RES_REQUESTS___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_E_RES_REQUESTS___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
    .withColumn('pyspark_data_action', when(col("o_UPDATE_VALIDATOR") ==(lit(1)) , lit(0)).when(col("o_UPDATE_VALIDATOR") ==(lit(2)) , lit(1)))


# COMMAND ----------

# Processing node Shortcut_to_E_RES_REQUESTS1, type TARGET 
# COLUMN COUNT: 33


Shortcut_to_E_RES_REQUESTS1 = UPD_E_RES_REQUESTS.selectExpr(
	"CAST(REQUEST_ID AS INT) as E_RES_REQUEST_ID",
	"CAST(CART_ID AS INT) as E_RES_CART_ID",
	"CAST(EXTERNAL_ID AS STRING) as E_RES_REQUEST_EXT_ID",
	"CAST(STORE_NUMBER AS INT) as STORE_NBR",
	"CAST(FIRST_NAME AS STRING) as CUST_FIRST_NAME",
	"CAST(LAST_NAME AS STRING) as CUST_LAST_NAME",
	"CAST(PRIMARY_PHONE AS STRING) as PRIMARY_PHONE",
	"CAST(SECONDARY_PHONE AS STRING) as SECONDARY_PHONE",
	"CAST(EMAIL AS STRING) as EMAIL",
	"CAST(ADDRESS1 AS STRING) as ADDR1",
	"CAST(ADDRESS2 AS STRING) as ADDR2",
	"CAST(CITY AS STRING) as CITY",
	"CAST(STATE_PROVINCE AS STRING) as STATE_PROV",
	"CAST(POSTAL AS STRING) as POSTAL_CD",
	"CAST(CHECK_IN AS TIMESTAMP) as CHECK_IN_TSTMP",
	"CAST(CHECK_OUT AS TIMESTAMP) as CHECK_OUT_TSTMP",
	"CAST(NOTES AS STRING) as NOTES",
	"CAST(TOTAL_AMOUNT AS DECIMAL(15,2)) as TOTAL_AMT",
	"CAST(CUSTOMER_CALLED AS TINYINT) as CUST_CALLED_FLAG",
	"CAST(CALL_REASON AS STRING) as CALL_REASON",
	"CAST(CALL_COMMENT AS STRING) as CALL_COMMENT",
	"CAST(CLOSING_COMMENT AS STRING) as CLOSING_COMMENT",
	"CAST(STATUS AS SMALLINT) as E_RES_STATUS_ID",
	"CAST(IS_DELETED AS TINYINT) as DELETED_FLAG",
	"CAST(SOURCE AS STRING) as E_RES_SOURCE",
	"CAST(LOCKED_BY AS STRING) as E_RES_LOCKED_BY",
	"CAST(LOCKED_DT AS TIMESTAMP) as E_RES_LOCKED_TSTMP",
	"CAST(CREATED_BY AS STRING) as E_RES_CREATED_BY",
	"CAST(CREATED_DT AS TIMESTAMP) as E_RES_CREATED_TSTMP",
	"CAST(UPDATED_BY AS STRING) as E_RES_UPDATED_BY",
	"CAST(UPDATED_DT AS TIMESTAMP) as E_RES_UPDATED_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.E_RES_REQUEST_ID = target.E_RES_REQUEST_ID"""
	refined_perf_table = f"{cust_sensitive}.legacy_e_res_requests"
 
	executeMerge(Shortcut_to_E_RES_REQUESTS1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("E_RES_REQUESTS", "E_RES_REQUESTS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("E_RES_REQUESTS", "E_RES_REQUESTS","Failed",str(e), f"{raw}.log_run_details", )
	raise e
