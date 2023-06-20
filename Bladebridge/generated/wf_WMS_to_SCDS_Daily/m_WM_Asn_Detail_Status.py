#Code converted on 2023-06-12 20:30:43
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from dbruntime import dbutils

# COMMAND ----------

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in job variables
# read_infa_paramfile('', 'm_WM_Asn_Detail_Status') ProcessingUtils

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE = spark.read.jdbc(os.environ.get('_CONNECT_STRING'), f"""SELECT
WM_ASN_DETAIL_STATUS_PRE.DC_NBR,
WM_ASN_DETAIL_STATUS_PRE.ASN_DETAIL_STATUS,
WM_ASN_DETAIL_STATUS_PRE.DESCRIPTION
FROM WM_ASN_DETAIL_STATUS_PRE""", 
properties={
'user': os.environ.get('_LOGIN'),
'password': os.environ.get('_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE_temp = SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE.toDF(*["SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE___" + col for col in SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE___DESCRIPTION as DESCRIPTION" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ASN_DETAIL_STATUS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_ASN_DETAIL_STATUS = spark.read.jdbc(os.environ.get('_CONNECT_STRING'), f"""SELECT
WM_ASN_DETAIL_STATUS.LOCATION_ID,
WM_ASN_DETAIL_STATUS.WM_ASN_DETAIL_STATUS,
WM_ASN_DETAIL_STATUS.WM_ASN_DETAIL_STATUS_DESC,
WM_ASN_DETAIL_STATUS.UPDATE_TSTMP,
WM_ASN_DETAIL_STATUS.LOAD_TSTMP
FROM WM_ASN_DETAIL_STATUS
WHERE WM_ASN_DETAIL_STATUS IN (SELECT ASN_DETAIL_STATUS FROM WM_ASN_DETAIL_STATUS_PRE)""", 
properties={
'user': os.environ.get('_LOGIN'),
'password': os.environ.get('_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.read.jdbc(os.environ.get('BIW_Prod_CONNECT_STRING'), f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""", 
properties={
'user': os.environ.get('BIW_Prod_LOGIN'),
'password': os.environ.get('BIW_Prod_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_INT_CONV_temp = EXP_INT_CONV.toDF(*["EXP_INT_CONV___" + col for col in EXP_INT_CONV.columns])
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])

JNR_SITE_PROFILE = EXP_INT_CONV_temp.join(SQ_Shortcut_to_SITE_PROFILE_temp,[EXP_INT_CONV_temp.EXP_INT_CONV___o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR],'inner').selectExpr( \
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR", \
	"EXP_INT_CONV___o_DC_NBR as DC_NBR", \
	"EXP_INT_CONV___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"EXP_INT_CONV___DESCRIPTION as DESCRIPTION")

# COMMAND ----------
# Processing node JNR_WM_ASN_DETAIL_STATUS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_temp = SQ_Shortcut_to_WM_ASN_DETAIL_STATUS.toDF(*["SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___" + col for col in SQ_Shortcut_to_WM_ASN_DETAIL_STATUS.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_ASN_DETAIL_STATUS = SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_temp.SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_temp.SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___WM_ASN_DETAIL_STATUS == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ASN_DETAIL_STATUS],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___WM_ASN_DETAIL_STATUS as i_WM_ASN_DETAIL_STATUS", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___WM_ASN_DETAIL_STATUS_DESC as i_WM_ASN_DETAIL_STATUS_DESC", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___UPDATE_TSTMP as i_UPDATE_TSTMP", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_ASN_DETAIL_STATUS_temp = JNR_WM_ASN_DETAIL_STATUS.toDF(*["JNR_WM_ASN_DETAIL_STATUS___" + col for col in JNR_WM_ASN_DETAIL_STATUS.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_ASN_DETAIL_STATUS_temp.selectExpr( \
	"JNR_WM_ASN_DETAIL_STATUS___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_ASN_DETAIL_STATUS___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"JNR_WM_ASN_DETAIL_STATUS___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_ASN_DETAIL_STATUS___i_LOCATION_ID as i_LOCATION_ID", \
	"JNR_WM_ASN_DETAIL_STATUS___i_WM_ASN_DETAIL_STATUS as i_WM_ASN_DETAIL_STATUS", \
	"JNR_WM_ASN_DETAIL_STATUS___i_WM_ASN_DETAIL_STATUS_DESC as i_WM_ASN_DETAIL_STATUS_DESC", \
	"JNR_WM_ASN_DETAIL_STATUS___i_UPDATE_TSTMP as i_UPDATE_TSTMP", \
	"JNR_WM_ASN_DETAIL_STATUS___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("i_WM_ASN_DETAIL_STATUS.isNull() OR ( NOT i_WM_ASN_DETAIL_STATUS.isNull() AND when((DESCRIPTION.isNull()),('')).otherwise(DESCRIPTION) != when((i_WM_ASN_DETAIL_STATUS_DESC.isNull()),('')).otherwise(i_WM_ASN_DETAIL_STATUS_DESC) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_OUTPUT_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_OUTPUT_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_WM_ASN_DETAIL_STATUS IS NULL, 1, 2) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXP_OUTPUT_VALIDATOR_temp = EXP_OUTPUT_VALIDATOR.toDF(*["EXP_OUTPUT_VALIDATOR___" + col for col in EXP_OUTPUT_VALIDATOR.columns])

UPD_INS_UPD = EXP_OUTPUT_VALIDATOR_temp.selectExpr( \
	"EXP_OUTPUT_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_OUTPUT_VALIDATOR___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"EXP_OUTPUT_VALIDATOR___DESCRIPTION as DESCRIPTION", \
	"EXP_OUTPUT_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_OUTPUT_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_OUTPUT_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)) , lit(0)) .when(EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_ASN_DETAIL_STATUS1, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_WM_ASN_DETAIL_STATUS1 = UPD_INS_UPD.selectExpr( \
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", \
	"CAST(ASN_DETAIL_STATUS AS BIGINT) as WM_ASN_DETAIL_STATUS", \
	"CAST(DESCRIPTION AS VARCHAR) as WM_ASN_DETAIL_STATUS_DESC", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)
Shortcut_to_WM_ASN_DETAIL_STATUS1.write.saveAsTable('WM_ASN_DETAIL_STATUS', mode = 'append')