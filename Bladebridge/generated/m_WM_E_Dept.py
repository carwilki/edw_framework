#Code converted on 2023-05-18 09:46:41
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
# read_infa_paramfile('', 'm_WM_E_Dept') ProcessingUtils

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_DEPT_PRE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_WM_E_DEPT_PRE = spark.read.jdbc(os.environ.get('NZ_SCDS_CONNECT_STRING'), f"""SELECT
WM_E_DEPT_PRE.DC_NBR,
WM_E_DEPT_PRE.DEPT_ID,
WM_E_DEPT_PRE.DEPT_CODE,
WM_E_DEPT_PRE.DESCRIPTION,
WM_E_DEPT_PRE.CREATE_DATE_TIME,
WM_E_DEPT_PRE.MOD_DATE_TIME,
WM_E_DEPT_PRE.USER_ID,
WM_E_DEPT_PRE.WHSE,
WM_E_DEPT_PRE.MISC_TXT_1,
WM_E_DEPT_PRE.MISC_TXT_2,
WM_E_DEPT_PRE.MISC_NUM_1,
WM_E_DEPT_PRE.MISC_NUM_2,
WM_E_DEPT_PRE.PERF_GOAL,
WM_E_DEPT_PRE.VERSION_ID,
WM_E_DEPT_PRE.CREATED_DTTM,
WM_E_DEPT_PRE.LAST_UPDATED_DTTM,
WM_E_DEPT_PRE.LOAD_TSTMP
FROM WM_E_DEPT_PRE""", 
properties={
'user': os.environ.get('NZ_SCDS_LOGIN'),
'password': os.environ.get('NZ_SCDS_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_DEPT, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_WM_E_DEPT = spark.read.jdbc(os.environ.get('NZ_SCDS_CONNECT_STRING'), f"""SELECT
WM_E_DEPT.LOCATION_ID,
WM_E_DEPT.WM_DEPT_ID,
WM_E_DEPT.WM_CREATED_TSTMP,
WM_E_DEPT.WM_LAST_UPDATED_TSTMP,
WM_E_DEPT.WM_CREATE_TSTMP,
WM_E_DEPT.WM_MOD_TSTMP,
WM_E_DEPT.LOAD_TSTMP
FROM WM_E_DEPT
WHERE WM_DEPT_ID IN (SELECT DEPT_ID FROM WM_E_DEPT_PRE)""", 
properties={
'user': os.environ.get('NZ_SCDS_LOGIN'),
'password': os.environ.get('NZ_SCDS_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_DEPT_PRE_temp = SQ_Shortcut_to_WM_E_DEPT_PRE.toDF(*["SQ_Shortcut_to_WM_E_DEPT_PRE___" + col for col in SQ_Shortcut_to_WM_E_DEPT_PRE.columns])
in_DC_NBR_temp = in_DC_NBR.toDF(*["in_DC_NBR___" + col for col in in_DC_NBR.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_E_DEPT_PRE_temp.selectExpr(
	"SQ_Shortcut_to_WM_E_DEPT_PRE___DC_NBR as in_DC_NBR",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___DEPT_ID as DEPT_ID",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___DEPT_CODE as DEPT_CODE",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___DESCRIPTION as DESCRIPTION",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___MOD_DATE_TIME as MOD_DATE_TIME",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___USER_ID as USER_ID",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___WHSE as WHSE",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___MISC_TXT_1 as MISC_TXT_1",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___MISC_TXT_2 as MISC_TXT_2",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___MISC_NUM_1 as MISC_NUM_1",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___MISC_NUM_2 as MISC_NUM_2",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___PERF_GOAL as PERF_GOAL",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___VERSION_ID as VERSION_ID",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___CREATED_DTTM as CREATED_DTTM",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___LOAD_TSTMP as LOAD_TSTMP").selectExpr(
	"SQ_Shortcut_to_WM_E_DEPT_PRE___sys_row_id as sys_row_id",
	"in_DC_NBR___cast(IntegerType()) as DC_NBR",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___DEPT_ID as DEPT_ID",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___DEPT_CODE as DEPT_CODE",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___DESCRIPTION as DESCRIPTION",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___MOD_DATE_TIME as MOD_DATE_TIME",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___USER_ID as USER_ID",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___WHSE as WHSE",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___MISC_TXT_1 as MISC_TXT_1",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___MISC_TXT_2 as MISC_TXT_2",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___MISC_NUM_1 as MISC_NUM_1",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___MISC_NUM_2 as MISC_NUM_2",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___PERF_GOAL as PERF_GOAL",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___VERSION_ID as VERSION_ID",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___CREATED_DTTM as CREATED_DTTM",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
	"SQ_Shortcut_to_WM_E_DEPT_PRE___LOAD_TSTMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.read.jdbc(os.environ.get('NZ_SCDS_CONNECT_STRING'), f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""", 
properties={
'user': os.environ.get('NZ_SCDS_LOGIN'),
'password': os.environ.get('NZ_SCDS_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 19

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONV,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONV.DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_E_DEPT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 24

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_E_DEPT_temp = SQ_Shortcut_to_WM_E_DEPT.toDF(*["SQ_Shortcut_to_WM_E_DEPT___" + col for col in SQ_Shortcut_to_WM_E_DEPT.columns])

JNR_WM_E_DEPT = SQ_Shortcut_to_WM_E_DEPT_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_E_DEPT_temp.SQ_Shortcut_to_WM_E_DEPT___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_E_DEPT_temp.SQ_Shortcut_to_WM_E_DEPT___WM_DEPT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___DEPT_ID],'right_outer').selectExpr(
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID",
	"JNR_SITE_PROFILE___DEPT_ID as DEPT_ID",
	"JNR_SITE_PROFILE___DEPT_CODE as DEPT_CODE",
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION",
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME",
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME",
	"JNR_SITE_PROFILE___USER_ID as USER_ID",
	"JNR_SITE_PROFILE___WHSE as WHSE",
	"JNR_SITE_PROFILE___MISC_TXT_1 as MISC_TXT_1",
	"JNR_SITE_PROFILE___MISC_TXT_2 as MISC_TXT_2",
	"JNR_SITE_PROFILE___MISC_NUM_1 as MISC_NUM_1",
	"JNR_SITE_PROFILE___MISC_NUM_2 as MISC_NUM_2",
	"JNR_SITE_PROFILE___PERF_GOAL as PERF_GOAL",
	"JNR_SITE_PROFILE___VERSION_ID as VERSION_ID",
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM",
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
	"JNR_SITE_PROFILE___LOAD_TSTMP as LOAD_TSTMP",
	"SQ_Shortcut_to_WM_E_DEPT___LOCATION_ID as in_LOCATION_ID",
	"SQ_Shortcut_to_WM_E_DEPT___WM_DEPT_ID as in_WM_DEPT_ID",
	"SQ_Shortcut_to_WM_E_DEPT___LOAD_TSTMP as in_LOAD_TSTMP",
	"SQ_Shortcut_to_WM_E_DEPT___WM_CREATE_TSTMP as in_WM_CREATE_TSTMP",
	"SQ_Shortcut_to_WM_E_DEPT___WM_MOD_TSTMP as in_WM_MOD_TSTMP",
	"SQ_Shortcut_to_WM_E_DEPT___WM_CREATED_TSTMP as in_WM_CREATED_TSTMP",
	"SQ_Shortcut_to_WM_E_DEPT___WM_LAST_UPDATED_TSTMP as in_WM_LAST_UPDATED_TSTMP")

# COMMAND ----------
# Processing node FIL_NO_CHANGE_REC, type FILTER 
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_E_DEPT_temp = JNR_WM_E_DEPT.toDF(*["JNR_WM_E_DEPT___" + col for col in JNR_WM_E_DEPT.columns])

FIL_NO_CHANGE_REC = JNR_WM_E_DEPT_temp.selectExpr(
	"JNR_WM_E_DEPT___LOCATION_ID as LOCATION_ID",
	"JNR_WM_E_DEPT___DEPT_ID as DEPT_ID",
	"JNR_WM_E_DEPT___DEPT_CODE as DEPT_CODE",
	"JNR_WM_E_DEPT___DESCRIPTION as DESCRIPTION",
	"JNR_WM_E_DEPT___CREATE_DATE_TIME as CREATE_DATE_TIME",
	"JNR_WM_E_DEPT___MOD_DATE_TIME as MOD_DATE_TIME",
	"JNR_WM_E_DEPT___USER_ID as USER_ID",
	"JNR_WM_E_DEPT___WHSE as WHSE",
	"JNR_WM_E_DEPT___MISC_TXT_1 as MISC_TXT_1",
	"JNR_WM_E_DEPT___MISC_TXT_2 as MISC_TXT_2",
	"JNR_WM_E_DEPT___MISC_NUM_1 as MISC_NUM_1",
	"JNR_WM_E_DEPT___MISC_NUM_2 as MISC_NUM_2",
	"JNR_WM_E_DEPT___PERF_GOAL as PERF_GOAL",
	"JNR_WM_E_DEPT___VERSION_ID as VERSION_ID",
	"JNR_WM_E_DEPT___CREATED_DTTM as CREATED_DTTM",
	"JNR_WM_E_DEPT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
	"JNR_WM_E_DEPT___in_WM_DEPT_ID as in_WM_DEPT_ID",
	"JNR_WM_E_DEPT___in_LOAD_TSTMP as in_LOAD_TSTMP",
	"JNR_WM_E_DEPT___in_WM_CREATE_TSTMP as in_WM_CREATE_TSTMP",
	"JNR_WM_E_DEPT___in_WM_MOD_TSTMP as in_WM_MOD_TSTMP",
	"JNR_WM_E_DEPT___in_WM_CREATED_TSTMP as in_WM_CREATED_TSTMP",
	"JNR_WM_E_DEPT___in_WM_LAST_UPDATED_TSTMP as in_WM_LAST_UPDATED_TSTMP").filter(f"in_WM_DEPT_ID.isNull() OR ( in_WM_DEPT_ID.isNotNull()  &  ( when((CREATE_DATE_TIME.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(CREATE_DATE_TIME) != when((in_WM_CREATE_TSTMP.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(in_WM_CREATE_TSTMP) OR when((MOD_DATE_TIME.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(MOD_DATE_TIME) != when((in_WM_MOD_TSTMP.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(in_WM_MOD_TSTMP) OR when((CREATED_DTTM.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(CREATED_DTTM) != when((in_WM_CREATED_TSTMP.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(in_WM_CREATED_TSTMP) OR when((LAST_UPDATED_DTTM.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(LAST_UPDATED_DTTM) != when((in_WM_LAST_UPDATED_TSTMP.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(in_WM_LAST_UPDATED_TSTMP) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_EVAL_VALUES, type EXPRESSION 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
FIL_NO_CHANGE_REC_temp = FIL_NO_CHANGE_REC.toDF(*["FIL_NO_CHANGE_REC___" + col for col in FIL_NO_CHANGE_REC.columns])

EXP_EVAL_VALUES = FIL_NO_CHANGE_REC_temp.selectExpr(
	"FIL_NO_CHANGE_REC___sys_row_id as sys_row_id",
	"FIL_NO_CHANGE_REC___LOCATION_ID as LOCATION_ID",
	"FIL_NO_CHANGE_REC___DEPT_ID as DEPT_ID",
	"FIL_NO_CHANGE_REC___DEPT_CODE as DEPT_CODE",
	"FIL_NO_CHANGE_REC___DESCRIPTION as DESCRIPTION",
	"FIL_NO_CHANGE_REC___CREATE_DATE_TIME as CREATE_DATE_TIME",
	"FIL_NO_CHANGE_REC___MOD_DATE_TIME as MOD_DATE_TIME",
	"FIL_NO_CHANGE_REC___USER_ID as USER_ID",
	"FIL_NO_CHANGE_REC___WHSE as WHSE",
	"FIL_NO_CHANGE_REC___MISC_TXT_1 as MISC_TXT_1",
	"FIL_NO_CHANGE_REC___MISC_TXT_2 as MISC_TXT_2",
	"FIL_NO_CHANGE_REC___MISC_NUM_1 as MISC_NUM_1",
	"FIL_NO_CHANGE_REC___MISC_NUM_2 as MISC_NUM_2",
	"FIL_NO_CHANGE_REC___PERF_GOAL as PERF_GOAL",
	"FIL_NO_CHANGE_REC___VERSION_ID as VERSION_ID",
	"FIL_NO_CHANGE_REC___CREATED_DTTM as CREATED_DTTM",
	"FIL_NO_CHANGE_REC___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
	"when((ISNULL ( FIL_NO_CHANGE_REC___in_LOAD_TSTMP )),(current_date())).otherwise(FIL_NO_CHANGE_REC___in_LOAD_TSTMP) as LOAD_TSTMP",
	"current_date() as UPDATE_TSTMP",
	"FIL_NO_CHANGE_REC___in_WM_DEPT_ID as in_WM_DEPT_ID"
)

# COMMAND ----------
# Processing node UPD_VALIDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
EXP_EVAL_VALUES_temp = EXP_EVAL_VALUES.toDF(*["EXP_EVAL_VALUES___" + col for col in EXP_EVAL_VALUES.columns])

UPD_VALIDATE = EXP_EVAL_VALUES_temp.selectExpr(
	"EXP_EVAL_VALUES___LOCATION_ID as LOCATION_ID",
	"EXP_EVAL_VALUES___DEPT_ID as DEPT_ID",
	"EXP_EVAL_VALUES___DEPT_CODE as DEPT_CODE",
	"EXP_EVAL_VALUES___DESCRIPTION as DESCRIPTION",
	"EXP_EVAL_VALUES___CREATE_DATE_TIME as CREATE_DATE_TIME",
	"EXP_EVAL_VALUES___MOD_DATE_TIME as MOD_DATE_TIME",
	"EXP_EVAL_VALUES___USER_ID as USER_ID",
	"EXP_EVAL_VALUES___WHSE as WHSE",
	"EXP_EVAL_VALUES___MISC_TXT_1 as MISC_TXT_1",
	"EXP_EVAL_VALUES___MISC_TXT_2 as MISC_TXT_2",
	"EXP_EVAL_VALUES___MISC_NUM_1 as MISC_NUM_1",
	"EXP_EVAL_VALUES___MISC_NUM_2 as MISC_NUM_2",
	"EXP_EVAL_VALUES___PERF_GOAL as PERF_GOAL",
	"EXP_EVAL_VALUES___VERSION_ID as VERSION_ID",
	"EXP_EVAL_VALUES___CREATED_DTTM as CREATED_DTTM",
	"EXP_EVAL_VALUES___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
	"EXP_EVAL_VALUES___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_EVAL_VALUES___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_EVAL_VALUES___in_WM_DEPT_ID as in_WM_DEPT_ID")
	.withColumn('pyspark_data_action', when((EXP_EVAL_VALUES.in_WM_DEPT_ID.isNull()) ,(lit(0))) .otherwise(lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_DEPT, type TARGET 
# COLUMN COUNT: 18

Shortcut_to_WM_E_DEPT = DeltaTable.forPath(spark, '/tmp/delta/WM_E_DEPT')
Shortcut_to_WM_E_DEPT.alias('WM_E_DEPT').merge(UPD_VALIDATE.alias('UPD_VALIDATE'),
'WM_E_DEPT.LOCATION_ID = UPD_VALIDATE.LOCATION_ID and WM_E_DEPT.WM_DEPT_ID = UPD_VALIDATE.DEPT_ID')
.whenMatchedUpdate(set = {
	'LOCATION_ID' : 'UPD_VALIDATE.LOCATION_ID',
	'WM_DEPT_ID' : 'UPD_VALIDATE.DEPT_ID',
	'WM_WHSE' : 'UPD_VALIDATE.WHSE',
	'WM_DEPT_CD' : 'UPD_VALIDATE.DEPT_CODE',
	'WM_DEPT_DESC' : 'UPD_VALIDATE.DESCRIPTION',
	'PERF_GOAL' : 'UPD_VALIDATE.PERF_GOAL',
	'MISC_TXT_1' : 'UPD_VALIDATE.MISC_TXT_1',
	'MISC_TXT_2' : 'UPD_VALIDATE.MISC_TXT_2',
	'MISC_NUM_1' : 'UPD_VALIDATE.MISC_NUM_1',
	'MISC_NUM_2' : 'UPD_VALIDATE.MISC_NUM_2',
	'WM_USER_ID' : 'UPD_VALIDATE.USER_ID',
	'WM_VERSION_ID' : 'UPD_VALIDATE.VERSION_ID',
	'WM_CREATED_TSTMP' : 'UPD_VALIDATE.CREATED_DTTM',
	'WM_LAST_UPDATED_TSTMP' : 'UPD_VALIDATE.LAST_UPDATED_DTTM',
	'WM_CREATE_TSTMP' : 'UPD_VALIDATE.CREATE_DATE_TIME',
	'WM_MOD_TSTMP' : 'UPD_VALIDATE.MOD_DATE_TIME',
	'UPDATE_TSTMP' : 'UPD_VALIDATE.LOAD_TSTMP',
	'LOAD_TSTMP' : 'UPD_VALIDATE.UPDATE_TSTMP'} )
.whenNotMatchedInsert(values = {
	'LOCATION_ID' : 'UPD_VALIDATE.LOCATION_ID',
	'WM_DEPT_ID' : 'UPD_VALIDATE.DEPT_ID',
	'WM_WHSE' : 'UPD_VALIDATE.WHSE',
	'WM_DEPT_CD' : 'UPD_VALIDATE.DEPT_CODE',
	'WM_DEPT_DESC' : 'UPD_VALIDATE.DESCRIPTION',
	'PERF_GOAL' : 'UPD_VALIDATE.PERF_GOAL',
	'MISC_TXT_1' : 'UPD_VALIDATE.MISC_TXT_1',
	'MISC_TXT_2' : 'UPD_VALIDATE.MISC_TXT_2',
	'MISC_NUM_1' : 'UPD_VALIDATE.MISC_NUM_1',
	'MISC_NUM_2' : 'UPD_VALIDATE.MISC_NUM_2',
	'WM_USER_ID' : 'UPD_VALIDATE.USER_ID',
	'WM_VERSION_ID' : 'UPD_VALIDATE.VERSION_ID',
	'WM_CREATED_TSTMP' : 'UPD_VALIDATE.CREATED_DTTM',
	'WM_LAST_UPDATED_TSTMP' : 'UPD_VALIDATE.LAST_UPDATED_DTTM',
	'WM_CREATE_TSTMP' : 'UPD_VALIDATE.CREATE_DATE_TIME',
	'WM_MOD_TSTMP' : 'UPD_VALIDATE.MOD_DATE_TIME',
	'UPDATE_TSTMP' : 'UPD_VALIDATE.LOAD_TSTMP',
	'LOAD_TSTMP' : 'UPD_VALIDATE.UPDATE_TSTMP'}).execute()

quit()