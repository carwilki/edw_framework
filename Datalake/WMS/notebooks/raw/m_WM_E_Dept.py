#Code converted on 2023-05-03 09:46:35
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import SparkContext;
from pyspark import SparkConf
from pyspark.sql.session import SparkSession
from datetime import datetime
from dbruntime import dbutils
#from PySparkBQWriter import *
#import ProcessingUtils;
#bqw = PySparkBQWriter()
#bqw.setDebug(True)

# COMMAND ----------

conf = SparkConf().setMaster('local')
sc = SparkContext.getOrCreate(conf = conf)
spark = SparkSession(sc)

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
# Conforming fields names to the component layout
SQ_Shortcut_to_WM_E_DEPT_PRE = SQ_Shortcut_to_WM_E_DEPT_PRE \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[0],'DC_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[1],'DEPT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[2],'DEPT_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[3],'DESCRIPTION') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[4],'CREATE_DATE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[5],'MOD_DATE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[6],'USER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[7],'WHSE') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[8],'MISC_TXT_1') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[9],'MISC_TXT_2') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[10],'MISC_NUM_1') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[11],'MISC_NUM_2') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[12],'PERF_GOAL') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[13],'VERSION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[14],'CREATED_DTTM') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[15],'LAST_UPDATED_DTTM') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT_PRE.columns[16],'LOAD_TSTMP')

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
# Conforming fields names to the component layout
SQ_Shortcut_to_WM_E_DEPT = SQ_Shortcut_to_WM_E_DEPT \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT.columns[1],'WM_DEPT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT.columns[2],'WM_CREATED_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT.columns[3],'WM_LAST_UPDATED_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT.columns[4],'WM_CREATE_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT.columns[5],'WM_MOD_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_WM_E_DEPT.columns[6],'LOAD_TSTMP')

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

EXP_INT_CONV = SQ_Shortcut_to_WM_E_DEPT_PRE.select( \
	SQ_Shortcut_to_WM_E_DEPT_PRE.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.DC_NBR.alias('in_DC_NBR'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.DEPT_ID.alias('DEPT_ID'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.DEPT_CODE.alias('DEPT_CODE'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.DESCRIPTION.alias('DESCRIPTION'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.CREATE_DATE_TIME.alias('CREATE_DATE_TIME'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.MOD_DATE_TIME.alias('MOD_DATE_TIME'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.USER_ID.alias('USER_ID'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.WHSE.alias('WHSE'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.MISC_TXT_1.alias('MISC_TXT_1'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.MISC_TXT_2.alias('MISC_TXT_2'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.MISC_NUM_1.alias('MISC_NUM_1'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.MISC_NUM_2.alias('MISC_NUM_2'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.PERF_GOAL.alias('PERF_GOAL'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.VERSION_ID.alias('VERSION_ID'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.CREATED_DTTM.alias('CREATED_DTTM'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	SQ_Shortcut_to_WM_E_DEPT_PRE.LOAD_TSTMP.alias('LOAD_TSTMP')).select( \
	(col('sys_row_id')).alias('sys_row_id'), \
	(TO_INTEGER(col('in_DC_NBR'))).alias('DC_NBR'), \
	col('DEPT_ID'), \
	col('DEPT_CODE'), \
	col('DESCRIPTION'), \
	col('CREATE_DATE_TIME'), \
	col('MOD_DATE_TIME'), \
	col('USER_ID'), \
	col('WHSE'), \
	col('MISC_TXT_1'), \
	col('MISC_TXT_2'), \
	col('MISC_NUM_1'), \
	col('MISC_NUM_2'), \
	col('PERF_GOAL'), \
	col('VERSION_ID'), \
	col('CREATED_DTTM'), \
	col('LAST_UPDATED_DTTM'), \
	col('LOAD_TSTMP') \
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
# Conforming fields names to the component layout
SQ_Shortcut_to_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[1],'STORE_NBR')

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 19

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONV,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONV.DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_E_DEPT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 24

JNR_WM_E_DEPT = SQ_Shortcut_to_WM_E_DEPT.join(JNR_SITE_PROFILE,[SQ_Shortcut_to_WM_E_DEPT.LOCATION_ID == JNR_SITE_PROFILE.LOCATION_ID, SQ_Shortcut_to_WM_E_DEPT.WM_DEPT_ID == JNR_SITE_PROFILE.DEPT_ID],'right_outer').select( \
	SQ_Shortcut_to_WM_E_DEPT.sys_row_id.alias('sys_row_id'), \
	JNR_SITE_PROFILE.LOCATION_ID.alias('LOCATION_ID'), \
	JNR_SITE_PROFILE.DEPT_ID.alias('DEPT_ID'), \
	JNR_SITE_PROFILE.DEPT_CODE.alias('DEPT_CODE'), \
	JNR_SITE_PROFILE.DESCRIPTION.alias('DESCRIPTION'), \
	JNR_SITE_PROFILE.CREATE_DATE_TIME.alias('CREATE_DATE_TIME'), \
	JNR_SITE_PROFILE.MOD_DATE_TIME.alias('MOD_DATE_TIME'), \
	JNR_SITE_PROFILE.USER_ID.alias('USER_ID'), \
	JNR_SITE_PROFILE.WHSE.alias('WHSE'), \
	JNR_SITE_PROFILE.MISC_TXT_1.alias('MISC_TXT_1'), \
	JNR_SITE_PROFILE.MISC_TXT_2.alias('MISC_TXT_2'), \
	JNR_SITE_PROFILE.MISC_NUM_1.alias('MISC_NUM_1'), \
	JNR_SITE_PROFILE.MISC_NUM_2.alias('MISC_NUM_2'), \
	JNR_SITE_PROFILE.PERF_GOAL.alias('PERF_GOAL'), \
	JNR_SITE_PROFILE.VERSION_ID.alias('VERSION_ID'), \
	JNR_SITE_PROFILE.CREATED_DTTM.alias('CREATED_DTTM'), \
	JNR_SITE_PROFILE.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	JNR_SITE_PROFILE.LOAD_TSTMP.alias('LOAD_TSTMP'), \
	SQ_Shortcut_to_WM_E_DEPT.LOCATION_ID.alias('in_LOCATION_ID'), \
	SQ_Shortcut_to_WM_E_DEPT.WM_DEPT_ID.alias('in_WM_DEPT_ID'), \
	SQ_Shortcut_to_WM_E_DEPT.LOAD_TSTMP.alias('in_LOAD_TSTMP'), \
	SQ_Shortcut_to_WM_E_DEPT.WM_CREATE_TSTMP.alias('in_WM_CREATE_TSTMP'), \
	SQ_Shortcut_to_WM_E_DEPT.WM_MOD_TSTMP.alias('in_WM_MOD_TSTMP'), \
	SQ_Shortcut_to_WM_E_DEPT.WM_CREATED_TSTMP.alias('in_WM_CREATED_TSTMP'), \
	SQ_Shortcut_to_WM_E_DEPT.WM_LAST_UPDATED_TSTMP.alias('in_WM_LAST_UPDATED_TSTMP'))

# COMMAND ----------
# Processing node FIL_NO_CHANGE_REC, type FILTER 
# COLUMN COUNT: 22

FIL_NO_CHANGE_REC = JNR_WM_E_DEPT.select( \
	JNR_WM_E_DEPT.LOCATION_ID.alias('LOCATION_ID'), \
	JNR_WM_E_DEPT.DEPT_ID.alias('DEPT_ID'), \
	JNR_WM_E_DEPT.DEPT_CODE.alias('DEPT_CODE'), \
	JNR_WM_E_DEPT.DESCRIPTION.alias('DESCRIPTION'), \
	JNR_WM_E_DEPT.CREATE_DATE_TIME.alias('CREATE_DATE_TIME'), \
	JNR_WM_E_DEPT.MOD_DATE_TIME.alias('MOD_DATE_TIME'), \
	JNR_WM_E_DEPT.USER_ID.alias('USER_ID'), \
	JNR_WM_E_DEPT.WHSE.alias('WHSE'), \
	JNR_WM_E_DEPT.MISC_TXT_1.alias('MISC_TXT_1'), \
	JNR_WM_E_DEPT.MISC_TXT_2.alias('MISC_TXT_2'), \
	JNR_WM_E_DEPT.MISC_NUM_1.alias('MISC_NUM_1'), \
	JNR_WM_E_DEPT.MISC_NUM_2.alias('MISC_NUM_2'), \
	JNR_WM_E_DEPT.PERF_GOAL.alias('PERF_GOAL'), \
	JNR_WM_E_DEPT.VERSION_ID.alias('VERSION_ID'), \
	JNR_WM_E_DEPT.CREATED_DTTM.alias('CREATED_DTTM'), \
	JNR_WM_E_DEPT.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	JNR_WM_E_DEPT.in_WM_DEPT_ID.alias('in_WM_DEPT_ID'), \
	JNR_WM_E_DEPT.in_LOAD_TSTMP.alias('in_LOAD_TSTMP'), \
	JNR_WM_E_DEPT.in_WM_CREATE_TSTMP.alias('in_WM_CREATE_TSTMP'), \
	JNR_WM_E_DEPT.in_WM_MOD_TSTMP.alias('in_WM_MOD_TSTMP'), \
	JNR_WM_E_DEPT.in_WM_CREATED_TSTMP.alias('in_WM_CREATED_TSTMP'), \
	JNR_WM_E_DEPT.in_WM_LAST_UPDATED_TSTMP.alias('in_WM_LAST_UPDATED_TSTMP')).filter("in_WM_DEPT_ID __DOT__ isNull() OR ( NOT in_WM_DEPT_ID __DOT__ isNull() AND ( when((CREATE_DATE_TIME __DOT__ isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))) __DOT__ otherwise(CREATE_DATE_TIME) != when((in_WM_CREATE_TSTMP __DOT__ isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))) __DOT__ otherwise(in_WM_CREATE_TSTMP) OR when((MOD_DATE_TIME __DOT__ isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))) __DOT__ otherwise(MOD_DATE_TIME) != when((in_WM_MOD_TSTMP __DOT__ isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))) __DOT__ otherwise(in_WM_MOD_TSTMP) OR when((CREATED_DTTM __DOT__ isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))) __DOT__ otherwise(CREATED_DTTM) != when((in_WM_CREATED_TSTMP __DOT__ isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))) __DOT__ otherwise(in_WM_CREATED_TSTMP) OR when((LAST_UPDATED_DTTM __DOT__ isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))) __DOT__ otherwise(LAST_UPDATED_DTTM) != when((in_WM_LAST_UPDATED_TSTMP __DOT__ isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))) __DOT__ otherwise(in_WM_LAST_UPDATED_TSTMP) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_EVAL_VALUES, type EXPRESSION 
# COLUMN COUNT: 19

EXP_EVAL_VALUES = FIL_NO_CHANGE_REC.select( \
	FIL_NO_CHANGE_REC.sys_row_id.alias('sys_row_id'), \
	FIL_NO_CHANGE_REC.LOCATION_ID.alias('LOCATION_ID'), \
	FIL_NO_CHANGE_REC.DEPT_ID.alias('DEPT_ID'), \
	FIL_NO_CHANGE_REC.DEPT_CODE.alias('DEPT_CODE'), \
	FIL_NO_CHANGE_REC.DESCRIPTION.alias('DESCRIPTION'), \
	FIL_NO_CHANGE_REC.CREATE_DATE_TIME.alias('CREATE_DATE_TIME'), \
	FIL_NO_CHANGE_REC.MOD_DATE_TIME.alias('MOD_DATE_TIME'), \
	FIL_NO_CHANGE_REC.USER_ID.alias('USER_ID'), \
	FIL_NO_CHANGE_REC.WHSE.alias('WHSE'), \
	FIL_NO_CHANGE_REC.MISC_TXT_1.alias('MISC_TXT_1'), \
	FIL_NO_CHANGE_REC.MISC_TXT_2.alias('MISC_TXT_2'), \
	FIL_NO_CHANGE_REC.MISC_NUM_1.alias('MISC_NUM_1'), \
	FIL_NO_CHANGE_REC.MISC_NUM_2.alias('MISC_NUM_2'), \
	FIL_NO_CHANGE_REC.PERF_GOAL.alias('PERF_GOAL'), \
	FIL_NO_CHANGE_REC.VERSION_ID.alias('VERSION_ID'), \
	FIL_NO_CHANGE_REC.CREATED_DTTM.alias('CREATED_DTTM'), \
	FIL_NO_CHANGE_REC.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	(when((col('in_LOAD_TSTMP').isNull()) ,(current_date())).otherwise(col('in_LOAD_TSTMP'))).alias('LOAD_TSTMP'), \
	(current_date()).alias('UPDATE_TSTMP'), \
	FIL_NO_CHANGE_REC.in_WM_DEPT_ID.alias('in_WM_DEPT_ID') \
)

# COMMAND ----------
# Processing node UPD_VALIDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 19

UPD_VALIDATE = EXP_EVAL_VALUES.select( \
	EXP_EVAL_VALUES.LOCATION_ID.alias('LOCATION_ID'), \
	EXP_EVAL_VALUES.DEPT_ID.alias('DEPT_ID'), \
	EXP_EVAL_VALUES.DEPT_CODE.alias('DEPT_CODE'), \
	EXP_EVAL_VALUES.DESCRIPTION.alias('DESCRIPTION'), \
	EXP_EVAL_VALUES.CREATE_DATE_TIME.alias('CREATE_DATE_TIME'), \
	EXP_EVAL_VALUES.MOD_DATE_TIME.alias('MOD_DATE_TIME'), \
	EXP_EVAL_VALUES.USER_ID.alias('USER_ID'), \
	EXP_EVAL_VALUES.WHSE.alias('WHSE'), \
	EXP_EVAL_VALUES.MISC_TXT_1.alias('MISC_TXT_1'), \
	EXP_EVAL_VALUES.MISC_TXT_2.alias('MISC_TXT_2'), \
	EXP_EVAL_VALUES.MISC_NUM_1.alias('MISC_NUM_1'), \
	EXP_EVAL_VALUES.MISC_NUM_2.alias('MISC_NUM_2'), \
	EXP_EVAL_VALUES.PERF_GOAL.alias('PERF_GOAL'), \
	EXP_EVAL_VALUES.VERSION_ID.alias('VERSION_ID'), \
	EXP_EVAL_VALUES.CREATED_DTTM.alias('CREATED_DTTM'), \
	EXP_EVAL_VALUES.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	EXP_EVAL_VALUES.LOAD_TSTMP.alias('LOAD_TSTMP'), \
	EXP_EVAL_VALUES.UPDATE_TSTMP.alias('UPDATE_TSTMP'), \
	EXP_EVAL_VALUES.in_WM_DEPT_ID.alias('in_WM_DEPT_ID')) \
	.withColumn('pyspark_data_action', when((EXP_EVAL_VALUES.in_WM_DEPT_ID.isNull()) ,(lit(0))).otherwise(lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_DEPT, type TARGET 
# COLUMN COUNT: 18


Shortcut_to_WM_E_DEPT = UPD_VALIDATE.select( \
	UPD_VALIDATE.LOCATION_ID.cast(LongType()).alias('LOCATION_ID'), \
	UPD_VALIDATE.DEPT_ID.cast(LongType()).alias('WM_DEPT_ID'), \
	UPD_VALIDATE.WHSE.cast(StringType()).alias('WM_WHSE'), \
	UPD_VALIDATE.DEPT_CODE.cast(StringType()).alias('WM_DEPT_CD'), \
	UPD_VALIDATE.DESCRIPTION.cast(StringType()).alias('WM_DEPT_DESC'), \
	UPD_VALIDATE.PERF_GOAL.cast(LongType()).alias('PERF_GOAL'), \
	UPD_VALIDATE.MISC_TXT_1.cast(StringType()).alias('MISC_TXT_1'), \
	UPD_VALIDATE.MISC_TXT_2.cast(StringType()).alias('MISC_TXT_2'), \
	UPD_VALIDATE.MISC_NUM_1.cast(LongType()).alias('MISC_NUM_1'), \
	UPD_VALIDATE.MISC_NUM_2.cast(LongType()).alias('MISC_NUM_2'), \
	UPD_VALIDATE.USER_ID.cast(StringType()).alias('WM_USER_ID'), \
	UPD_VALIDATE.VERSION_ID.cast(LongType()).alias('WM_VERSION_ID'), \
	UPD_VALIDATE.CREATED_DTTM.cast(TimestampType()).alias('WM_CREATED_TSTMP'), \
	UPD_VALIDATE.LAST_UPDATED_DTTM.cast(TimestampType()).alias('WM_LAST_UPDATED_TSTMP'), \
	UPD_VALIDATE.CREATE_DATE_TIME.cast(TimestampType()).alias('WM_CREATE_TSTMP'), \
	UPD_VALIDATE.MOD_DATE_TIME.cast(TimestampType()).alias('WM_MOD_TSTMP'), \
	UPD_VALIDATE.LOAD_TSTMP.cast(TimestampType()).alias('UPDATE_TSTMP'), \
	UPD_VALIDATE.UPDATE_TSTMP.cast(TimestampType()).alias('LOAD_TSTMP'), \
	UPD_VALIDATE.pyspark_data_action.alias('pyspark_data_action') \
)
Shortcut_to_WM_E_DEPT.write.saveAsTable('WM_E_DEPT', mode = 'append')

quit()