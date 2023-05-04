#Code converted on 2023-05-03 09:47:12
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
# read_infa_paramfile('', 'm_WM_E_Dept_PRE') ProcessingUtils

# COMMAND ----------
# Variable_declaration_comment
dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='Prev_Run_Dt', defaultValue='1/1/1901')
dbutils.widgets.text(name='Initial_Load', defaultValue='')

# COMMAND ----------
# Processing node SQ_Shortcut_to_E_DEPT, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_E_DEPT = spark.read.jdbc(os.environ.get('DBConnection_Source_CONNECT_STRING'), f"""SELECT
E_DEPT.DEPT_ID,
E_DEPT.DEPT_CODE,
E_DEPT.DESCRIPTION,
E_DEPT.CREATE_DATE_TIME,
E_DEPT.MOD_DATE_TIME,
E_DEPT.USER_ID,
E_DEPT.WHSE,
E_DEPT.MISC_TXT_1,
E_DEPT.MISC_TXT_2,
E_DEPT.MISC_NUM_1,
E_DEPT.MISC_NUM_2,
E_DEPT.PERF_GOAL,
E_DEPT.VERSION_ID,
E_DEPT.CREATED_DTTM,
E_DEPT.LAST_UPDATED_DTTM
FROM E_DEPT
WHERE $$Initial_Load (date_trunc('DD', CREATE_DATE_TIME) >= date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) OR (date_trunc('DD', MOD_DATE_TIME) >=  date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) OR (date_trunc('DD', CREATED_DTTM) >= date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) OR (date_trunc('DD', LAST_UPDATED_DTTM) >=  date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) AND



1=1""", 
properties={
'user': os.environ.get('DBConnection_Source_LOGIN'),
'password': os.environ.get('DBConnection_Source_PASSWORD'),
'driver': os.environ.get('ORACLE_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_E_DEPT = SQ_Shortcut_to_E_DEPT \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[0],'DEPT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[1],'DEPT_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[2],'DESCRIPTION') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[3],'CREATE_DATE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[4],'MOD_DATE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[5],'USER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[6],'WHSE') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[7],'MISC_TXT_1') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[8],'MISC_TXT_2') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[9],'MISC_NUM_1') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[10],'MISC_NUM_2') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[11],'PERF_GOAL') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[12],'VERSION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[13],'CREATED_DTTM') \
	.withColumnRenamed(SQ_Shortcut_to_E_DEPT.columns[14],'LAST_UPDATED_DTTM')

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 17

EXPTRANS = SQ_Shortcut_to_E_DEPT.select( \
	SQ_Shortcut_to_E_DEPT.sys_row_id.alias('sys_row_id'), \
	(os.environ.get(lit('DC_NBR'))).alias('DC_NBR_EXP'), \
	SQ_Shortcut_to_E_DEPT.DEPT_ID.alias('DEPT_ID'), \
	SQ_Shortcut_to_E_DEPT.DEPT_CODE.alias('DEPT_CODE'), \
	SQ_Shortcut_to_E_DEPT.DESCRIPTION.alias('DESCRIPTION'), \
	SQ_Shortcut_to_E_DEPT.CREATE_DATE_TIME.alias('CREATE_DATE_TIME'), \
	SQ_Shortcut_to_E_DEPT.MOD_DATE_TIME.alias('MOD_DATE_TIME'), \
	SQ_Shortcut_to_E_DEPT.USER_ID.alias('USER_ID'), \
	SQ_Shortcut_to_E_DEPT.WHSE.alias('WHSE'), \
	SQ_Shortcut_to_E_DEPT.MISC_TXT_1.alias('MISC_TXT_1'), \
	SQ_Shortcut_to_E_DEPT.MISC_TXT_2.alias('MISC_TXT_2'), \
	SQ_Shortcut_to_E_DEPT.MISC_NUM_1.alias('MISC_NUM_1'), \
	SQ_Shortcut_to_E_DEPT.MISC_NUM_2.alias('MISC_NUM_2'), \
	SQ_Shortcut_to_E_DEPT.PERF_GOAL.alias('PERF_GOAL'), \
	SQ_Shortcut_to_E_DEPT.VERSION_ID.alias('VERSION_ID'), \
	SQ_Shortcut_to_E_DEPT.CREATED_DTTM.alias('CREATED_DTTM'), \
	SQ_Shortcut_to_E_DEPT.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	(current_timestamp()()).alias('LOAD_TSTMP_EXP') \
)

# COMMAND ----------
# Processing node Shortcut_to_WM_E_DEPT_PRE, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_WM_E_DEPT_PRE = EXPTRANS.select( \
	EXPTRANS.DC_NBR_EXP.cast(LongType()).alias('DC_NBR'), \
	EXPTRANS.DEPT_ID.cast(LongType()).alias('DEPT_ID'), \
	EXPTRANS.DEPT_CODE.cast(StringType()).alias('DEPT_CODE'), \
	EXPTRANS.DESCRIPTION.cast(StringType()).alias('DESCRIPTION'), \
	EXPTRANS.CREATE_DATE_TIME.cast(TimestampType()).alias('CREATE_DATE_TIME'), \
	EXPTRANS.MOD_DATE_TIME.cast(TimestampType()).alias('MOD_DATE_TIME'), \
	EXPTRANS.USER_ID.cast(StringType()).alias('USER_ID'), \
	EXPTRANS.WHSE.cast(StringType()).alias('WHSE'), \
	EXPTRANS.MISC_TXT_1.cast(StringType()).alias('MISC_TXT_1'), \
	EXPTRANS.MISC_TXT_2.cast(StringType()).alias('MISC_TXT_2'), \
	EXPTRANS.MISC_NUM_1.cast(LongType()).alias('MISC_NUM_1'), \
	EXPTRANS.MISC_NUM_2.cast(LongType()).alias('MISC_NUM_2'), \
	EXPTRANS.PERF_GOAL.cast(LongType()).alias('PERF_GOAL'), \
	EXPTRANS.VERSION_ID.cast(LongType()).alias('VERSION_ID'), \
	EXPTRANS.CREATED_DTTM.cast(TimestampType()).alias('CREATED_DTTM'), \
	EXPTRANS.LAST_UPDATED_DTTM.cast(TimestampType()).alias('LAST_UPDATED_DTTM'), \
	EXPTRANS.LOAD_TSTMP_EXP.cast(TimestampType()).alias('LOAD_TSTMP') \
)
Shortcut_to_WM_E_DEPT_PRE.write.saveAsTable('WM_E_DEPT_PRE', mode = 'overwrite')