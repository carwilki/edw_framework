#Code converted on 2023-05-03 09:46:28
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
# read_infa_paramfile('', 'm_Last_Run_Dt') ProcessingUtils

# COMMAND ----------
# Variable_declaration_comment
dbutils.widgets.text(name='Last_Run_Dt', defaultValue='1/1/1900')

# COMMAND ----------
# Processing node SQ_Shortcut_to_DAYS, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_DAYS = spark.read.jdbc(os.environ.get('NZ_EDW_CONNECT_STRING'), f"""SELECT
DAYS.DAY_DT
FROM DAYS
WHERE DAY_DT = CURRENT_DATE""", 
properties={
'user': os.environ.get('NZ_EDW_LOGIN'),
'password': os.environ.get('NZ_EDW_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_DAYS = SQ_Shortcut_to_DAYS \
	.withColumnRenamed(SQ_Shortcut_to_DAYS.columns[0],'DAY_DT')

# COMMAND ----------
# Processing node EXP_LAST_RUN_DT, type EXPRESSION 
# COLUMN COUNT: 1

EXP_LAST_RUN_DT = SQ_Shortcut_to_DAYS.withColumn("SET_LAST_RUN_DT", SETVARIABLE(os.environ.get(lit('Last_Run_Dt')) , trunc((to_timestamp(lit(lit(starttime))))))).select( \
	SQ_Shortcut_to_DAYS.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_to_DAYS.DAY_DT.alias('DAY_DT') \
)

# COMMAND ----------
# Processing node FIL_FALSE, type FILTER 
# COLUMN COUNT: 1

FIL_FALSE = EXP_LAST_RUN_DT.select( \
	EXP_LAST_RUN_DT.DAY_DT.alias('DAY_DT')).filter("false").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_WM_YARD, type TARGET 
# COLUMN COUNT: 34


Shortcut_to_WM_YARD = FIL_FALSE.select( \
	lit(None).cast(LongType()).alias('LOCATION_ID'), \
	lit(None).cast(LongType()).alias('WM_YARD_ID'), \
	lit(None).cast(LongType()).alias('WM_TC_COMPANY_ID'), \
	lit(None).cast(StringType()).alias('WM_YARD_NAME'), \
	lit(None).cast(LongType()).alias('WM_LOCATION_ID'), \
	lit(None).cast(LongType()).alias('WM_TIME_ZONE_ID'), \
	lit(None).cast(LongType()).alias('GENERATE_MOVE_TASK_FLAG'), \
	lit(None).cast(LongType()).alias('GENERATE_NEXT_EQUIP_FLAG'), \
	lit(None).cast(LongType()).alias('RANGE_TASKS_FLAG'), \
	lit(None).cast(LongType()).alias('SEAL_TASK_TRGD_FLAG'), \
	lit(None).cast(LongType()).alias('OVERRIDE_SYSTEM_TASKS_FLAG'), \
	lit(None).cast(LongType()).alias('TASKING_ALLOWED_FLAG'), \
	lit(None).cast(LongType()).alias('LOCK_TRAILER_ON_MOVE_TO_DOOR_FLAG'), \
	lit(None).cast(StringType()).alias('YARD_SVG_FILE'), \
	lit(None).cast(StringType()).alias('ADDRESS'), \
	lit(None).cast(StringType()).alias('CITY'), \
	lit(None).cast(StringType()).alias('STATE_PROV'), \
	lit(None).cast(StringType()).alias('POSTAL_CD'), \
	lit(None).cast(StringType()).alias('COUNTY'), \
	lit(None).cast(StringType()).alias('COUNTRY_CD'), \
	lit(None).cast(LongType()).alias('MAX_EQUIPMENT_ALLOWED'), \
	lit(None).cast(LongType()).alias('UPPER_CHECK_IN_TIME_MINS'), \
	lit(None).cast(LongType()).alias('LOWER_CHECK_IN_TIME_MINS'), \
	lit(None).cast(LongType()).alias('FIXED_TIME_MINS'), \
	lit(None).cast(LongType()).alias('THRESHOLD_PERCENT'), \
	lit(None).cast(LongType()).alias('MARK_FOR_DELETION'), \
	lit(None).cast(LongType()).alias('WM_CREATED_SOURCE_TYPE'), \
	lit(None).cast(StringType()).alias('WM_CREATED_SOURCE'), \
	lit(None).cast(TimestampType()).alias('WM_CREATED_TSTMP'), \
	lit(None).cast(LongType()).alias('WM_LAST_UPDATED_SOURCE_TYPE'), \
	lit(None).cast(StringType()).alias('WM_LAST_UPDATED_SOURCE'), \
	lit(None).cast(TimestampType()).alias('WM_LAST_UPDATED_TSTMP'), \
	lit(None).cast(TimestampType()).alias('UPDATE_TSTMP'), \
	FIL_FALSE.DAY_DT.cast(TimestampType()).alias('LOAD_TSTMP') \
)
Shortcut_to_WM_YARD.write.saveAsTable('WM_YARD', mode = 'append')

quit()