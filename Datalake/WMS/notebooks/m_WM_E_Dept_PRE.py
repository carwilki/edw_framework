#Code converted on 2023-05-03 09:47:12
import os
from pyspark.dbutils import DBUtils
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime
# COMMAND ----------
# configure spark and dbutils. This is required when building notebooks outside of
# the notebook itself.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
dbutils = DBUtils(sc)

# COMMAND ----------
# Variable_declaration_comment
# Read in job variables
# read_infa_paramfile('', 'm_WM_E_Consol_Perf_Smry_PRE') ProcessingUtils
dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='Prev_Run_Dt', defaultValue='01/01/1901')
dbutils.widgets.text(name='Initial_Load', defaultValue='')
dbutils.widgets.text(name='catalog', defaultValue='dev')

# Set global variables
starttime = datetime.now() #start timestamp of the script
catalog = dbutils.widgets.get('catalog', as_type=str)
dcnbr = dbutils.widgets.get('DC_NBR', as_type=str)
prev_run_dt = dbutils.widgets.get('Prev_Run_Dt', as_type=str)	
initial_load = dbutils.widgets.get('Initial_Load', as_type=str)


# COMMAND ----------
# Set the catalog to use.
spark.sql(f"USE CATALOG {catalog}")

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
WHERE 
$$Initial_Load (date_trunc('DD', CREATE_DATE_TIME) >= date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) 
OR (date_trunc('DD', MOD_DATE_TIME) >=  date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) OR (date_trunc('DD', CREATED_DTTM) >= date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) OR (date_trunc('DD', LAST_UPDATED_DTTM) >=  date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) 
AND 1=1""", 
properties={
'user': os.environ.get('DBConnection_Source_LOGIN'),
'password': os.environ.get('DBConnection_Source_PASSWORD'),
'driver': os.environ.get('ORACLE_DRIVER')})

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 17

EXPTRANS = SQ_Shortcut_to_E_DEPT.select( \
	(os.environ.get(lit(f'{dcnbr}'))).cast(LongType()).alias('DC_NBR'), \
	SQ_Shortcut_to_E_DEPT.DEPT_ID.cast(LongType()).alias('DEPT_ID'), \
	SQ_Shortcut_to_E_DEPT.DEPT_CODE.cast(StringType()).alias('DEPT_CODE'), \
	SQ_Shortcut_to_E_DEPT.DESCRIPTION.cast(StringType()).alias('DESCRIPTION'), \
	SQ_Shortcut_to_E_DEPT.CREATE_DATE_TIME.cast(TimestampType()).alias('CREATE_DATE_TIME'), \
	SQ_Shortcut_to_E_DEPT.MOD_DATE_TIME.cast(TimestampType()).alias('MOD_DATE_TIME'), \
	SQ_Shortcut_to_E_DEPT.USER_ID.cast(StringType()).alias('USER_ID'), \
	SQ_Shortcut_to_E_DEPT.WHSE.cast(StringType()).alias('WHSE'), \
	SQ_Shortcut_to_E_DEPT.MISC_TXT_1.cast(StringType()).alias('MISC_TXT_1'), \
	SQ_Shortcut_to_E_DEPT.MISC_TXT_2.cast(StringType()).alias('MISC_TXT_2'), \
	SQ_Shortcut_to_E_DEPT.MISC_NUM_1.cast(LongType()).alias('MISC_NUM_1'), \
	SQ_Shortcut_to_E_DEPT.MISC_NUM_2.cast(LongType()).alias('MISC_NUM_2'), \
	SQ_Shortcut_to_E_DEPT.PERF_GOAL.cast(LongType()).alias('PERF_GOAL'), \
	SQ_Shortcut_to_E_DEPT.VERSION_ID.cast(LongType()).alias('VERSION_ID'), \
	SQ_Shortcut_to_E_DEPT.CREATED_DTTM.cast(TimestampType()).alias('CREATED_DTTM'), \
	SQ_Shortcut_to_E_DEPT.LAST_UPDATED_DTTM.cast(TimestampType()).alias('LAST_UPDATED_DTTM'), 
	lit(f'{starttime}').cast(TimestampType()).alias('LOAD_TSTMP') \
)
#this needs to be a merge statement
#EXPTRANS.write.saveAsTable(f'WM_E_DEPT_PRE', mode = 'overwrite')