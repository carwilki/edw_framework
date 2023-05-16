#Code converted on 2023-05-16 15:07:16
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
WHERE '$$Initial_Load' (trunc(CREATE_DATE_TIME) >= trunc(to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) | (trunc(MOD_DATE_TIME) >=  trunc(to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) | (trunc(CREATED_DTTM) >= trunc(to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1) | (trunc(LAST_UPDATED_DTTM) >=  trunc(to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS'))-1)  & 1=1""", 
properties={
'user': os.environ.get('DBConnection_Source_LOGIN'),
'password': os.environ.get('DBConnection_Source_PASSWORD'),
'driver': os.environ.get('ORACLE_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 17

EXPTRANS = SQ_Shortcut_to_E_DEPT.selectExpr(
	"sys_row_id as sys_row_id",
	"'$$DC_NBR' as DC_NBR_EXP",
	"SQ_Shortcut_to_E_DEPT.DEPT_ID as DEPT_ID",
	"SQ_Shortcut_to_E_DEPT.DEPT_CODE as DEPT_CODE",
	"SQ_Shortcut_to_E_DEPT.DESCRIPTION as DESCRIPTION",
	"SQ_Shortcut_to_E_DEPT.CREATE_DATE_TIME as CREATE_DATE_TIME",
	"SQ_Shortcut_to_E_DEPT.MOD_DATE_TIME as MOD_DATE_TIME",
	"SQ_Shortcut_to_E_DEPT.USER_ID as USER_ID",
	"SQ_Shortcut_to_E_DEPT.WHSE as WHSE",
	"SQ_Shortcut_to_E_DEPT.MISC_TXT_1 as MISC_TXT_1",
	"SQ_Shortcut_to_E_DEPT.MISC_TXT_2 as MISC_TXT_2",
	"SQ_Shortcut_to_E_DEPT.MISC_NUM_1 as MISC_NUM_1",
	"SQ_Shortcut_to_E_DEPT.MISC_NUM_2 as MISC_NUM_2",
	"SQ_Shortcut_to_E_DEPT.PERF_GOAL as PERF_GOAL",
	"SQ_Shortcut_to_E_DEPT.VERSION_ID as VERSION_ID",
	"SQ_Shortcut_to_E_DEPT.CREATED_DTTM as CREATED_DTTM",
	"SQ_Shortcut_to_E_DEPT.LAST_UPDATED_DTTM as LAST_UPDATED_DTTM",
	"current_timestamp() () as LOAD_TSTMP_EXP"
)

# COMMAND ----------
# Processing node Shortcut_to_WM_E_DEPT_PRE, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_WM_E_DEPT_PRE = EXPTRANS.selectExpr(
	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR",
	"CAST(DEPT_ID AS BIGINT) as DEPT_ID",
	"CAST(DEPT_CODE AS VARCHAR) as DEPT_CODE",
	"CAST(DESCRIPTION AS VARCHAR) as DESCRIPTION",
	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as CREATE_DATE_TIME",
	"CAST(MOD_DATE_TIME AS TIMESTAMP) as MOD_DATE_TIME",
	"CAST(USER_ID AS VARCHAR) as USER_ID",
	"CAST(WHSE AS VARCHAR) as WHSE",
	"CAST(MISC_TXT_1 AS VARCHAR) as MISC_TXT_1",
	"CAST(MISC_TXT_2 AS VARCHAR) as MISC_TXT_2",
	"CAST(MISC_NUM_1 AS BIGINT) as MISC_NUM_1",
	"CAST(MISC_NUM_2 AS BIGINT) as MISC_NUM_2",
	"CAST(PERF_GOAL AS BIGINT) as PERF_GOAL",
	"CAST(VERSION_ID AS BIGINT) as VERSION_ID",
	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM",
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM",
	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_WM_E_DEPT_PRE.write.saveAsTable('WM_E_DEPT_PRE', mode = 'append')

quit()