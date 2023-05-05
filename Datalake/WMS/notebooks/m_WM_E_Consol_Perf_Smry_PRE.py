#Code converted on 2023-05-03 09:47:02
import os
from pyspark.dbutils import DBUtils
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import SparkContext;
from pyspark.sql.session import SparkSession
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
# Processing node SQ_Shortcut_to_E_CONSOL_PERF_SMRY, type SOURCE 
# COLUMN COUNT: 84
SQ_Shortcut_to_E_CONSOL_PERF_SMRY = spark.read.jdbc(os.environ.get('DBConnection_Source_CONNECT_STRING'), f"""SELECT
E_CONSOL_PERF_SMRY.PERF_SMRY_TRAN_ID,
E_CONSOL_PERF_SMRY.WHSE,
E_CONSOL_PERF_SMRY.LOGIN_USER_ID,
E_CONSOL_PERF_SMRY.JOB_FUNCTION_NAME,
E_CONSOL_PERF_SMRY.SPVSR_LOGIN_USER_ID,
E_CONSOL_PERF_SMRY.DEPT_CODE,
E_CONSOL_PERF_SMRY.CLOCK_IN_DATE,
E_CONSOL_PERF_SMRY.CLOCK_IN_STATUS,
E_CONSOL_PERF_SMRY.TOTAL_SAM,
E_CONSOL_PERF_SMRY.TOTAL_PAM,
E_CONSOL_PERF_SMRY.TOTAL_TIME,
E_CONSOL_PERF_SMRY.OSDL,
E_CONSOL_PERF_SMRY.OSIL,
E_CONSOL_PERF_SMRY.NSDL,
E_CONSOL_PERF_SMRY.SIL,
E_CONSOL_PERF_SMRY.UDIL,
E_CONSOL_PERF_SMRY.UIL,
E_CONSOL_PERF_SMRY.ADJ_OSDL,
E_CONSOL_PERF_SMRY.ADJ_OSIL,
E_CONSOL_PERF_SMRY.ADJ_UDIL,
E_CONSOL_PERF_SMRY.ADJ_NSDL,
E_CONSOL_PERF_SMRY.PAID_BRK,
E_CONSOL_PERF_SMRY.UNPAID_BRK,
E_CONSOL_PERF_SMRY.REF_OSDL,
E_CONSOL_PERF_SMRY.REF_OSIL,
E_CONSOL_PERF_SMRY.REF_UDIL,
E_CONSOL_PERF_SMRY.REF_NSDL,
E_CONSOL_PERF_SMRY.REF_ADJ_OSDL,
E_CONSOL_PERF_SMRY.REF_ADJ_OSIL,
E_CONSOL_PERF_SMRY.REF_ADJ_UDIL,
E_CONSOL_PERF_SMRY.REF_ADJ_NSDL,
E_CONSOL_PERF_SMRY.MISC_NUMBER_1,
E_CONSOL_PERF_SMRY.CREATE_DATE_TIME,
E_CONSOL_PERF_SMRY.MOD_DATE_TIME,
E_CONSOL_PERF_SMRY.USER_ID,
E_CONSOL_PERF_SMRY.MISC_1,
E_CONSOL_PERF_SMRY.MISC_2,
E_CONSOL_PERF_SMRY.CLOCK_OUT_DATE,
E_CONSOL_PERF_SMRY.SHIFT_CODE,
E_CONSOL_PERF_SMRY.EVENT_COUNT,
E_CONSOL_PERF_SMRY.START_DATE_TIME,
E_CONSOL_PERF_SMRY.END_DATE_TIME,
E_CONSOL_PERF_SMRY.LEVEL_1,
E_CONSOL_PERF_SMRY.LEVEL_2,
E_CONSOL_PERF_SMRY.LEVEL_3,
E_CONSOL_PERF_SMRY.LEVEL_4,
E_CONSOL_PERF_SMRY.LEVEL_5,
E_CONSOL_PERF_SMRY.WHSE_DATE,
E_CONSOL_PERF_SMRY.OPS_CODE,
E_CONSOL_PERF_SMRY.REF_SAM,
E_CONSOL_PERF_SMRY.REF_PAM,
E_CONSOL_PERF_SMRY.REPORT_SHIFT,
E_CONSOL_PERF_SMRY.MISC_TXT_1,
E_CONSOL_PERF_SMRY.MISC_TXT_2,
E_CONSOL_PERF_SMRY.MISC_NUM_1,
E_CONSOL_PERF_SMRY.MISC_NUM_2,
E_CONSOL_PERF_SMRY.EVNT_CTGRY_1,
E_CONSOL_PERF_SMRY.EVNT_CTGRY_2,
E_CONSOL_PERF_SMRY.EVNT_CTGRY_3,
E_CONSOL_PERF_SMRY.EVNT_CTGRY_4,
E_CONSOL_PERF_SMRY.EVNT_CTGRY_5,
E_CONSOL_PERF_SMRY.LABOR_COST_RATE,
E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSDL,
E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSDL,
E_CONSOL_PERF_SMRY.PAID_OVERLAP_NSDL,
E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_NSDL,
E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSIL,
E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSIL,
E_CONSOL_PERF_SMRY.PAID_OVERLAP_UDIL,
E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_UDIL,
E_CONSOL_PERF_SMRY.VERSION_ID,
E_CONSOL_PERF_SMRY.TEAM_CODE,
E_CONSOL_PERF_SMRY.DEFAULT_JF_FLAG,
E_CONSOL_PERF_SMRY.EMP_PERF_SMRY_ID,
E_CONSOL_PERF_SMRY.TOTAL_QTY,
E_CONSOL_PERF_SMRY.REF_NBR,
E_CONSOL_PERF_SMRY.TEAM_BEGIN_TIME,
E_CONSOL_PERF_SMRY.THRUPUT_MIN,
E_CONSOL_PERF_SMRY.DISPLAY_UOM_QTY,
E_CONSOL_PERF_SMRY.DISPLAY_UOM,
E_CONSOL_PERF_SMRY.LOCN_GRP_ATTR,
E_CONSOL_PERF_SMRY.RESOURCE_GROUP_ID,
E_CONSOL_PERF_SMRY.COMP_ASSIGNMENT_ID,
E_CONSOL_PERF_SMRY.REFLECTIVE_CODE
FROM E_CONSOL_PERF_SMRY
WHERE 
$$Initial_Load (date_trunc('DD', E_CONSOL_PERF_SMRY.CREATE_DATE_TIME) >= date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS')) - 1 ) 
OR (date_trunc('DD', E_CONSOL_PERF_SMRY.MOD_DATE_TIME) >= date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS')) - 1) 
AND 1=1""", 
properties={
'user': os.environ.get('DBConnection_Source_LOGIN'),
'password': os.environ.get('DBConnection_Source_PASSWORD'),
'driver': os.environ.get('ORACLE_DRIVER')})

# COMMAND ----------
# Processing node Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE, type TARGET 
# COLUMN COUNT: 86


Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE = SQ_Shortcut_to_E_CONSOL_PERF_SMRY.select( \
	(os.environ.get(lit(f'{dcnbr}'))).cast(LongType()).alias('DC_NBR'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PERF_SMRY_TRAN_ID.cast(LongType()).alias('PERF_SMRY_TRAN_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.WHSE.cast(StringType()).alias('WHSE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LOGIN_USER_ID.cast(StringType()).alias('LOGIN_USER_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.JOB_FUNCTION_NAME.cast(StringType()).alias('JOB_FUNCTION_NAME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.SPVSR_LOGIN_USER_ID.cast(StringType()).alias('SPVSR_LOGIN_USER_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DEPT_CODE.cast(StringType()).alias('DEPT_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CLOCK_IN_DATE.cast(TimestampType()).alias('CLOCK_IN_DATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CLOCK_IN_STATUS.cast(LongType()).alias('CLOCK_IN_STATUS'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_SAM.cast(LongType()).alias('TOTAL_SAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_PAM.cast(LongType()).alias('TOTAL_PAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_TIME.cast(LongType()).alias('TOTAL_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.OSDL.cast(LongType()).alias('OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.OSIL.cast(LongType()).alias('OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.NSDL.cast(LongType()).alias('NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.SIL.cast(LongType()).alias('SIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UDIL.cast(LongType()).alias('UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UIL.cast(LongType()).alias('UIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_OSDL.cast(LongType()).alias('ADJ_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_OSIL.cast(LongType()).alias('ADJ_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_UDIL.cast(LongType()).alias('ADJ_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_NSDL.cast(LongType()).alias('ADJ_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_BRK.cast(LongType()).alias('PAID_BRK'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_BRK.cast(LongType()).alias('UNPAID_BRK'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_OSDL.cast(LongType()).alias('REF_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_OSIL.cast(LongType()).alias('REF_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_UDIL.cast(LongType()).alias('REF_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_NSDL.cast(LongType()).alias('REF_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_OSDL.cast(LongType()).alias('REF_ADJ_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_OSIL.cast(LongType()).alias('REF_ADJ_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_UDIL.cast(LongType()).alias('REF_ADJ_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_NSDL.cast(LongType()).alias('REF_ADJ_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_NUMBER_1.cast(LongType()).alias('MISC_NUMBER_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CREATE_DATE_TIME.cast(TimestampType()).alias('CREATE_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MOD_DATE_TIME.cast(TimestampType()).alias('MOD_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.USER_ID.cast(StringType()).alias('USER_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_1.cast(StringType()).alias('MISC_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_2.cast(StringType()).alias('MISC_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CLOCK_OUT_DATE.cast(TimestampType()).alias('CLOCK_OUT_DATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.SHIFT_CODE.cast(StringType()).alias('SHIFT_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVENT_COUNT.cast(LongType()).alias('EVENT_COUNT'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.START_DATE_TIME.cast(TimestampType()).alias('START_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.END_DATE_TIME.cast(TimestampType()).alias('END_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_1.cast(StringType()).alias('LEVEL_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_2.cast(StringType()).alias('LEVEL_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_3.cast(StringType()).alias('LEVEL_3'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_4.cast(StringType()).alias('LEVEL_4'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_5.cast(StringType()).alias('LEVEL_5'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.WHSE_DATE.cast(TimestampType()).alias('WHSE_DATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.OPS_CODE.cast(StringType()).alias('OPS_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_SAM.cast(LongType()).alias('REF_SAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_PAM.cast(LongType()).alias('REF_PAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REPORT_SHIFT.cast(StringType()).alias('REPORT_SHIFT'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_TXT_1.cast(StringType()).alias('MISC_TXT_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_TXT_2.cast(StringType()).alias('MISC_TXT_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_NUM_1.cast(LongType()).alias('MISC_NUM_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_NUM_2.cast(LongType()).alias('MISC_NUM_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_1.cast(StringType()).alias('EVNT_CTGRY_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_2.cast(StringType()).alias('EVNT_CTGRY_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_3.cast(StringType()).alias('EVNT_CTGRY_3'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_4.cast(StringType()).alias('EVNT_CTGRY_4'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_5.cast(StringType()).alias('EVNT_CTGRY_5'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LABOR_COST_RATE.cast(LongType()).alias('LABOR_COST_RATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSDL.cast(LongType()).alias('PAID_OVERLAP_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSDL.cast(LongType()).alias('UNPAID_OVERLAP_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_NSDL.cast(LongType()).alias('PAID_OVERLAP_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_NSDL.cast(LongType()).alias('UNPAID_OVERLAP_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSIL.cast(LongType()).alias('PAID_OVERLAP_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSIL.cast(LongType()).alias('UNPAID_OVERLAP_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_UDIL.cast(LongType()).alias('PAID_OVERLAP_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_UDIL.cast(LongType()).alias('UNPAID_OVERLAP_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.VERSION_ID.cast(LongType()).alias('VERSION_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TEAM_CODE.cast(StringType()).alias('TEAM_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DEFAULT_JF_FLAG.cast(LongType()).alias('DEFAULT_JF_FLAG'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EMP_PERF_SMRY_ID.cast(LongType()).alias('EMP_PERF_SMRY_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_QTY.cast(LongType()).alias('TOTAL_QTY'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_NBR.cast(StringType()).alias('REF_NBR'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TEAM_BEGIN_TIME.cast(TimestampType()).alias('TEAM_BEGIN_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.THRUPUT_MIN.cast(LongType()).alias('THRUPUT_MIN'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DISPLAY_UOM_QTY.cast(LongType()).alias('DISPLAY_UOM_QTY'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DISPLAY_UOM.cast(StringType()).alias('DISPLAY_UOM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LOCN_GRP_ATTR.cast(StringType()).alias('LOCN_GRP_ATTR'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.RESOURCE_GROUP_ID.cast(StringType()).alias('RESOURCE_GROUP_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.COMP_ASSIGNMENT_ID.cast(StringType()).alias('COMP_ASSIGNMENT_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REFLECTIVE_CODE.cast(StringType()).alias('REFLECTIVE_CODE'), \
	(current_timestamp()()).cast(TimestampType()).alias('LOAD_TSTMP') \
)

#this needs to be a merge statement
#Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE.write.saveAsTable(f'pre.WM_E_CONSOL_PERF_SMRY_PRE_{dcnbr}', mode = 'append')