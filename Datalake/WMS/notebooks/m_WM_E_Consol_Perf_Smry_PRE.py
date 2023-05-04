#Code converted on 2023-05-03 09:47:02
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
# read_infa_paramfile('', 'm_WM_E_Consol_Perf_Smry_PRE') ProcessingUtils

# COMMAND ----------
# Variable_declaration_comment
dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='Prev_Run_Dt', defaultValue='01/01/1901')
dbutils.widgets.text(name='Initial_Load', defaultValue='')

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
WHERE $$Initial_Load (date_trunc('DD', E_CONSOL_PERF_SMRY.CREATE_DATE_TIME) >= date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS')) - 1 ) OR (date_trunc('DD', E_CONSOL_PERF_SMRY.MOD_DATE_TIME) >= date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS')) - 1) AND 



1=1""", 
properties={
'user': os.environ.get('DBConnection_Source_LOGIN'),
'password': os.environ.get('DBConnection_Source_PASSWORD'),
'driver': os.environ.get('ORACLE_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_E_CONSOL_PERF_SMRY = SQ_Shortcut_to_E_CONSOL_PERF_SMRY \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[0],'PERF_SMRY_TRAN_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[1],'WHSE') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[2],'LOGIN_USER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[3],'JOB_FUNCTION_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[4],'SPVSR_LOGIN_USER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[5],'DEPT_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[6],'CLOCK_IN_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[7],'CLOCK_IN_STATUS') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[8],'TOTAL_SAM') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[9],'TOTAL_PAM') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[10],'TOTAL_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[11],'OSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[12],'OSIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[13],'NSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[14],'SIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[15],'UDIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[16],'UIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[17],'ADJ_OSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[18],'ADJ_OSIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[19],'ADJ_UDIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[20],'ADJ_NSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[21],'PAID_BRK') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[22],'UNPAID_BRK') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[23],'REF_OSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[24],'REF_OSIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[25],'REF_UDIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[26],'REF_NSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[27],'REF_ADJ_OSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[28],'REF_ADJ_OSIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[29],'REF_ADJ_UDIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[30],'REF_ADJ_NSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[31],'MISC_NUMBER_1') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[32],'CREATE_DATE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[33],'MOD_DATE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[34],'USER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[35],'MISC_1') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[36],'MISC_2') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[37],'CLOCK_OUT_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[38],'SHIFT_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[39],'EVENT_COUNT') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[40],'START_DATE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[41],'END_DATE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[42],'LEVEL_1') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[43],'LEVEL_2') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[44],'LEVEL_3') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[45],'LEVEL_4') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[46],'LEVEL_5') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[47],'WHSE_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[48],'OPS_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[49],'REF_SAM') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[50],'REF_PAM') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[51],'REPORT_SHIFT') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[52],'MISC_TXT_1') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[53],'MISC_TXT_2') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[54],'MISC_NUM_1') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[55],'MISC_NUM_2') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[56],'EVNT_CTGRY_1') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[57],'EVNT_CTGRY_2') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[58],'EVNT_CTGRY_3') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[59],'EVNT_CTGRY_4') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[60],'EVNT_CTGRY_5') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[61],'LABOR_COST_RATE') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[62],'PAID_OVERLAP_OSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[63],'UNPAID_OVERLAP_OSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[64],'PAID_OVERLAP_NSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[65],'UNPAID_OVERLAP_NSDL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[66],'PAID_OVERLAP_OSIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[67],'UNPAID_OVERLAP_OSIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[68],'PAID_OVERLAP_UDIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[69],'UNPAID_OVERLAP_UDIL') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[70],'VERSION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[71],'TEAM_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[72],'DEFAULT_JF_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[73],'EMP_PERF_SMRY_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[74],'TOTAL_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[75],'REF_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[76],'TEAM_BEGIN_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[77],'THRUPUT_MIN') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[78],'DISPLAY_UOM_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[79],'DISPLAY_UOM') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[80],'LOCN_GRP_ATTR') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[81],'RESOURCE_GROUP_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[82],'COMP_ASSIGNMENT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_E_CONSOL_PERF_SMRY.columns[83],'REFLECTIVE_CODE')

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 86

EXPTRANS = SQ_Shortcut_to_E_CONSOL_PERF_SMRY.select( \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.sys_row_id.alias('sys_row_id'), \
	(os.environ.get(lit('DC_NBR'))).alias('DC_NBR_EXP'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PERF_SMRY_TRAN_ID.alias('PERF_SMRY_TRAN_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.WHSE.alias('WHSE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LOGIN_USER_ID.alias('LOGIN_USER_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.JOB_FUNCTION_NAME.alias('JOB_FUNCTION_NAME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.SPVSR_LOGIN_USER_ID.alias('SPVSR_LOGIN_USER_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DEPT_CODE.alias('DEPT_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CLOCK_IN_DATE.alias('CLOCK_IN_DATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CLOCK_IN_STATUS.alias('CLOCK_IN_STATUS'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_SAM.alias('TOTAL_SAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_PAM.alias('TOTAL_PAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_TIME.alias('TOTAL_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.OSDL.alias('OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.OSIL.alias('OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.NSDL.alias('NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.SIL.alias('SIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UDIL.alias('UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UIL.alias('UIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_OSDL.alias('ADJ_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_OSIL.alias('ADJ_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_UDIL.alias('ADJ_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_NSDL.alias('ADJ_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_BRK.alias('PAID_BRK'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_BRK.alias('UNPAID_BRK'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_OSDL.alias('REF_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_OSIL.alias('REF_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_UDIL.alias('REF_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_NSDL.alias('REF_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_OSDL.alias('REF_ADJ_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_OSIL.alias('REF_ADJ_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_UDIL.alias('REF_ADJ_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_NSDL.alias('REF_ADJ_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_NUMBER_1.alias('MISC_NUMBER_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CREATE_DATE_TIME.alias('CREATE_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MOD_DATE_TIME.alias('MOD_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.USER_ID.alias('USER_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_1.alias('MISC_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_2.alias('MISC_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CLOCK_OUT_DATE.alias('CLOCK_OUT_DATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.SHIFT_CODE.alias('SHIFT_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVENT_COUNT.alias('EVENT_COUNT'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.START_DATE_TIME.alias('START_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.END_DATE_TIME.alias('END_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_1.alias('LEVEL_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_2.alias('LEVEL_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_3.alias('LEVEL_3'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_4.alias('LEVEL_4'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_5.alias('LEVEL_5'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.WHSE_DATE.alias('WHSE_DATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.OPS_CODE.alias('OPS_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_SAM.alias('REF_SAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_PAM.alias('REF_PAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REPORT_SHIFT.alias('REPORT_SHIFT'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_TXT_1.alias('MISC_TXT_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_TXT_2.alias('MISC_TXT_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_NUM_1.alias('MISC_NUM_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_NUM_2.alias('MISC_NUM_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_1.alias('EVNT_CTGRY_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_2.alias('EVNT_CTGRY_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_3.alias('EVNT_CTGRY_3'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_4.alias('EVNT_CTGRY_4'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_5.alias('EVNT_CTGRY_5'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LABOR_COST_RATE.alias('LABOR_COST_RATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSDL.alias('PAID_OVERLAP_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSDL.alias('UNPAID_OVERLAP_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_NSDL.alias('PAID_OVERLAP_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_NSDL.alias('UNPAID_OVERLAP_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSIL.alias('PAID_OVERLAP_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSIL.alias('UNPAID_OVERLAP_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_UDIL.alias('PAID_OVERLAP_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_UDIL.alias('UNPAID_OVERLAP_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.VERSION_ID.alias('VERSION_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TEAM_CODE.alias('TEAM_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DEFAULT_JF_FLAG.alias('DEFAULT_JF_FLAG'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EMP_PERF_SMRY_ID.alias('EMP_PERF_SMRY_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_QTY.alias('TOTAL_QTY'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_NBR.alias('REF_NBR'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TEAM_BEGIN_TIME.alias('TEAM_BEGIN_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.THRUPUT_MIN.alias('THRUPUT_MIN'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DISPLAY_UOM_QTY.alias('DISPLAY_UOM_QTY'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DISPLAY_UOM.alias('DISPLAY_UOM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LOCN_GRP_ATTR.alias('LOCN_GRP_ATTR'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.RESOURCE_GROUP_ID.alias('RESOURCE_GROUP_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.COMP_ASSIGNMENT_ID.alias('COMP_ASSIGNMENT_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REFLECTIVE_CODE.alias('REFLECTIVE_CODE'), \
	(current_timestamp()()).alias('LOAD_TSTMP_EXP') \
)

# COMMAND ----------
# Processing node Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE, type TARGET 
# COLUMN COUNT: 86


Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE = EXPTRANS.select( \
	EXPTRANS.DC_NBR_EXP.cast(LongType()).alias('DC_NBR'), \
	EXPTRANS.PERF_SMRY_TRAN_ID.cast(LongType()).alias('PERF_SMRY_TRAN_ID'), \
	EXPTRANS.WHSE.cast(StringType()).alias('WHSE'), \
	EXPTRANS.LOGIN_USER_ID.cast(StringType()).alias('LOGIN_USER_ID'), \
	EXPTRANS.JOB_FUNCTION_NAME.cast(StringType()).alias('JOB_FUNCTION_NAME'), \
	EXPTRANS.SPVSR_LOGIN_USER_ID.cast(StringType()).alias('SPVSR_LOGIN_USER_ID'), \
	EXPTRANS.DEPT_CODE.cast(StringType()).alias('DEPT_CODE'), \
	EXPTRANS.CLOCK_IN_DATE.cast(TimestampType()).alias('CLOCK_IN_DATE'), \
	EXPTRANS.CLOCK_IN_STATUS.cast(LongType()).alias('CLOCK_IN_STATUS'), \
	EXPTRANS.TOTAL_SAM.cast(LongType()).alias('TOTAL_SAM'), \
	EXPTRANS.TOTAL_PAM.cast(LongType()).alias('TOTAL_PAM'), \
	EXPTRANS.TOTAL_TIME.cast(LongType()).alias('TOTAL_TIME'), \
	EXPTRANS.OSDL.cast(LongType()).alias('OSDL'), \
	EXPTRANS.OSIL.cast(LongType()).alias('OSIL'), \
	EXPTRANS.NSDL.cast(LongType()).alias('NSDL'), \
	EXPTRANS.SIL.cast(LongType()).alias('SIL'), \
	EXPTRANS.UDIL.cast(LongType()).alias('UDIL'), \
	EXPTRANS.UIL.cast(LongType()).alias('UIL'), \
	EXPTRANS.ADJ_OSDL.cast(LongType()).alias('ADJ_OSDL'), \
	EXPTRANS.ADJ_OSIL.cast(LongType()).alias('ADJ_OSIL'), \
	EXPTRANS.ADJ_UDIL.cast(LongType()).alias('ADJ_UDIL'), \
	EXPTRANS.ADJ_NSDL.cast(LongType()).alias('ADJ_NSDL'), \
	EXPTRANS.PAID_BRK.cast(LongType()).alias('PAID_BRK'), \
	EXPTRANS.UNPAID_BRK.cast(LongType()).alias('UNPAID_BRK'), \
	EXPTRANS.REF_OSDL.cast(LongType()).alias('REF_OSDL'), \
	EXPTRANS.REF_OSIL.cast(LongType()).alias('REF_OSIL'), \
	EXPTRANS.REF_UDIL.cast(LongType()).alias('REF_UDIL'), \
	EXPTRANS.REF_NSDL.cast(LongType()).alias('REF_NSDL'), \
	EXPTRANS.REF_ADJ_OSDL.cast(LongType()).alias('REF_ADJ_OSDL'), \
	EXPTRANS.REF_ADJ_OSIL.cast(LongType()).alias('REF_ADJ_OSIL'), \
	EXPTRANS.REF_ADJ_UDIL.cast(LongType()).alias('REF_ADJ_UDIL'), \
	EXPTRANS.REF_ADJ_NSDL.cast(LongType()).alias('REF_ADJ_NSDL'), \
	EXPTRANS.MISC_NUMBER_1.cast(LongType()).alias('MISC_NUMBER_1'), \
	EXPTRANS.CREATE_DATE_TIME.cast(TimestampType()).alias('CREATE_DATE_TIME'), \
	EXPTRANS.MOD_DATE_TIME.cast(TimestampType()).alias('MOD_DATE_TIME'), \
	EXPTRANS.USER_ID.cast(StringType()).alias('USER_ID'), \
	EXPTRANS.MISC_1.cast(StringType()).alias('MISC_1'), \
	EXPTRANS.MISC_2.cast(StringType()).alias('MISC_2'), \
	EXPTRANS.CLOCK_OUT_DATE.cast(TimestampType()).alias('CLOCK_OUT_DATE'), \
	EXPTRANS.SHIFT_CODE.cast(StringType()).alias('SHIFT_CODE'), \
	EXPTRANS.EVENT_COUNT.cast(LongType()).alias('EVENT_COUNT'), \
	EXPTRANS.START_DATE_TIME.cast(TimestampType()).alias('START_DATE_TIME'), \
	EXPTRANS.END_DATE_TIME.cast(TimestampType()).alias('END_DATE_TIME'), \
	EXPTRANS.LEVEL_1.cast(StringType()).alias('LEVEL_1'), \
	EXPTRANS.LEVEL_2.cast(StringType()).alias('LEVEL_2'), \
	EXPTRANS.LEVEL_3.cast(StringType()).alias('LEVEL_3'), \
	EXPTRANS.LEVEL_4.cast(StringType()).alias('LEVEL_4'), \
	EXPTRANS.LEVEL_5.cast(StringType()).alias('LEVEL_5'), \
	EXPTRANS.WHSE_DATE.cast(TimestampType()).alias('WHSE_DATE'), \
	EXPTRANS.OPS_CODE.cast(StringType()).alias('OPS_CODE'), \
	EXPTRANS.REF_SAM.cast(LongType()).alias('REF_SAM'), \
	EXPTRANS.REF_PAM.cast(LongType()).alias('REF_PAM'), \
	EXPTRANS.REPORT_SHIFT.cast(StringType()).alias('REPORT_SHIFT'), \
	EXPTRANS.MISC_TXT_1.cast(StringType()).alias('MISC_TXT_1'), \
	EXPTRANS.MISC_TXT_2.cast(StringType()).alias('MISC_TXT_2'), \
	EXPTRANS.MISC_NUM_1.cast(LongType()).alias('MISC_NUM_1'), \
	EXPTRANS.MISC_NUM_2.cast(LongType()).alias('MISC_NUM_2'), \
	EXPTRANS.EVNT_CTGRY_1.cast(StringType()).alias('EVNT_CTGRY_1'), \
	EXPTRANS.EVNT_CTGRY_2.cast(StringType()).alias('EVNT_CTGRY_2'), \
	EXPTRANS.EVNT_CTGRY_3.cast(StringType()).alias('EVNT_CTGRY_3'), \
	EXPTRANS.EVNT_CTGRY_4.cast(StringType()).alias('EVNT_CTGRY_4'), \
	EXPTRANS.EVNT_CTGRY_5.cast(StringType()).alias('EVNT_CTGRY_5'), \
	EXPTRANS.LABOR_COST_RATE.cast(LongType()).alias('LABOR_COST_RATE'), \
	EXPTRANS.PAID_OVERLAP_OSDL.cast(LongType()).alias('PAID_OVERLAP_OSDL'), \
	EXPTRANS.UNPAID_OVERLAP_OSDL.cast(LongType()).alias('UNPAID_OVERLAP_OSDL'), \
	EXPTRANS.PAID_OVERLAP_NSDL.cast(LongType()).alias('PAID_OVERLAP_NSDL'), \
	EXPTRANS.UNPAID_OVERLAP_NSDL.cast(LongType()).alias('UNPAID_OVERLAP_NSDL'), \
	EXPTRANS.PAID_OVERLAP_OSIL.cast(LongType()).alias('PAID_OVERLAP_OSIL'), \
	EXPTRANS.UNPAID_OVERLAP_OSIL.cast(LongType()).alias('UNPAID_OVERLAP_OSIL'), \
	EXPTRANS.PAID_OVERLAP_UDIL.cast(LongType()).alias('PAID_OVERLAP_UDIL'), \
	EXPTRANS.UNPAID_OVERLAP_UDIL.cast(LongType()).alias('UNPAID_OVERLAP_UDIL'), \
	EXPTRANS.VERSION_ID.cast(LongType()).alias('VERSION_ID'), \
	EXPTRANS.TEAM_CODE.cast(StringType()).alias('TEAM_CODE'), \
	EXPTRANS.DEFAULT_JF_FLAG.cast(LongType()).alias('DEFAULT_JF_FLAG'), \
	EXPTRANS.EMP_PERF_SMRY_ID.cast(LongType()).alias('EMP_PERF_SMRY_ID'), \
	EXPTRANS.TOTAL_QTY.cast(LongType()).alias('TOTAL_QTY'), \
	EXPTRANS.REF_NBR.cast(StringType()).alias('REF_NBR'), \
	EXPTRANS.TEAM_BEGIN_TIME.cast(TimestampType()).alias('TEAM_BEGIN_TIME'), \
	EXPTRANS.THRUPUT_MIN.cast(LongType()).alias('THRUPUT_MIN'), \
	EXPTRANS.DISPLAY_UOM_QTY.cast(LongType()).alias('DISPLAY_UOM_QTY'), \
	EXPTRANS.DISPLAY_UOM.cast(StringType()).alias('DISPLAY_UOM'), \
	EXPTRANS.LOCN_GRP_ATTR.cast(StringType()).alias('LOCN_GRP_ATTR'), \
	EXPTRANS.RESOURCE_GROUP_ID.cast(StringType()).alias('RESOURCE_GROUP_ID'), \
	EXPTRANS.COMP_ASSIGNMENT_ID.cast(StringType()).alias('COMP_ASSIGNMENT_ID'), \
	EXPTRANS.REFLECTIVE_CODE.cast(StringType()).alias('REFLECTIVE_CODE'), \
	EXPTRANS.LOAD_TSTMP_EXP.cast(TimestampType()).alias('LOAD_TSTMP') \
)
Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE.write.saveAsTable('WM_E_CONSOL_PERF_SMRY_PRE', mode = 'append')

quit()