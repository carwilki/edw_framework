#Code converted on 2023-08-01 13:49:16
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


parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = or_kro_read(env)
# print((username, password, connection_string))
# COMMAND ----------
# Processing node SQ_Shortcut_to_VP_TIMESHTPUNCHV42, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_VP_TIMESHTPUNCHV42 = jdbcOracleConnection(f"""SELECT 

PE.PERSONNUM, 

TI.EVENTDTM, 

TI.STARTDTM, 

TI.ENDDTM, 

TI.TIMESHEETITEMID, 

OJ1.ORGPATHTXT 

FROM 

TIMESHEETITEM TI

INNER JOIN PERSON PE 

ON (PE.PERSONID = TI.EMPLOYEEID)         

LEFT OUTER JOIN WFCJOBORG OJ1 

ON (

   TI.ORGIDSID = OJ1.ORGIDSID AND 

   OJ1.EFFECTIVEDTM <= TI.EVENTDTM AND 

   OJ1.EXPIRATIONDTM > TI.EVENTDTM

   )

WHERE 

TI.EVENTDTM <= TO_DATE('2023-08-10','YYYY-MM-DD')

AND 

TI.EVENTDTM > TO_DATE('2023-08-10','YYYY-MM-DD') - 7

AND

TI.DELETEDSW = 0

AND

(TI.STARTDTM IS NOT NULL

OR

TI.ENDDTM IS NOT NULL)""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
#
# replaced
# TI.EVENTDTM = TO_DATE(TO_CHAR(CURRENT_DATE,'YYYYMMDD'),'YYYYMMDD')
# with 
# TI.EVENTDTM = TO_DATE('2023-08-10','YYYY-MM-DD')
# for testing purpose
#
#


SQ_Shortcut_to_VP_TIMESHTPUNCHV42 = SQ_Shortcut_to_VP_TIMESHTPUNCHV42 \
	.withColumnRenamed(SQ_Shortcut_to_VP_TIMESHTPUNCHV42.columns[0],'PERSONNUM') \
	.withColumnRenamed(SQ_Shortcut_to_VP_TIMESHTPUNCHV42.columns[1],'EVENTDATE') \
	.withColumnRenamed(SQ_Shortcut_to_VP_TIMESHTPUNCHV42.columns[2],'STARTDTM') \
	.withColumnRenamed(SQ_Shortcut_to_VP_TIMESHTPUNCHV42.columns[3],'ENDDTM') \
	.withColumnRenamed(SQ_Shortcut_to_VP_TIMESHTPUNCHV42.columns[4],'TIMESHEETITEMID') \
	.withColumnRenamed(SQ_Shortcut_to_VP_TIMESHTPUNCHV42.columns[5],'ORGPATHTXT')

# COMMAND ----------
# Processing node EXP_TIME_SHEET, type EXPRESSION 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_VP_TIMESHTPUNCHV42_temp = SQ_Shortcut_to_VP_TIMESHTPUNCHV42.toDF(*["SQ_Shortcut_to_VP_TIMESHTPUNCHV42___" + col for col in SQ_Shortcut_to_VP_TIMESHTPUNCHV42.columns])

# select substr('first/second/thrid/fourth/fifth/sixth/seventh/eight' ,len(substring_index('first/second/thrid/fourth/fifth/sixth/seventh/eight', '/' ,5 )) +2 ,4)

# "SUBSTR ( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , INSTR ( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/','1','7' ) + 1 , ( ( INSTR ( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/','1','8' ) - 1 ) - INSTR ( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/','1','7' ) ) ) as DEPT_DESC", \


EXP_TIME_SHEET = SQ_Shortcut_to_VP_TIMESHTPUNCHV42_temp.selectExpr( \
	"SQ_Shortcut_to_VP_TIMESHTPUNCHV42___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_VP_TIMESHTPUNCHV42___PERSONNUM as PERSONNUM", \
	"SQ_Shortcut_to_VP_TIMESHTPUNCHV42___EVENTDATE as EVENTDATE", \
	"SQ_Shortcut_to_VP_TIMESHTPUNCHV42___STARTDTM as STARTDTM", \
	"SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ENDDTM as ENDDTM", \
	"SQ_Shortcut_to_VP_TIMESHTPUNCHV42___TIMESHEETITEMID as TIMESHEETITEMID", \
	"cast(SUBSTR ( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , len(substring_index( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/',5 )) + 2 , 4 ) as int) as STORE_NUMBER", \
	"SUBSTR ( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , len(substring_index( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/',6 )) + 2 , ( ( len(substring_index( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/',7 ))  ) - len(substring_index( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/',6 )) +1 ) ) as BUSN_AREA_DESC", \
	"SUBSTR ( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , len(substring_index( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/',7 )) + 2 , ( ( len(substring_index( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/',8 ))  ) - len(substring_index( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/',7 ))+1 ) ) as DEPT_DESC", \
	"SUBSTR ( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , len(substring_index( SQ_Shortcut_to_VP_TIMESHTPUNCHV42___ORGPATHTXT , '/',8 )) + 2 , 100 ) as TASK_DESC", \
	"CURRENT_TIMESTAMP as LOAD_DT" \
).withColumn('BUSN_AREA_DESC',expr("substring(BUSN_AREA_DESC,1,length(BUSN_AREA_DESC)-2)")) \
.withColumn('DEPT_DESC',expr("substring(DEPT_DESC,1,length(DEPT_DESC)-2)"))

# COMMAND ----------
# Processing node Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE = EXP_TIME_SHEET.selectExpr( \
	"CAST(EVENTDATE AS TIMESTAMP) as DAY_DT", \
	"CAST(TIMESHEETITEMID AS bigint) as TIME_SHEET_ITEM_ID", \
	"CAST(STARTDTM AS TIMESTAMP) as STRT_DTM", \
	"CAST(ENDDTM AS TIMESTAMP) as END_DTM", \
	"CAST(STORE_NUMBER AS INT) as STORE_NBR", \
	"CAST(PERSONNUM as bigint) as EMPLOYEE_ID", \
	"CAST(BUSN_AREA_DESC AS STRING) as WFA_BUSN_AREA_DESC", \
	"CAST(DEPT_DESC AS STRING) as WFA_DEPT_DESC", \
	"CAST(TASK_DESC AS STRING) as WFA_TASK_DESC", \
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT" \
)
# TODO is this an overwrite ?? from session mapping looks like insert
Shortcut_to_WFA_TIME_SHEET_PUNCH_PRE.write.saveAsTable(f'{raw}.WFA_TIME_SHEET_PUNCH_PRE',mode='overwrite')