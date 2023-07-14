#Code converted on 2023-06-26 09:55:42
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

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
refined_perf_table = f"{refine}.WM_E_MSRMNT_RULE"
raw_perf_table = f"{raw}.WM_E_MSRMNT_RULE_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE, type SOURCE 
# COLUMN COUNT: 18

SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE = spark.sql(f"""SELECT
DC_NBR,
MSRMNT_ID,
RULE_NBR,
DESCRIPTION,
STATUS_FLAG,
THEN_STATEMENT,
ELSE_STATEMENT,
NOTE,
MISC,
CREATE_DATE_TIME,
MOD_DATE_TIME,
USER_ID,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
VERSION_ID,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE_temp = SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE.toDF(*["SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___" + col for col in SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___MSRMNT_ID as MSRMNT_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___RULE_NBR as RULE_NBR", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___STATUS_FLAG as STATUS_FLAG", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___THEN_STATEMENT as THEN_STATEMENT", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___ELSE_STATEMENT as ELSE_STATEMENT", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___NOTE as NOTE", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___MISC as MISC", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_MSRMNT_RULE, type SOURCE 
# COLUMN COUNT: 19

SQ_Shortcut_to_WM_E_MSRMNT_RULE = spark.sql(f"""SELECT
LOCATION_ID,
WM_MSRMNT_ID as MSRMNT_ID ,
WM_RULE_NBR as RULE_NBR ,
WM_MSRMNT_RULE_DESC as DESCRIPTION ,
STATUS_FLAG,
THEN_STATEMENT,
ELSE_STATEMENT,
NOTE,
MISC,
WM_CREATE_TSTMP as CREATE_DATE_TIME ,
WM_MOD_TSTMP as MOD_DATE_TIME ,
WM_USER_ID as USER_ID ,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
WM_VERSION_ID as VERSION_ID ,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_MSRMNT_ID IN (SELECT MSRMNT_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 20

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONV,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONV.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_E_MSRMRNT_RULE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 36

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_MSRMNT_RULE_temp = SQ_Shortcut_to_WM_E_MSRMNT_RULE.toDF(*["SQ_Shortcut_to_WM_E_MSRMNT_RULE___" + col for col in SQ_Shortcut_to_WM_E_MSRMNT_RULE.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_E_MSRMRNT_RULE = SQ_Shortcut_to_WM_E_MSRMNT_RULE_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_E_MSRMNT_RULE_temp.SQ_Shortcut_to_WM_E_MSRMNT_RULE___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_E_MSRMNT_RULE_temp.SQ_Shortcut_to_WM_E_MSRMNT_RULE___MSRMNT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___MSRMNT_ID, SQ_Shortcut_to_WM_E_MSRMNT_RULE_temp.SQ_Shortcut_to_WM_E_MSRMNT_RULE___RULE_NBR == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___RULE_NBR],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___MSRMNT_ID as MSRMNT_ID", \
	"JNR_SITE_PROFILE___RULE_NBR as RULE_NBR", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_SITE_PROFILE___STATUS_FLAG as STATUS_FLAG", \
	"JNR_SITE_PROFILE___THEN_STATEMENT as THEN_STATEMENT", \
	"JNR_SITE_PROFILE___ELSE_STATEMENT as ELSE_STATEMENT", \
	"JNR_SITE_PROFILE___NOTE as NOTE", \
	"JNR_SITE_PROFILE___MISC as MISC", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_SITE_PROFILE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_SITE_PROFILE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_SITE_PROFILE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_SITE_PROFILE___VERSION_ID as VERSION_ID", \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___LOCATION_ID as in_WM_LOCATION_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___MSRMNT_ID as in_WM_MSRMNT_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___RULE_NBR as in_WM_RULE_NBR", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___DESCRIPTION as in_WM_DESCRIPTION", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___STATUS_FLAG as in_WM_STATUS_FLAG", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___THEN_STATEMENT as in_WM_THEN_STATEMENT", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___ELSE_STATEMENT as in_WM_ELSE_STATEMENT", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___NOTE as in_WM_NOTE", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___MISC as in_WM_MISC", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___CREATE_DATE_TIME as in_WM_CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___MOD_DATE_TIME as in_WM_MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___USER_ID as in_WM_USER_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___MISC_TXT_1 as in_WM_MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___MISC_TXT_2 as in_WM_MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___MISC_NUM_1 as in_WM_MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___MISC_NUM_2 as in_WM_MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___VERSION_ID as in_WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___UPDATE_TSTMP as in_WM_UPDATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_MSRMNT_RULE___LOAD_TSTMP as in_WM_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_NO_CHANGE_REC, type FILTER 
# COLUMN COUNT: 36

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_E_MSRMRNT_RULE_temp = JNR_WM_E_MSRMRNT_RULE.toDF(*["JNR_WM_E_MSRMRNT_RULE___" + col for col in JNR_WM_E_MSRMRNT_RULE.columns])

FIL_NO_CHANGE_REC = JNR_WM_E_MSRMRNT_RULE_temp.selectExpr( \
	"JNR_WM_E_MSRMRNT_RULE___MSRMNT_ID as MSRMNT_ID", \
	"JNR_WM_E_MSRMRNT_RULE___RULE_NBR as RULE_NBR", \
	"JNR_WM_E_MSRMRNT_RULE___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_E_MSRMRNT_RULE___STATUS_FLAG as STATUS_FLAG", \
	"JNR_WM_E_MSRMRNT_RULE___THEN_STATEMENT as THEN_STATEMENT", \
	"JNR_WM_E_MSRMRNT_RULE___ELSE_STATEMENT as ELSE_STATEMENT", \
	"JNR_WM_E_MSRMRNT_RULE___NOTE as NOTE", \
	"JNR_WM_E_MSRMRNT_RULE___MISC as MISC", \
	"JNR_WM_E_MSRMRNT_RULE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_E_MSRMRNT_RULE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_E_MSRMRNT_RULE___USER_ID as USER_ID", \
	"JNR_WM_E_MSRMRNT_RULE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_WM_E_MSRMRNT_RULE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_WM_E_MSRMRNT_RULE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_WM_E_MSRMRNT_RULE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_WM_E_MSRMRNT_RULE___VERSION_ID as VERSION_ID", \
	"JNR_WM_E_MSRMRNT_RULE___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_LOCATION_ID as in_WM_LOCATION_ID", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_MSRMNT_ID as in_WM_MSRMNT_ID", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_RULE_NBR as in_WM_RULE_NBR", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_DESCRIPTION as in_WM_DESCRIPTION", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_STATUS_FLAG as in_WM_STATUS_FLAG", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_THEN_STATEMENT as in_WM_THEN_STATEMENT", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_ELSE_STATEMENT as in_WM_ELSE_STATEMENT", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_NOTE as in_WM_NOTE", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_MISC as in_WM_MISC", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_CREATE_DATE_TIME as in_WM_CREATE_DATE_TIME", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_MOD_DATE_TIME as in_WM_MOD_DATE_TIME", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_USER_ID as in_WM_USER_ID", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_MISC_TXT_1 as in_WM_MISC_TXT_1", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_MISC_TXT_2 as in_WM_MISC_TXT_2", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_MISC_NUM_1 as in_WM_MISC_NUM_1", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_MISC_NUM_2 as in_WM_MISC_NUM_2", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_VERSION_ID as in_WM_VERSION_ID", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_UPDATE_TSTMP as in_WM_UPDATE_TSTMP", \
	"JNR_WM_E_MSRMRNT_RULE___in_WM_LOAD_TSTMP as in_WM_LOAD_TSTMP") \
    .filter("in_WM_MSRMNT_ID is Null OR (  in_WM_MSRMNT_ID is not Null AND \
            ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(in_WM_CREATE_DATE_TIME, date'1900-01-01') \
             OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(in_WM_MOD_DATE_TIME, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_EVAL_VALUES, type EXPRESSION 
# COLUMN COUNT: 37

# for each involved DataFrame, append the dataframe name to each column
FIL_NO_CHANGE_REC_temp = FIL_NO_CHANGE_REC.toDF(*["FIL_NO_CHANGE_REC___" + col for col in FIL_NO_CHANGE_REC.columns])

EXP_EVAL_VALUES = FIL_NO_CHANGE_REC_temp.selectExpr( \
	"FIL_NO_CHANGE_REC___sys_row_id as sys_row_id", \
	"FIL_NO_CHANGE_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_NO_CHANGE_REC___MSRMNT_ID as MSRMNT_ID", \
	"FIL_NO_CHANGE_REC___RULE_NBR as RULE_NBR", \
	"FIL_NO_CHANGE_REC___DESCRIPTION as DESCRIPTION", \
	"FIL_NO_CHANGE_REC___STATUS_FLAG as STATUS_FLAG", \
	"FIL_NO_CHANGE_REC___THEN_STATEMENT as THEN_STATEMENT", \
	"FIL_NO_CHANGE_REC___ELSE_STATEMENT as ELSE_STATEMENT", \
	"FIL_NO_CHANGE_REC___NOTE as NOTE", \
	"FIL_NO_CHANGE_REC___MISC as MISC", \
	"FIL_NO_CHANGE_REC___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_NO_CHANGE_REC___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_NO_CHANGE_REC___USER_ID as USER_ID", \
	"FIL_NO_CHANGE_REC___MISC_TXT_1 as MISC_TXT_1", \
	"FIL_NO_CHANGE_REC___MISC_TXT_2 as MISC_TXT_2", \
	"FIL_NO_CHANGE_REC___MISC_NUM_1 as MISC_NUM_1", \
	"FIL_NO_CHANGE_REC___MISC_NUM_2 as MISC_NUM_2", \
	"FIL_NO_CHANGE_REC___VERSION_ID as VERSION_ID", \
	"FIL_NO_CHANGE_REC___in_WM_LOCATION_ID as in_WM_LOCATION_ID", \
	"FIL_NO_CHANGE_REC___in_WM_MSRMNT_ID as in_WM_MSRMNT_ID", \
	"FIL_NO_CHANGE_REC___in_WM_RULE_NBR as in_WM_RULE_NBR", \
	"FIL_NO_CHANGE_REC___in_WM_DESCRIPTION as in_WM_DESCRIPTION", \
	"FIL_NO_CHANGE_REC___in_WM_STATUS_FLAG as in_WM_STATUS_FLAG", \
	"FIL_NO_CHANGE_REC___in_WM_THEN_STATEMENT as in_WM_THEN_STATEMENT", \
	"FIL_NO_CHANGE_REC___in_WM_ELSE_STATEMENT as in_WM_ELSE_STATEMENT", \
	"FIL_NO_CHANGE_REC___in_WM_NOTE as in_WM_NOTE", \
	"FIL_NO_CHANGE_REC___in_WM_MISC as in_WM_MISC", \
	"FIL_NO_CHANGE_REC___in_WM_CREATE_DATE_TIME as in_WM_CREATE_DATE_TIME", \
	"FIL_NO_CHANGE_REC___in_WM_MOD_DATE_TIME as in_WM_MOD_DATE_TIME", \
	"FIL_NO_CHANGE_REC___in_WM_USER_ID as in_WM_USER_ID", \
	"FIL_NO_CHANGE_REC___in_WM_MISC_TXT_1 as in_WM_MISC_TXT_1", \
	"FIL_NO_CHANGE_REC___in_WM_MISC_TXT_2 as in_WM_MISC_TXT_2", \
	"FIL_NO_CHANGE_REC___in_WM_MISC_NUM_1 as in_WM_MISC_NUM_1", \
	"FIL_NO_CHANGE_REC___in_WM_MISC_NUM_2 as in_WM_MISC_NUM_2", \
	"FIL_NO_CHANGE_REC___in_WM_VERSION_ID as in_WM_VERSION_ID", \
	"IF(FIL_NO_CHANGE_REC___in_WM_MSRMNT_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR", \
	"CURRENT_TIMESTAMP as UPDATE_TSTSMP", \
	"IF(FIL_NO_CHANGE_REC___in_WM_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_NO_CHANGE_REC___in_WM_LOAD_TSTMP) as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node UPD_VALIDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
EXP_EVAL_VALUES_temp = EXP_EVAL_VALUES.toDF(*["EXP_EVAL_VALUES___" + col for col in EXP_EVAL_VALUES.columns])

UPD_VALIDATE = EXP_EVAL_VALUES_temp.selectExpr( \
	"EXP_EVAL_VALUES___LOCATION_ID as LOCATION_ID", \
	"EXP_EVAL_VALUES___MSRMNT_ID as MSRMNT_ID", \
	"EXP_EVAL_VALUES___RULE_NBR as RULE_NBR", \
	"EXP_EVAL_VALUES___DESCRIPTION as DESCRIPTION", \
	"EXP_EVAL_VALUES___STATUS_FLAG as STATUS_FLAG", \
	"EXP_EVAL_VALUES___THEN_STATEMENT as THEN_STATEMENT", \
	"EXP_EVAL_VALUES___ELSE_STATEMENT as ELSE_STATEMENT", \
	"EXP_EVAL_VALUES___NOTE as NOTE", \
	"EXP_EVAL_VALUES___MISC as MISC", \
	"EXP_EVAL_VALUES___MISC_TXT_1 as MISC_TXT_1", \
	"EXP_EVAL_VALUES___MISC_TXT_2 as MISC_TXT_2", \
	"EXP_EVAL_VALUES___MISC_NUM_1 as MISC_NUM_1", \
	"EXP_EVAL_VALUES___MISC_NUM_2 as MISC_NUM_2", \
	"EXP_EVAL_VALUES___USER_ID as USER_ID", \
	"EXP_EVAL_VALUES___VERSION_ID as VERSION_ID", \
	"EXP_EVAL_VALUES___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_EVAL_VALUES___MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_EVAL_VALUES___UPDATE_TSTSMP as UPDATE_TSTSMP", \
	"EXP_EVAL_VALUES___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_EVAL_VALUES___in_WM_MSRMNT_ID as in_WM_MSRMNT_ID", \
	"EXP_EVAL_VALUES___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)), lit(0)).when(col('o_UPDATE_VALIDATOR') ==(lit(2)), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_MSRMNT_RULE1, type TARGET 
# COLUMN COUNT: 19

Shortcut_to_WM_E_MSRMNT_RULE1 = UPD_VALIDATE.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(MSRMNT_ID AS INT) as WM_MSRMNT_ID",
	"CAST(RULE_NBR AS INT) as WM_RULE_NBR",
	"CAST(DESCRIPTION AS STRING) as WM_MSRMNT_RULE_DESC",
	"CAST(STATUS_FLAG AS STRING) as STATUS_FLAG",
	"CAST(THEN_STATEMENT AS STRING) as THEN_STATEMENT",
	"CAST(ELSE_STATEMENT AS STRING) as ELSE_STATEMENT",
	"CAST(NOTE AS STRING) as NOTE",
	"CAST(MISC AS STRING) as MISC",
	"CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1",
	"CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2",
	"CAST(MISC_NUM_1 AS DECIMAL(20,7)) as MISC_NUM_1",
	"CAST(MISC_NUM_2 AS DECIMAL(20,7)) as MISC_NUM_2",
	"CAST(USER_ID AS STRING) as WM_USER_ID",
	"CAST(VERSION_ID AS INT) as WM_VERSION_ID",
	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as WM_CREATE_TSTMP",
	"CAST(MOD_DATE_TIME AS TIMESTAMP) as WM_MOD_TSTMP",
	"CAST(UPDATE_TSTSMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_MSRMNT_ID = target.WM_MSRMNT_ID AND source.WM_RULE_NBR = target.WM_RULE_NBR"""
#   refined_perf_table = "WM_E_MSRMNT_RULE"
  executeMerge(Shortcut_to_WM_E_MSRMNT_RULE1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_E_MSRMNT_RULE", "WM_E_MSRMNT_RULE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_E_MSRMNT_RULE", "WM_E_MSRMNT_RULE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	