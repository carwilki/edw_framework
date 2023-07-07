#Code converted on 2023-06-22 21:04:15
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

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
refined_perf_table = f"{refine}.WM_VEND_PERF_TRAN"
raw_perf_table = f"{raw}.WM_VEND_PERF_TRAN_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_VEND_PERF_TRAN, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_VEND_PERF_TRAN = spark.sql(f"""SELECT
LOCATION_ID,
WM_VEND_PERF_TRAN_ID,
WM_CREATE_TSTMP,
WM_MOD_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_VEND_PERF_TRAN_ID IN (SELECT VEND_PERF_TRAN_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE, type SOURCE 
# COLUMN COUNT: 27

SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE = spark.sql(f"""SELECT
DC_NBR,
VEND_PERF_TRAN_ID,
PERF_CODE,
WHSE,
SHPMT_NBR,
PO_NBR,
CASE_NBR,
UOM,
QTY,
SAMS,
STAT_CODE,
CREATE_DATE_TIME,
MOD_DATE_TIME,
USER_ID,
CHRG_AMT,
BILL_FLAG,
LOAD_NBR,
ILM_APPT_NBR,
VENDOR_MASTER_ID,
CD_MASTER_ID,
CMNT,
CREATED_BY_USER_ID,
WM_VERSION_ID,
PO_HDR_ID,
ASN_HDR_ID,
CASE_HDR_ID,
ITEM_ID
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 27

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE_temp = SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE.toDF(*["SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___" + col for col in SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___VEND_PERF_TRAN_ID as VEND_PERF_TRAN_ID", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___PERF_CODE as PERF_CODE", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___WHSE as WHSE", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___SHPMT_NBR as SHPMT_NBR", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___PO_NBR as PO_NBR", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___CASE_NBR as CASE_NBR", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___UOM as UOM", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___QTY as QTY", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___SAMS as SAMS", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___STAT_CODE as STAT_CODE", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___USER_ID as USER_ID", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___CHRG_AMT as CHRG_AMT", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___BILL_FLAG as BILL_FLAG", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___LOAD_NBR as LOAD_NBR", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___ILM_APPT_NBR as ILM_APPT_NBR", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___VENDOR_MASTER_ID as VENDOR_MASTER_ID", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___CD_MASTER_ID as CD_MASTER_ID", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___CMNT as CMNT", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___CREATED_BY_USER_ID as CREATED_BY_USER_ID", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___WM_VERSION_ID as WM_VERSION_ID", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___PO_HDR_ID as PO_HDR_ID", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___ASN_HDR_ID as ASN_HDR_ID", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___CASE_HDR_ID as CASE_HDR_ID", 
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN_PRE___ITEM_ID as ITEM_ID" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 29

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node EXP_PASS_THROUGH, type EXPRESSION 
# COLUMN COUNT: 27

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

EXP_PASS_THROUGH = JNR_SITE_PROFILE_temp.selectExpr( \
	"JNR_SITE_PROFILE___sys_row_id as sys_row_id", \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___VEND_PERF_TRAN_ID as VEND_PERF_TRAN_ID", \
	"JNR_SITE_PROFILE___PERF_CODE as PERF_CODE", \
	"JNR_SITE_PROFILE___WHSE as WHSE", \
	"JNR_SITE_PROFILE___SHPMT_NBR as SHPMT_NBR", \
	"JNR_SITE_PROFILE___PO_NBR as PO_NBR", \
	"JNR_SITE_PROFILE___CASE_NBR as CASE_NBR", \
	"JNR_SITE_PROFILE___UOM as UOM", \
	"JNR_SITE_PROFILE___QTY as QTY", \
	"JNR_SITE_PROFILE___SAMS as SAMS", \
	"JNR_SITE_PROFILE___STAT_CODE as STAT_CODE", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___CHRG_AMT as CHRG_AMT", \
	"JNR_SITE_PROFILE___BILL_FLAG as BILL_FLAG", \
	"JNR_SITE_PROFILE___LOAD_NBR as LOAD_NBR", \
	"JNR_SITE_PROFILE___ILM_APPT_NBR as ILM_APPT_NBR", \
	"JNR_SITE_PROFILE___VENDOR_MASTER_ID as VENDOR_MASTER_ID", \
	"JNR_SITE_PROFILE___CD_MASTER_ID as CD_MASTER_ID", \
	"JNR_SITE_PROFILE___CMNT as CMNT", \
	"JNR_SITE_PROFILE___CREATED_BY_USER_ID as CREATED_BY_USER_ID", \
	"JNR_SITE_PROFILE___WM_VERSION_ID as WM_VERSION_ID", \
	"JNR_SITE_PROFILE___PO_HDR_ID as PO_HDR_ID", \
	"JNR_SITE_PROFILE___ASN_HDR_ID as ASN_HDR_ID", \
	"JNR_SITE_PROFILE___CASE_HDR_ID as CASE_HDR_ID", \
	"JNR_SITE_PROFILE___ITEM_ID as ITEM_ID" \
)

# COMMAND ----------
# Processing node JNR_WM_VEND_PERF_TRAN, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_VEND_PERF_TRAN_temp = SQ_Shortcut_to_WM_VEND_PERF_TRAN.toDF(*["SQ_Shortcut_to_WM_VEND_PERF_TRAN___" + col for col in SQ_Shortcut_to_WM_VEND_PERF_TRAN.columns])
EXP_PASS_THROUGH_temp = EXP_PASS_THROUGH.toDF(*["EXP_PASS_THROUGH___" + col for col in EXP_PASS_THROUGH.columns])

JNR_WM_VEND_PERF_TRAN = SQ_Shortcut_to_WM_VEND_PERF_TRAN_temp.join(EXP_PASS_THROUGH_temp,[SQ_Shortcut_to_WM_VEND_PERF_TRAN_temp.SQ_Shortcut_to_WM_VEND_PERF_TRAN___LOCATION_ID == EXP_PASS_THROUGH_temp.EXP_PASS_THROUGH___LOCATION_ID, SQ_Shortcut_to_WM_VEND_PERF_TRAN_temp.SQ_Shortcut_to_WM_VEND_PERF_TRAN___WM_VEND_PERF_TRAN_ID == EXP_PASS_THROUGH_temp.EXP_PASS_THROUGH___VEND_PERF_TRAN_ID],'right_outer').selectExpr( \
	"EXP_PASS_THROUGH___LOCATION_ID as LOCATION_ID", \
	"EXP_PASS_THROUGH___VEND_PERF_TRAN_ID as VEND_PERF_TRAN_ID", \
	"EXP_PASS_THROUGH___PERF_CODE as PERF_CODE", \
	"EXP_PASS_THROUGH___WHSE as WHSE", \
	"EXP_PASS_THROUGH___SHPMT_NBR as SHPMT_NBR", \
	"EXP_PASS_THROUGH___PO_NBR as PO_NBR", \
	"EXP_PASS_THROUGH___CASE_NBR as CASE_NBR", \
	"EXP_PASS_THROUGH___UOM as UOM", \
	"EXP_PASS_THROUGH___QTY as QTY", \
	"EXP_PASS_THROUGH___SAMS as SAMS", \
	"EXP_PASS_THROUGH___STAT_CODE as STAT_CODE", \
	"EXP_PASS_THROUGH___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_PASS_THROUGH___MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_PASS_THROUGH___USER_ID as USER_ID", \
	"EXP_PASS_THROUGH___CHRG_AMT as CHRG_AMT", \
	"EXP_PASS_THROUGH___BILL_FLAG as BILL_FLAG", \
	"EXP_PASS_THROUGH___LOAD_NBR as LOAD_NBR", \
	"EXP_PASS_THROUGH___ILM_APPT_NBR as ILM_APPT_NBR", \
	"EXP_PASS_THROUGH___VENDOR_MASTER_ID as VENDOR_MASTER_ID", \
	"EXP_PASS_THROUGH___CD_MASTER_ID as CD_MASTER_ID", \
	"EXP_PASS_THROUGH___CMNT as CMNT", \
	"EXP_PASS_THROUGH___CREATED_BY_USER_ID as CREATED_BY_USER_ID", \
	"EXP_PASS_THROUGH___WM_VERSION_ID as WM_VERSION_ID", \
	"EXP_PASS_THROUGH___PO_HDR_ID as PO_HDR_ID", \
	"EXP_PASS_THROUGH___ASN_HDR_ID as ASN_HDR_ID", \
	"EXP_PASS_THROUGH___CASE_HDR_ID as CASE_HDR_ID", \
	"EXP_PASS_THROUGH___ITEM_ID as ITEM_ID", \
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN___WM_VEND_PERF_TRAN_ID as i_WM_VEND_PERF_TRAN_ID", \
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN___WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_VEND_PERF_TRAN___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_VEND_PERF_TRAN_temp = JNR_WM_VEND_PERF_TRAN.toDF(*["JNR_WM_VEND_PERF_TRAN___" + col for col in JNR_WM_VEND_PERF_TRAN.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_VEND_PERF_TRAN_temp.selectExpr( \
	"JNR_WM_VEND_PERF_TRAN___LOCATION_ID as LOCATION_ID1", \
	"JNR_WM_VEND_PERF_TRAN___VEND_PERF_TRAN_ID as VEND_PERF_TRAN_ID", \
	"JNR_WM_VEND_PERF_TRAN___PERF_CODE as PERF_CODE", \
	"JNR_WM_VEND_PERF_TRAN___WHSE as WHSE", \
	"JNR_WM_VEND_PERF_TRAN___SHPMT_NBR as SHPMT_NBR", \
	"JNR_WM_VEND_PERF_TRAN___PO_NBR as PO_NBR", \
	"JNR_WM_VEND_PERF_TRAN___CASE_NBR as CASE_NBR", \
	"JNR_WM_VEND_PERF_TRAN___UOM as UOM1", \
	"JNR_WM_VEND_PERF_TRAN___QTY as QTY1", \
	"JNR_WM_VEND_PERF_TRAN___SAMS as SAMS1", \
	"JNR_WM_VEND_PERF_TRAN___STAT_CODE as STAT_CODE", \
	"JNR_WM_VEND_PERF_TRAN___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_VEND_PERF_TRAN___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_VEND_PERF_TRAN___USER_ID as USER_ID", \
	"JNR_WM_VEND_PERF_TRAN___CHRG_AMT as CHRG_AMT1", \
	"JNR_WM_VEND_PERF_TRAN___BILL_FLAG as BILL_FLAG1", \
	"JNR_WM_VEND_PERF_TRAN___LOAD_NBR as LOAD_NBR", \
	"JNR_WM_VEND_PERF_TRAN___ILM_APPT_NBR as ILM_APPT_NBR", \
	"JNR_WM_VEND_PERF_TRAN___VENDOR_MASTER_ID as VENDOR_MASTER_ID", \
	"JNR_WM_VEND_PERF_TRAN___CD_MASTER_ID as CD_MASTER_ID", \
	"JNR_WM_VEND_PERF_TRAN___CMNT as CMNT", \
	"JNR_WM_VEND_PERF_TRAN___CREATED_BY_USER_ID as CREATED_BY_USER_ID", \
	"JNR_WM_VEND_PERF_TRAN___WM_VERSION_ID as WM_VERSION_ID1", \
	"JNR_WM_VEND_PERF_TRAN___PO_HDR_ID as PO_HDR_ID", \
	"JNR_WM_VEND_PERF_TRAN___ASN_HDR_ID as ASN_HDR_ID", \
	"JNR_WM_VEND_PERF_TRAN___CASE_HDR_ID as CASE_HDR_ID", \
	"JNR_WM_VEND_PERF_TRAN___ITEM_ID as ITEM_ID", \
	"JNR_WM_VEND_PERF_TRAN___i_WM_VEND_PERF_TRAN_ID as i_WM_VEND_PERF_TRAN_ID", \
	"JNR_WM_VEND_PERF_TRAN___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"JNR_WM_VEND_PERF_TRAN___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"JNR_WM_VEND_PERF_TRAN___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("i_WM_VEND_PERF_TRAN_ID IS NULL OR (NOT i_WM_VEND_PERF_TRAN_ID IS NULL AND (COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01')) OR (COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01'))))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 30

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID1 as LOCATION_ID1", 
	"FIL_UNCHANGED_RECORDS___VEND_PERF_TRAN_ID as VEND_PERF_TRAN_ID", 
	"FIL_UNCHANGED_RECORDS___PERF_CODE as PERF_CODE", 
	"FIL_UNCHANGED_RECORDS___WHSE as WHSE", 
	"FIL_UNCHANGED_RECORDS___SHPMT_NBR as SHPMT_NBR", 
	"FIL_UNCHANGED_RECORDS___PO_NBR as PO_NBR", 
	"FIL_UNCHANGED_RECORDS___CASE_NBR as CASE_NBR", 
	"FIL_UNCHANGED_RECORDS___UOM1 as UOM1", 
	"FIL_UNCHANGED_RECORDS___QTY1 as QTY1", 
	"FIL_UNCHANGED_RECORDS___SAMS1 as SAMS1", 
	"FIL_UNCHANGED_RECORDS___STAT_CODE as STAT_CODE", 
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___USER_ID as USER_ID", 
	"FIL_UNCHANGED_RECORDS___CHRG_AMT1 as CHRG_AMT1", 
	"FIL_UNCHANGED_RECORDS___BILL_FLAG1 as BILL_FLAG1", 
	"FIL_UNCHANGED_RECORDS___LOAD_NBR as LOAD_NBR", 
	"FIL_UNCHANGED_RECORDS___ILM_APPT_NBR as ILM_APPT_NBR", 
	"FIL_UNCHANGED_RECORDS___VENDOR_MASTER_ID as VENDOR_MASTER_ID", 
	"FIL_UNCHANGED_RECORDS___CD_MASTER_ID as CD_MASTER_ID", 
	"FIL_UNCHANGED_RECORDS___CMNT as CMNT", 
	"FIL_UNCHANGED_RECORDS___CREATED_BY_USER_ID as CREATED_BY_USER_ID", 
	"FIL_UNCHANGED_RECORDS___WM_VERSION_ID1 as WM_VERSION_ID1", 
	"FIL_UNCHANGED_RECORDS___PO_HDR_ID as PO_HDR_ID", 
	"FIL_UNCHANGED_RECORDS___ASN_HDR_ID as ASN_HDR_ID", 
	"FIL_UNCHANGED_RECORDS___CASE_HDR_ID as CASE_HDR_ID", 
	"FIL_UNCHANGED_RECORDS___ITEM_ID as ITEM_ID", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_WM_VEND_PERF_TRAN_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 30

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( 
	"EXP_UPD_VALIDATOR___LOCATION_ID1 as LOCATION_ID1", 
	"EXP_UPD_VALIDATOR___VEND_PERF_TRAN_ID as VEND_PERF_TRAN_ID", 
	"EXP_UPD_VALIDATOR___PERF_CODE as PERF_CODE", 
	"EXP_UPD_VALIDATOR___WHSE as WHSE", 
	"EXP_UPD_VALIDATOR___SHPMT_NBR as SHPMT_NBR", 
	"EXP_UPD_VALIDATOR___PO_NBR as PO_NBR", 
	"EXP_UPD_VALIDATOR___CASE_NBR as CASE_NBR", 
	"EXP_UPD_VALIDATOR___UOM1 as UOM1", 
	"EXP_UPD_VALIDATOR___QTY1 as QTY1", 
	"EXP_UPD_VALIDATOR___SAMS1 as SAMS1", 
	"EXP_UPD_VALIDATOR___STAT_CODE as STAT_CODE", 
	"EXP_UPD_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"EXP_UPD_VALIDATOR___MOD_DATE_TIME as MOD_DATE_TIME", 
	"EXP_UPD_VALIDATOR___USER_ID as USER_ID", 
	"EXP_UPD_VALIDATOR___CHRG_AMT1 as CHRG_AMT1", 
	"EXP_UPD_VALIDATOR___BILL_FLAG1 as BILL_FLAG1", 
	"EXP_UPD_VALIDATOR___LOAD_NBR as LOAD_NBR", 
	"EXP_UPD_VALIDATOR___ILM_APPT_NBR as ILM_APPT_NBR", 
	"EXP_UPD_VALIDATOR___VENDOR_MASTER_ID as VENDOR_MASTER_ID", 
	"EXP_UPD_VALIDATOR___CD_MASTER_ID as CD_MASTER_ID", 
	"EXP_UPD_VALIDATOR___CMNT as CMNT", 
	"EXP_UPD_VALIDATOR___CREATED_BY_USER_ID as CREATED_BY_USER_ID", 
	"EXP_UPD_VALIDATOR___WM_VERSION_ID1 as WM_VERSION_ID1", 
	"EXP_UPD_VALIDATOR___PO_HDR_ID as PO_HDR_ID", 
	"EXP_UPD_VALIDATOR___ASN_HDR_ID as ASN_HDR_ID", 
	"EXP_UPD_VALIDATOR___CASE_HDR_ID as CASE_HDR_ID", 
	"EXP_UPD_VALIDATOR___ITEM_ID as ITEM_ID", 
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1))lit(0)).when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2))lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_VEND_PERF_TRAN, type TARGET 
# COLUMN COUNT: 29

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_VEND_PERF_TRAN_ID = target.WM_VEND_PERF_TRAN_ID"""
  # refined_perf_table = "WM_VEND_PERF_TRAN"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_VEND_PERF_TRAN", "WM_VEND_PERF_TRAN", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_VEND_PERF_TRAN", "WM_VEND_PERF_TRAN","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	