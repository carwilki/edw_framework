#Code converted on 2023-06-26 17:06:03
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from utils.genericUtilities import *
from utils.configs import *
from utils.mergeUtils import *
from utils.logger import *
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

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PIX_TRAN, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_PIX_TRAN = spark.sql(f"""SELECT
WM_PIX_TRAN.LOCATION_ID,
WM_PIX_TRAN.WM_PIX_TRAN_ID,
WM_PIX_TRAN.WM_CREATE_TSTMP,
WM_PIX_TRAN.WM_MOD_TSTMP,
WM_PIX_TRAN.LOAD_TSTMP
FROM WM_PIX_TRAN
WHERE WM_PIX_TRAN_ID IN (SELECT PIX_TRAN_ID FROM WM_PIX_TRAN_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PIX_TRAN_PRE, type SOURCE 
# COLUMN COUNT: 78

SQ_Shortcut_to_WM_PIX_TRAN_PRE = spark.sql(f"""SELECT
WM_PIX_TRAN_PRE.DC_NBR,
WM_PIX_TRAN_PRE.PIX_TRAN_ID,
WM_PIX_TRAN_PRE.TRAN_TYPE,
WM_PIX_TRAN_PRE.TRAN_CODE,
WM_PIX_TRAN_PRE.TRAN_NBR,
WM_PIX_TRAN_PRE.PIX_SEQ_NBR,
WM_PIX_TRAN_PRE.PROC_STAT_CODE,
WM_PIX_TRAN_PRE.WHSE,
WM_PIX_TRAN_PRE.CASE_NBR,
WM_PIX_TRAN_PRE.SEASON,
WM_PIX_TRAN_PRE.SEASON_YR,
WM_PIX_TRAN_PRE.STYLE,
WM_PIX_TRAN_PRE.STYLE_SFX,
WM_PIX_TRAN_PRE.COLOR,
WM_PIX_TRAN_PRE.COLOR_SFX,
WM_PIX_TRAN_PRE.SEC_DIM,
WM_PIX_TRAN_PRE.QUAL,
WM_PIX_TRAN_PRE.SIZE_DESC,
WM_PIX_TRAN_PRE.SIZE_RANGE_CODE,
WM_PIX_TRAN_PRE.SIZE_REL_POSN_IN_TABLE,
WM_PIX_TRAN_PRE.INVN_TYPE,
WM_PIX_TRAN_PRE.PROD_STAT,
WM_PIX_TRAN_PRE.BATCH_NBR,
WM_PIX_TRAN_PRE.SKU_ATTR_1,
WM_PIX_TRAN_PRE.SKU_ATTR_2,
WM_PIX_TRAN_PRE.SKU_ATTR_3,
WM_PIX_TRAN_PRE.SKU_ATTR_4,
WM_PIX_TRAN_PRE.SKU_ATTR_5,
WM_PIX_TRAN_PRE.CNTRY_OF_ORGN,
WM_PIX_TRAN_PRE.INVN_ADJMT_QTY,
WM_PIX_TRAN_PRE.INVN_ADJMT_TYPE,
WM_PIX_TRAN_PRE.WT_ADJMT_QTY,
WM_PIX_TRAN_PRE.WT_ADJMT_TYPE,
WM_PIX_TRAN_PRE.UOM,
WM_PIX_TRAN_PRE.REF_WHSE,
WM_PIX_TRAN_PRE.RSN_CODE,
WM_PIX_TRAN_PRE.RCPT_VARI,
WM_PIX_TRAN_PRE.RCPT_CMPL,
WM_PIX_TRAN_PRE.CASES_SHPD,
WM_PIX_TRAN_PRE.UNITS_SHPD,
WM_PIX_TRAN_PRE.CASES_RCVD,
WM_PIX_TRAN_PRE.UNITS_RCVD,
WM_PIX_TRAN_PRE.ACTN_CODE,
WM_PIX_TRAN_PRE.CUSTOM_REF,
WM_PIX_TRAN_PRE.DATE_PROC,
WM_PIX_TRAN_PRE.SYS_USER_ID,
WM_PIX_TRAN_PRE.ERROR_CMNT,
WM_PIX_TRAN_PRE.REF_CODE_ID_1,
WM_PIX_TRAN_PRE.REF_FIELD_1,
WM_PIX_TRAN_PRE.REF_CODE_ID_2,
WM_PIX_TRAN_PRE.REF_FIELD_2,
WM_PIX_TRAN_PRE.REF_CODE_ID_3,
WM_PIX_TRAN_PRE.REF_FIELD_3,
WM_PIX_TRAN_PRE.REF_CODE_ID_4,
WM_PIX_TRAN_PRE.REF_FIELD_4,
WM_PIX_TRAN_PRE.REF_CODE_ID_5,
WM_PIX_TRAN_PRE.REF_FIELD_5,
WM_PIX_TRAN_PRE.REF_CODE_ID_6,
WM_PIX_TRAN_PRE.REF_FIELD_6,
WM_PIX_TRAN_PRE.REF_CODE_ID_7,
WM_PIX_TRAN_PRE.REF_FIELD_7,
WM_PIX_TRAN_PRE.REF_CODE_ID_8,
WM_PIX_TRAN_PRE.REF_FIELD_8,
WM_PIX_TRAN_PRE.REF_CODE_ID_9,
WM_PIX_TRAN_PRE.REF_FIELD_9,
WM_PIX_TRAN_PRE.REF_CODE_ID_10,
WM_PIX_TRAN_PRE.REF_FIELD_10,
WM_PIX_TRAN_PRE.XML_GROUP_ID,
WM_PIX_TRAN_PRE.CREATE_DATE_TIME,
WM_PIX_TRAN_PRE.MOD_DATE_TIME,
WM_PIX_TRAN_PRE.USER_ID,
WM_PIX_TRAN_PRE.ITEM_ID,
WM_PIX_TRAN_PRE.TC_COMPANY_ID,
WM_PIX_TRAN_PRE.WM_VERSION_ID,
WM_PIX_TRAN_PRE.FACILITY_ID,
WM_PIX_TRAN_PRE.COMPANY_CODE,
WM_PIX_TRAN_PRE.ITEM_NAME,
WM_PIX_TRAN_PRE.ESIGN_USER_NAME
FROM WM_PIX_TRAN_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 78

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_PIX_TRAN_PRE_temp = SQ_Shortcut_to_WM_PIX_TRAN_PRE.toDF(*["SQ_Shortcut_to_WM_PIX_TRAN_PRE___" + col for col in SQ_Shortcut_to_WM_PIX_TRAN_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_PIX_TRAN_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_PIX_TRAN_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___PIX_TRAN_ID as PIX_TRAN_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___TRAN_TYPE as TRAN_TYPE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___TRAN_CODE as TRAN_CODE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___TRAN_NBR as TRAN_NBR", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___PIX_SEQ_NBR as PIX_SEQ_NBR", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___PROC_STAT_CODE as PROC_STAT_CODE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___WHSE as WHSE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___CASE_NBR as CASE_NBR", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SEASON as SEASON", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SEASON_YR as SEASON_YR", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___STYLE as STYLE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___STYLE_SFX as STYLE_SFX", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___COLOR as COLOR", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___COLOR_SFX as COLOR_SFX", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SEC_DIM as SEC_DIM", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___QUAL as QUAL", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SIZE_DESC as SIZE_DESC", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___INVN_TYPE as INVN_TYPE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___PROD_STAT as PROD_STAT", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___BATCH_NBR as BATCH_NBR", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SKU_ATTR_1 as SKU_ATTR_1", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SKU_ATTR_2 as SKU_ATTR_2", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SKU_ATTR_3 as SKU_ATTR_3", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SKU_ATTR_4 as SKU_ATTR_4", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SKU_ATTR_5 as SKU_ATTR_5", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___INVN_ADJMT_QTY as INVN_ADJMT_QTY", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___INVN_ADJMT_TYPE as INVN_ADJMT_TYPE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___WT_ADJMT_QTY as WT_ADJMT_QTY", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___WT_ADJMT_TYPE as WT_ADJMT_TYPE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___UOM as UOM", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_WHSE as REF_WHSE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___RSN_CODE as RSN_CODE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___RCPT_VARI as RCPT_VARI", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___RCPT_CMPL as RCPT_CMPL", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___CASES_SHPD as CASES_SHPD", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___UNITS_SHPD as UNITS_SHPD", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___CASES_RCVD as CASES_RCVD", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___UNITS_RCVD as UNITS_RCVD", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___ACTN_CODE as ACTN_CODE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___CUSTOM_REF as CUSTOM_REF", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___DATE_PROC as DATE_PROC", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___SYS_USER_ID as SYS_USER_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___ERROR_CMNT as ERROR_CMNT", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_CODE_ID_1 as REF_CODE_ID_1", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_FIELD_1 as REF_FIELD_1", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_CODE_ID_2 as REF_CODE_ID_2", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_FIELD_2 as REF_FIELD_2", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_CODE_ID_3 as REF_CODE_ID_3", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_FIELD_3 as REF_FIELD_3", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_CODE_ID_4 as REF_CODE_ID_4", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_FIELD_4 as REF_FIELD_4", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_CODE_ID_5 as REF_CODE_ID_5", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_FIELD_5 as REF_FIELD_5", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_CODE_ID_6 as REF_CODE_ID_6", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_FIELD_6 as REF_FIELD_6", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_CODE_ID_7 as REF_CODE_ID_7", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_FIELD_7 as REF_FIELD_7", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_CODE_ID_8 as REF_CODE_ID_8", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_FIELD_8 as REF_FIELD_8", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_CODE_ID_9 as REF_CODE_ID_9", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_FIELD_9 as REF_FIELD_9", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_CODE_ID_10 as REF_CODE_ID_10", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___REF_FIELD_10 as REF_FIELD_10", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___XML_GROUP_ID as XML_GROUP_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___ITEM_ID as ITEM_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___TC_COMPANY_ID as TC_COMPANY_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___WM_VERSION_ID as WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___FACILITY_ID as FACILITY_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___COMPANY_CODE as COMPANY_CODE", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___ITEM_NAME as ITEM_NAME", \
	"SQ_Shortcut_to_WM_PIX_TRAN_PRE___ESIGN_USER_NAME as ESIGN_USER_NAME" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 85

# for each involved DataFrame, append the dataframe name to each column
EXP_INT_CONVERSION_temp = EXP_INT_CONVERSION.toDF(*["EXP_INT_CONVERSION___" + col for col in EXP_INT_CONVERSION.columns])
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE_temp.join([SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR == EXP_INT_CONVERSION_temp.EXP_INT_CONVERSION___o_DC_NBR],'inner').selectExpr( \
	"EXP_INT_CONVERSION___o_DC_NBR as o_DC_NBR", \
	"EXP_INT_CONVERSION___PIX_TRAN_ID as PIX_TRAN_ID", \
	"EXP_INT_CONVERSION___TRAN_TYPE as TRAN_TYPE", \
	"EXP_INT_CONVERSION___TRAN_CODE as TRAN_CODE", \
	"EXP_INT_CONVERSION___TRAN_NBR as TRAN_NBR", \
	"EXP_INT_CONVERSION___PIX_SEQ_NBR as PIX_SEQ_NBR", \
	"EXP_INT_CONVERSION___PROC_STAT_CODE as PROC_STAT_CODE", \
	"EXP_INT_CONVERSION___WHSE as WHSE", \
	"EXP_INT_CONVERSION___CASE_NBR as CASE_NBR", \
	"EXP_INT_CONVERSION___SEASON as SEASON", \
	"EXP_INT_CONVERSION___SEASON_YR as SEASON_YR", \
	"EXP_INT_CONVERSION___STYLE as STYLE", \
	"EXP_INT_CONVERSION___STYLE_SFX as STYLE_SFX", \
	"EXP_INT_CONVERSION___COLOR as COLOR", \
	"EXP_INT_CONVERSION___COLOR_SFX as COLOR_SFX", \
	"EXP_INT_CONVERSION___SEC_DIM as SEC_DIM", \
	"EXP_INT_CONVERSION___QUAL as QUAL", \
	"EXP_INT_CONVERSION___SIZE_DESC as SIZE_DESC", \
	"EXP_INT_CONVERSION___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"EXP_INT_CONVERSION___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"EXP_INT_CONVERSION___INVN_TYPE as INVN_TYPE", \
	"EXP_INT_CONVERSION___PROD_STAT as PROD_STAT", \
	"EXP_INT_CONVERSION___BATCH_NBR as BATCH_NBR", \
	"EXP_INT_CONVERSION___SKU_ATTR_1 as SKU_ATTR_1", \
	"EXP_INT_CONVERSION___SKU_ATTR_2 as SKU_ATTR_2", \
	"EXP_INT_CONVERSION___SKU_ATTR_3 as SKU_ATTR_3", \
	"EXP_INT_CONVERSION___SKU_ATTR_4 as SKU_ATTR_4", \
	"EXP_INT_CONVERSION___SKU_ATTR_5 as SKU_ATTR_5", \
	"EXP_INT_CONVERSION___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"EXP_INT_CONVERSION___INVN_ADJMT_QTY as INVN_ADJMT_QTY", \
	"EXP_INT_CONVERSION___INVN_ADJMT_TYPE as INVN_ADJMT_TYPE", \
	"EXP_INT_CONVERSION___WT_ADJMT_QTY as WT_ADJMT_QTY", \
	"EXP_INT_CONVERSION___WT_ADJMT_TYPE as WT_ADJMT_TYPE", \
	"EXP_INT_CONVERSION___UOM as UOM", \
	"EXP_INT_CONVERSION___REF_WHSE as REF_WHSE", \
	"EXP_INT_CONVERSION___RSN_CODE as RSN_CODE", \
	"EXP_INT_CONVERSION___RCPT_VARI as RCPT_VARI", \
	"EXP_INT_CONVERSION___RCPT_CMPL as RCPT_CMPL", \
	"EXP_INT_CONVERSION___CASES_SHPD as CASES_SHPD", \
	"EXP_INT_CONVERSION___UNITS_SHPD as UNITS_SHPD", \
	"EXP_INT_CONVERSION___CASES_RCVD as CASES_RCVD", \
	"EXP_INT_CONVERSION___UNITS_RCVD as UNITS_RCVD", \
	"EXP_INT_CONVERSION___ACTN_CODE as ACTN_CODE", \
	"EXP_INT_CONVERSION___CUSTOM_REF as CUSTOM_REF", \
	"EXP_INT_CONVERSION___DATE_PROC as DATE_PROC", \
	"EXP_INT_CONVERSION___SYS_USER_ID as SYS_USER_ID", \
	"EXP_INT_CONVERSION___ERROR_CMNT as ERROR_CMNT", \
	"EXP_INT_CONVERSION___REF_CODE_ID_1 as REF_CODE_ID_1", \
	"EXP_INT_CONVERSION___REF_FIELD_1 as REF_FIELD_1", \
	"EXP_INT_CONVERSION___REF_CODE_ID_2 as REF_CODE_ID_2", \
	"EXP_INT_CONVERSION___REF_FIELD_2 as REF_FIELD_2", \
	"EXP_INT_CONVERSION___REF_CODE_ID_3 as REF_CODE_ID_3", \
	"EXP_INT_CONVERSION___REF_FIELD_3 as REF_FIELD_3", \
	"EXP_INT_CONVERSION___REF_CODE_ID_4 as REF_CODE_ID_4", \
	"EXP_INT_CONVERSION___REF_FIELD_4 as REF_FIELD_4", \
	"EXP_INT_CONVERSION___REF_CODE_ID_5 as REF_CODE_ID_5", \
	"EXP_INT_CONVERSION___REF_FIELD_5 as REF_FIELD_5", \
	"EXP_INT_CONVERSION___REF_CODE_ID_6 as REF_CODE_ID_6", \
	"EXP_INT_CONVERSION___REF_FIELD_6 as REF_FIELD_6", \
	"EXP_INT_CONVERSION___REF_CODE_ID_7 as REF_CODE_ID_7", \
	"EXP_INT_CONVERSION___REF_FIELD_7 as REF_FIELD_7", \
	"EXP_INT_CONVERSION___REF_CODE_ID_8 as REF_CODE_ID_8", \
	"EXP_INT_CONVERSION___REF_FIELD_8 as REF_FIELD_8", \
	"EXP_INT_CONVERSION___REF_CODE_ID_9 as REF_CODE_ID_9", \
	"EXP_INT_CONVERSION___REF_FIELD_9 as REF_FIELD_9", \
	"EXP_INT_CONVERSION___REF_CODE_ID_10 as REF_CODE_ID_10", \
	"EXP_INT_CONVERSION___REF_FIELD_10 as REF_FIELD_10", \
	"EXP_INT_CONVERSION___XML_GROUP_ID as XML_GROUP_ID", \
	"EXP_INT_CONVERSION___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_INT_CONVERSION___MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_INT_CONVERSION___USER_ID as USER_ID", \
	"EXP_INT_CONVERSION___ITEM_ID as ITEM_ID", \
	"EXP_INT_CONVERSION___TC_COMPANY_ID as TC_COMPANY_ID", \
	"EXP_INT_CONVERSION___WM_VERSION_ID as WM_VERSION_ID", \
	"EXP_INT_CONVERSION___FACILITY_ID as FACILITY_ID", \
	"EXP_INT_CONVERSION___COMPANY_CODE as COMPANY_CODE", \
	"EXP_INT_CONVERSION___ITEM_NAME as ITEM_NAME", \
	"EXP_INT_CONVERSION___ESIGN_USER_NAME as ESIGN_USER_NAME", \
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR") \
	.withColumn('LOCATION_ID1', lit(None)) \
	.withColumn('WM_PIX_TRAN_ID', lit(None)) \
	.withColumn('WM_CREATE_TSTMP', lit(None)) \
	.withColumn('WM_MOD_TSTMP', lit(None)) \
	.withColumn('LOAD_TSTMP', lit(None)) \
	

# COMMAND ----------
# Processing node JNR_WM_PIX_TRAN, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 83

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_PIX_TRAN_temp = SQ_Shortcut_to_WM_PIX_TRAN.toDF(*["SQ_Shortcut_to_WM_PIX_TRAN___" + col for col in SQ_Shortcut_to_WM_PIX_TRAN.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_PIX_TRAN = SQ_Shortcut_to_WM_PIX_TRAN_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_PIX_TRAN_temp.SQ_Shortcut_to_WM_PIX_TRAN___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_PIX_TRAN_temp.SQ_Shortcut_to_WM_PIX_TRAN___WM_PIX_TRAN_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___PIX_TRAN_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___PIX_TRAN_ID as PIX_TRAN_ID", \
	"JNR_SITE_PROFILE___TRAN_TYPE as TRAN_TYPE", \
	"JNR_SITE_PROFILE___TRAN_CODE as TRAN_CODE", \
	"JNR_SITE_PROFILE___TRAN_NBR as TRAN_NBR", \
	"JNR_SITE_PROFILE___PIX_SEQ_NBR as PIX_SEQ_NBR", \
	"JNR_SITE_PROFILE___PROC_STAT_CODE as PROC_STAT_CODE", \
	"JNR_SITE_PROFILE___WHSE as WHSE", \
	"JNR_SITE_PROFILE___CASE_NBR as CASE_NBR", \
	"JNR_SITE_PROFILE___SEASON as SEASON", \
	"JNR_SITE_PROFILE___SEASON_YR as SEASON_YR", \
	"JNR_SITE_PROFILE___STYLE as STYLE", \
	"JNR_SITE_PROFILE___STYLE_SFX as STYLE_SFX", \
	"JNR_SITE_PROFILE___COLOR as COLOR", \
	"JNR_SITE_PROFILE___COLOR_SFX as COLOR_SFX", \
	"JNR_SITE_PROFILE___SEC_DIM as SEC_DIM", \
	"JNR_SITE_PROFILE___QUAL as QUAL", \
	"JNR_SITE_PROFILE___SIZE_DESC as SIZE_DESC", \
	"JNR_SITE_PROFILE___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"JNR_SITE_PROFILE___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"JNR_SITE_PROFILE___INVN_TYPE as INVN_TYPE", \
	"JNR_SITE_PROFILE___PROD_STAT as PROD_STAT", \
	"JNR_SITE_PROFILE___BATCH_NBR as BATCH_NBR", \
	"JNR_SITE_PROFILE___SKU_ATTR_1 as SKU_ATTR_1", \
	"JNR_SITE_PROFILE___SKU_ATTR_2 as SKU_ATTR_2", \
	"JNR_SITE_PROFILE___SKU_ATTR_3 as SKU_ATTR_3", \
	"JNR_SITE_PROFILE___SKU_ATTR_4 as SKU_ATTR_4", \
	"JNR_SITE_PROFILE___SKU_ATTR_5 as SKU_ATTR_5", \
	"JNR_SITE_PROFILE___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"JNR_SITE_PROFILE___INVN_ADJMT_QTY as INVN_ADJMT_QTY", \
	"JNR_SITE_PROFILE___INVN_ADJMT_TYPE as INVN_ADJMT_TYPE", \
	"JNR_SITE_PROFILE___WT_ADJMT_QTY as WT_ADJMT_QTY", \
	"JNR_SITE_PROFILE___WT_ADJMT_TYPE as WT_ADJMT_TYPE", \
	"JNR_SITE_PROFILE___UOM as UOM", \
	"JNR_SITE_PROFILE___REF_WHSE as REF_WHSE", \
	"JNR_SITE_PROFILE___RSN_CODE as RSN_CODE", \
	"JNR_SITE_PROFILE___RCPT_VARI as RCPT_VARI", \
	"JNR_SITE_PROFILE___RCPT_CMPL as RCPT_CMPL", \
	"JNR_SITE_PROFILE___CASES_SHPD as CASES_SHPD", \
	"JNR_SITE_PROFILE___UNITS_SHPD as UNITS_SHPD", \
	"JNR_SITE_PROFILE___CASES_RCVD as CASES_RCVD", \
	"JNR_SITE_PROFILE___UNITS_RCVD as UNITS_RCVD", \
	"JNR_SITE_PROFILE___ACTN_CODE as ACTN_CODE", \
	"JNR_SITE_PROFILE___CUSTOM_REF as CUSTOM_REF", \
	"JNR_SITE_PROFILE___DATE_PROC as DATE_PROC", \
	"JNR_SITE_PROFILE___SYS_USER_ID as SYS_USER_ID", \
	"JNR_SITE_PROFILE___ERROR_CMNT as ERROR_CMNT", \
	"JNR_SITE_PROFILE___REF_CODE_ID_1 as REF_CODE_ID_1", \
	"JNR_SITE_PROFILE___REF_FIELD_1 as REF_FIELD_1", \
	"JNR_SITE_PROFILE___REF_CODE_ID_2 as REF_CODE_ID_2", \
	"JNR_SITE_PROFILE___REF_FIELD_2 as REF_FIELD_2", \
	"JNR_SITE_PROFILE___REF_CODE_ID_3 as REF_CODE_ID_3", \
	"JNR_SITE_PROFILE___REF_FIELD_3 as REF_FIELD_3", \
	"JNR_SITE_PROFILE___REF_CODE_ID_4 as REF_CODE_ID_4", \
	"JNR_SITE_PROFILE___REF_FIELD_4 as REF_FIELD_4", \
	"JNR_SITE_PROFILE___REF_CODE_ID_5 as REF_CODE_ID_5", \
	"JNR_SITE_PROFILE___REF_FIELD_5 as REF_FIELD_5", \
	"JNR_SITE_PROFILE___REF_CODE_ID_6 as REF_CODE_ID_6", \
	"JNR_SITE_PROFILE___REF_FIELD_6 as REF_FIELD_6", \
	"JNR_SITE_PROFILE___REF_CODE_ID_7 as REF_CODE_ID_7", \
	"JNR_SITE_PROFILE___REF_FIELD_7 as REF_FIELD_7", \
	"JNR_SITE_PROFILE___REF_CODE_ID_8 as REF_CODE_ID_8", \
	"JNR_SITE_PROFILE___REF_FIELD_8 as REF_FIELD_8", \
	"JNR_SITE_PROFILE___REF_CODE_ID_9 as REF_CODE_ID_9", \
	"JNR_SITE_PROFILE___REF_FIELD_9 as REF_FIELD_9", \
	"JNR_SITE_PROFILE___REF_CODE_ID_10 as REF_CODE_ID_10", \
	"JNR_SITE_PROFILE___REF_FIELD_10 as REF_FIELD_10", \
	"JNR_SITE_PROFILE___XML_GROUP_ID as XML_GROUP_ID", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___ITEM_ID as ITEM_ID", \
	"JNR_SITE_PROFILE___TC_COMPANY_ID as TC_COMPANY_ID", \
	"JNR_SITE_PROFILE___WM_VERSION_ID as WM_VERSION_ID", \
	"JNR_SITE_PROFILE___FACILITY_ID as FACILITY_ID", \
	"JNR_SITE_PROFILE___COMPANY_CODE as COMPANY_CODE", \
	"JNR_SITE_PROFILE___ITEM_NAME as ITEM_NAME", \
	"JNR_SITE_PROFILE___ESIGN_USER_NAME as ESIGN_USER_NAME", \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN___WM_PIX_TRAN_ID as i_WM_PIX_TRAN_ID", \
	"SQ_Shortcut_to_WM_PIX_TRAN___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_PIX_TRAN___WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_PIX_TRAN___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 82

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_PIX_TRAN_temp = JNR_WM_PIX_TRAN.toDF(*["JNR_WM_PIX_TRAN___" + col for col in JNR_WM_PIX_TRAN.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_PIX_TRAN_temp.selectExpr( \
	"JNR_WM_PIX_TRAN___PIX_TRAN_ID as PIX_TRAN_ID", \
	"JNR_WM_PIX_TRAN___TRAN_TYPE as TRAN_TYPE", \
	"JNR_WM_PIX_TRAN___TRAN_CODE as TRAN_CODE", \
	"JNR_WM_PIX_TRAN___TRAN_NBR as TRAN_NBR", \
	"JNR_WM_PIX_TRAN___PIX_SEQ_NBR as PIX_SEQ_NBR", \
	"JNR_WM_PIX_TRAN___PROC_STAT_CODE as PROC_STAT_CODE", \
	"JNR_WM_PIX_TRAN___WHSE as WHSE", \
	"JNR_WM_PIX_TRAN___CASE_NBR as CASE_NBR", \
	"JNR_WM_PIX_TRAN___SEASON as SEASON", \
	"JNR_WM_PIX_TRAN___SEASON_YR as SEASON_YR", \
	"JNR_WM_PIX_TRAN___STYLE as STYLE", \
	"JNR_WM_PIX_TRAN___STYLE_SFX as STYLE_SFX", \
	"JNR_WM_PIX_TRAN___COLOR as COLOR", \
	"JNR_WM_PIX_TRAN___COLOR_SFX as COLOR_SFX", \
	"JNR_WM_PIX_TRAN___SEC_DIM as SEC_DIM", \
	"JNR_WM_PIX_TRAN___QUAL as QUAL", \
	"JNR_WM_PIX_TRAN___SIZE_DESC as SIZE_DESC", \
	"JNR_WM_PIX_TRAN___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"JNR_WM_PIX_TRAN___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"JNR_WM_PIX_TRAN___INVN_TYPE as INVN_TYPE", \
	"JNR_WM_PIX_TRAN___PROD_STAT as PROD_STAT", \
	"JNR_WM_PIX_TRAN___BATCH_NBR as BATCH_NBR", \
	"JNR_WM_PIX_TRAN___SKU_ATTR_1 as SKU_ATTR_1", \
	"JNR_WM_PIX_TRAN___SKU_ATTR_2 as SKU_ATTR_2", \
	"JNR_WM_PIX_TRAN___SKU_ATTR_3 as SKU_ATTR_3", \
	"JNR_WM_PIX_TRAN___SKU_ATTR_4 as SKU_ATTR_4", \
	"JNR_WM_PIX_TRAN___SKU_ATTR_5 as SKU_ATTR_5", \
	"JNR_WM_PIX_TRAN___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"JNR_WM_PIX_TRAN___INVN_ADJMT_QTY as INVN_ADJMT_QTY", \
	"JNR_WM_PIX_TRAN___INVN_ADJMT_TYPE as INVN_ADJMT_TYPE", \
	"JNR_WM_PIX_TRAN___WT_ADJMT_QTY as WT_ADJMT_QTY", \
	"JNR_WM_PIX_TRAN___WT_ADJMT_TYPE as WT_ADJMT_TYPE", \
	"JNR_WM_PIX_TRAN___UOM as UOM", \
	"JNR_WM_PIX_TRAN___REF_WHSE as REF_WHSE", \
	"JNR_WM_PIX_TRAN___RSN_CODE as RSN_CODE", \
	"JNR_WM_PIX_TRAN___RCPT_VARI as RCPT_VARI", \
	"JNR_WM_PIX_TRAN___RCPT_CMPL as RCPT_CMPL", \
	"JNR_WM_PIX_TRAN___CASES_SHPD as CASES_SHPD", \
	"JNR_WM_PIX_TRAN___UNITS_SHPD as UNITS_SHPD", \
	"JNR_WM_PIX_TRAN___CASES_RCVD as CASES_RCVD", \
	"JNR_WM_PIX_TRAN___UNITS_RCVD as UNITS_RCVD", \
	"JNR_WM_PIX_TRAN___ACTN_CODE as ACTN_CODE", \
	"JNR_WM_PIX_TRAN___CUSTOM_REF as CUSTOM_REF", \
	"JNR_WM_PIX_TRAN___DATE_PROC as DATE_PROC", \
	"JNR_WM_PIX_TRAN___SYS_USER_ID as SYS_USER_ID", \
	"JNR_WM_PIX_TRAN___ERROR_CMNT as ERROR_CMNT", \
	"JNR_WM_PIX_TRAN___REF_CODE_ID_1 as REF_CODE_ID_1", \
	"JNR_WM_PIX_TRAN___REF_FIELD_1 as REF_FIELD_1", \
	"JNR_WM_PIX_TRAN___REF_CODE_ID_2 as REF_CODE_ID_2", \
	"JNR_WM_PIX_TRAN___REF_FIELD_2 as REF_FIELD_2", \
	"JNR_WM_PIX_TRAN___REF_CODE_ID_3 as REF_CODE_ID_3", \
	"JNR_WM_PIX_TRAN___REF_FIELD_3 as REF_FIELD_3", \
	"JNR_WM_PIX_TRAN___REF_CODE_ID_4 as REF_CODE_ID_4", \
	"JNR_WM_PIX_TRAN___REF_FIELD_4 as REF_FIELD_4", \
	"JNR_WM_PIX_TRAN___REF_CODE_ID_5 as REF_CODE_ID_5", \
	"JNR_WM_PIX_TRAN___REF_FIELD_5 as REF_FIELD_5", \
	"JNR_WM_PIX_TRAN___REF_CODE_ID_6 as REF_CODE_ID_6", \
	"JNR_WM_PIX_TRAN___REF_FIELD_6 as REF_FIELD_6", \
	"JNR_WM_PIX_TRAN___REF_CODE_ID_7 as REF_CODE_ID_7", \
	"JNR_WM_PIX_TRAN___REF_FIELD_7 as REF_FIELD_7", \
	"JNR_WM_PIX_TRAN___REF_CODE_ID_8 as REF_CODE_ID_8", \
	"JNR_WM_PIX_TRAN___REF_FIELD_8 as REF_FIELD_8", \
	"JNR_WM_PIX_TRAN___REF_CODE_ID_9 as REF_CODE_ID_9", \
	"JNR_WM_PIX_TRAN___REF_FIELD_9 as REF_FIELD_9", \
	"JNR_WM_PIX_TRAN___REF_CODE_ID_10 as REF_CODE_ID_10", \
	"JNR_WM_PIX_TRAN___REF_FIELD_10 as REF_FIELD_10", \
	"JNR_WM_PIX_TRAN___XML_GROUP_ID as XML_GROUP_ID", \
	"JNR_WM_PIX_TRAN___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_PIX_TRAN___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_PIX_TRAN___USER_ID as USER_ID", \
	"JNR_WM_PIX_TRAN___ITEM_ID as ITEM_ID", \
	"JNR_WM_PIX_TRAN___TC_COMPANY_ID as TC_COMPANY_ID", \
	"JNR_WM_PIX_TRAN___WM_VERSION_ID as WM_VERSION_ID", \
	"JNR_WM_PIX_TRAN___FACILITY_ID as FACILITY_ID", \
	"JNR_WM_PIX_TRAN___COMPANY_CODE as COMPANY_CODE", \
	"JNR_WM_PIX_TRAN___ITEM_NAME as ITEM_NAME", \
	"JNR_WM_PIX_TRAN___ESIGN_USER_NAME as ESIGN_USER_NAME", \
	"JNR_WM_PIX_TRAN___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_PIX_TRAN___i_WM_PIX_TRAN_ID as i_WM_PIX_TRAN_ID", \
	"JNR_WM_PIX_TRAN___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"JNR_WM_PIX_TRAN___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"JNR_WM_PIX_TRAN___i_LOAD_TSTMP as i_LOAD_TSTMP")\
    .filter("i_WM_PIX_TRAN_ID is Null OR (  i_WM_PIX_TRAN_ID is NOT Null AND ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01') \
             OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 81

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___PIX_TRAN_ID as PIX_TRAN_ID", \
	"FIL_UNCHANGED_RECORDS___TRAN_TYPE as TRAN_TYPE", \
	"FIL_UNCHANGED_RECORDS___TRAN_CODE as TRAN_CODE", \
	"FIL_UNCHANGED_RECORDS___TRAN_NBR as TRAN_NBR", \
	"FIL_UNCHANGED_RECORDS___PIX_SEQ_NBR as PIX_SEQ_NBR", \
	"FIL_UNCHANGED_RECORDS___PROC_STAT_CODE as PROC_STAT_CODE", \
	"FIL_UNCHANGED_RECORDS___WHSE as WHSE", \
	"FIL_UNCHANGED_RECORDS___CASE_NBR as CASE_NBR", \
	"FIL_UNCHANGED_RECORDS___SEASON as SEASON", \
	"FIL_UNCHANGED_RECORDS___SEASON_YR as SEASON_YR", \
	"FIL_UNCHANGED_RECORDS___STYLE as STYLE", \
	"FIL_UNCHANGED_RECORDS___STYLE_SFX as STYLE_SFX", \
	"FIL_UNCHANGED_RECORDS___COLOR as COLOR", \
	"FIL_UNCHANGED_RECORDS___COLOR_SFX as COLOR_SFX", \
	"FIL_UNCHANGED_RECORDS___SEC_DIM as SEC_DIM", \
	"FIL_UNCHANGED_RECORDS___QUAL as QUAL", \
	"FIL_UNCHANGED_RECORDS___SIZE_DESC as SIZE_DESC", \
	"FIL_UNCHANGED_RECORDS___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"FIL_UNCHANGED_RECORDS___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"FIL_UNCHANGED_RECORDS___INVN_TYPE as INVN_TYPE", \
	"FIL_UNCHANGED_RECORDS___PROD_STAT as PROD_STAT", \
	"FIL_UNCHANGED_RECORDS___BATCH_NBR as BATCH_NBR", \
	"FIL_UNCHANGED_RECORDS___SKU_ATTR_1 as SKU_ATTR_1", \
	"FIL_UNCHANGED_RECORDS___SKU_ATTR_2 as SKU_ATTR_2", \
	"FIL_UNCHANGED_RECORDS___SKU_ATTR_3 as SKU_ATTR_3", \
	"FIL_UNCHANGED_RECORDS___SKU_ATTR_4 as SKU_ATTR_4", \
	"FIL_UNCHANGED_RECORDS___SKU_ATTR_5 as SKU_ATTR_5", \
	"FIL_UNCHANGED_RECORDS___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"FIL_UNCHANGED_RECORDS___INVN_ADJMT_QTY as INVN_ADJMT_QTY", \
	"FIL_UNCHANGED_RECORDS___INVN_ADJMT_TYPE as INVN_ADJMT_TYPE", \
	"FIL_UNCHANGED_RECORDS___WT_ADJMT_QTY as WT_ADJMT_QTY", \
	"FIL_UNCHANGED_RECORDS___WT_ADJMT_TYPE as WT_ADJMT_TYPE", \
	"FIL_UNCHANGED_RECORDS___UOM as UOM", \
	"FIL_UNCHANGED_RECORDS___REF_WHSE as REF_WHSE", \
	"FIL_UNCHANGED_RECORDS___RSN_CODE as RSN_CODE", \
	"decode ( ltrim ( rtrim ( upper ( FIL_UNCHANGED_RECORDS___RCPT_VARI ) ) ) , '1','1' , 'Y','1','0' ) as RCPT_VARI_EXP", \
	"decode ( ltrim ( rtrim ( upper ( FIL_UNCHANGED_RECORDS___RCPT_CMPL ) ) ) , '1','1' , 'Y','1','0' ) as RCPT_CMPL_EXP", \
	"FIL_UNCHANGED_RECORDS___CASES_SHPD as CASES_SHPD", \
	"FIL_UNCHANGED_RECORDS___UNITS_SHPD as UNITS_SHPD", \
	"FIL_UNCHANGED_RECORDS___CASES_RCVD as CASES_RCVD", \
	"FIL_UNCHANGED_RECORDS___UNITS_RCVD as UNITS_RCVD", \
	"FIL_UNCHANGED_RECORDS___ACTN_CODE as ACTN_CODE", \
	"FIL_UNCHANGED_RECORDS___CUSTOM_REF as CUSTOM_REF", \
	"FIL_UNCHANGED_RECORDS___DATE_PROC as DATE_PROC", \
	"FIL_UNCHANGED_RECORDS___SYS_USER_ID as SYS_USER_ID", \
	"FIL_UNCHANGED_RECORDS___ERROR_CMNT as ERROR_CMNT", \
	"FIL_UNCHANGED_RECORDS___REF_CODE_ID_1 as REF_CODE_ID_1", \
	"FIL_UNCHANGED_RECORDS___REF_FIELD_1 as REF_FIELD_1", \
	"FIL_UNCHANGED_RECORDS___REF_CODE_ID_2 as REF_CODE_ID_2", \
	"FIL_UNCHANGED_RECORDS___REF_FIELD_2 as REF_FIELD_2", \
	"FIL_UNCHANGED_RECORDS___REF_CODE_ID_3 as REF_CODE_ID_3", \
	"FIL_UNCHANGED_RECORDS___REF_FIELD_3 as REF_FIELD_3", \
	"FIL_UNCHANGED_RECORDS___REF_CODE_ID_4 as REF_CODE_ID_4", \
	"FIL_UNCHANGED_RECORDS___REF_FIELD_4 as REF_FIELD_4", \
	"FIL_UNCHANGED_RECORDS___REF_CODE_ID_5 as REF_CODE_ID_5", \
	"FIL_UNCHANGED_RECORDS___REF_FIELD_5 as REF_FIELD_5", \
	"FIL_UNCHANGED_RECORDS___REF_CODE_ID_6 as REF_CODE_ID_6", \
	"FIL_UNCHANGED_RECORDS___REF_FIELD_6 as REF_FIELD_6", \
	"FIL_UNCHANGED_RECORDS___REF_CODE_ID_7 as REF_CODE_ID_7", \
	"FIL_UNCHANGED_RECORDS___REF_FIELD_7 as REF_FIELD_7", \
	"FIL_UNCHANGED_RECORDS___REF_CODE_ID_8 as REF_CODE_ID_8", \
	"FIL_UNCHANGED_RECORDS___REF_FIELD_8 as REF_FIELD_8", \
	"FIL_UNCHANGED_RECORDS___REF_CODE_ID_9 as REF_CODE_ID_9", \
	"FIL_UNCHANGED_RECORDS___REF_FIELD_9 as REF_FIELD_9", \
	"FIL_UNCHANGED_RECORDS___REF_CODE_ID_10 as REF_CODE_ID_10", \
	"FIL_UNCHANGED_RECORDS___REF_FIELD_10 as REF_FIELD_10", \
	"FIL_UNCHANGED_RECORDS___XML_GROUP_ID as XML_GROUP_ID", \
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___USER_ID as USER_ID", \
	"FIL_UNCHANGED_RECORDS___ITEM_ID as ITEM_ID", \
	"FIL_UNCHANGED_RECORDS___TC_COMPANY_ID as TC_COMPANY_ID", \
	"FIL_UNCHANGED_RECORDS___WM_VERSION_ID as WM_VERSION_ID", \
	"FIL_UNCHANGED_RECORDS___FACILITY_ID as FACILITY_ID", \
	"FIL_UNCHANGED_RECORDS___COMPANY_CODE as COMPANY_CODE", \
	"FIL_UNCHANGED_RECORDS___ITEM_NAME as ITEM_NAME", \
	"FIL_UNCHANGED_RECORDS___ESIGN_USER_NAME as ESIGN_USER_NAME", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_WM_PIX_TRAN_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 81

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___PIX_TRAN_ID as PIX_TRAN_ID", \
	"EXP_UPD_VALIDATOR___TRAN_TYPE as TRAN_TYPE", \
	"EXP_UPD_VALIDATOR___TRAN_CODE as TRAN_CODE", \
	"EXP_UPD_VALIDATOR___TRAN_NBR as TRAN_NBR", \
	"EXP_UPD_VALIDATOR___PIX_SEQ_NBR as PIX_SEQ_NBR", \
	"EXP_UPD_VALIDATOR___PROC_STAT_CODE as PROC_STAT_CODE", \
	"EXP_UPD_VALIDATOR___WHSE as WHSE", \
	"EXP_UPD_VALIDATOR___CASE_NBR as CASE_NBR", \
	"EXP_UPD_VALIDATOR___SEASON as SEASON", \
	"EXP_UPD_VALIDATOR___SEASON_YR as SEASON_YR", \
	"EXP_UPD_VALIDATOR___STYLE as STYLE", \
	"EXP_UPD_VALIDATOR___STYLE_SFX as STYLE_SFX", \
	"EXP_UPD_VALIDATOR___COLOR as COLOR", \
	"EXP_UPD_VALIDATOR___COLOR_SFX as COLOR_SFX", \
	"EXP_UPD_VALIDATOR___SEC_DIM as SEC_DIM", \
	"EXP_UPD_VALIDATOR___QUAL as QUAL", \
	"EXP_UPD_VALIDATOR___SIZE_DESC as SIZE_DESC", \
	"EXP_UPD_VALIDATOR___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"EXP_UPD_VALIDATOR___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"EXP_UPD_VALIDATOR___INVN_TYPE as INVN_TYPE", \
	"EXP_UPD_VALIDATOR___PROD_STAT as PROD_STAT", \
	"EXP_UPD_VALIDATOR___BATCH_NBR as BATCH_NBR", \
	"EXP_UPD_VALIDATOR___SKU_ATTR_1 as SKU_ATTR_1", \
	"EXP_UPD_VALIDATOR___SKU_ATTR_2 as SKU_ATTR_2", \
	"EXP_UPD_VALIDATOR___SKU_ATTR_3 as SKU_ATTR_3", \
	"EXP_UPD_VALIDATOR___SKU_ATTR_4 as SKU_ATTR_4", \
	"EXP_UPD_VALIDATOR___SKU_ATTR_5 as SKU_ATTR_5", \
	"EXP_UPD_VALIDATOR___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"EXP_UPD_VALIDATOR___INVN_ADJMT_QTY as INVN_ADJMT_QTY", \
	"EXP_UPD_VALIDATOR___INVN_ADJMT_TYPE as INVN_ADJMT_TYPE", \
	"EXP_UPD_VALIDATOR___WT_ADJMT_QTY as WT_ADJMT_QTY", \
	"EXP_UPD_VALIDATOR___WT_ADJMT_TYPE as WT_ADJMT_TYPE", \
	"EXP_UPD_VALIDATOR___UOM as UOM", \
	"EXP_UPD_VALIDATOR___REF_WHSE as REF_WHSE", \
	"EXP_UPD_VALIDATOR___RSN_CODE as RSN_CODE", \
	"EXP_UPD_VALIDATOR___RCPT_VARI_EXP as RCPT_VARI", \
	"EXP_UPD_VALIDATOR___RCPT_CMPL_EXP as RCPT_CMPL", \
	"EXP_UPD_VALIDATOR___CASES_SHPD as CASES_SHPD", \
	"EXP_UPD_VALIDATOR___UNITS_SHPD as UNITS_SHPD", \
	"EXP_UPD_VALIDATOR___CASES_RCVD as CASES_RCVD", \
	"EXP_UPD_VALIDATOR___UNITS_RCVD as UNITS_RCVD", \
	"EXP_UPD_VALIDATOR___ACTN_CODE as ACTN_CODE", \
	"EXP_UPD_VALIDATOR___CUSTOM_REF as CUSTOM_REF", \
	"EXP_UPD_VALIDATOR___DATE_PROC as DATE_PROC", \
	"EXP_UPD_VALIDATOR___SYS_USER_ID as SYS_USER_ID", \
	"EXP_UPD_VALIDATOR___ERROR_CMNT as ERROR_CMNT", \
	"EXP_UPD_VALIDATOR___REF_CODE_ID_1 as REF_CODE_ID_1", \
	"EXP_UPD_VALIDATOR___REF_FIELD_1 as REF_FIELD_1", \
	"EXP_UPD_VALIDATOR___REF_CODE_ID_2 as REF_CODE_ID_2", \
	"EXP_UPD_VALIDATOR___REF_FIELD_2 as REF_FIELD_2", \
	"EXP_UPD_VALIDATOR___REF_CODE_ID_3 as REF_CODE_ID_3", \
	"EXP_UPD_VALIDATOR___REF_FIELD_3 as REF_FIELD_3", \
	"EXP_UPD_VALIDATOR___REF_CODE_ID_4 as REF_CODE_ID_4", \
	"EXP_UPD_VALIDATOR___REF_FIELD_4 as REF_FIELD_4", \
	"EXP_UPD_VALIDATOR___REF_CODE_ID_5 as REF_CODE_ID_5", \
	"EXP_UPD_VALIDATOR___REF_FIELD_5 as REF_FIELD_5", \
	"EXP_UPD_VALIDATOR___REF_CODE_ID_6 as REF_CODE_ID_6", \
	"EXP_UPD_VALIDATOR___REF_FIELD_6 as REF_FIELD_6", \
	"EXP_UPD_VALIDATOR___REF_CODE_ID_7 as REF_CODE_ID_7", \
	"EXP_UPD_VALIDATOR___REF_FIELD_7 as REF_FIELD_7", \
	"EXP_UPD_VALIDATOR___REF_CODE_ID_8 as REF_CODE_ID_8", \
	"EXP_UPD_VALIDATOR___REF_FIELD_8 as REF_FIELD_8", \
	"EXP_UPD_VALIDATOR___REF_CODE_ID_9 as REF_CODE_ID_9", \
	"EXP_UPD_VALIDATOR___REF_FIELD_9 as REF_FIELD_9", \
	"EXP_UPD_VALIDATOR___REF_CODE_ID_10 as REF_CODE_ID_10", \
	"EXP_UPD_VALIDATOR___REF_FIELD_10 as REF_FIELD_10", \
	"EXP_UPD_VALIDATOR___XML_GROUP_ID as XML_GROUP_ID", \
	"EXP_UPD_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_UPD_VALIDATOR___MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_UPD_VALIDATOR___USER_ID as USER_ID", \
	"EXP_UPD_VALIDATOR___ITEM_ID as ITEM_ID", \
	"EXP_UPD_VALIDATOR___TC_COMPANY_ID as TC_COMPANY_ID", \
	"EXP_UPD_VALIDATOR___WM_VERSION_ID as WM_VERSION_ID", \
	"EXP_UPD_VALIDATOR___FACILITY_ID as FACILITY_ID", \
	"EXP_UPD_VALIDATOR___COMPANY_CODE as COMPANY_CODE", \
	"EXP_UPD_VALIDATOR___ITEM_NAME as ITEM_NAME", \
	"EXP_UPD_VALIDATOR___ESIGN_USER_NAME as ESIGN_USER_NAME", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)) , lit(0)) .when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_PIX_TRAN1, type TARGET 
# COLUMN COUNT: 80

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_PIX_TRAN_ID = target.WM_PIX_TRAN_ID"""
  refined_perf_table = "WM_PIX_TRAN"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_PIX_TRAN", "WM_PIX_TRAN", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_PIX_TRAN", "WM_PIX_TRAN","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	