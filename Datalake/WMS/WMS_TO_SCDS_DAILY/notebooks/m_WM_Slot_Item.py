#Code converted on 2023-06-22 20:59:58
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

# Read in relation source variables
# (username, password, connection_string) = getConfig(DC_NBR, env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SLOT_ITEM_PRE, type SOURCE 
# COLUMN COUNT: 88

SQ_Shortcut_to_WM_SLOT_ITEM_PRE = spark.sql(f"""SELECT
WM_SLOT_ITEM_PRE.DC_NBR,
WM_SLOT_ITEM_PRE.SLOT_ITEM_ID,
WM_SLOT_ITEM_PRE.SLOT_ID,
WM_SLOT_ITEM_PRE.SKU_ID,
WM_SLOT_ITEM_PRE.SLOT_UNIT,
WM_SLOT_ITEM_PRE.SHIP_UNIT,
WM_SLOT_ITEM_PRE.MAX_LANES,
WM_SLOT_ITEM_PRE.DEEPS,
WM_SLOT_ITEM_PRE.SLOT_LOCKED,
WM_SLOT_ITEM_PRE.PRIMARY_MOVE,
WM_SLOT_ITEM_PRE.SLOT_WIDTH,
WM_SLOT_ITEM_PRE.EST_HITS,
WM_SLOT_ITEM_PRE.EST_INVENTORY,
WM_SLOT_ITEM_PRE.EST_MOVEMENT,
WM_SLOT_ITEM_PRE.HIST_MATCH,
WM_SLOT_ITEM_PRE.LAST_CHANGE,
WM_SLOT_ITEM_PRE.BIN_UNIT,
WM_SLOT_ITEM_PRE.PALLETE_PATTERN,
WM_SLOT_ITEM_PRE.SCORE,
WM_SLOT_ITEM_PRE.CUR_ORIENTATION,
WM_SLOT_ITEM_PRE.IGN_FOR_RESLOT,
WM_SLOT_ITEM_PRE.REC_LANES,
WM_SLOT_ITEM_PRE.REC_STACKING,
WM_SLOT_ITEM_PRE.PALLET_MOV,
WM_SLOT_ITEM_PRE.CASE_MOV,
WM_SLOT_ITEM_PRE.INNER_MOV,
WM_SLOT_ITEM_PRE.EACH_MOV,
WM_SLOT_ITEM_PRE.BIN_MOV,
WM_SLOT_ITEM_PRE.PALLET_INVEN,
WM_SLOT_ITEM_PRE.CASE_INVEN,
WM_SLOT_ITEM_PRE.INNER_INVEN,
WM_SLOT_ITEM_PRE.EACH_INVEN,
WM_SLOT_ITEM_PRE.BIN_INVEN,
WM_SLOT_ITEM_PRE.CALC_HITS,
WM_SLOT_ITEM_PRE.CURRENT_BIN,
WM_SLOT_ITEM_PRE.CURRENT_PALLET,
WM_SLOT_ITEM_PRE.OPT_PALLET_PATTERN,
WM_SLOT_ITEM_PRE.ALLOW_EXPAND,
WM_SLOT_ITEM_PRE.PALLET_HI,
WM_SLOT_ITEM_PRE.NEEDED_RACK_TYPE,
WM_SLOT_ITEM_PRE.LEGAL_FIT_REASON,
WM_SLOT_ITEM_PRE.ITEM_COST,
WM_SLOT_ITEM_PRE.USER_DEFINED,
WM_SLOT_ITEM_PRE.LEGAL_FIT,
WM_SLOT_ITEM_PRE.SCORE_DIRTY,
WM_SLOT_ITEM_PRE.USE_ESTIMATED_HIST,
WM_SLOT_ITEM_PRE.OPT_FLUID_VOL,
WM_SLOT_ITEM_PRE.CALC_VISC,
WM_SLOT_ITEM_PRE.EST_VISC,
WM_SLOT_ITEM_PRE.SESSION_ID,
WM_SLOT_ITEM_PRE.RANK,
WM_SLOT_ITEM_PRE.INFO1,
WM_SLOT_ITEM_PRE.INFO2,
WM_SLOT_ITEM_PRE.INFO3,
WM_SLOT_ITEM_PRE.INFO4,
WM_SLOT_ITEM_PRE.INFO5,
WM_SLOT_ITEM_PRE.INFO6,
WM_SLOT_ITEM_PRE.RESERVED_1,
WM_SLOT_ITEM_PRE.RESERVED_2,
WM_SLOT_ITEM_PRE.RESERVED_3,
WM_SLOT_ITEM_PRE.RESERVED_4,
WM_SLOT_ITEM_PRE.CREATE_DATE_TIME,
WM_SLOT_ITEM_PRE.MOD_DATE_TIME,
WM_SLOT_ITEM_PRE.MOD_USER,
WM_SLOT_ITEM_PRE.SI_NUM_1,
WM_SLOT_ITEM_PRE.SI_NUM_2,
WM_SLOT_ITEM_PRE.SI_NUM_3,
WM_SLOT_ITEM_PRE.SI_NUM_4,
WM_SLOT_ITEM_PRE.SI_NUM_5,
WM_SLOT_ITEM_PRE.SI_NUM_6,
WM_SLOT_ITEM_PRE.SLOT_UNIT_WEIGHT,
WM_SLOT_ITEM_PRE.TOTAL_ITEM_WT,
WM_SLOT_ITEM_PRE.BORROWING_OBJECT,
WM_SLOT_ITEM_PRE.BORROWING_SPECIFIC,
WM_SLOT_ITEM_PRE.EST_MVMT_CAN_BORROW,
WM_SLOT_ITEM_PRE.EST_HITS_CAN_BORROW,
WM_SLOT_ITEM_PRE.PLB_SCORE,
WM_SLOT_ITEM_PRE.MULT_LOC_GRP,
WM_SLOT_ITEM_PRE.DELETE_MULT,
WM_SLOT_ITEM_PRE.GAVE_HIST_TO,
WM_SLOT_ITEM_PRE.ABW_TEMP_TAG,
WM_SLOT_ITEM_PRE.FORECAST_BORROWED,
WM_SLOT_ITEM_PRE.NUM_VERT_DIV,
WM_SLOT_ITEM_PRE.REPLEN_GROUP,
WM_SLOT_ITEM_PRE.ADDED_FOR_MLM,
WM_SLOT_ITEM_PRE.RESERVED_5,
WM_SLOT_ITEM_PRE.ADJ_GRP_ID,
WM_SLOT_ITEM_PRE.IGA_SCORE
FROM WM_SLOT_ITEM_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SLOT_ITEM, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_SLOT_ITEM = spark.sql(f"""SELECT
WM_SLOT_ITEM.LOCATION_ID,
WM_SLOT_ITEM.WM_SLOT_ITEM_ID,
WM_SLOT_ITEM.WM_CREATE_TSTMP,
WM_SLOT_ITEM.WM_MOD_TSTMP,
WM_SLOT_ITEM.LOAD_TSTMP
FROM WM_SLOT_ITEM
WHERE WM_SLOT_ITEM_ID IN (SELECT SLOT_ITEM_ID FROM WM_SLOT_ITEM_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_TRANS, type EXPRESSION 
# COLUMN COUNT: 89

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_SLOT_ITEM_PRE_temp = SQ_Shortcut_to_WM_SLOT_ITEM_PRE.toDF(*["SQ_Shortcut_to_WM_SLOT_ITEM_PRE___" + col for col in SQ_Shortcut_to_WM_SLOT_ITEM_PRE.columns])

EXP_TRANS = SQ_Shortcut_to_WM_SLOT_ITEM_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___sys_row_id as sys_row_id", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___DC_NBR as DC_NBR", 
	"cast(SQ_Shortcut_to_WM_SLOT_ITEM_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SLOT_ITEM_ID as SLOT_ITEM_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SLOT_ID as SLOT_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SKU_ID as SKU_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SLOT_UNIT as SLOT_UNIT", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SHIP_UNIT as SHIP_UNIT", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___MAX_LANES as MAX_LANES", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___DEEPS as DEEPS", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SLOT_LOCKED as SLOT_LOCKED", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___PRIMARY_MOVE as PRIMARY_MOVE", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SLOT_WIDTH as SLOT_WIDTH", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___EST_HITS as EST_HITS", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___EST_INVENTORY as EST_INVENTORY", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___EST_MOVEMENT as EST_MOVEMENT", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___HIST_MATCH as HIST_MATCH", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___LAST_CHANGE as LAST_CHANGE", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___BIN_UNIT as BIN_UNIT", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___PALLETE_PATTERN as PALLETE_PATTERN", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SCORE as SCORE", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___CUR_ORIENTATION as CUR_ORIENTATION", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___IGN_FOR_RESLOT as IGN_FOR_RESLOT", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___REC_LANES as REC_LANES", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___REC_STACKING as REC_STACKING", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___PALLET_MOV as PALLET_MOV", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___CASE_MOV as CASE_MOV", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___INNER_MOV as INNER_MOV", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___EACH_MOV as EACH_MOV", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___BIN_MOV as BIN_MOV", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___PALLET_INVEN as PALLET_INVEN", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___CASE_INVEN as CASE_INVEN", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___INNER_INVEN as INNER_INVEN", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___EACH_INVEN as EACH_INVEN", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___BIN_INVEN as BIN_INVEN", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___CALC_HITS as CALC_HITS", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___CURRENT_BIN as CURRENT_BIN", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___CURRENT_PALLET as CURRENT_PALLET", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___ALLOW_EXPAND as ALLOW_EXPAND", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___PALLET_HI as PALLET_HI", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___NEEDED_RACK_TYPE as NEEDED_RACK_TYPE", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___LEGAL_FIT_REASON as LEGAL_FIT_REASON", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___ITEM_COST as ITEM_COST", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___USER_DEFINED as USER_DEFINED", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___LEGAL_FIT as LEGAL_FIT", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SCORE_DIRTY as SCORE_DIRTY", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___USE_ESTIMATED_HIST as USE_ESTIMATED_HIST", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___OPT_FLUID_VOL as OPT_FLUID_VOL", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___CALC_VISC as CALC_VISC", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___EST_VISC as EST_VISC", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SESSION_ID as SESSION_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___RANK as RANK", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___INFO1 as INFO1", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___INFO2 as INFO2", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___INFO3 as INFO3", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___INFO4 as INFO4", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___INFO5 as INFO5", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___INFO6 as INFO6", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___RESERVED_1 as RESERVED_1", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___RESERVED_2 as RESERVED_2", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___RESERVED_3 as RESERVED_3", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___RESERVED_4 as RESERVED_4", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___MOD_USER as MOD_USER", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SI_NUM_1 as SI_NUM_1", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SI_NUM_2 as SI_NUM_2", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SI_NUM_3 as SI_NUM_3", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SI_NUM_4 as SI_NUM_4", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SI_NUM_5 as SI_NUM_5", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SI_NUM_6 as SI_NUM_6", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___SLOT_UNIT_WEIGHT as SLOT_UNIT_WEIGHT", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___TOTAL_ITEM_WT as TOTAL_ITEM_WT", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___BORROWING_OBJECT as BORROWING_OBJECT", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___BORROWING_SPECIFIC as BORROWING_SPECIFIC", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___EST_MVMT_CAN_BORROW as EST_MVMT_CAN_BORROW", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___EST_HITS_CAN_BORROW as EST_HITS_CAN_BORROW", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___PLB_SCORE as PLB_SCORE", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___MULT_LOC_GRP as MULT_LOC_GRP", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___DELETE_MULT as DELETE_MULT", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___GAVE_HIST_TO as GAVE_HIST_TO", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___ABW_TEMP_TAG as ABW_TEMP_TAG", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___FORECAST_BORROWED as FORECAST_BORROWED", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___NUM_VERT_DIV as NUM_VERT_DIV", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___REPLEN_GROUP as REPLEN_GROUP", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___ADDED_FOR_MLM as ADDED_FOR_MLM", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___RESERVED_5 as RESERVED_5", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___ADJ_GRP_ID as ADJ_GRP_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_PRE___IGA_SCORE as IGA_SCORE" 
)

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 90

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_TRANS,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_TRANS.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_SLOT_ITEM, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 93

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_SLOT_ITEM_temp = SQ_Shortcut_to_WM_SLOT_ITEM.toDF(*["SQ_Shortcut_to_WM_SLOT_ITEM___" + col for col in SQ_Shortcut_to_WM_SLOT_ITEM.columns])

JNR_WM_SLOT_ITEM = SQ_Shortcut_to_WM_SLOT_ITEM_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_SLOT_ITEM_temp.SQ_Shortcut_to_WM_SLOT_ITEM___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_SLOT_ITEM_temp.SQ_Shortcut_to_WM_SLOT_ITEM___WM_SLOT_ITEM_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___SLOT_ITEM_ID],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___SLOT_ITEM_ID as SLOT_ITEM_ID", 
	"JNR_SITE_PROFILE___SLOT_ID as SLOT_ID", 
	"JNR_SITE_PROFILE___SKU_ID as SKU_ID", 
	"JNR_SITE_PROFILE___SLOT_UNIT as SLOT_UNIT", 
	"JNR_SITE_PROFILE___SHIP_UNIT as SHIP_UNIT", 
	"JNR_SITE_PROFILE___MAX_LANES as MAX_LANES", 
	"JNR_SITE_PROFILE___DEEPS as DEEPS", 
	"JNR_SITE_PROFILE___SLOT_LOCKED as SLOT_LOCKED", 
	"JNR_SITE_PROFILE___PRIMARY_MOVE as PRIMARY_MOVE", 
	"JNR_SITE_PROFILE___SLOT_WIDTH as SLOT_WIDTH", 
	"JNR_SITE_PROFILE___EST_HITS as EST_HITS", 
	"JNR_SITE_PROFILE___EST_INVENTORY as EST_INVENTORY", 
	"JNR_SITE_PROFILE___EST_MOVEMENT as EST_MOVEMENT", 
	"JNR_SITE_PROFILE___HIST_MATCH as HIST_MATCH", 
	"JNR_SITE_PROFILE___LAST_CHANGE as LAST_CHANGE", 
	"JNR_SITE_PROFILE___BIN_UNIT as BIN_UNIT", 
	"JNR_SITE_PROFILE___PALLETE_PATTERN as PALLETE_PATTERN", 
	"JNR_SITE_PROFILE___SCORE as SCORE", 
	"JNR_SITE_PROFILE___CUR_ORIENTATION as CUR_ORIENTATION", 
	"JNR_SITE_PROFILE___IGN_FOR_RESLOT as IGN_FOR_RESLOT", 
	"JNR_SITE_PROFILE___REC_LANES as REC_LANES", 
	"JNR_SITE_PROFILE___REC_STACKING as REC_STACKING", 
	"JNR_SITE_PROFILE___PALLET_MOV as PALLET_MOV", 
	"JNR_SITE_PROFILE___CASE_MOV as CASE_MOV", 
	"JNR_SITE_PROFILE___INNER_MOV as INNER_MOV", 
	"JNR_SITE_PROFILE___EACH_MOV as EACH_MOV", 
	"JNR_SITE_PROFILE___BIN_MOV as BIN_MOV", 
	"JNR_SITE_PROFILE___PALLET_INVEN as PALLET_INVEN", 
	"JNR_SITE_PROFILE___CASE_INVEN as CASE_INVEN", 
	"JNR_SITE_PROFILE___INNER_INVEN as INNER_INVEN", 
	"JNR_SITE_PROFILE___EACH_INVEN as EACH_INVEN", 
	"JNR_SITE_PROFILE___BIN_INVEN as BIN_INVEN", 
	"JNR_SITE_PROFILE___CALC_HITS as CALC_HITS", 
	"JNR_SITE_PROFILE___CURRENT_BIN as CURRENT_BIN", 
	"JNR_SITE_PROFILE___CURRENT_PALLET as CURRENT_PALLET", 
	"JNR_SITE_PROFILE___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", 
	"JNR_SITE_PROFILE___ALLOW_EXPAND as ALLOW_EXPAND", 
	"JNR_SITE_PROFILE___PALLET_HI as PALLET_HI", 
	"JNR_SITE_PROFILE___NEEDED_RACK_TYPE as NEEDED_RACK_TYPE", 
	"JNR_SITE_PROFILE___LEGAL_FIT_REASON as LEGAL_FIT_REASON", 
	"JNR_SITE_PROFILE___ITEM_COST as ITEM_COST", 
	"JNR_SITE_PROFILE___USER_DEFINED as USER_DEFINED", 
	"JNR_SITE_PROFILE___LEGAL_FIT as LEGAL_FIT", 
	"JNR_SITE_PROFILE___SCORE_DIRTY as SCORE_DIRTY", 
	"JNR_SITE_PROFILE___USE_ESTIMATED_HIST as USE_ESTIMATED_HIST", 
	"JNR_SITE_PROFILE___OPT_FLUID_VOL as OPT_FLUID_VOL", 
	"JNR_SITE_PROFILE___CALC_VISC as CALC_VISC", 
	"JNR_SITE_PROFILE___EST_VISC as EST_VISC", 
	"JNR_SITE_PROFILE___SESSION_ID as SESSION_ID", 
	"JNR_SITE_PROFILE___RANK as RANK", 
	"JNR_SITE_PROFILE___INFO1 as INFO1", 
	"JNR_SITE_PROFILE___INFO2 as INFO2", 
	"JNR_SITE_PROFILE___INFO3 as INFO3", 
	"JNR_SITE_PROFILE___INFO4 as INFO4", 
	"JNR_SITE_PROFILE___INFO5 as INFO5", 
	"JNR_SITE_PROFILE___INFO6 as INFO6", 
	"JNR_SITE_PROFILE___RESERVED_1 as RESERVED_1", 
	"JNR_SITE_PROFILE___RESERVED_2 as RESERVED_2", 
	"JNR_SITE_PROFILE___RESERVED_3 as RESERVED_3", 
	"JNR_SITE_PROFILE___RESERVED_4 as RESERVED_4", 
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"JNR_SITE_PROFILE___MOD_USER as MOD_USER", 
	"JNR_SITE_PROFILE___SI_NUM_1 as SI_NUM_1", 
	"JNR_SITE_PROFILE___SI_NUM_2 as SI_NUM_2", 
	"JNR_SITE_PROFILE___SI_NUM_3 as SI_NUM_3", 
	"JNR_SITE_PROFILE___SI_NUM_4 as SI_NUM_4", 
	"JNR_SITE_PROFILE___SI_NUM_5 as SI_NUM_5", 
	"JNR_SITE_PROFILE___SI_NUM_6 as SI_NUM_6", 
	"JNR_SITE_PROFILE___SLOT_UNIT_WEIGHT as SLOT_UNIT_WEIGHT", 
	"JNR_SITE_PROFILE___TOTAL_ITEM_WT as TOTAL_ITEM_WT", 
	"JNR_SITE_PROFILE___BORROWING_OBJECT as BORROWING_OBJECT", 
	"JNR_SITE_PROFILE___BORROWING_SPECIFIC as BORROWING_SPECIFIC", 
	"JNR_SITE_PROFILE___EST_MVMT_CAN_BORROW as EST_MVMT_CAN_BORROW", 
	"JNR_SITE_PROFILE___EST_HITS_CAN_BORROW as EST_HITS_CAN_BORROW", 
	"JNR_SITE_PROFILE___PLB_SCORE as PLB_SCORE", 
	"JNR_SITE_PROFILE___MULT_LOC_GRP as MULT_LOC_GRP", 
	"JNR_SITE_PROFILE___DELETE_MULT as DELETE_MULT", 
	"JNR_SITE_PROFILE___GAVE_HIST_TO as GAVE_HIST_TO", 
	"JNR_SITE_PROFILE___ABW_TEMP_TAG as ABW_TEMP_TAG", 
	"JNR_SITE_PROFILE___FORECAST_BORROWED as FORECAST_BORROWED", 
	"JNR_SITE_PROFILE___NUM_VERT_DIV as NUM_VERT_DIV", 
	"JNR_SITE_PROFILE___REPLEN_GROUP as REPLEN_GROUP", 
	"JNR_SITE_PROFILE___ADDED_FOR_MLM as ADDED_FOR_MLM", 
	"JNR_SITE_PROFILE___RESERVED_5 as RESERVED_5", 
	"JNR_SITE_PROFILE___ADJ_GRP_ID as ADJ_GRP_ID", 
	"JNR_SITE_PROFILE___IGA_SCORE as IGA_SCORE", 
	"SQ_Shortcut_to_WM_SLOT_ITEM___LOCATION_ID as i_LOCATION_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM___WM_SLOT_ITEM_ID as i_WM_SLOT_ITEM_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", 
	"SQ_Shortcut_to_WM_SLOT_ITEM___WM_MOD_TSTMP as i_WM_MOD_TSTMP", 
	"SQ_Shortcut_to_WM_SLOT_ITEM___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 92

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_SLOT_ITEM_temp = JNR_WM_SLOT_ITEM.toDF(*["JNR_WM_SLOT_ITEM___" + col for col in JNR_WM_SLOT_ITEM.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_SLOT_ITEM_temp.selectExpr( 
	"JNR_WM_SLOT_ITEM___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_SLOT_ITEM___SLOT_ITEM_ID as SLOT_ITEM_ID", 
	"JNR_WM_SLOT_ITEM___SLOT_ID as SLOT_ID", 
	"JNR_WM_SLOT_ITEM___SKU_ID as SKU_ID", 
	"JNR_WM_SLOT_ITEM___SLOT_UNIT as SLOT_UNIT", 
	"JNR_WM_SLOT_ITEM___SHIP_UNIT as SHIP_UNIT", 
	"JNR_WM_SLOT_ITEM___MAX_LANES as MAX_LANES", 
	"JNR_WM_SLOT_ITEM___DEEPS as DEEPS", 
	"JNR_WM_SLOT_ITEM___SLOT_LOCKED as SLOT_LOCKED", 
	"JNR_WM_SLOT_ITEM___PRIMARY_MOVE as PRIMARY_MOVE", 
	"JNR_WM_SLOT_ITEM___SLOT_WIDTH as SLOT_WIDTH", 
	"JNR_WM_SLOT_ITEM___EST_HITS as EST_HITS", 
	"JNR_WM_SLOT_ITEM___EST_INVENTORY as EST_INVENTORY", 
	"JNR_WM_SLOT_ITEM___EST_MOVEMENT as EST_MOVEMENT", 
	"JNR_WM_SLOT_ITEM___HIST_MATCH as HIST_MATCH", 
	"JNR_WM_SLOT_ITEM___LAST_CHANGE as LAST_CHANGE", 
	"JNR_WM_SLOT_ITEM___BIN_UNIT as BIN_UNIT", 
	"JNR_WM_SLOT_ITEM___PALLETE_PATTERN as PALLETE_PATTERN", 
	"JNR_WM_SLOT_ITEM___SCORE as SCORE", 
	"JNR_WM_SLOT_ITEM___CUR_ORIENTATION as CUR_ORIENTATION", 
	"JNR_WM_SLOT_ITEM___IGN_FOR_RESLOT as IGN_FOR_RESLOT", 
	"JNR_WM_SLOT_ITEM___REC_LANES as REC_LANES", 
	"JNR_WM_SLOT_ITEM___REC_STACKING as REC_STACKING", 
	"JNR_WM_SLOT_ITEM___PALLET_MOV as PALLET_MOV", 
	"JNR_WM_SLOT_ITEM___CASE_MOV as CASE_MOV", 
	"JNR_WM_SLOT_ITEM___INNER_MOV as INNER_MOV", 
	"JNR_WM_SLOT_ITEM___EACH_MOV as EACH_MOV", 
	"JNR_WM_SLOT_ITEM___BIN_MOV as BIN_MOV", 
	"JNR_WM_SLOT_ITEM___PALLET_INVEN as PALLET_INVEN", 
	"JNR_WM_SLOT_ITEM___CASE_INVEN as CASE_INVEN", 
	"JNR_WM_SLOT_ITEM___INNER_INVEN as INNER_INVEN", 
	"JNR_WM_SLOT_ITEM___EACH_INVEN as EACH_INVEN", 
	"JNR_WM_SLOT_ITEM___BIN_INVEN as BIN_INVEN", 
	"JNR_WM_SLOT_ITEM___CALC_HITS as CALC_HITS", 
	"JNR_WM_SLOT_ITEM___CURRENT_BIN as CURRENT_BIN", 
	"JNR_WM_SLOT_ITEM___CURRENT_PALLET as CURRENT_PALLET", 
	"JNR_WM_SLOT_ITEM___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", 
	"JNR_WM_SLOT_ITEM___ALLOW_EXPAND as ALLOW_EXPAND", 
	"JNR_WM_SLOT_ITEM___PALLET_HI as PALLET_HI", 
	"JNR_WM_SLOT_ITEM___NEEDED_RACK_TYPE as NEEDED_RACK_TYPE", 
	"JNR_WM_SLOT_ITEM___LEGAL_FIT_REASON as LEGAL_FIT_REASON", 
	"JNR_WM_SLOT_ITEM___ITEM_COST as ITEM_COST", 
	"JNR_WM_SLOT_ITEM___USER_DEFINED as USER_DEFINED", 
	"JNR_WM_SLOT_ITEM___LEGAL_FIT as LEGAL_FIT", 
	"JNR_WM_SLOT_ITEM___SCORE_DIRTY as SCORE_DIRTY", 
	"JNR_WM_SLOT_ITEM___USE_ESTIMATED_HIST as USE_ESTIMATED_HIST", 
	"JNR_WM_SLOT_ITEM___OPT_FLUID_VOL as OPT_FLUID_VOL", 
	"JNR_WM_SLOT_ITEM___CALC_VISC as CALC_VISC", 
	"JNR_WM_SLOT_ITEM___EST_VISC as EST_VISC", 
	"JNR_WM_SLOT_ITEM___SESSION_ID as SESSION_ID", 
	"JNR_WM_SLOT_ITEM___RANK as RANK", 
	"JNR_WM_SLOT_ITEM___INFO1 as INFO1", 
	"JNR_WM_SLOT_ITEM___INFO2 as INFO2", 
	"JNR_WM_SLOT_ITEM___INFO3 as INFO3", 
	"JNR_WM_SLOT_ITEM___INFO4 as INFO4", 
	"JNR_WM_SLOT_ITEM___INFO5 as INFO5", 
	"JNR_WM_SLOT_ITEM___INFO6 as INFO6", 
	"JNR_WM_SLOT_ITEM___RESERVED_1 as RESERVED_1", 
	"JNR_WM_SLOT_ITEM___RESERVED_2 as RESERVED_2", 
	"JNR_WM_SLOT_ITEM___RESERVED_3 as RESERVED_3", 
	"JNR_WM_SLOT_ITEM___RESERVED_4 as RESERVED_4", 
	"JNR_WM_SLOT_ITEM___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"JNR_WM_SLOT_ITEM___MOD_DATE_TIME as MOD_DATE_TIME", 
	"JNR_WM_SLOT_ITEM___MOD_USER as MOD_USER", 
	"JNR_WM_SLOT_ITEM___SI_NUM_1 as SI_NUM_1", 
	"JNR_WM_SLOT_ITEM___SI_NUM_2 as SI_NUM_2", 
	"JNR_WM_SLOT_ITEM___SI_NUM_3 as SI_NUM_3", 
	"JNR_WM_SLOT_ITEM___SI_NUM_4 as SI_NUM_4", 
	"JNR_WM_SLOT_ITEM___SI_NUM_5 as SI_NUM_5", 
	"JNR_WM_SLOT_ITEM___SI_NUM_6 as SI_NUM_6", 
	"JNR_WM_SLOT_ITEM___SLOT_UNIT_WEIGHT as SLOT_UNIT_WEIGHT", 
	"JNR_WM_SLOT_ITEM___TOTAL_ITEM_WT as TOTAL_ITEM_WT", 
	"JNR_WM_SLOT_ITEM___BORROWING_OBJECT as BORROWING_OBJECT", 
	"JNR_WM_SLOT_ITEM___BORROWING_SPECIFIC as BORROWING_SPECIFIC", 
	"JNR_WM_SLOT_ITEM___EST_MVMT_CAN_BORROW as EST_MVMT_CAN_BORROW", 
	"JNR_WM_SLOT_ITEM___EST_HITS_CAN_BORROW as EST_HITS_CAN_BORROW", 
	"JNR_WM_SLOT_ITEM___PLB_SCORE as PLB_SCORE", 
	"JNR_WM_SLOT_ITEM___MULT_LOC_GRP as MULT_LOC_GRP", 
	"JNR_WM_SLOT_ITEM___DELETE_MULT as DELETE_MULT", 
	"JNR_WM_SLOT_ITEM___GAVE_HIST_TO as GAVE_HIST_TO", 
	"JNR_WM_SLOT_ITEM___ABW_TEMP_TAG as ABW_TEMP_TAG", 
	"JNR_WM_SLOT_ITEM___FORECAST_BORROWED as FORECAST_BORROWED", 
	"JNR_WM_SLOT_ITEM___NUM_VERT_DIV as NUM_VERT_DIV", 
	"JNR_WM_SLOT_ITEM___REPLEN_GROUP as REPLEN_GROUP", 
	"JNR_WM_SLOT_ITEM___ADDED_FOR_MLM as ADDED_FOR_MLM", 
	"JNR_WM_SLOT_ITEM___RESERVED_5 as RESERVED_5", 
	"JNR_WM_SLOT_ITEM___ADJ_GRP_ID as ADJ_GRP_ID", 
	"JNR_WM_SLOT_ITEM___IGA_SCORE as IGA_SCORE", 
	"JNR_WM_SLOT_ITEM___i_WM_SLOT_ITEM_ID as i_WM_SLOT_ITEM_ID", 
	"JNR_WM_SLOT_ITEM___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", 
	"JNR_WM_SLOT_ITEM___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", 
	"JNR_WM_SLOT_ITEM___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("i_WM_SLOT_ITEM_ID IS NULL OR (NOT i_WM_SLOT_ITEM_ID IS NULL AND (COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01')) OR (COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 93

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___SLOT_ITEM_ID as SLOT_ITEM_ID", 
	"FIL_UNCHANGED_RECORDS___SLOT_ID as SLOT_ID", 
	"FIL_UNCHANGED_RECORDS___SKU_ID as SKU_ID", 
	"FIL_UNCHANGED_RECORDS___SLOT_UNIT as SLOT_UNIT", 
	"FIL_UNCHANGED_RECORDS___SHIP_UNIT as SHIP_UNIT", 
	"FIL_UNCHANGED_RECORDS___MAX_LANES as MAX_LANES", 
	"FIL_UNCHANGED_RECORDS___DEEPS as DEEPS", 
	"FIL_UNCHANGED_RECORDS___SLOT_LOCKED as SLOT_LOCKED", 
	"FIL_UNCHANGED_RECORDS___PRIMARY_MOVE as PRIMARY_MOVE", 
	"FIL_UNCHANGED_RECORDS___SLOT_WIDTH as SLOT_WIDTH", 
	"FIL_UNCHANGED_RECORDS___EST_HITS as EST_HITS", 
	"FIL_UNCHANGED_RECORDS___EST_INVENTORY as EST_INVENTORY", 
	"FIL_UNCHANGED_RECORDS___EST_MOVEMENT as EST_MOVEMENT", 
	"FIL_UNCHANGED_RECORDS___HIST_MATCH as HIST_MATCH", 
	"FIL_UNCHANGED_RECORDS___LAST_CHANGE as LAST_CHANGE", 
	"FIL_UNCHANGED_RECORDS___BIN_UNIT as BIN_UNIT", 
	"FIL_UNCHANGED_RECORDS___PALLETE_PATTERN as PALLETE_PATTERN", 
	"FIL_UNCHANGED_RECORDS___SCORE as SCORE", 
	"FIL_UNCHANGED_RECORDS___CUR_ORIENTATION as CUR_ORIENTATION", 
	"FIL_UNCHANGED_RECORDS___IGN_FOR_RESLOT as IGN_FOR_RESLOT", 
	"FIL_UNCHANGED_RECORDS___REC_LANES as REC_LANES", 
	"FIL_UNCHANGED_RECORDS___REC_STACKING as REC_STACKING", 
	"FIL_UNCHANGED_RECORDS___PALLET_MOV as PALLET_MOV", 
	"FIL_UNCHANGED_RECORDS___CASE_MOV as CASE_MOV", 
	"FIL_UNCHANGED_RECORDS___INNER_MOV as INNER_MOV", 
	"FIL_UNCHANGED_RECORDS___EACH_MOV as EACH_MOV", 
	"FIL_UNCHANGED_RECORDS___BIN_MOV as BIN_MOV", 
	"FIL_UNCHANGED_RECORDS___PALLET_INVEN as PALLET_INVEN", 
	"FIL_UNCHANGED_RECORDS___CASE_INVEN as CASE_INVEN", 
	"FIL_UNCHANGED_RECORDS___INNER_INVEN as INNER_INVEN", 
	"FIL_UNCHANGED_RECORDS___EACH_INVEN as EACH_INVEN", 
	"FIL_UNCHANGED_RECORDS___BIN_INVEN as BIN_INVEN", 
	"FIL_UNCHANGED_RECORDS___CALC_HITS as CALC_HITS", 
	"FIL_UNCHANGED_RECORDS___CURRENT_BIN as CURRENT_BIN", 
	"FIL_UNCHANGED_RECORDS___CURRENT_PALLET as CURRENT_PALLET", 
	"FIL_UNCHANGED_RECORDS___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", 
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND as ALLOW_EXPAND", 
	"FIL_UNCHANGED_RECORDS___PALLET_HI as PALLET_HI", 
	"FIL_UNCHANGED_RECORDS___NEEDED_RACK_TYPE as NEEDED_RACK_TYPE", 
	"FIL_UNCHANGED_RECORDS___LEGAL_FIT_REASON as LEGAL_FIT_REASON", 
	"FIL_UNCHANGED_RECORDS___ITEM_COST as ITEM_COST", 
	"FIL_UNCHANGED_RECORDS___USER_DEFINED as USER_DEFINED", 
	"FIL_UNCHANGED_RECORDS___LEGAL_FIT as LEGAL_FIT", 
	"FIL_UNCHANGED_RECORDS___SCORE_DIRTY as SCORE_DIRTY", 
	"FIL_UNCHANGED_RECORDS___USE_ESTIMATED_HIST as USE_ESTIMATED_HIST", 
	"FIL_UNCHANGED_RECORDS___OPT_FLUID_VOL as OPT_FLUID_VOL", 
	"FIL_UNCHANGED_RECORDS___CALC_VISC as CALC_VISC", 
	"FIL_UNCHANGED_RECORDS___EST_VISC as EST_VISC", 
	"FIL_UNCHANGED_RECORDS___SESSION_ID as SESSION_ID", 
	"FIL_UNCHANGED_RECORDS___RANK as RANK", 
	"FIL_UNCHANGED_RECORDS___INFO1 as INFO1", 
	"FIL_UNCHANGED_RECORDS___INFO2 as INFO2", 
	"FIL_UNCHANGED_RECORDS___INFO3 as INFO3", 
	"FIL_UNCHANGED_RECORDS___INFO4 as INFO4", 
	"FIL_UNCHANGED_RECORDS___INFO5 as INFO5", 
	"FIL_UNCHANGED_RECORDS___INFO6 as INFO6", 
	"FIL_UNCHANGED_RECORDS___RESERVED_1 as RESERVED_1", 
	"FIL_UNCHANGED_RECORDS___RESERVED_2 as RESERVED_2", 
	"FIL_UNCHANGED_RECORDS___RESERVED_3 as RESERVED_3", 
	"FIL_UNCHANGED_RECORDS___RESERVED_4 as RESERVED_4", 
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___MOD_USER as MOD_USER", 
	"FIL_UNCHANGED_RECORDS___SI_NUM_1 as SI_NUM_1", 
	"FIL_UNCHANGED_RECORDS___SI_NUM_2 as SI_NUM_2", 
	"FIL_UNCHANGED_RECORDS___SI_NUM_3 as SI_NUM_3", 
	"FIL_UNCHANGED_RECORDS___SI_NUM_4 as SI_NUM_4", 
	"FIL_UNCHANGED_RECORDS___SI_NUM_5 as SI_NUM_5", 
	"FIL_UNCHANGED_RECORDS___SI_NUM_6 as SI_NUM_6", 
	"FIL_UNCHANGED_RECORDS___SLOT_UNIT_WEIGHT as SLOT_UNIT_WEIGHT", 
	"FIL_UNCHANGED_RECORDS___TOTAL_ITEM_WT as TOTAL_ITEM_WT", 
	"FIL_UNCHANGED_RECORDS___BORROWING_OBJECT as BORROWING_OBJECT", 
	"FIL_UNCHANGED_RECORDS___BORROWING_SPECIFIC as BORROWING_SPECIFIC", 
	"FIL_UNCHANGED_RECORDS___EST_MVMT_CAN_BORROW as EST_MVMT_CAN_BORROW", 
	"FIL_UNCHANGED_RECORDS___EST_HITS_CAN_BORROW as EST_HITS_CAN_BORROW", 
	"FIL_UNCHANGED_RECORDS___PLB_SCORE as PLB_SCORE", 
	"FIL_UNCHANGED_RECORDS___MULT_LOC_GRP as MULT_LOC_GRP", 
	"FIL_UNCHANGED_RECORDS___DELETE_MULT as DELETE_MULT", 
	"FIL_UNCHANGED_RECORDS___GAVE_HIST_TO as GAVE_HIST_TO", 
	"FIL_UNCHANGED_RECORDS___ABW_TEMP_TAG as ABW_TEMP_TAG", 
	"FIL_UNCHANGED_RECORDS___FORECAST_BORROWED as FORECAST_BORROWED", 
	"FIL_UNCHANGED_RECORDS___NUM_VERT_DIV as NUM_VERT_DIV", 
	"FIL_UNCHANGED_RECORDS___REPLEN_GROUP as REPLEN_GROUP", 
	"FIL_UNCHANGED_RECORDS___ADDED_FOR_MLM as ADDED_FOR_MLM", 
	"FIL_UNCHANGED_RECORDS___RESERVED_5 as RESERVED_5", 
	"FIL_UNCHANGED_RECORDS___ADJ_GRP_ID as ADJ_GRP_ID", 
	"FIL_UNCHANGED_RECORDS___IGA_SCORE as IGA_SCORE", 
	"FIL_UNCHANGED_RECORDS___i_WM_SLOT_ITEM_ID as i_WM_SLOT_ITEM_ID", 
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___i_WM_SLOT_ITEM_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 91

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPDATE_VALIDATOR_temp.selectExpr( 
	"EXP_UPDATE_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_UPDATE_VALIDATOR___SLOT_ITEM_ID as SLOT_ITEM_ID", 
	"EXP_UPDATE_VALIDATOR___SLOT_ID as SLOT_ID", 
	"EXP_UPDATE_VALIDATOR___SKU_ID as SKU_ID", 
	"EXP_UPDATE_VALIDATOR___SLOT_UNIT as SLOT_UNIT", 
	"EXP_UPDATE_VALIDATOR___SHIP_UNIT as SHIP_UNIT", 
	"EXP_UPDATE_VALIDATOR___MAX_LANES as MAX_LANES", 
	"EXP_UPDATE_VALIDATOR___DEEPS as DEEPS", 
	"EXP_UPDATE_VALIDATOR___SLOT_LOCKED as SLOT_LOCKED", 
	"EXP_UPDATE_VALIDATOR___PRIMARY_MOVE as PRIMARY_MOVE", 
	"EXP_UPDATE_VALIDATOR___SLOT_WIDTH as SLOT_WIDTH", 
	"EXP_UPDATE_VALIDATOR___EST_HITS as EST_HITS", 
	"EXP_UPDATE_VALIDATOR___EST_INVENTORY as EST_INVENTORY", 
	"EXP_UPDATE_VALIDATOR___EST_MOVEMENT as EST_MOVEMENT", 
	"EXP_UPDATE_VALIDATOR___HIST_MATCH as HIST_MATCH", 
	"EXP_UPDATE_VALIDATOR___LAST_CHANGE as LAST_CHANGE", 
	"EXP_UPDATE_VALIDATOR___BIN_UNIT as BIN_UNIT", 
	"EXP_UPDATE_VALIDATOR___PALLETE_PATTERN as PALLETE_PATTERN", 
	"EXP_UPDATE_VALIDATOR___SCORE as SCORE", 
	"EXP_UPDATE_VALIDATOR___CUR_ORIENTATION as CUR_ORIENTATION", 
	"EXP_UPDATE_VALIDATOR___IGN_FOR_RESLOT as IGN_FOR_RESLOT", 
	"EXP_UPDATE_VALIDATOR___REC_LANES as REC_LANES", 
	"EXP_UPDATE_VALIDATOR___REC_STACKING as REC_STACKING", 
	"EXP_UPDATE_VALIDATOR___PALLET_MOV as PALLET_MOV", 
	"EXP_UPDATE_VALIDATOR___CASE_MOV as CASE_MOV", 
	"EXP_UPDATE_VALIDATOR___INNER_MOV as INNER_MOV", 
	"EXP_UPDATE_VALIDATOR___EACH_MOV as EACH_MOV", 
	"EXP_UPDATE_VALIDATOR___BIN_MOV as BIN_MOV", 
	"EXP_UPDATE_VALIDATOR___PALLET_INVEN as PALLET_INVEN", 
	"EXP_UPDATE_VALIDATOR___CASE_INVEN as CASE_INVEN", 
	"EXP_UPDATE_VALIDATOR___INNER_INVEN as INNER_INVEN", 
	"EXP_UPDATE_VALIDATOR___EACH_INVEN as EACH_INVEN", 
	"EXP_UPDATE_VALIDATOR___BIN_INVEN as BIN_INVEN", 
	"EXP_UPDATE_VALIDATOR___CALC_HITS as CALC_HITS", 
	"EXP_UPDATE_VALIDATOR___CURRENT_BIN as CURRENT_BIN", 
	"EXP_UPDATE_VALIDATOR___CURRENT_PALLET as CURRENT_PALLET", 
	"EXP_UPDATE_VALIDATOR___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", 
	"EXP_UPDATE_VALIDATOR___ALLOW_EXPAND as ALLOW_EXPAND", 
	"EXP_UPDATE_VALIDATOR___PALLET_HI as PALLET_HI", 
	"EXP_UPDATE_VALIDATOR___NEEDED_RACK_TYPE as NEEDED_RACK_TYPE", 
	"EXP_UPDATE_VALIDATOR___LEGAL_FIT_REASON as LEGAL_FIT_REASON", 
	"EXP_UPDATE_VALIDATOR___ITEM_COST as ITEM_COST", 
	"EXP_UPDATE_VALIDATOR___USER_DEFINED as USER_DEFINED", 
	"EXP_UPDATE_VALIDATOR___LEGAL_FIT as LEGAL_FIT", 
	"EXP_UPDATE_VALIDATOR___SCORE_DIRTY as SCORE_DIRTY", 
	"EXP_UPDATE_VALIDATOR___USE_ESTIMATED_HIST as USE_ESTIMATED_HIST", 
	"EXP_UPDATE_VALIDATOR___OPT_FLUID_VOL as OPT_FLUID_VOL", 
	"EXP_UPDATE_VALIDATOR___CALC_VISC as CALC_VISC", 
	"EXP_UPDATE_VALIDATOR___EST_VISC as EST_VISC", 
	"EXP_UPDATE_VALIDATOR___SESSION_ID as SESSION_ID", 
	"EXP_UPDATE_VALIDATOR___RANK as RANK", 
	"EXP_UPDATE_VALIDATOR___INFO1 as INFO1", 
	"EXP_UPDATE_VALIDATOR___INFO2 as INFO2", 
	"EXP_UPDATE_VALIDATOR___INFO3 as INFO3", 
	"EXP_UPDATE_VALIDATOR___INFO4 as INFO4", 
	"EXP_UPDATE_VALIDATOR___INFO5 as INFO5", 
	"EXP_UPDATE_VALIDATOR___INFO6 as INFO6", 
	"EXP_UPDATE_VALIDATOR___RESERVED_1 as RESERVED_1", 
	"EXP_UPDATE_VALIDATOR___RESERVED_2 as RESERVED_2", 
	"EXP_UPDATE_VALIDATOR___RESERVED_3 as RESERVED_3", 
	"EXP_UPDATE_VALIDATOR___RESERVED_4 as RESERVED_4", 
	"EXP_UPDATE_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"EXP_UPDATE_VALIDATOR___MOD_DATE_TIME as MOD_DATE_TIME", 
	"EXP_UPDATE_VALIDATOR___MOD_USER as MOD_USER", 
	"EXP_UPDATE_VALIDATOR___SI_NUM_1 as SI_NUM_1", 
	"EXP_UPDATE_VALIDATOR___SI_NUM_2 as SI_NUM_2", 
	"EXP_UPDATE_VALIDATOR___SI_NUM_3 as SI_NUM_3", 
	"EXP_UPDATE_VALIDATOR___SI_NUM_4 as SI_NUM_4", 
	"EXP_UPDATE_VALIDATOR___SI_NUM_5 as SI_NUM_5", 
	"EXP_UPDATE_VALIDATOR___SI_NUM_6 as SI_NUM_6", 
	"EXP_UPDATE_VALIDATOR___SLOT_UNIT_WEIGHT as SLOT_UNIT_WEIGHT", 
	"EXP_UPDATE_VALIDATOR___TOTAL_ITEM_WT as TOTAL_ITEM_WT", 
	"EXP_UPDATE_VALIDATOR___BORROWING_OBJECT as BORROWING_OBJECT", 
	"EXP_UPDATE_VALIDATOR___BORROWING_SPECIFIC as BORROWING_SPECIFIC", 
	"EXP_UPDATE_VALIDATOR___EST_MVMT_CAN_BORROW as EST_MVMT_CAN_BORROW", 
	"EXP_UPDATE_VALIDATOR___EST_HITS_CAN_BORROW as EST_HITS_CAN_BORROW", 
	"EXP_UPDATE_VALIDATOR___PLB_SCORE as PLB_SCORE", 
	"EXP_UPDATE_VALIDATOR___MULT_LOC_GRP as MULT_LOC_GRP", 
	"EXP_UPDATE_VALIDATOR___DELETE_MULT as DELETE_MULT", 
	"EXP_UPDATE_VALIDATOR___GAVE_HIST_TO as GAVE_HIST_TO", 
	"EXP_UPDATE_VALIDATOR___ABW_TEMP_TAG as ABW_TEMP_TAG", 
	"EXP_UPDATE_VALIDATOR___FORECAST_BORROWED as FORECAST_BORROWED", 
	"EXP_UPDATE_VALIDATOR___NUM_VERT_DIV as NUM_VERT_DIV", 
	"EXP_UPDATE_VALIDATOR___REPLEN_GROUP as REPLEN_GROUP", 
	"EXP_UPDATE_VALIDATOR___ADDED_FOR_MLM as ADDED_FOR_MLM", 
	"EXP_UPDATE_VALIDATOR___RESERVED_5 as RESERVED_5", 
	"EXP_UPDATE_VALIDATOR___ADJ_GRP_ID as ADJ_GRP_ID", 
	"EXP_UPDATE_VALIDATOR___IGA_SCORE as IGA_SCORE", 
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPDATE_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_UPDATE_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)), lit(0)).when(EXP_UPDATE_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_SLOT_ITEM1, type TARGET 
# COLUMN COUNT: 90

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_SLOT_ITEM_ID = target.WM_SLOT_ITEM_ID"""
  refined_perf_table = "WM_SLOT_ITEM"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_SLOT_ITEM", "WM_SLOT_ITEM", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_SLOT_ITEM", "WM_SLOT_ITEM","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	