#Code converted on 2023-08-09 16:37:39
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
# COMMAND ----------

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


# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_PACKAGE_PROMO, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_TRAINING_PACKAGE_PROMO = spark.sql(f"""SELECT
TRAINING_PACKAGE_PROMO_ID,
TRAINING_PACKAGE_PROMO_NAME,
TRAINING_PACKAGE_PROMO_DESC,
TRAINING_PACKAGE_ID,
COUNTRY_CD,
START_DT,
END_DT,
DISC_AMT,
LOAD_TSTMP
FROM {legacy}.TRAINING_PACKAGE_PROMO
WHERE DELETED_FLAG = 0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE = spark.sql(f"""SELECT
PACKAGE_PROMOTION_ID,
NAME,
DESCRIPTION,
DISCOUNT_AMOUNT,
START_DATE,
END_DATE,
PACKAGE_ID,
COUNTRY_ABBREVIATION
FROM {raw}.TRAINING_PACKAGE_PROMOTION_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_PACKAGE_PROMO, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_PACKAGE_PROMO_temp = SQ_Shortcut_to_TRAINING_PACKAGE_PROMO.toDF(*["SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___" + col for col in SQ_Shortcut_to_TRAINING_PACKAGE_PROMO.columns])
SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE_temp = SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE.toDF(*["SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE___" + col for col in SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE.columns])

JNR_TRAINING_PACKAGE_PROMO = SQ_Shortcut_to_TRAINING_PACKAGE_PROMO_temp.join(SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE_temp,[SQ_Shortcut_to_TRAINING_PACKAGE_PROMO_temp.SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___TRAINING_PACKAGE_PROMO_ID == SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE_temp.SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE___PACKAGE_PROMOTION_ID],'left_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE___PACKAGE_PROMOTION_ID as PACKAGE_PROMOTION_ID", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE___NAME as NAME", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE___DISCOUNT_AMOUNT as DISCOUNT_AMOUNT", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE___START_DATE as START_DATE", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE___END_DATE as END_DATE", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE___PACKAGE_ID as PACKAGE_ID", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMOTION_PRE___COUNTRY_ABBREVIATION as COUNTRY_ABBREVIATION", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___TRAINING_PACKAGE_PROMO_ID as lkp_TRAINING_PACKAGE_PROMO_ID", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___TRAINING_PACKAGE_PROMO_NAME as lkp_TRAINING_PACKAGE_PROMO_NAME", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___TRAINING_PACKAGE_PROMO_DESC as lkp_TRAINING_PACKAGE_PROMO_DESC", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___TRAINING_PACKAGE_ID as lkp_TRAINING_PACKAGE_ID", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___COUNTRY_CD as lkp_COUNTRY_CD", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___START_DT as lkp_START_DT", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___END_DT as lkp_END_DT", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___DISC_AMT as lkp_DISC_AMT", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_PROMO___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_PACKAGE_PROMO_temp = JNR_TRAINING_PACKAGE_PROMO.toDF(*["JNR_TRAINING_PACKAGE_PROMO___" + col for col in JNR_TRAINING_PACKAGE_PROMO.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_PACKAGE_PROMO_temp.selectExpr( \
	"JNR_TRAINING_PACKAGE_PROMO___PACKAGE_PROMOTION_ID as PACKAGE_PROMOTION_ID", \
	"JNR_TRAINING_PACKAGE_PROMO___NAME as NAME", \
	"JNR_TRAINING_PACKAGE_PROMO___DESCRIPTION as DESCRIPTION", \
	"JNR_TRAINING_PACKAGE_PROMO___DISCOUNT_AMOUNT as DISCOUNT_AMOUNT", \
	"JNR_TRAINING_PACKAGE_PROMO___START_DATE as START_DATE", \
	"JNR_TRAINING_PACKAGE_PROMO___END_DATE as END_DATE", \
	"JNR_TRAINING_PACKAGE_PROMO___PACKAGE_ID as PACKAGE_ID", \
	"JNR_TRAINING_PACKAGE_PROMO___COUNTRY_ABBREVIATION as COUNTRY_ABBREVIATION", \
	"JNR_TRAINING_PACKAGE_PROMO___lkp_TRAINING_PACKAGE_PROMO_ID as lkp_TRAINING_PACKAGE_PROMO_ID", \
	"JNR_TRAINING_PACKAGE_PROMO___lkp_TRAINING_PACKAGE_PROMO_NAME as lkp_TRAINING_PACKAGE_PROMO_NAME", \
	"JNR_TRAINING_PACKAGE_PROMO___lkp_TRAINING_PACKAGE_PROMO_DESC as lkp_TRAINING_PACKAGE_PROMO_DESC", \
	"JNR_TRAINING_PACKAGE_PROMO___lkp_TRAINING_PACKAGE_ID as lkp_TRAINING_PACKAGE_ID", \
	"JNR_TRAINING_PACKAGE_PROMO___lkp_COUNTRY_CD as lkp_COUNTRY_CD", \
	"JNR_TRAINING_PACKAGE_PROMO___lkp_START_DT as lkp_START_DT", \
	"JNR_TRAINING_PACKAGE_PROMO___lkp_END_DT as lkp_END_DT", \
	"JNR_TRAINING_PACKAGE_PROMO___lkp_DISC_AMT as lkp_DISC_AMT", \
	"JNR_TRAINING_PACKAGE_PROMO___lkp_LOAD_TSTMP as lkp_LOAD_TSTMP").filter("lkp_TRAINING_PACKAGE_PROMO_ID IS NOT NULL AND PACKAGE_PROMOTION_ID IS NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___lkp_TRAINING_PACKAGE_PROMO_ID as lkp_TRAINING_PACKAGE_PROMO_ID", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"1 as DELETE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___lkp_TRAINING_PACKAGE_PROMO_ID as lkp_TRAINING_PACKAGE_PROMO_ID", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___DELETE_FLAG as DELETE_FLAG") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_PACKAGE_PROMO11, type TARGET 
# COLUMN COUNT: 11

UPD_UPDATE.createOrReplaceTempView('UPD_UPDATE_TMP')
spark.sql(f"""
		MERGE INTO {legacy}.TRAINING_PACKAGE_PROMO target
        USING UPD_UPDATE_TMP source
        ON source.lkp_TRAINING_PACKAGE_PROMO_ID = target.TRAINING_PACKAGE_PROMO_ID
        WHEN MATCHED THEN UPDATE SET target.DELETED_FLAG = source.DELETE_FLAG , target.UPDATE_TSTMP = source.UPDATE_TSTMP
""")



# Shortcut_to_TRAINING_PACKAGE_PROMO11 = UPD_UPDATE.selectExpr( \
# 	"CAST(lkp_TRAINING_PACKAGE_PROMO_ID AS BIGINT) as TRAINING_PACKAGE_PROMO_ID", \
# 	"CAST(NULL AS STRING) as TRAINING_PACKAGE_PROMO_NAME", \
# 	"CAST(NULL AS STRING) as TRAINING_PACKAGE_PROMO_DESC", \
# 	"CAST(NULL AS BIGINT) as TRAINING_PACKAGE_ID", \
# 	"CAST(NULL AS STRING) as COUNTRY_CD", \
# 	"CAST(NULL AS DATE) as START_DT", \
# 	"CAST(NULL AS DATE) as END_DT", \
# 	"CAST(NULL AS DECIMAL(19,4)) as DISC_AMT", \
# 	"CAST(DELETE_FLAG AS TINYINT) as DELETED_FLAG", \
# 	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
# 	"CAST(NULL AS TIMESTAMP) as LOAD_TSTMP", \
# 	"pyspark_data_action as pyspark_data_action" \
# )
# Shortcut_to_TRAINING_PACKAGE_PROMO11.write.saveAsTable(f'{raw}.TRAINING_PACKAGE_PROMO', mode = 'overwrite')