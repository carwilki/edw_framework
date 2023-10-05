# Code converted on 2023-07-28 11:37:20
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
from Datalake.utils.pk.pk import DuplicateChecker

# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument("env", type=str, help="Env Variable")

# uncomment before checking in
args = parser.parse_args()
env = args.env

# remove before checking in
# env = 'dev'

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"


# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_ADD_ONS_PRE, type SOURCE
# COLUMN COUNT: 19

SQ_Shortcut_to_E_RES_ADD_ONS_PRE = spark.sql(
    f"""SELECT
ADD_ON_ID,
ADD_ON_NAME,
SKU_NBR,
ADD_ON_DESCRIPTION,
ADD_ON_CATEGORY_ID,
GROUP_ID,
SPECIES_ID,
ALLOW_MULTIPLE,
SCHEDULABLE,
BREED_QUALIFIER,
RATE_TYPE,
IMAGE_URL,
SORT_ORDER,
IS_DELETED,
CREATED_AT,
UPDATED_BY,
CREATED_BY,
UPDATE_AT,
LOAD_TSTMP
FROM {raw}.E_RES_ADD_ONS_PRE"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_E_RES_ADD_ONS_PRE, type EXPRESSION
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_E_RES_ADD_ONS_PRE_temp = SQ_Shortcut_to_E_RES_ADD_ONS_PRE.toDF(
    *[
        "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___" + col
        for col in SQ_Shortcut_to_E_RES_ADD_ONS_PRE.columns
    ]
)

EXP_E_RES_ADD_ONS_PRE = SQ_Shortcut_to_E_RES_ADD_ONS_PRE_temp.selectExpr(
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___sys_row_id as sys_row_id",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___ADD_ON_ID as ADD_ON_ID",
    "regexp_replace(SQ_Shortcut_to_E_RES_ADD_ONS_PRE___ADD_ON_NAME, '\\((\\\\r|\\\\n)\\)', '$1') as o_ADD_ON_NAME",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___SKU_NBR as SKU_NBR",
    "regexp_replace(SQ_Shortcut_to_E_RES_ADD_ONS_PRE___ADD_ON_DESCRIPTION, '\\((\\\\r|\\\\n)\\)', '$1') as o_ADD_ON_DESCRIPTION",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___ADD_ON_CATEGORY_ID as ADD_ON_CATEGORY_ID",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___GROUP_ID as GROUP_ID",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___SPECIES_ID as SPECIES_ID",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___ALLOW_MULTIPLE as ALLOW_MULTIPLE",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___SCHEDULABLE as SCHEDULABLE",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___BREED_QUALIFIER as BREED_QUALIFIER",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___RATE_TYPE as RATE_TYPE",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___IMAGE_URL as IMAGE_URL",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___SORT_ORDER as SORT_ORDER",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___IS_DELETED as IS_DELETED",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___CREATED_AT as CREATED_AT",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___UPDATED_BY as UPDATED_BY",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___CREATED_BY as CREATED_BY",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___UPDATE_AT as UPDATE_AT",
    "SQ_Shortcut_to_E_RES_ADD_ONS_PRE___LOAD_TSTMP as LOAD_TSTMP",
)


# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_ADD_ONS, type SOURCE
# COLUMN COUNT: 19

SQ_Shortcut_to_E_RES_ADD_ONS = spark.sql(
    f"""SELECT
E_RES_ADD_ON_ID,
ADD_ON_NAME,
SKU_NBR,
ADD_ON_DESC,
E_RES_ADD_ON_CATEGORY_ID,
E_RES_GROUP_ID,
PETM_PET_SPECIES_ID,
ALLOW_MULTIPLE_ADD_ONS_FLAG,
SCHEDULABLE_FLAG,
BREED_QUALIFIER_FLAG,
E_RES_ADD_ON_RATE_TYPE_ID,
IMAGE_URL,
SORT_ORDER_NBR,
DELETED_FLAG,
E_RES_CREATED_TSTMP,
E_RES_CREATED_BY,
E_RES_UPDATE_TSTMP,
E_RES_UPDATED_BY,
UPDATE_TSTMP
FROM {legacy}.E_RES_ADD_ONS"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_E_RES_ADD_ON, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 38

# for each involved DataFrame, append the dataframe name to each column
EXP_E_RES_ADD_ONS_PRE_temp = EXP_E_RES_ADD_ONS_PRE.toDF(
    *["EXP_E_RES_ADD_ONS_PRE___" + col for col in EXP_E_RES_ADD_ONS_PRE.columns]
)
SQ_Shortcut_to_E_RES_ADD_ONS_temp = SQ_Shortcut_to_E_RES_ADD_ONS.toDF(
    *[
        "SQ_Shortcut_to_E_RES_ADD_ONS___" + col
        for col in SQ_Shortcut_to_E_RES_ADD_ONS.columns
    ]
)

JNR_E_RES_ADD_ON = SQ_Shortcut_to_E_RES_ADD_ONS_temp.join(
    EXP_E_RES_ADD_ONS_PRE_temp,
    [
        SQ_Shortcut_to_E_RES_ADD_ONS_temp.SQ_Shortcut_to_E_RES_ADD_ONS___E_RES_ADD_ON_ID
        == EXP_E_RES_ADD_ONS_PRE_temp.EXP_E_RES_ADD_ONS_PRE___ADD_ON_ID
    ],
    "right_outer",
).selectExpr(
    "EXP_E_RES_ADD_ONS_PRE___ADD_ON_ID as ADD_ON_ID",
    "EXP_E_RES_ADD_ONS_PRE___o_ADD_ON_NAME as ADD_ON_NAME",
    "EXP_E_RES_ADD_ONS_PRE___SKU_NBR as SKU_NBR",
    "EXP_E_RES_ADD_ONS_PRE___o_ADD_ON_DESCRIPTION as ADD_ON_DESCRIPTION",
    "EXP_E_RES_ADD_ONS_PRE___ADD_ON_CATEGORY_ID as ADD_ON_CATEGORY_ID",
    "EXP_E_RES_ADD_ONS_PRE___GROUP_ID as GROUP_ID",
    "EXP_E_RES_ADD_ONS_PRE___SPECIES_ID as SPECIES_ID",
    "EXP_E_RES_ADD_ONS_PRE___ALLOW_MULTIPLE as ALLOW_MULTIPLE",
    "EXP_E_RES_ADD_ONS_PRE___SCHEDULABLE as SCHEDULABLE",
    "EXP_E_RES_ADD_ONS_PRE___BREED_QUALIFIER as BREED_QUALIFIER",
    "EXP_E_RES_ADD_ONS_PRE___RATE_TYPE as RATE_TYPE",
    "EXP_E_RES_ADD_ONS_PRE___IMAGE_URL as IMAGE_URL",
    "EXP_E_RES_ADD_ONS_PRE___SORT_ORDER as SORT_ORDER",
    "EXP_E_RES_ADD_ONS_PRE___IS_DELETED as IS_DELETED",
    "EXP_E_RES_ADD_ONS_PRE___CREATED_AT as CREATED_AT",
    "EXP_E_RES_ADD_ONS_PRE___UPDATED_BY as UPDATED_BY",
    "EXP_E_RES_ADD_ONS_PRE___CREATED_BY as CREATED_BY",
    "EXP_E_RES_ADD_ONS_PRE___UPDATE_AT as UPDATE_AT",
    "EXP_E_RES_ADD_ONS_PRE___LOAD_TSTMP as LOAD_TSTMP",
    "SQ_Shortcut_to_E_RES_ADD_ONS___E_RES_ADD_ON_ID as i_E_RES_ADD_ON_ID",
    "SQ_Shortcut_to_E_RES_ADD_ONS___ADD_ON_NAME as i_ADD_ON_NAME",
    "SQ_Shortcut_to_E_RES_ADD_ONS___SKU_NBR as i_SKU_NBR",
    "SQ_Shortcut_to_E_RES_ADD_ONS___ADD_ON_DESC as i_ADD_ON_DESC",
    "SQ_Shortcut_to_E_RES_ADD_ONS___E_RES_ADD_ON_CATEGORY_ID as i_E_RES_ADD_ON_CATEGORY_ID",
    "SQ_Shortcut_to_E_RES_ADD_ONS___E_RES_GROUP_ID as i_E_RES_GROUP_ID",
    "SQ_Shortcut_to_E_RES_ADD_ONS___PETM_PET_SPECIES_ID as i_PETM_PET_SPECIES_ID",
    "SQ_Shortcut_to_E_RES_ADD_ONS___ALLOW_MULTIPLE_ADD_ONS_FLAG as i_ALLOW_MULTIPLE_ADD_ONS_FLAG",
    "SQ_Shortcut_to_E_RES_ADD_ONS___SCHEDULABLE_FLAG as i_SCHEDULABLE_FLAG",
    "SQ_Shortcut_to_E_RES_ADD_ONS___BREED_QUALIFIER_FLAG as i_BREED_QUALIFIER_FLAG",
    "SQ_Shortcut_to_E_RES_ADD_ONS___E_RES_ADD_ON_RATE_TYPE_ID as i_E_RES_ADD_ON_RATE_TYPE_ID",
    "SQ_Shortcut_to_E_RES_ADD_ONS___IMAGE_URL as i_IMAGE_URL",
    "SQ_Shortcut_to_E_RES_ADD_ONS___SORT_ORDER_NBR as i_SORT_ORDER_NBR",
    "SQ_Shortcut_to_E_RES_ADD_ONS___DELETED_FLAG as i_DELETED_FLAG",
    "SQ_Shortcut_to_E_RES_ADD_ONS___E_RES_CREATED_TSTMP as i_E_RES_CREATED_TSTMP",
    "SQ_Shortcut_to_E_RES_ADD_ONS___E_RES_CREATED_BY as i_E_RES_CREATED_BY",
    "SQ_Shortcut_to_E_RES_ADD_ONS___E_RES_UPDATE_TSTMP as i_E_RES_UPDATE_TSTMP",
    "SQ_Shortcut_to_E_RES_ADD_ONS___E_RES_UPDATED_BY as i_E_RES_UPDATED_BY",
    "SQ_Shortcut_to_E_RES_ADD_ONS___UPDATE_TSTMP as i_LOAD_TSTMP",
)

# COMMAND ----------

# Processing node FIL_E_RES_ADD_ON, type FILTER
# COLUMN COUNT: 38

# for each involved DataFrame, append the dataframe name to each column
JNR_E_RES_ADD_ON_temp = JNR_E_RES_ADD_ON.toDF(
    *["JNR_E_RES_ADD_ON___" + col for col in JNR_E_RES_ADD_ON.columns]
)

FIL_E_RES_ADD_ON = (
    JNR_E_RES_ADD_ON_temp.selectExpr(
        "JNR_E_RES_ADD_ON___ADD_ON_ID as ADD_ON_ID",
        "JNR_E_RES_ADD_ON___ADD_ON_NAME as ADD_ON_NAME",
        "JNR_E_RES_ADD_ON___SKU_NBR as SKU_NBR",
        "JNR_E_RES_ADD_ON___ADD_ON_DESCRIPTION as ADD_ON_DESCRIPTION",
        "JNR_E_RES_ADD_ON___ADD_ON_CATEGORY_ID as ADD_ON_CATEGORY_ID",
        "JNR_E_RES_ADD_ON___GROUP_ID as GROUP_ID",
        "JNR_E_RES_ADD_ON___SPECIES_ID as SPECIES_ID",
        "JNR_E_RES_ADD_ON___ALLOW_MULTIPLE as ALLOW_MULTIPLE",
        "JNR_E_RES_ADD_ON___SCHEDULABLE as SCHEDULABLE",
        "JNR_E_RES_ADD_ON___BREED_QUALIFIER as BREED_QUALIFIER",
        "JNR_E_RES_ADD_ON___RATE_TYPE as RATE_TYPE",
        "JNR_E_RES_ADD_ON___IMAGE_URL as IMAGE_URL",
        "JNR_E_RES_ADD_ON___SORT_ORDER as SORT_ORDER",
        "JNR_E_RES_ADD_ON___IS_DELETED as IS_DELETED",
        "JNR_E_RES_ADD_ON___CREATED_AT as CREATED_AT",
        "JNR_E_RES_ADD_ON___UPDATED_BY as UPDATED_BY",
        "JNR_E_RES_ADD_ON___CREATED_BY as CREATED_BY",
        "JNR_E_RES_ADD_ON___UPDATE_AT as UPDATE_AT",
        "JNR_E_RES_ADD_ON___LOAD_TSTMP as LOAD_TSTMP",
        "JNR_E_RES_ADD_ON___i_E_RES_ADD_ON_ID as i_E_RES_ADD_ON_ID",
        "JNR_E_RES_ADD_ON___i_ADD_ON_NAME as i_ADD_ON_NAME",
        "JNR_E_RES_ADD_ON___i_SKU_NBR as i_SKU_NBR",
        "JNR_E_RES_ADD_ON___i_ADD_ON_DESC as i_ADD_ON_DESC",
        "JNR_E_RES_ADD_ON___i_E_RES_ADD_ON_CATEGORY_ID as i_E_RES_ADD_ON_CATEGORY_ID",
        "JNR_E_RES_ADD_ON___i_E_RES_GROUP_ID as i_E_RES_GROUP_ID",
        "JNR_E_RES_ADD_ON___i_PETM_PET_SPECIES_ID as i_PETM_PET_SPECIES_ID",
        "JNR_E_RES_ADD_ON___i_ALLOW_MULTIPLE_ADD_ONS_FLAG as i_ALLOW_MULTIPLE_ADD_ONS_FLAG",
        "JNR_E_RES_ADD_ON___i_SCHEDULABLE_FLAG as i_SCHEDULABLE_FLAG",
        "JNR_E_RES_ADD_ON___i_BREED_QUALIFIER_FLAG as i_BREED_QUALIFIER_FLAG",
        "JNR_E_RES_ADD_ON___i_E_RES_ADD_ON_RATE_TYPE_ID as i_E_RES_ADD_ON_RATE_TYPE_ID",
        "JNR_E_RES_ADD_ON___i_IMAGE_URL as i_IMAGE_URL",
        "JNR_E_RES_ADD_ON___i_SORT_ORDER_NBR as i_SORT_ORDER_NBR",
        "JNR_E_RES_ADD_ON___i_DELETED_FLAG as i_DELETED_FLAG",
        "JNR_E_RES_ADD_ON___i_E_RES_CREATED_TSTMP as i_E_RES_CREATED_TSTMP",
        "JNR_E_RES_ADD_ON___i_E_RES_CREATED_BY as i_E_RES_CREATED_BY",
        "JNR_E_RES_ADD_ON___i_E_RES_UPDATE_TSTMP as i_E_RES_UPDATE_TSTMP",
        "JNR_E_RES_ADD_ON___i_E_RES_UPDATED_BY as i_E_RES_UPDATED_BY",
        "JNR_E_RES_ADD_ON___i_LOAD_TSTMP as i_LOAD_TSTMP",
    )
    .filter(
        "i_E_RES_ADD_ON_ID IS NULL OR ( i_E_RES_ADD_ON_ID IS NOT NULL AND ( IF (ADD_ON_NAME IS NULL, ' ', ADD_ON_NAME) != IF (i_ADD_ON_NAME IS NULL, ' ', i_ADD_ON_NAME) OR IF (SKU_NBR IS NULL, ' ', SKU_NBR) != IF (i_SKU_NBR IS NULL, ' ', i_SKU_NBR) OR IF (ADD_ON_DESCRIPTION IS NULL, ' ', ADD_ON_DESCRIPTION) != IF (i_ADD_ON_DESC IS NULL, ' ', i_ADD_ON_DESC) OR IF (ADD_ON_CATEGORY_ID IS NULL, - 99999, ADD_ON_CATEGORY_ID) != IF (i_E_RES_ADD_ON_CATEGORY_ID IS NULL, - 99999, i_E_RES_ADD_ON_CATEGORY_ID) OR IF (GROUP_ID IS NULL, - 99999, GROUP_ID) != IF (i_E_RES_GROUP_ID IS NULL, - 99999, i_E_RES_GROUP_ID) OR IF (SPECIES_ID IS NULL, - 99999, SPECIES_ID) != IF (i_PETM_PET_SPECIES_ID IS NULL, - 99999, i_PETM_PET_SPECIES_ID) OR IF (ALLOW_MULTIPLE IS NULL, 0, ALLOW_MULTIPLE) != IF (i_ALLOW_MULTIPLE_ADD_ONS_FLAG IS NULL, 0, i_ALLOW_MULTIPLE_ADD_ONS_FLAG) OR IF (SCHEDULABLE IS NULL, 0, SCHEDULABLE) != IF (i_SCHEDULABLE_FLAG IS NULL, 0, i_SCHEDULABLE_FLAG) OR IF (BREED_QUALIFIER IS NULL, 0, BREED_QUALIFIER) != IF (i_BREED_QUALIFIER_FLAG IS NULL, 0, i_BREED_QUALIFIER_FLAG) OR IF (RATE_TYPE IS NULL, - 99999, RATE_TYPE) != IF (i_E_RES_ADD_ON_RATE_TYPE_ID IS NULL, - 99999, i_E_RES_ADD_ON_RATE_TYPE_ID) OR IF (IMAGE_URL IS NULL, ' ', IMAGE_URL) != IF (i_IMAGE_URL IS NULL, ' ', i_IMAGE_URL) OR IF (SORT_ORDER IS NULL, - 99999, SORT_ORDER) != IF (i_SORT_ORDER_NBR IS NULL, - 99999, i_SORT_ORDER_NBR) OR IF (IS_DELETED IS NULL, 0, IS_DELETED) != IF (i_DELETED_FLAG IS NULL, 0, i_DELETED_FLAG) OR IF (CREATED_AT IS NULL, date'1900-01-01', CREATED_AT) != IF (i_E_RES_CREATED_TSTMP IS NULL, date'1900-01-01', i_E_RES_CREATED_TSTMP) OR IF (CREATED_BY IS NULL, ' ', CREATED_BY) != IF (i_E_RES_CREATED_BY IS NULL, ' ', i_E_RES_CREATED_BY) OR IF (UPDATED_BY IS NULL, ' ', UPDATED_BY) != IF (i_E_RES_UPDATED_BY IS NULL, ' ', i_E_RES_UPDATED_BY) OR IF (UPDATE_AT IS NULL, date'1900-01-01', UPDATE_AT) != IF (i_E_RES_UPDATE_TSTMP IS NULL, date'1900-01-01', i_E_RES_UPDATE_TSTMP) ) )"
    )
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node EXP_E_RES_ADD_ON, type EXPRESSION
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
FIL_E_RES_ADD_ON_temp = FIL_E_RES_ADD_ON.toDF(
    *["FIL_E_RES_ADD_ON___" + col for col in FIL_E_RES_ADD_ON.columns]
)

EXP_E_RES_ADD_ON = FIL_E_RES_ADD_ON_temp.selectExpr(
    "FIL_E_RES_ADD_ON___sys_row_id as sys_row_id",
    "FIL_E_RES_ADD_ON___ADD_ON_ID as ADD_ON_ID",
    "FIL_E_RES_ADD_ON___ADD_ON_NAME as ADD_ON_NAME",
    "FIL_E_RES_ADD_ON___SKU_NBR as SKU_NBR",
    "FIL_E_RES_ADD_ON___ADD_ON_DESCRIPTION as ADD_ON_DESCRIPTION",
    "FIL_E_RES_ADD_ON___ADD_ON_CATEGORY_ID as ADD_ON_CATEGORY_ID",
    "FIL_E_RES_ADD_ON___GROUP_ID as GROUP_ID",
    "FIL_E_RES_ADD_ON___SPECIES_ID as SPECIES_ID",
    "FIL_E_RES_ADD_ON___ALLOW_MULTIPLE as ALLOW_MULTIPLE",
    "FIL_E_RES_ADD_ON___SCHEDULABLE as SCHEDULABLE",
    "FIL_E_RES_ADD_ON___BREED_QUALIFIER as BREED_QUALIFIER",
    "FIL_E_RES_ADD_ON___RATE_TYPE as RATE_TYPE",
    "FIL_E_RES_ADD_ON___IMAGE_URL as IMAGE_URL",
    "FIL_E_RES_ADD_ON___SORT_ORDER as SORT_ORDER",
    "FIL_E_RES_ADD_ON___IS_DELETED as IS_DELETED",
    "FIL_E_RES_ADD_ON___CREATED_AT as CREATED_AT",
    "FIL_E_RES_ADD_ON___UPDATED_BY as UPDATED_BY",
    "FIL_E_RES_ADD_ON___CREATED_BY as CREATED_BY",
    "FIL_E_RES_ADD_ON___UPDATE_AT as UPDATE_AT",
    "CURRENT_TIMESTAMP as UPDATE_TSTMP",
    "IF (FIL_E_RES_ADD_ON___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_E_RES_ADD_ON___i_LOAD_TSTMP) as LOAD_TSTMP",
    "IF (FIL_E_RES_ADD_ON___i_E_RES_ADD_ON_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR",
)

# COMMAND ----------

# Processing node UPD_E_RES_ADD_ON, type UPDATE_STRATEGY
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
EXP_E_RES_ADD_ON_temp = EXP_E_RES_ADD_ON.toDF(
    *["EXP_E_RES_ADD_ON___" + col for col in EXP_E_RES_ADD_ON.columns]
)

UPD_E_RES_ADD_ON = EXP_E_RES_ADD_ON_temp.selectExpr(
    "EXP_E_RES_ADD_ON___ADD_ON_ID as ADD_ON_ID",
    "EXP_E_RES_ADD_ON___ADD_ON_NAME as ADD_ON_NAME",
    "EXP_E_RES_ADD_ON___SKU_NBR as SKU_NBR",
    "EXP_E_RES_ADD_ON___ADD_ON_DESCRIPTION as ADD_ON_DESCRIPTION",
    "EXP_E_RES_ADD_ON___ADD_ON_CATEGORY_ID as ADD_ON_CATEGORY_ID",
    "EXP_E_RES_ADD_ON___GROUP_ID as GROUP_ID",
    "EXP_E_RES_ADD_ON___SPECIES_ID as SPECIES_ID",
    "EXP_E_RES_ADD_ON___ALLOW_MULTIPLE as ALLOW_MULTIPLE",
    "EXP_E_RES_ADD_ON___SCHEDULABLE as SCHEDULABLE",
    "EXP_E_RES_ADD_ON___BREED_QUALIFIER as BREED_QUALIFIER",
    "EXP_E_RES_ADD_ON___RATE_TYPE as RATE_TYPE",
    "EXP_E_RES_ADD_ON___IMAGE_URL as IMAGE_URL",
    "EXP_E_RES_ADD_ON___SORT_ORDER as SORT_ORDER",
    "EXP_E_RES_ADD_ON___IS_DELETED as IS_DELETED",
    "EXP_E_RES_ADD_ON___CREATED_AT as CREATED_AT",
    "EXP_E_RES_ADD_ON___UPDATED_BY as UPDATED_BY",
    "EXP_E_RES_ADD_ON___CREATED_BY as CREATED_BY",
    "EXP_E_RES_ADD_ON___UPDATE_AT as UPDATE_AT",
    "EXP_E_RES_ADD_ON___UPDATE_TSTMP as UPDATE_TSTMP",
    "EXP_E_RES_ADD_ON___LOAD_TSTMP as LOAD_TSTMP",
    "EXP_E_RES_ADD_ON___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR",
).withColumn(
    "pyspark_data_action",
    when(col("o_UPDATE_VALIDATOR") == (lit(1)), lit(0)).when(
        col("o_UPDATE_VALIDATOR") == (lit(2)), lit(1)
    ),
)

# COMMAND ----------

# Processing node Shortcut_to_E_RES_ADD_ONS1, type TARGET
# COLUMN COUNT: 20


Shortcut_to_E_RES_ADD_ONS1 = UPD_E_RES_ADD_ON.selectExpr(
    "CAST(ADD_ON_ID AS INT) as E_RES_ADD_ON_ID",
    "CAST(ADD_ON_NAME AS STRING) as ADD_ON_NAME",
    "CAST(SKU_NBR AS STRING) as SKU_NBR",
    "CAST(ADD_ON_DESCRIPTION AS STRING) as ADD_ON_DESC",
    "CAST(ADD_ON_CATEGORY_ID AS INT) as E_RES_ADD_ON_CATEGORY_ID",
    "CAST(GROUP_ID AS INT) as E_RES_GROUP_ID",
    "CAST(SPECIES_ID AS INT) as PETM_PET_SPECIES_ID",
    "CAST(ALLOW_MULTIPLE AS TINYINT) as ALLOW_MULTIPLE_ADD_ONS_FLAG",
    "CAST(SCHEDULABLE AS TINYINT) as SCHEDULABLE_FLAG",
    "CAST(BREED_QUALIFIER AS TINYINT) as BREED_QUALIFIER_FLAG",
    "CAST(RATE_TYPE AS SMALLINT) as E_RES_ADD_ON_RATE_TYPE_ID",
    "CAST(IMAGE_URL AS STRING) as IMAGE_URL",
    "CAST(SORT_ORDER AS BIGINT) as SORT_ORDER_NBR",
    "CAST(IS_DELETED AS TINYINT) as DELETED_FLAG",
    "CAST(CREATED_AT AS TIMESTAMP) as E_RES_CREATED_TSTMP",
    "CAST(CREATED_BY AS STRING) as E_RES_CREATED_BY",
    "CAST(UPDATE_AT AS TIMESTAMP) as E_RES_UPDATE_TSTMP",
    "CAST(UPDATED_BY AS STRING) as E_RES_UPDATED_BY",
    "CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
    "pyspark_data_action as pyspark_data_action",
)

try:
    primary_key = """source.E_RES_ADD_ON_ID = target.E_RES_ADD_ON_ID"""
    refined_perf_table = f"{legacy}.E_RES_ADD_ONS"
    DuplicateChecker.check_for_duplicate_primary_keys(Shortcut_to_E_RES_ADD_ONS1, ['E_RES_ADD_ON_ID'])
    executeMerge(Shortcut_to_E_RES_ADD_ONS1, refined_perf_table, primary_key)
    logger.info(f"Merge with {refined_perf_table} completed]")
    logPrevRunDt(
        "E_RES_ADD_ONS", "E_RES_ADD_ONS", "Completed", "N/A", f"{raw}.log_run_details"
    )
except Exception as e:
    logPrevRunDt(
        "E_RES_ADD_ONS",
        "E_RES_ADD_ONS",
        "Failed",
        str(e),
        f"{raw}.log_run_details",
    )
    raise e


# COMMAND ----------
