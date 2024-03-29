# Code converted on 2023-07-28 11:37:12
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
cust_sensitive = getEnvPrefix(env) + "cust_sensitive"


# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_PETS_PRE, type SOURCE
# COLUMN COUNT: 21

SQ_Shortcut_to_E_RES_PETS_PRE = spark.sql(
    f"""SELECT
PET_ID,
EXTERNAL_ID,
REQUEST_ID,
NAME,
SPECIES_ID,
BREED_ID,
COLOR_ID,
GENDER,
MIXED,
FIXED,
DATE_OF_BIRTH,
WEIGHT,
ROOM_SHARED,
INSTRUCTIONS,
ROOM_TYPE,
ROOM_UNIT_PRICE,
ROOM_TOTAL,
VET_NAME,
VET_PHONE,
PET_TOTAL,
BOOKING_REFERENCE
FROM {cust_sensitive}.raw_e_res_pets_pre"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_E_RES_PETS_PRE, type EXPRESSION
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_E_RES_PETS_PRE_temp = SQ_Shortcut_to_E_RES_PETS_PRE.toDF(
    *[
        "SQ_Shortcut_to_E_RES_PETS_PRE___" + col
        for col in SQ_Shortcut_to_E_RES_PETS_PRE.columns
    ]
)

EXP_E_RES_PETS_PRE = SQ_Shortcut_to_E_RES_PETS_PRE_temp.selectExpr(
    "SQ_Shortcut_to_E_RES_PETS_PRE___sys_row_id as sys_row_id",
    "SQ_Shortcut_to_E_RES_PETS_PRE___PET_ID as PET_ID",
    "SQ_Shortcut_to_E_RES_PETS_PRE___EXTERNAL_ID as EXTERNAL_ID",
    "SQ_Shortcut_to_E_RES_PETS_PRE___REQUEST_ID as REQUEST_ID",
    "SQ_Shortcut_to_E_RES_PETS_PRE___NAME as NAME",
    "SQ_Shortcut_to_E_RES_PETS_PRE___SPECIES_ID as SPECIES_ID",
    "SQ_Shortcut_to_E_RES_PETS_PRE___BREED_ID as BREED_ID",
    "SQ_Shortcut_to_E_RES_PETS_PRE___COLOR_ID as COLOR_ID",
    "SQ_Shortcut_to_E_RES_PETS_PRE___GENDER as GENDER",
    "SQ_Shortcut_to_E_RES_PETS_PRE___MIXED as MIXED",
    "SQ_Shortcut_to_E_RES_PETS_PRE___FIXED as FIXED",
    "SQ_Shortcut_to_E_RES_PETS_PRE___DATE_OF_BIRTH as DATE_OF_BIRTH",
    "SQ_Shortcut_to_E_RES_PETS_PRE___WEIGHT as WEIGHT",
    "SQ_Shortcut_to_E_RES_PETS_PRE___ROOM_SHARED as ROOM_SHARED",
    "regexp_replace(SQ_Shortcut_to_E_RES_PETS_PRE___INSTRUCTIONS, '\\((\\\\r|\\\\n)\\)', '$1') as o_INSTRUCTIONS",
    "SQ_Shortcut_to_E_RES_PETS_PRE___ROOM_TYPE as ROOM_TYPE",
    "SQ_Shortcut_to_E_RES_PETS_PRE___ROOM_UNIT_PRICE as ROOM_UNIT_PRICE",
    "SQ_Shortcut_to_E_RES_PETS_PRE___ROOM_TOTAL as ROOM_TOTAL",
    "regexp_replace(SQ_Shortcut_to_E_RES_PETS_PRE___VET_NAME, '\\((\\\\r|\\\\n)\\)', '$1') as o_VET_NAME",
    "SQ_Shortcut_to_E_RES_PETS_PRE___VET_PHONE as VET_PHONE",
    "SQ_Shortcut_to_E_RES_PETS_PRE___PET_TOTAL as PET_TOTAL",
    "SQ_Shortcut_to_E_RES_PETS_PRE___BOOKING_REFERENCE as BOOKING_REFERENCE",
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_PETS, type SOURCE
# COLUMN COUNT: 23

SQ_Shortcut_to_E_RES_PETS = spark.sql(
    f"""SELECT
E_RES_PET_ID,
E_RES_PET_EXT_ID,
E_RES_REQUEST_ID,
PET_NAME,
PETM_PET_SPECIES_ID,
PETM_PET_BREED_ID,
PETM_PET_COLOR_ID,
PET_GENDER_CD,
MIXED_BREED_FLAG,
FIXED_FLAG,
PET_BIRTH_DT,
PET_WEIGHT,
ROOM_SHARED_FLAG,
ROOM_TYPE,
ROOM_UNIT_PRICE,
ROOM_TOTAL_AMT,
INSTRUCTIONS,
VET_NAME,
VET_PHONE,
PET_TOTAL_AMT,
BOOKING_REFERENCE,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {cust_sensitive}.legacy_e_res_pets"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_E_RES_PETS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 44

# for each involved DataFrame, append the dataframe name to each column
EXP_E_RES_PETS_PRE_temp = EXP_E_RES_PETS_PRE.toDF(
    *["EXP_E_RES_PETS_PRE___" + col for col in EXP_E_RES_PETS_PRE.columns]
)
SQ_Shortcut_to_E_RES_PETS_temp = SQ_Shortcut_to_E_RES_PETS.toDF(
    *["SQ_Shortcut_to_E_RES_PETS___" + col for col in SQ_Shortcut_to_E_RES_PETS.columns]
)

JNR_E_RES_PETS = SQ_Shortcut_to_E_RES_PETS_temp.join(
    EXP_E_RES_PETS_PRE_temp,
    [
        SQ_Shortcut_to_E_RES_PETS_temp.SQ_Shortcut_to_E_RES_PETS___E_RES_PET_ID
        == EXP_E_RES_PETS_PRE_temp.EXP_E_RES_PETS_PRE___PET_ID
    ],
    "right_outer",
).selectExpr(
    "EXP_E_RES_PETS_PRE___PET_ID as PET_ID",
    "EXP_E_RES_PETS_PRE___EXTERNAL_ID as EXTERNAL_ID",
    "EXP_E_RES_PETS_PRE___REQUEST_ID as REQUEST_ID",
    "EXP_E_RES_PETS_PRE___NAME as NAME",
    "EXP_E_RES_PETS_PRE___SPECIES_ID as SPECIES_ID",
    "EXP_E_RES_PETS_PRE___BREED_ID as BREED_ID",
    "EXP_E_RES_PETS_PRE___COLOR_ID as COLOR_ID",
    "EXP_E_RES_PETS_PRE___GENDER as GENDER",
    "EXP_E_RES_PETS_PRE___MIXED as MIXED",
    "EXP_E_RES_PETS_PRE___FIXED as FIXED",
    "EXP_E_RES_PETS_PRE___DATE_OF_BIRTH as DATE_OF_BIRTH",
    "EXP_E_RES_PETS_PRE___WEIGHT as WEIGHT",
    "EXP_E_RES_PETS_PRE___ROOM_SHARED as ROOM_SHARED",
    "EXP_E_RES_PETS_PRE___o_INSTRUCTIONS as INSTRUCTIONS",
    "EXP_E_RES_PETS_PRE___ROOM_TYPE as ROOM_TYPE",
    "EXP_E_RES_PETS_PRE___ROOM_UNIT_PRICE as ROOM_UNIT_PRICE",
    "EXP_E_RES_PETS_PRE___ROOM_TOTAL as ROOM_TOTAL",
    "EXP_E_RES_PETS_PRE___o_VET_NAME as VET_NAME",
    "EXP_E_RES_PETS_PRE___VET_PHONE as VET_PHONE",
    "EXP_E_RES_PETS_PRE___PET_TOTAL as PET_TOTAL",
    "EXP_E_RES_PETS_PRE___BOOKING_REFERENCE as BOOKING_REFERENCE",
    "SQ_Shortcut_to_E_RES_PETS___E_RES_PET_ID as i_E_RES_PET_ID",
    "SQ_Shortcut_to_E_RES_PETS___E_RES_PET_EXT_ID as i_E_RES_PET_EXT_ID",
    "SQ_Shortcut_to_E_RES_PETS___E_RES_REQUEST_ID as i_E_RES_REQUEST_ID",
    "SQ_Shortcut_to_E_RES_PETS___PET_NAME as i_PET_NAME",
    "SQ_Shortcut_to_E_RES_PETS___PETM_PET_SPECIES_ID as i_PETM_PET_SPECIES_ID",
    "SQ_Shortcut_to_E_RES_PETS___PETM_PET_BREED_ID as i_PETM_PET_BREED_ID",
    "SQ_Shortcut_to_E_RES_PETS___PETM_PET_COLOR_ID as i_PETM_PET_COLOR_ID",
    "SQ_Shortcut_to_E_RES_PETS___PET_GENDER_CD as i_PET_GENDER_CD",
    "SQ_Shortcut_to_E_RES_PETS___MIXED_BREED_FLAG as i_MIXED_BREED_FLAG",
    "SQ_Shortcut_to_E_RES_PETS___FIXED_FLAG as i_FIXED_FLAG",
    "SQ_Shortcut_to_E_RES_PETS___PET_BIRTH_DT as i_PET_BIRTH_DT",
    "SQ_Shortcut_to_E_RES_PETS___PET_WEIGHT as i_PET_WEIGHT",
    "SQ_Shortcut_to_E_RES_PETS___ROOM_SHARED_FLAG as i_ROOM_SHARED_FLAG",
    "SQ_Shortcut_to_E_RES_PETS___ROOM_TYPE as i_ROOM_TYPE1",
    "SQ_Shortcut_to_E_RES_PETS___ROOM_UNIT_PRICE as i_ROOM_UNIT_PRICE1",
    "SQ_Shortcut_to_E_RES_PETS___ROOM_TOTAL_AMT as i_ROOM_TOTAL_AMT",
    "SQ_Shortcut_to_E_RES_PETS___INSTRUCTIONS as i_INSTRUCTIONS",
    "SQ_Shortcut_to_E_RES_PETS___VET_NAME as i_VET_NAME",
    "SQ_Shortcut_to_E_RES_PETS___VET_PHONE as i_VET_PHONE",
    "SQ_Shortcut_to_E_RES_PETS___PET_TOTAL_AMT as i_PET_TOTAL_AMT",
    "SQ_Shortcut_to_E_RES_PETS___BOOKING_REFERENCE as i_BOOKING_REFERENCE",
    "SQ_Shortcut_to_E_RES_PETS___UPDATE_TSTMP as i_UPDATE_TSTMP",
    "SQ_Shortcut_to_E_RES_PETS___LOAD_TSTMP as i_LOAD_TSTMP",
)

# COMMAND ----------

# Processing node FIL_E_RES_PETS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 45

# for each involved DataFrame, append the dataframe name to each column
JNR_E_RES_PETS_temp = JNR_E_RES_PETS.toDF(
    *["JNR_E_RES_PETS___" + col for col in JNR_E_RES_PETS.columns]
)

FIL_E_RES_PETS = (
    JNR_E_RES_PETS_temp.selectExpr(
        "JNR_E_RES_PETS___PET_ID as PET_ID",
        "JNR_E_RES_PETS___EXTERNAL_ID as EXTERNAL_ID",
        "JNR_E_RES_PETS___REQUEST_ID as REQUEST_ID",
        "JNR_E_RES_PETS___NAME as NAME",
        "JNR_E_RES_PETS___SPECIES_ID as SPECIES_ID",
        "JNR_E_RES_PETS___BREED_ID as BREED_ID",
        "JNR_E_RES_PETS___COLOR_ID as COLOR_ID",
        "JNR_E_RES_PETS___GENDER as GENDER",
        "JNR_E_RES_PETS___MIXED as MIXED",
        "JNR_E_RES_PETS___FIXED as FIXED",
        "JNR_E_RES_PETS___DATE_OF_BIRTH as DATE_OF_BIRTH",
        "JNR_E_RES_PETS___WEIGHT as WEIGHT",
        "JNR_E_RES_PETS___ROOM_SHARED as ROOM_SHARED",
        "JNR_E_RES_PETS___INSTRUCTIONS as INSTRUCTIONS",
        "JNR_E_RES_PETS___ROOM_TYPE as ROOM_TYPE",
        "JNR_E_RES_PETS___ROOM_UNIT_PRICE as ROOM_UNIT_PRICE",
        "JNR_E_RES_PETS___ROOM_TOTAL as ROOM_TOTAL",
        "JNR_E_RES_PETS___VET_NAME as VET_NAME",
        "JNR_E_RES_PETS___VET_PHONE as VET_PHONE",
        "JNR_E_RES_PETS___PET_TOTAL as PET_TOTAL",
        "JNR_E_RES_PETS___BOOKING_REFERENCE as BOOKING_REFERENCE",
        "JNR_E_RES_PETS___i_E_RES_PET_ID as i_E_RES_PET_ID",
        "JNR_E_RES_PETS___i_E_RES_PET_EXT_ID as i_E_RES_PET_EXT_ID",
        "JNR_E_RES_PETS___i_E_RES_REQUEST_ID as i_E_RES_REQUEST_ID",
        "JNR_E_RES_PETS___i_PET_NAME as i_PET_NAME",
        "JNR_E_RES_PETS___i_PETM_PET_SPECIES_ID as i_PETM_PET_SPECIES_ID",
        "JNR_E_RES_PETS___i_PETM_PET_BREED_ID as i_PETM_PET_BREED_ID",
        "JNR_E_RES_PETS___i_PETM_PET_COLOR_ID as i_PETM_PET_COLOR_ID",
        "JNR_E_RES_PETS___i_PET_GENDER_CD as i_PET_GENDER_CD",
        "JNR_E_RES_PETS___i_MIXED_BREED_FLAG as i_MIXED_BREED_FLAG",
        "JNR_E_RES_PETS___i_FIXED_FLAG as i_FIXED_FLAG",
        "JNR_E_RES_PETS___i_PET_BIRTH_DT as i_PET_BIRTH_DT",
        "JNR_E_RES_PETS___i_PET_WEIGHT as i_PET_WEIGHT",
        "JNR_E_RES_PETS___i_ROOM_SHARED_FLAG as i_ROOM_SHARED_FLAG",
        "JNR_E_RES_PETS___i_ROOM_TYPE1 as i_ROOM_TYPE",
        "JNR_E_RES_PETS___i_ROOM_UNIT_PRICE1 as i_ROOM_UNIT_PRICE",
        "JNR_E_RES_PETS___i_ROOM_TOTAL_AMT as i_ROOM_TOTAL_AMT",
        "JNR_E_RES_PETS___i_INSTRUCTIONS as i_INSTRUCTIONS",
        "JNR_E_RES_PETS___i_VET_NAME as i_VET_NAME",
        "JNR_E_RES_PETS___i_VET_PHONE as i_VET_PHONE",
        "JNR_E_RES_PETS___i_PET_TOTAL_AMT as i_PET_TOTAL_AMT",
        "JNR_E_RES_PETS___i_BOOKING_REFERENCE as i_BOOKING_REFERENCE",
        "JNR_E_RES_PETS___i_UPDATE_TSTMP as i_UPDATE_TSTMP",
        "JNR_E_RES_PETS___i_LOAD_TSTMP as i_LOAD_TSTMP",
    )
    .withColumn("LOAD_TSTMP", lit(None))
    .filter(
        "i_E_RES_PET_ID IS NULL OR ( i_E_RES_PET_ID IS NOT NULL AND ( IF(EXTERNAL_ID IS NULL, '', EXTERNAL_ID) != IF(i_E_RES_PET_EXT_ID IS NULL, '', i_E_RES_PET_EXT_ID) OR IF(REQUEST_ID IS NULL, - 99999, REQUEST_ID) != IF(i_E_RES_REQUEST_ID IS NULL, - 99999, i_E_RES_REQUEST_ID) OR IF(NAME IS NULL, '', NAME) != IF(i_PET_NAME IS NULL, '', i_PET_NAME) OR IF(SPECIES_ID IS NULL, - 99999, SPECIES_ID) != IF(i_PETM_PET_SPECIES_ID IS NULL, - 99999, i_PETM_PET_SPECIES_ID) OR IF(BREED_ID IS NULL, - 99999, BREED_ID) != IF(i_PETM_PET_BREED_ID IS NULL, - 99999, i_PETM_PET_BREED_ID) OR IF(COLOR_ID IS NULL, - 99999, COLOR_ID) != IF(i_PETM_PET_COLOR_ID IS NULL, - 99999, i_PETM_PET_COLOR_ID) OR IF(GENDER IS NULL, '', GENDER) != IF(i_PET_GENDER_CD IS NULL, '', i_PET_GENDER_CD) OR IF(MIXED IS NULL, 0, MIXED) != IF(i_MIXED_BREED_FLAG IS NULL, 0, i_MIXED_BREED_FLAG) OR IF(FIXED IS NULL, 0, FIXED) != IF(i_FIXED_FLAG IS NULL, 0, i_FIXED_FLAG) OR IF(DATE_OF_BIRTH IS NULL, date'1900-01-01', DATE_OF_BIRTH) != IF(i_PET_BIRTH_DT IS NULL, date'1900-01-01', i_PET_BIRTH_DT) OR IF(WEIGHT IS NULL, - 99999, WEIGHT) != IF(i_PET_WEIGHT IS NULL, - 99999, i_PET_WEIGHT) OR IF(ROOM_SHARED IS NULL, 0, ROOM_SHARED) != IF(i_ROOM_SHARED_FLAG IS NULL, 0, i_ROOM_SHARED_FLAG) OR IF(INSTRUCTIONS IS NULL, '', INSTRUCTIONS) != IF(i_INSTRUCTIONS IS NULL, '', i_INSTRUCTIONS) OR IF(ROOM_TYPE IS NULL, '', ROOM_TYPE) != IF(i_ROOM_TYPE IS NULL, '', i_ROOM_TYPE) OR IF(ROOM_UNIT_PRICE IS NULL, - 99999, ROOM_UNIT_PRICE) != IF(i_ROOM_UNIT_PRICE IS NULL, - 99999, i_ROOM_UNIT_PRICE) OR IF(ROOM_TOTAL IS NULL, - 99999, ROOM_TOTAL) != IF(i_ROOM_TOTAL_AMT IS NULL, - 99999, i_ROOM_TOTAL_AMT) OR IF(VET_NAME IS NULL, '', VET_NAME) != IF(i_VET_NAME IS NULL, '', i_VET_NAME) OR IF(VET_PHONE IS NULL, '', VET_PHONE) != IF(i_VET_PHONE IS NULL, '', i_VET_PHONE) OR IF(PET_TOTAL IS NULL, - 99999, PET_TOTAL) != IF(i_PET_TOTAL_AMT IS NULL, - 99999, i_PET_TOTAL_AMT) OR IF(BOOKING_REFERENCE IS NULL, '', BOOKING_REFERENCE) != IF(i_BOOKING_REFERENCE IS NULL, '', i_BOOKING_REFERENCE) ) )"
    )
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node EXP_E_RES_PETS, type EXPRESSION
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
FIL_E_RES_PETS_temp = FIL_E_RES_PETS.toDF(
    *["FIL_E_RES_PETS___" + col for col in FIL_E_RES_PETS.columns]
)

EXP_E_RES_PETS = FIL_E_RES_PETS_temp.selectExpr(
    "FIL_E_RES_PETS___sys_row_id as sys_row_id",
    "FIL_E_RES_PETS___PET_ID as PET_ID",
    "FIL_E_RES_PETS___EXTERNAL_ID as EXTERNAL_ID",
    "FIL_E_RES_PETS___REQUEST_ID as REQUEST_ID",
    "FIL_E_RES_PETS___NAME as NAME",
    "FIL_E_RES_PETS___SPECIES_ID as SPECIES_ID",
    "FIL_E_RES_PETS___BREED_ID as BREED_ID",
    "FIL_E_RES_PETS___COLOR_ID as COLOR_ID",
    "FIL_E_RES_PETS___GENDER as GENDER",
    "FIL_E_RES_PETS___MIXED as MIXED",
    "FIL_E_RES_PETS___FIXED as FIXED",
    "FIL_E_RES_PETS___DATE_OF_BIRTH as DATE_OF_BIRTH",
    "FIL_E_RES_PETS___WEIGHT as WEIGHT",
    "FIL_E_RES_PETS___ROOM_SHARED as ROOM_SHARED",
    "FIL_E_RES_PETS___INSTRUCTIONS as INSTRUCTIONS",
    "FIL_E_RES_PETS___ROOM_TYPE as ROOM_TYPE",
    "FIL_E_RES_PETS___ROOM_UNIT_PRICE as ROOM_UNIT_PRICE",
    "FIL_E_RES_PETS___ROOM_TOTAL as ROOM_TOTAL",
    "FIL_E_RES_PETS___VET_NAME as VET_NAME",
    "FIL_E_RES_PETS___VET_PHONE as VET_PHONE",
    "FIL_E_RES_PETS___PET_TOTAL as PET_TOTAL",
    "FIL_E_RES_PETS___BOOKING_REFERENCE as BOOKING_REFERENCE",
    "CURRENT_TIMESTAMP as UPDATE_TSTMP",
    "IF(FIL_E_RES_PETS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_E_RES_PETS___i_LOAD_TSTMP) as LOAD_TSTMP",
    "IF(FIL_E_RES_PETS___i_E_RES_PET_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR",
    "FIL_E_RES_PETS___i_E_RES_PET_ID as i_E_RES_PET_ID",
    "FIL_E_RES_PETS___i_LOAD_TSTMP as i_LOAD_TSTMP",
)

# COMMAND ----------

# Processing node UPD_E_RES_PETS, type UPDATE_STRATEGY
# COLUMN COUNT: 24

# for each involved DataFrame, append the dataframe name to each column
EXP_E_RES_PETS_temp = EXP_E_RES_PETS.toDF(
    *["EXP_E_RES_PETS___" + col for col in EXP_E_RES_PETS.columns]
)

UPD_E_RES_PETS = EXP_E_RES_PETS_temp.selectExpr(
    "EXP_E_RES_PETS___PET_ID as PET_ID",
    "EXP_E_RES_PETS___EXTERNAL_ID as EXTERNAL_ID",
    "EXP_E_RES_PETS___REQUEST_ID as REQUEST_ID",
    "EXP_E_RES_PETS___NAME as NAME",
    "EXP_E_RES_PETS___SPECIES_ID as SPECIES_ID",
    "EXP_E_RES_PETS___BREED_ID as BREED_ID",
    "EXP_E_RES_PETS___COLOR_ID as COLOR_ID",
    "EXP_E_RES_PETS___GENDER as GENDER",
    "EXP_E_RES_PETS___MIXED as MIXED",
    "EXP_E_RES_PETS___FIXED as FIXED",
    "EXP_E_RES_PETS___DATE_OF_BIRTH as DATE_OF_BIRTH",
    "EXP_E_RES_PETS___WEIGHT as WEIGHT",
    "EXP_E_RES_PETS___ROOM_SHARED as ROOM_SHARED",
    "EXP_E_RES_PETS___INSTRUCTIONS as INSTRUCTIONS",
    "EXP_E_RES_PETS___ROOM_TYPE as ROOM_TYPE",
    "EXP_E_RES_PETS___ROOM_UNIT_PRICE as ROOM_UNIT_PRICE",
    "EXP_E_RES_PETS___ROOM_TOTAL as ROOM_TOTAL",
    "EXP_E_RES_PETS___VET_NAME as VET_NAME",
    "EXP_E_RES_PETS___VET_PHONE as VET_PHONE",
    "EXP_E_RES_PETS___PET_TOTAL as PET_TOTAL",
    "EXP_E_RES_PETS___BOOKING_REFERENCE as BOOKING_REFERENCE",
    "EXP_E_RES_PETS___UPDATE_TSTMP as UPDATE_TSTMP",
    "EXP_E_RES_PETS___LOAD_TSTMP as LOAD_TSTMP",
    "EXP_E_RES_PETS___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR",
).withColumn(
    "pyspark_data_action",
    when(col("o_UPDATE_VALIDATOR") == (lit(1)), lit(0)).when(
        col("o_UPDATE_VALIDATOR") == (lit(2)), lit(1)
    ),
)

# COMMAND ----------

# Processing node Shortcut_to_E_RES_PETS_1, type TARGET
# COLUMN COUNT: 23


Shortcut_to_E_RES_PETS_1 = UPD_E_RES_PETS.selectExpr(
    "CAST(PET_ID AS INT) as E_RES_PET_ID",
    "CAST(EXTERNAL_ID AS STRING) as E_RES_PET_EXT_ID",
    "CAST(REQUEST_ID AS INT) as E_RES_REQUEST_ID",
    "CAST(NAME AS STRING) as PET_NAME",
    "CAST(SPECIES_ID AS INT) as PETM_PET_SPECIES_ID",
    "CAST(BREED_ID AS INT) as PETM_PET_BREED_ID",
    "CAST(COLOR_ID AS INT) as PETM_PET_COLOR_ID",
    "CAST(GENDER AS STRING) as PET_GENDER_CD",
    "CAST(MIXED AS TINYINT) as MIXED_BREED_FLAG",
    "CAST(FIXED AS TINYINT) as FIXED_FLAG",
    "CAST(DATE_OF_BIRTH AS DATE) as PET_BIRTH_DT",
    "CAST(WEIGHT AS DECIMAL(15,2)) as PET_WEIGHT",
    "CAST(ROOM_SHARED AS TINYINT) as ROOM_SHARED_FLAG",
    "CAST(ROOM_TYPE AS STRING) as ROOM_TYPE",
    "CAST(ROOM_UNIT_PRICE AS DECIMAL(15,2)) as ROOM_UNIT_PRICE",
    "CAST(ROOM_TOTAL AS DECIMAL(15,2)) as ROOM_TOTAL_AMT",
    "CAST(INSTRUCTIONS AS STRING) as INSTRUCTIONS",
    "CAST(VET_NAME AS STRING) as VET_NAME",
    "CAST(VET_PHONE AS STRING) as VET_PHONE",
    "CAST(PET_TOTAL AS DECIMAL(15,2)) as PET_TOTAL_AMT",
    "CAST(BOOKING_REFERENCE AS STRING) as BOOKING_REFERENCE",
    "CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
    "pyspark_data_action as pyspark_data_action",
)

try:
    primary_key = """source.E_RES_PET_ID = target.E_RES_PET_ID"""
    refined_perf_table = f"{cust_sensitive}.legacy_e_res_pets"
    DuplicateChecker.check_for_duplicate_primary_keys(
        Shortcut_to_E_RES_PETS_1, ["E_RES_PET_ID"]
    )
    executeMerge(Shortcut_to_E_RES_PETS_1, refined_perf_table, primary_key)
    logger.info(f"Merge with {refined_perf_table} completed]")
    logPrevRunDt(
        "E_RES_PETS", "E_RES_PETS", "Completed", "N/A", f"{raw}.log_run_details"
    )
except Exception as e:
    logPrevRunDt(
        "E_RES_PETS",
        "E_RES_PETS",
        "Failed",
        str(e),
        f"{raw}.log_run_details",
    )
    raise e
