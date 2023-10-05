# Code converted on 2023-07-28 11:37:07
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

# Processing node SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS, type SOURCE
# COLUMN COUNT: 12

SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS = spark.sql(
    f"""SELECT
E_RES_SELECTED_MEDICATION_ID,
E_RES_PET_ID,
MEDICATION_NAME,
MEDICATION_DOSE,
MEDICATION_TIME_OF_DAY,
MEDICATION_FREQ,
AILMENTS,
MEDICATION_INSTRUCTIONS,
UNIT_PRICE_AMT,
TOTAL_PRICE_AMT,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {legacy}.E_RES_SELECTED_MEDICATIONS"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE, type SOURCE
# COLUMN COUNT: 11

SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE = spark.sql(
    f"""SELECT
SELECTED_MEDICATION_ID,
PET_ID,
NAME,
DOSE,
TIME_OF_DAY,
AILMENTS,
INSTRUCTIONS,
FREQUENCY,
UNIT_PRICE,
TOTAL_PRICE,
LOAD_TSTMP
FROM {raw}.E_RES_SELECTED_MEDICATIONS_PRE"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_E_RES_SELECTED_MEDICATIONS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_temp = (
    SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS.toDF(
        *[
            "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___" + col
            for col in SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS.columns
        ]
    )
)
SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE_temp = (
    SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE.toDF(
        *[
            "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___" + col
            for col in SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE.columns
        ]
    )
)

JNR_E_RES_SELECTED_MEDICATIONS = SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_temp.join(
    SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE_temp,
    [
        SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_temp.SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___E_RES_SELECTED_MEDICATION_ID
        == SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE_temp.SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___SELECTED_MEDICATION_ID
    ],
    "right_outer",
).selectExpr(
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___SELECTED_MEDICATION_ID as SELECTED_MEDICATION_ID",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___PET_ID as PET_ID",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___NAME as NAME",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___DOSE as DOSE",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___TIME_OF_DAY as TIME_OF_DAY",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___AILMENTS as AILMENTS",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___INSTRUCTIONS as INSTRUCTIONS",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___FREQUENCY as FREQUENCY",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___UNIT_PRICE as UNIT_PRICE",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___TOTAL_PRICE as TOTAL_PRICE",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS_PRE___LOAD_TSTMP as LOAD_TSTMP",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___E_RES_SELECTED_MEDICATION_ID as i_E_RES_SELECTED_MEDICATION_ID",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___E_RES_PET_ID as i_E_RES_PET_ID",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___MEDICATION_NAME as i_MEDICATION_NAME",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___MEDICATION_DOSE as i_MEDICATION_DOSE",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___MEDICATION_TIME_OF_DAY as i_MEDICATION_TIME_OF_DAY",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___MEDICATION_FREQ as i_MEDICATION_FREQ",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___AILMENTS as i_AILMENTS1",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___MEDICATION_INSTRUCTIONS as i_MEDICATION_INSTRUCTIONS",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___UNIT_PRICE_AMT as i_UNIT_PRICE_AMT",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___TOTAL_PRICE_AMT as i_TOTAL_PRICE_AMT",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___UPDATE_TSTMP as i_UPDATE_TSTMP",
    "SQ_Shortcut_to_E_RES_SELECTED_MEDICATIONS___LOAD_TSTMP as i_LOAD_TSTMP",
)

# COMMAND ----------

# Processing node FIL_E_RES_SELECTED_MEDICATIONS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
JNR_E_RES_SELECTED_MEDICATIONS_temp = JNR_E_RES_SELECTED_MEDICATIONS.toDF(
    *[
        "JNR_E_RES_SELECTED_MEDICATIONS___" + col
        for col in JNR_E_RES_SELECTED_MEDICATIONS.columns
    ]
)

FIL_E_RES_SELECTED_MEDICATIONS = (
    JNR_E_RES_SELECTED_MEDICATIONS_temp.selectExpr(
        "JNR_E_RES_SELECTED_MEDICATIONS___SELECTED_MEDICATION_ID as SELECTED_MEDICATION_ID",
        "JNR_E_RES_SELECTED_MEDICATIONS___PET_ID as PET_ID",
        "JNR_E_RES_SELECTED_MEDICATIONS___NAME as NAME",
        "JNR_E_RES_SELECTED_MEDICATIONS___DOSE as DOSE",
        "JNR_E_RES_SELECTED_MEDICATIONS___TIME_OF_DAY as TIME_OF_DAY",
        "JNR_E_RES_SELECTED_MEDICATIONS___AILMENTS as AILMENTS",
        "JNR_E_RES_SELECTED_MEDICATIONS___INSTRUCTIONS as INSTRUCTIONS",
        "JNR_E_RES_SELECTED_MEDICATIONS___FREQUENCY as FREQUENCY",
        "JNR_E_RES_SELECTED_MEDICATIONS___UNIT_PRICE as UNIT_PRICE",
        "JNR_E_RES_SELECTED_MEDICATIONS___TOTAL_PRICE as TOTAL_PRICE",
        "JNR_E_RES_SELECTED_MEDICATIONS___LOAD_TSTMP as LOAD_TSTMP",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_E_RES_SELECTED_MEDICATION_ID as i_E_RES_SELECTED_MEDICATION_ID",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_E_RES_PET_ID as i_E_RES_PET_ID",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_MEDICATION_NAME as i_MEDICATION_NAME",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_MEDICATION_DOSE as i_MEDICATION_DOSE",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_MEDICATION_TIME_OF_DAY as i_MEDICATION_TIME_OF_DAY",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_MEDICATION_FREQ as i_MEDICATION_FREQ",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_AILMENTS1 as i_AILMENTS",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_MEDICATION_INSTRUCTIONS as i_MEDICATION_INSTRUCTIONS",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_UNIT_PRICE_AMT as i_UNIT_PRICE_AMT",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_TOTAL_PRICE_AMT as i_TOTAL_PRICE_AMT",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_UPDATE_TSTMP as i_UPDATE_TSTMP",
        "JNR_E_RES_SELECTED_MEDICATIONS___i_LOAD_TSTMP as i_LOAD_TSTMP",
    )
    .filter(
        "i_E_RES_SELECTED_MEDICATION_ID IS NULL OR ( i_E_RES_SELECTED_MEDICATION_ID IS NOT NULL AND ( IF(i_E_RES_PET_ID IS NULL, - 99999, i_E_RES_PET_ID) != IF(PET_ID IS NULL, - 99999, PET_ID) OR IF(i_MEDICATION_NAME IS NULL, '', i_MEDICATION_NAME) != IF(NAME IS NULL, '', NAME) OR IF(i_MEDICATION_DOSE IS NULL, '', i_MEDICATION_DOSE) != IF(DOSE IS NULL, '', DOSE) OR IF(i_MEDICATION_TIME_OF_DAY IS NULL, '', i_MEDICATION_TIME_OF_DAY) != IF(TIME_OF_DAY IS NULL, '', TIME_OF_DAY) OR IF(i_AILMENTS IS NULL, '', i_AILMENTS) != IF(AILMENTS IS NULL, '', AILMENTS) OR IF(i_MEDICATION_INSTRUCTIONS IS NULL, '', i_MEDICATION_INSTRUCTIONS) != IF(INSTRUCTIONS IS NULL, '', INSTRUCTIONS) OR IF(i_MEDICATION_FREQ IS NULL, '', i_MEDICATION_FREQ) != IF(FREQUENCY IS NULL, '', FREQUENCY) OR IF(i_UNIT_PRICE_AMT IS NULL, - 99999, i_UNIT_PRICE_AMT) != IF(UNIT_PRICE IS NULL, - 99999, UNIT_PRICE) OR IF(i_TOTAL_PRICE_AMT IS NULL, - 99999, i_TOTAL_PRICE_AMT) != IF(TOTAL_PRICE IS NULL, - 99999, TOTAL_PRICE) ) )"
    )
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node EXP_E_RES_SELECTED_MEDICATIONS, type EXPRESSION
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
FIL_E_RES_SELECTED_MEDICATIONS_temp = FIL_E_RES_SELECTED_MEDICATIONS.toDF(
    *[
        "FIL_E_RES_SELECTED_MEDICATIONS___" + col
        for col in FIL_E_RES_SELECTED_MEDICATIONS.columns
    ]
)

EXP_E_RES_SELECTED_MEDICATIONS = FIL_E_RES_SELECTED_MEDICATIONS_temp.selectExpr(
    "FIL_E_RES_SELECTED_MEDICATIONS___sys_row_id as sys_row_id",
    "FIL_E_RES_SELECTED_MEDICATIONS___SELECTED_MEDICATION_ID as SELECTED_MEDICATION_ID",
    "FIL_E_RES_SELECTED_MEDICATIONS___PET_ID as PET_ID",
    "FIL_E_RES_SELECTED_MEDICATIONS___NAME as NAME",
    "FIL_E_RES_SELECTED_MEDICATIONS___DOSE as DOSE",
    "FIL_E_RES_SELECTED_MEDICATIONS___TIME_OF_DAY as TIME_OF_DAY",
    "FIL_E_RES_SELECTED_MEDICATIONS___AILMENTS as AILMENTS",
    "FIL_E_RES_SELECTED_MEDICATIONS___INSTRUCTIONS as INSTRUCTIONS",
    "FIL_E_RES_SELECTED_MEDICATIONS___FREQUENCY as FREQUENCY",
    "FIL_E_RES_SELECTED_MEDICATIONS___UNIT_PRICE as UNIT_PRICE",
    "FIL_E_RES_SELECTED_MEDICATIONS___TOTAL_PRICE as TOTAL_PRICE",
    "FIL_E_RES_SELECTED_MEDICATIONS___i_E_RES_SELECTED_MEDICATION_ID as i_E_RES_SELECTED_MEDICATION_ID",
    "FIL_E_RES_SELECTED_MEDICATIONS___i_LOAD_TSTMP as i_LOAD_TSTMP",
    "CURRENT_TIMESTAMP as UPDATE_TSTMP",
    "IF(FIL_E_RES_SELECTED_MEDICATIONS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_E_RES_SELECTED_MEDICATIONS___i_LOAD_TSTMP) as LOAD_TSTMP",
    "IF(FIL_E_RES_SELECTED_MEDICATIONS___i_E_RES_SELECTED_MEDICATION_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR",
)

# COMMAND ----------

# Processing node UPD_E_RES_SELECTED_MEDICATIONS, type UPDATE_STRATEGY
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
EXP_E_RES_SELECTED_MEDICATIONS_temp = EXP_E_RES_SELECTED_MEDICATIONS.toDF(
    *[
        "EXP_E_RES_SELECTED_MEDICATIONS___" + col
        for col in EXP_E_RES_SELECTED_MEDICATIONS.columns
    ]
)

UPD_E_RES_SELECTED_MEDICATIONS = EXP_E_RES_SELECTED_MEDICATIONS_temp.selectExpr(
    "EXP_E_RES_SELECTED_MEDICATIONS___SELECTED_MEDICATION_ID as SELECTED_MEDICATION_ID",
    "EXP_E_RES_SELECTED_MEDICATIONS___PET_ID as PET_ID",
    "EXP_E_RES_SELECTED_MEDICATIONS___NAME as NAME",
    "EXP_E_RES_SELECTED_MEDICATIONS___DOSE as DOSE",
    "EXP_E_RES_SELECTED_MEDICATIONS___TIME_OF_DAY as TIME_OF_DAY",
    "EXP_E_RES_SELECTED_MEDICATIONS___AILMENTS as AILMENTS",
    "EXP_E_RES_SELECTED_MEDICATIONS___INSTRUCTIONS as INSTRUCTIONS",
    "EXP_E_RES_SELECTED_MEDICATIONS___FREQUENCY as FREQUENCY",
    "EXP_E_RES_SELECTED_MEDICATIONS___UNIT_PRICE as UNIT_PRICE",
    "EXP_E_RES_SELECTED_MEDICATIONS___TOTAL_PRICE as TOTAL_PRICE",
    "EXP_E_RES_SELECTED_MEDICATIONS___UPDATE_TSTMP as UPDATE_TSTMP",
    "EXP_E_RES_SELECTED_MEDICATIONS___LOAD_TSTMP as LOAD_TSTMP",
    "EXP_E_RES_SELECTED_MEDICATIONS___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR",
).withColumn(
    "pyspark_data_action",
    when(col("o_UPDATE_VALIDATOR") == (lit(1)), lit(0)).when(
        col("o_UPDATE_VALIDATOR") == (lit(2)), lit(1)
    ),
)

# COMMAND ----------

# Processing node Shortcut_to_E_RES_SELECTED_MEDICATIONS1, type TARGET
# COLUMN COUNT: 12


Shortcut_to_E_RES_SELECTED_MEDICATIONS1 = UPD_E_RES_SELECTED_MEDICATIONS.selectExpr(
    "CAST(SELECTED_MEDICATION_ID AS INT) as E_RES_SELECTED_MEDICATION_ID",
    "CAST(PET_ID AS INT) as E_RES_PET_ID",
    "CAST(NAME AS STRING) as MEDICATION_NAME",
    "CAST(DOSE AS STRING) as MEDICATION_DOSE",
    "CAST(TIME_OF_DAY AS STRING) as MEDICATION_TIME_OF_DAY",
    "CAST(FREQUENCY AS STRING) as MEDICATION_FREQ",
    "CAST(AILMENTS AS STRING) as AILMENTS",
    "CAST(INSTRUCTIONS AS STRING) as MEDICATION_INSTRUCTIONS",
    "CAST(UNIT_PRICE AS DECIMAL(15,2)) as UNIT_PRICE_AMT",
    "CAST(TOTAL_PRICE AS DECIMAL(15,2)) as TOTAL_PRICE_AMT",
    "CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
    "pyspark_data_action as pyspark_data_action",
)

try:
    primary_key = (
        """source.E_RES_SELECTED_MEDICATION_ID = target.E_RES_SELECTED_MEDICATION_ID"""
    )
    refined_perf_table = f"{legacy}.E_RES_SELECTED_MEDICATIONS"
    DuplicateChecker.check_for_duplicate_primary_keys(
        Shortcut_to_E_RES_SELECTED_MEDICATIONS1, ["E_RES_SELECTED_MEDICATION_ID"]
    )
    executeMerge(
        Shortcut_to_E_RES_SELECTED_MEDICATIONS1,
        refined_perf_table,
        primary_key,
    )
    logger.info(f"Merge with {refined_perf_table} completed]")
    logPrevRunDt(
        "E_RES_SELECTED_MEDICATIONS",
        "E_RES_SELECTED_MEDICATIONS",
        "Completed",
        "N/A",
        f"{raw}.log_run_details",
    )
except Exception as e:
    logPrevRunDt(
        "E_RES_SELECTED_MEDICATIONS",
        "E_RES_SELECTED_MEDICATIONS",
        "Failed",
        str(e),
        f"{raw}.log_run_details",
    )
    raise e
