# Code converted on 2023-08-10 12:41:12
import argparse
import os
from datetime import datetime

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from Datalake.utils.configs import *
from Datalake.utils.genericUtilities import *
from Datalake.utils.logger import *
from Datalake.utils.mergeUtils import *

# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument("env", type=str, help="Env Variable")

args = parser.parse_args()
env = args.env

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"
cust_sensitive = getEnvPrefix(env) + "cust_sensitive"

(username, password, connection_string) = mtx_prd_sqlServer(env)
sqlTable = "staging.DB_SALON_LAPSED_SEASON_CUST_CALL_LOG_PRE"

Prev_Run_Dt=genPrevRunDt("SALON_LAPSED_SEASON_CUST_CALL_LOG", refine, raw)
print("The prev run date is " + Prev_Run_Dt)

# COMMAND ----------

# Processing node LKP_PODS_PET_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 3

LKP_PODS_PET_SRC = spark.sql(
    f"""SELECT
PODS_PET_ID,
ACTIVE_FLAG
FROM {legacy}.PODS_PET"""
)#.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Conforming fields names to the component layout
LKP_PODS_PET_SRC = LKP_PODS_PET_SRC.withColumnRenamed(
    LKP_PODS_PET_SRC.columns[0], "PODS_PET_ID"
).withColumnRenamed(LKP_PODS_PET_SRC.columns[1], "ACTIVE_FLAG")
# 	.withColumnRenamed(LKP_PODS_PET_SRC.columns[2],'PODS_PET_ID1')

# COMMAND ----------

# Processing node SQ_Shortcut_to_SDS_ORDER_ITEM_RPT, type SOURCE
# COLUMN COUNT: 6

SQ_Shortcut_to_SDS_ORDER_ITEM_RPT = spark.sql(
    f"""SELECT
PODS_CUSTOMER_ID,
PRODUCT_ID,
APPT_TSTMP,
SVCS_APPT_STATUS_GID,
SVCS_ORDER_STATUS_GID,
APPT_SERVICE_CANCEL_FLAG
FROM {cust_sensitive}.legacy_sds_order_item_rpt
WHERE COALESCE(APPT_TSTMP,date('1900-01-01'))> to_date('{Prev_Run_Dt}') - 1"""
)  # .withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG, type SOURCE
# COLUMN COUNT: 20

_sql = f"""
SELECT
    SALON_LAPSED_SEASON_CUST_CALL_LOG.SALON_LAPSED_SEASON_CUST_CALL_LOG_ID,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.PODS_CUSTOMER_ID,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.PODS_PET_ID,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.LOCATION_ID,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.CUSTOMER_FIRST_NAME,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.CUSTOMER_LAST_NAME,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.CUSTOMER_PHONE_NBR,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.PET_NAME,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.PET_SPECIES_ID,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.PET_SPECIES_DESC,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.PET_BREED_ID,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.PET_BREED_DESC,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.PET_STATUS,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.PET_LAST_APPT_DT,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.LAST_SEASONAL_SPECIAL_USED,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.CALL_RESULT_ID,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.CALL_REASON_ID,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.UPDATE_USER_ID,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.UPDATE_TSTMP,
    SALON_LAPSED_SEASON_CUST_CALL_LOG.LOAD_TSTMP
FROM SALON_LAPSED_SEASON_CUST_CALL_LOG
WHERE SALON_LAPSED_SEASON_CUST_CALL_LOG.CALL_RESULT_ID=1
"""

SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG = jdbcSqlServerConnection(
    f"({_sql}) as src", username, password, connection_string
).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node LKP_PODS_PET, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3


LKP_PODS_PET_lookup_result = (
    SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG.selectExpr(
        "PODS_PET_ID as PODS_PET_ID1", "sys_row_id"
    ).join(LKP_PODS_PET_SRC, (col("PODS_PET_ID") == col("PODS_PET_ID1")), "left")
)

LKP_PODS_PET = LKP_PODS_PET_lookup_result.select(
    LKP_PODS_PET_lookup_result.sys_row_id, col("ACTIVE_FLAG")
)

# COMMAND ----------

# Processing node FILTRANS, type FILTER
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_ORDER_ITEM_RPT_temp = SQ_Shortcut_to_SDS_ORDER_ITEM_RPT.toDF(
    *[
        "SQ_Shortcut_to_SDS_ORDER_ITEM_RPT___" + col
        for col in SQ_Shortcut_to_SDS_ORDER_ITEM_RPT.columns
    ]
)

FILTRANS = (
    SQ_Shortcut_to_SDS_ORDER_ITEM_RPT_temp.selectExpr(
        "SQ_Shortcut_to_SDS_ORDER_ITEM_RPT___PODS_CUSTOMER_ID as PODS_CUSTOMER_ID",
        "SQ_Shortcut_to_SDS_ORDER_ITEM_RPT___APPT_TSTMP as APPT_TSTMP",
        "SQ_Shortcut_to_SDS_ORDER_ITEM_RPT___PRODUCT_ID as PRODUCT_ID",
        "SQ_Shortcut_to_SDS_ORDER_ITEM_RPT___SVCS_APPT_STATUS_GID as SVCS_APPT_STATUS_GID",
        "SQ_Shortcut_to_SDS_ORDER_ITEM_RPT___SVCS_ORDER_STATUS_GID as SVCS_ORDER_STATUS_GID",
        "SQ_Shortcut_to_SDS_ORDER_ITEM_RPT___APPT_SERVICE_CANCEL_FLAG as APPT_SERVICE_CANCEL_FLAG",
    )
    .filter(
        "SVCS_APPT_STATUS_GID IN ( 3,4,5 ) AND SVCS_ORDER_STATUS_GID != 7 AND APPT_SERVICE_CANCEL_FLAG = 0"
    )
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node EXP_ACTIVE_FLAG, type EXPRESSION
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG_temp = (
    SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG.toDF(
        *[
            "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___" + col
            for col in SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG.columns
        ]
    )
)
LKP_PODS_PET_temp = LKP_PODS_PET.toDF(
    *["LKP_PODS_PET___" + col for col in LKP_PODS_PET.columns]
)

# Joining dataframes SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG, LKP_PODS_PET to form EXP_ACTIVE_FLAG
EXP_ACTIVE_FLAG_joined = SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG_temp.join(
    LKP_PODS_PET_temp,
    col("LKP_PODS_PET___sys_row_id")
    == col("SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___sys_row_id"),
    "inner",
)

EXP_ACTIVE_FLAG = EXP_ACTIVE_FLAG_joined.selectExpr(
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___sys_row_id as sys_row_id",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___SALON_LAPSED_SEASON_CUST_CALL_LOG_ID as SALON_LAPSED_SEASON_CUST_CALL_LOG_ID",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___PODS_CUSTOMER_ID as PODS_CUSTOMER_ID",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___PODS_PET_ID as PODS_PET_ID",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___LOCATION_ID as LOCATION_ID",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___CUSTOMER_FIRST_NAME as CUSTOMER_FIRST_NAME",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___CUSTOMER_LAST_NAME as CUSTOMER_LAST_NAME",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___CUSTOMER_PHONE_NBR as CUSTOMER_PHONE_NBR",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___PET_NAME as PET_NAME",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___PET_SPECIES_ID as PET_SPECIES_ID",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___PET_SPECIES_DESC as PET_SPECIES_DESC",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___PET_BREED_ID as PET_BREED_ID",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___PET_BREED_DESC as PET_BREED_DESC",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___PET_STATUS as PET_STATUS",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___PET_LAST_APPT_DT as PET_LAST_APPT_DT",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___LAST_SEASONAL_SPECIAL_USED as LAST_SEASONAL_SPECIAL_USED",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___CALL_RESULT_ID as CALL_RESULT_ID",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___CALL_REASON_ID as CALL_REASON_ID",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___UPDATE_USER_ID as UPDATE_USER_ID",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___UPDATE_TSTMP as UPDATE_TSTMP",
    "SQ_Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG___LOAD_TSTMP as LOAD_TSTMP",
    "LKP_PODS_PET___ACTIVE_FLAG as ACTIVE_FLAG",
)

# COMMAND ----------

# Processing node POD_CUST_FUTURE_APPT, type AGGREGATOR
# COLUMN COUNT: 1

POD_CUST_FUTURE_APPT = (
    FILTRANS.groupBy("PODS_CUSTOMER_ID")
    .agg(count("*").alias("cnt"))
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node JNR_SALON_LAPSED_CUST_FUTURE_BOOKED, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
EXP_ACTIVE_FLAG_temp = EXP_ACTIVE_FLAG.toDF(
    *["EXP_ACTIVE_FLAG___" + col for col in EXP_ACTIVE_FLAG.columns]
)
POD_CUST_FUTURE_APPT_temp = POD_CUST_FUTURE_APPT.toDF(
    *["POD_CUST_FUTURE_APPT___" + col for col in POD_CUST_FUTURE_APPT.columns]
)

JNR_SALON_LAPSED_CUST_FUTURE_BOOKED = POD_CUST_FUTURE_APPT_temp.join(
    EXP_ACTIVE_FLAG_temp,
    [
        POD_CUST_FUTURE_APPT_temp.POD_CUST_FUTURE_APPT___PODS_CUSTOMER_ID
        == EXP_ACTIVE_FLAG_temp.EXP_ACTIVE_FLAG___PODS_CUSTOMER_ID
    ],
    "right_outer",
).selectExpr(
    "EXP_ACTIVE_FLAG___SALON_LAPSED_SEASON_CUST_CALL_LOG_ID as SALON_LAPSED_SEASON_CUST_CALL_LOG_ID",
    "EXP_ACTIVE_FLAG___PODS_CUSTOMER_ID as PODS_CUSTOMER_ID",
    "EXP_ACTIVE_FLAG___PODS_PET_ID as PODS_PET_ID",
    "EXP_ACTIVE_FLAG___LOCATION_ID as LOCATION_ID",
    "EXP_ACTIVE_FLAG___CUSTOMER_FIRST_NAME as CUSTOMER_FIRST_NAME",
    "EXP_ACTIVE_FLAG___CUSTOMER_LAST_NAME as CUSTOMER_LAST_NAME",
    "EXP_ACTIVE_FLAG___CUSTOMER_PHONE_NBR as CUSTOMER_PHONE_NBR",
    "EXP_ACTIVE_FLAG___PET_NAME as PET_NAME",
    "EXP_ACTIVE_FLAG___PET_SPECIES_ID as PET_SPECIES_ID",
    "EXP_ACTIVE_FLAG___PET_SPECIES_DESC as PET_SPECIES_DESC",
    "EXP_ACTIVE_FLAG___PET_BREED_ID as PET_BREED_ID",
    "EXP_ACTIVE_FLAG___PET_BREED_DESC as PET_BREED_DESC",
    "EXP_ACTIVE_FLAG___PET_STATUS as PET_STATUS",
    "EXP_ACTIVE_FLAG___PET_LAST_APPT_DT as PET_LAST_APPT_DT",
    "EXP_ACTIVE_FLAG___LAST_SEASONAL_SPECIAL_USED as LAST_SEASONAL_SPECIAL_USED",
    "EXP_ACTIVE_FLAG___CALL_RESULT_ID as CALL_RESULT_ID",
    "EXP_ACTIVE_FLAG___CALL_REASON_ID as CALL_REASON_ID",
    "EXP_ACTIVE_FLAG___UPDATE_USER_ID as UPDATE_USER_ID",
    "EXP_ACTIVE_FLAG___UPDATE_TSTMP as UPDATE_TSTMP",
    "EXP_ACTIVE_FLAG___LOAD_TSTMP as LOAD_TSTMP",
    "POD_CUST_FUTURE_APPT___PODS_CUSTOMER_ID as PODS_CUSTOMER_ID1",
    "EXP_ACTIVE_FLAG___ACTIVE_FLAG as ACTIVE_FLAG",
)

# COMMAND ----------

# Processing node EXP_UPDATE_VALUES, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
JNR_SALON_LAPSED_CUST_FUTURE_BOOKED_temp = JNR_SALON_LAPSED_CUST_FUTURE_BOOKED.toDF(
    *[
        "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___" + col
        for col in JNR_SALON_LAPSED_CUST_FUTURE_BOOKED.columns
    ]
)

# COMMAND ----------

JNR_SALON_LAPSED_CUST_FUTURE_BOOKED_temp.columns

# COMMAND ----------


EXP_UPDATE_VALUES = JNR_SALON_LAPSED_CUST_FUTURE_BOOKED_temp.selectExpr(
    # "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___sys_row_id as sys_row_id",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___SALON_LAPSED_SEASON_CUST_CALL_LOG_ID as SALON_LAPSED_SEASON_CUST_CALL_LOG_ID",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___PODS_CUSTOMER_ID as PODS_CUSTOMER_ID",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___PODS_PET_ID as PODS_PET_ID",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___LOCATION_ID as LOCATION_ID",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___CUSTOMER_FIRST_NAME as CUSTOMER_FIRST_NAME",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___CUSTOMER_LAST_NAME as CUSTOMER_LAST_NAME",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___CUSTOMER_PHONE_NBR as CUSTOMER_PHONE_NBR",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___PET_NAME as PET_NAME",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___PET_SPECIES_ID as PET_SPECIES_ID",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___PET_SPECIES_DESC as PET_SPECIES_DESC",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___PET_BREED_ID as PET_BREED_ID",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___PET_BREED_DESC as PET_BREED_DESC",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___PET_STATUS as PET_STATUS",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___PET_LAST_APPT_DT as PET_LAST_APPT_DT",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___LAST_SEASONAL_SPECIAL_USED as LAST_SEASONAL_SPECIAL_USED",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___CALL_RESULT_ID as CALL_RESULT_ID",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___CALL_REASON_ID as CALL_REASON_ID",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___UPDATE_USER_ID as UPDATE_USER_ID",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___UPDATE_TSTMP as UPDATE_TSTMP",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___LOAD_TSTMP as LOAD_TSTMP",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___PODS_CUSTOMER_ID1 as FUTURE_APPT_PODS_CUSTOMER_ID",
    "JNR_SALON_LAPSED_CUST_FUTURE_BOOKED___ACTIVE_FLAG as ACTIVE_FLAG",
).selectExpr(
    # "sys_row_id",
    "SALON_LAPSED_SEASON_CUST_CALL_LOG_ID",
    "PODS_CUSTOMER_ID",
    "PODS_PET_ID",
    "LOCATION_ID",
    "CUSTOMER_FIRST_NAME",
    "CUSTOMER_LAST_NAME",
    "CUSTOMER_PHONE_NBR",
    "PET_NAME",
    "PET_SPECIES_ID",
    "PET_SPECIES_DESC",
    "PET_BREED_ID",
    "PET_BREED_DESC",
    "PET_STATUS",
    "PET_LAST_APPT_DT",
    "LAST_SEASONAL_SPECIAL_USED",
    "CASE WHEN FUTURE_APPT_PODS_CUSTOMER_ID IS NOT NULL THEN 7 WHEN ACTIVE_FLAG = 0 THEN 5 ELSE CALL_RESULT_ID END as o_CALL_RESULT_ID",
    "CASE WHEN FUTURE_APPT_PODS_CUSTOMER_ID IS NOT NULL THEN CALL_REASON_ID WHEN ACTIVE_FLAG = 0 THEN 3 ELSE CALL_REASON_ID END as o_CALL_REASON_ID",
    "CASE WHEN FUTURE_APPT_PODS_CUSTOMER_ID IS NOT NULL THEN 'Default-Future Appt' WHEN ACTIVE_FLAG = 0 THEN 'Default-InactivePet' END as o_UPDATE_USER_ID",
    "CURRENT_TIMESTAMP() as o_UPDATE_TSTMP",
    "LOAD_TSTMP",
    "FUTURE_APPT_PODS_CUSTOMER_ID",
    "ACTIVE_FLAG",
)

# COMMAND ----------

# Processing node FILTRANS1, type FILTER
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALUES_temp = EXP_UPDATE_VALUES.toDF(
    *["EXP_UPDATE_VALUES___" + col for col in EXP_UPDATE_VALUES.columns]
)

FILTRANS1 = (
    EXP_UPDATE_VALUES_temp.selectExpr(
        "EXP_UPDATE_VALUES___SALON_LAPSED_SEASON_CUST_CALL_LOG_ID as SALON_LAPSED_SEASON_CUST_CALL_LOG_ID",
        "EXP_UPDATE_VALUES___o_CALL_RESULT_ID as o_CALL_RESULT_ID",
        "EXP_UPDATE_VALUES___o_CALL_REASON_ID as o_CALL_REASON_ID",
        "EXP_UPDATE_VALUES___o_UPDATE_USER_ID as o_UPDATE_USER_ID",
        "EXP_UPDATE_VALUES___o_UPDATE_TSTMP as o_UPDATE_TSTMP",
        "EXP_UPDATE_VALUES___FUTURE_APPT_PODS_CUSTOMER_ID as FUTURE_APPT_PODS_CUSTOMER_ID",
        "EXP_UPDATE_VALUES___ACTIVE_FLAG as ACTIVE_FLAG",
    )
    .filter("FUTURE_APPT_PODS_CUSTOMER_ID IS NOT NULL OR ACTIVE_FLAG = 0 ")
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node UPD_CALL_RESULT, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
FILTRANS1_temp = FILTRANS1.toDF(*["FILTRANS1___" + col for col in FILTRANS1.columns])

UPD_CALL_RESULT = FILTRANS1_temp.selectExpr(
    "FILTRANS1___SALON_LAPSED_SEASON_CUST_CALL_LOG_ID as SALON_LAPSED_SEASON_CUST_CALL_LOG_ID",
    "FILTRANS1___o_CALL_RESULT_ID as CALL_RESULT_ID",
    "FILTRANS1___o_CALL_REASON_ID as CALL_REASON_ID",
    "FILTRANS1___o_UPDATE_USER_ID as UPDATE_USER_ID",
    "FILTRANS1___o_UPDATE_TSTMP as UPDATE_TSTMP",
).withColumn("pyspark_data_action", lit(1))

# COMMAND ----------

# Processing node Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG1, type TARGET
# COLUMN COUNT: 23


Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG1 = UPD_CALL_RESULT.selectExpr(
    "SALON_LAPSED_SEASON_CUST_CALL_LOG_ID",
    "CAST(NULL AS BIGINT) AS PODS_CUSTOMER_ID",
    "CAST(NULL AS BIGINT) AS PODS_PET_ID",
    "CAST(NULL AS INT) AS LOCATION_ID",
    "CAST(NULL AS STRING) as CUSTOMER_FIRST_NAME",
    "CAST(NULL AS STRING) as CUSTOMER_LAST_NAME",
    "CAST(NULL AS BIGINT) as CUSTOMER_PHONE_NBR",
    "CAST(NULL AS STRING) as PET_NAME",
    "CAST(NULL AS INT) as PET_SPECIES_ID",
    "CAST(NULL AS STRING) as PET_SPECIES_DESC",
    "CAST(NULL AS INT) as PET_BREED_ID",
    "CAST(NULL AS STRING) as PET_BREED_DESC",
    "CAST(NULL AS STRING) as PET_STATUS",
    "CAST(NULL AS DATE) AS PET_LAST_APPT_DT",
    "CAST(NULL AS STRING) as LAST_SEASONAL_SPECIAL_USED",
    "CAST(NULL AS STRING) as LIST_NAME",
    "CALL_RESULT_ID",
    "CALL_REASON_ID",
    "CAST(UPDATE_USER_ID AS STRING) as UPDATE_USER_ID",
    "UPDATE_TSTMP",
    "CAST(NULL AS DATE) as LOAD_TSTMP"
)

# Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG1.write.mode("overwrite").saveAsTable(
#     f"{raw}.SALON_LAPSED_SEASON_CUST_CALL_LOG"
# )

# call SP truncate pre table staging.DB_SALON_LAPSED_CUST_CALL_LOG_PR
res_trunc = execSP("databricks.DBA_TruncateAllowedTable 'staging.DB_SALON_LAPSED_SEASON_CUST_CALL_LOG_PRE', ?", connection_string, username, password)

if res_trunc == 1:
  loadDFtoSQLTarget(Shortcut_to_SALON_LAPSED_SEASON_CUST_CALL_LOG1,sqlTable,username,password,connection_string)

  # call SP merge batch
  res_merge = execSP("exec databricks.UpdateInBatches_SALON_LAPSED_SEASON_CUST_CALL_LOG  ?", connection_string, username, password)
  if res_merge == 0:
    logPrevRunDt("SALON_LAPSED_SEASON_CUST_CALL_LOG", "SALON_LAPSED_SEASON_CUST_CALL_LOG","Failed", "Error while running merge SP", f"{raw}.log_run_details", )
    raise Exception('Error while merging pre into base')
  logPrevRunDt("SALON_LAPSED_SEASON_CUST_CALL_LOG", "SALON_LAPSED_SEASON_CUST_CALL_LOG", "Completed", "N/A", f"{raw}.log_run_details")
else:
  logPrevRunDt("SALON_LAPSED_SEASON_CUST_CALL_LOG", "SALON_LAPSED_SEASON_CUST_CALL_LOG","Failed","Error while running truncate SP", f"{raw}.log_run_details", )
  raise Exception('Error while truncating the pre table')

