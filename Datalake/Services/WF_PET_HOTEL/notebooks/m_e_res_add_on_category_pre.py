# Code converted on 2023-07-28 11:37:45
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

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

# Variable_declaration_comment
# CREATE_DATE=args.CREATE_DATE
# UPDATE_DATE=args.UPDATE_DATE

refined_perf_table = "E_RES_ADD_ON_CATEGORY"
prev_run_dt = genPrevRunDt(refined_perf_table, legacy, raw)
print("The prev run date is " + prev_run_dt)
CREATE_DATE = (datetime.strptime(prev_run_dt, "%Y-%m-%d") - timedelta(days=1)).strftime(
    "%Y-%m-%d"
)
UPDATE_DATE = (datetime.strptime(prev_run_dt, "%Y-%m-%d") - timedelta(days=1)).strftime(
    "%Y-%m-%d"
)


# COMMAND ----------

(username, password, connection_string) = petHotel_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_AddonCategories, type SOURCE
# COLUMN COUNT: 11

SQ_Shortcut_to_AddonCategories = jdbcSqlServerConnection(
    f"""(SELECT
AddonCategories.AddonCategoryId,
AddonCategories.AddonCategoryName,
AddonCategories.AllowMultipleServices,
AddonCategories.CreatedAt,
AddonCategories.UpdatedBy,
AddonCategories.CreatedBy,
AddonCategories.UpdatedAt,
AddonCategories.IsDeleted,
AddonCategories.ImageUrl,
AddonCategories.Description,
AddonCategories.SortOrder
FROM eReservations.dbo.AddonCategories
WHERE CreatedAt >=  '{CREATE_DATE}' OR UpdatedAt >=  '{UPDATE_DATE}') as src""",
    username,
    password,
    connection_string,
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_LOAD_TSTMP, type EXPRESSION
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_AddonCategories_temp = SQ_Shortcut_to_AddonCategories.toDF(
    *[
        "SQ_Shortcut_to_AddonCategories___" + col
        for col in SQ_Shortcut_to_AddonCategories.columns
    ]
)

EXP_LOAD_TSTMP = SQ_Shortcut_to_AddonCategories_temp.selectExpr(
    "SQ_Shortcut_to_AddonCategories___sys_row_id as sys_row_id",
    "SQ_Shortcut_to_AddonCategories___AddonCategoryId as AddonCategoryId",
    "SQ_Shortcut_to_AddonCategories___AddonCategoryName as AddonCategoryName",
    "CASE WHEN TRIM(SQ_Shortcut_to_AddonCategories___AllowMultipleServices) = True THEN '1' WHEN TRIM(SQ_Shortcut_to_AddonCategories___AllowMultipleServices) = False THEN '0' ELSE NULL END as o_AllowMultipleServices",
    "SQ_Shortcut_to_AddonCategories___CreatedAt as CreatedAt",
    "SQ_Shortcut_to_AddonCategories___UpdatedBy as UpdatedBy",
    "SQ_Shortcut_to_AddonCategories___CreatedBy as CreatedBy",
    "SQ_Shortcut_to_AddonCategories___UpdatedAt as UpdatedAt",
    "CASE WHEN TRIM(SQ_Shortcut_to_AddonCategories___IsDeleted) = True THEN '1' WHEN TRIM(SQ_Shortcut_to_AddonCategories___IsDeleted) = False THEN '0' ELSE NULL END as o_IsDeleted",
    "SQ_Shortcut_to_AddonCategories___ImageUrl as ImageUrl",
    "regexp_replace(SQ_Shortcut_to_AddonCategories___Description, '\\((\\\\r|\\\\n)\\)', '$1') as o_Description",
    "SQ_Shortcut_to_AddonCategories___SortOrder as SortOrder",
    "CURRENT_TIMESTAMP as LOAD_TSTMP",
)




# COMMAND ----------

# Processing node Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE, type TARGET
# COLUMN COUNT: 12


Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE = EXP_LOAD_TSTMP.selectExpr(
    "CAST(AddonCategoryId AS INT) as ADD_ON_CATEGORY_ID",
    "CAST(AddonCategoryName AS STRING) as ADD_ON_CATEGORY_NAME",
    "CAST(o_AllowMultipleServices AS TINYINT) as ALLOW_MULTIPLE_SERVICES",
    "CAST(CreatedAt AS TIMESTAMP) as CREATED_AT",
    "CAST(UpdatedBy AS STRING) as UPDATED_BY",
    "CAST(CreatedBy AS STRING) as CREATED_BY",
    "CAST(UpdatedAt AS TIMESTAMP) as UPDATED_AT",
    "CAST(o_IsDeleted AS TINYINT) as IS_DELETED",
    "CAST(ImageUrl AS STRING) as IMAGE_URL",
    "CAST(o_Description AS STRING) as DESCRIPTION",
    "CAST(SortOrder AS TINYINT) as SORT_ORDER",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
)

Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE.write.mode("overwrite").saveAsTable(
    f"{raw}.E_RES_ADD_ON_CATEGORY_PRE"
)
