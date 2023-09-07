# Code converted on 2023-07-28 11:37:42
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

refined_perf_table = "E_RES_ADD_ONS"
prev_run_dt = genPrevRunDt(refined_perf_table, legacy, raw)
print("The prev run date is " + prev_run_dt)
CREATE_DATE = (datetime.strptime(prev_run_dt, "%Y-%m-%d") - timedelta(days=1)).strftime(
    "%Y-%m-%d"
)
UPDATE_DATE = (datetime.strptime(prev_run_dt, "%Y-%m-%d") - timedelta(days=1)).strftime(
    "%Y-%m-%d"
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_Addons, type SOURCE
# COLUMN COUNT: 18

(username, password, connection_string) = mtx_prd_sqlServer(env)

_sql = f"""
    SELECT
        Addons.AddonId,
        Addons.AddonName,
        Addons.Sku,
        Addons.AddonDescription,
        Addons.AddonCategoryId,
        Addons.GroupId,
        Addons.SpeciesId,
        Addons.AllowMultiple,
        Addons.Schedulable,
        Addons.BreedQualifier,
        Addons.CreatedAt,
        Addons.UpdatedBy,
        Addons.CreatedBy,
        Addons.UpdatedAt,
        Addons.IsDeleted,
        Addons.RateType,
        Addons.ImageUrl,
        Addons.SortOrder
    FROM eReservations.dbo.Addons
    WHERE CreatedAt >=  '{CREATE_DATE}' OR UpdatedAt >=  '{UPDATE_DATE}'
"""

SQ_Shortcut_to_Addons = SQ_Shortcut_to_AddonCategories = jdbcSqlServerConnection(
    f"({_sql}) as src", username, password, connection_string
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_E_RES_ADD_ONS_PRE, type EXPRESSION
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Addons_temp = SQ_Shortcut_to_Addons.toDF(
    *["SQ_Shortcut_to_Addons___" + col for col in SQ_Shortcut_to_Addons.columns]
)

EXP_E_RES_ADD_ONS_PRE = SQ_Shortcut_to_Addons_temp.selectExpr(
    "SQ_Shortcut_to_Addons___sys_row_id as sys_row_id",
    "SQ_Shortcut_to_Addons___AddonId as AddonId",
    "regexp_replace(SQ_Shortcut_to_Addons___AddonName, '\\((\\\\r|\\\\n)\\)', '$1') as o_AddonName",
    "SQ_Shortcut_to_Addons___Sku as Sku",
    "regexp_replace(SQ_Shortcut_to_Addons___AddonDescription, '\\((\\\\r|\\\\n)\\)', '$1') as o_AddonDescription",
    "SQ_Shortcut_to_Addons___AddonCategoryId as AddonCategoryId",
    "SQ_Shortcut_to_Addons___GroupId as GroupId",
    "SQ_Shortcut_to_Addons___SpeciesId as SpeciesId",
    "CASE WHEN TRIM(SQ_Shortcut_to_Addons___AllowMultiple) = True THEN '1' WHEN TRIM(SQ_Shortcut_to_Addons___AllowMultiple) = False THEN '0' ELSE NULL END as o_AllowMultiple",
    "CASE WHEN TRIM(SQ_Shortcut_to_Addons___Schedulable) = True THEN '1' WHEN TRIM(SQ_Shortcut_to_Addons___Schedulable) = False THEN '0' ELSE NULL END as o_Schedulable",
    "CASE WHEN TRIM(SQ_Shortcut_to_Addons___BreedQualifier) = True THEN '1' WHEN TRIM(SQ_Shortcut_to_Addons___BreedQualifier) = False THEN '0' ELSE NULL END as o_BreedQualifier",
    "SQ_Shortcut_to_Addons___CreatedAt as CreatedAt",
    "SQ_Shortcut_to_Addons___UpdatedBy as UpdatedBy",
    "SQ_Shortcut_to_Addons___CreatedBy as CreatedBy",
    "SQ_Shortcut_to_Addons___UpdatedAt as UpdatedAt",
    "CASE WHEN TRIM(SQ_Shortcut_to_Addons___IsDeleted) = True THEN '1' WHEN TRIM(SQ_Shortcut_to_Addons___IsDeleted) = False THEN '0' ELSE '' END as o_IsDeleted",
    "SQ_Shortcut_to_Addons___RateType as RateType",
    "SQ_Shortcut_to_Addons___ImageUrl as ImageUrl",
    "SQ_Shortcut_to_Addons___SortOrder as SortOrder",
    "CURRENT_TIMESTAMP as LOAD_TSTMP",
)



# COMMAND ----------

# Processing node Shortcut_to_E_RES_ADD_ONS_PRE, type TARGET
# COLUMN COUNT: 19


Shortcut_to_E_RES_ADD_ONS_PRE = EXP_E_RES_ADD_ONS_PRE.selectExpr(
    "CAST(AddonId AS INT) as ADD_ON_ID",
    "CAST(o_AddonName AS STRING) as ADD_ON_NAME",
    "CAST(Sku AS STRING) as SKU_NBR",
    "CAST(o_AddonDescription AS STRING) as ADD_ON_DESCRIPTION",
    "CAST(AddonCategoryId AS TINYINT) as ADD_ON_CATEGORY_ID",
    "CAST(GroupId AS TINYINT) as GROUP_ID",
    "CAST(SpeciesId AS TINYINT) as SPECIES_ID",
    "CAST(o_AllowMultiple AS TINYINT) as ALLOW_MULTIPLE",
    "CAST(o_Schedulable AS TINYINT) as SCHEDULABLE",
    "CAST(o_BreedQualifier AS TINYINT) as BREED_QUALIFIER",
    "RateType as RATE_TYPE",
    "CAST(ImageUrl AS STRING) as IMAGE_URL",
    "CAST(SortOrder AS TINYINT) as SORT_ORDER",
    "CAST(o_IsDeleted AS TINYINT) as IS_DELETED",
    "CAST(CreatedAt AS TIMESTAMP) as CREATED_AT",
    "CAST(UpdatedBy AS STRING) as UPDATED_BY",
    "CAST(CreatedBy AS STRING) as CREATED_BY",
    "CAST(UpdatedAt AS TIMESTAMP) as UPDATE_AT",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
)

Shortcut_to_E_RES_ADD_ONS_PRE.write.mode("overwrite").saveAsTable(
    f"{raw}.E_RES_ADD_ONS_PRE"
)
