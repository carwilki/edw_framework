# Code converted on 2023-07-28 11:37:48
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
cust_sensitive = getEnvPrefix(env) + "cust_sensitive"

# COMMAND ----------

# Variable_declaration_comment
# CREATE_DATE=args.CREATE_DATE
# UPDATE_DATE=args.UPDATE_DATE

refined_perf_table = "legacy_e_res_requests"
prev_run_dt = genPrevRunDt(refined_perf_table, cust_sensitive, raw)
print("The prev run date is " + prev_run_dt)

CREATE_DATE = (datetime.strptime(prev_run_dt, "%Y-%m-%d") - timedelta(days=1)).strftime(
    "%Y-%m-%d"
)
UPDATE_DATE = (datetime.strptime(prev_run_dt, "%Y-%m-%d") - timedelta(days=1)).strftime(
    "%Y-%m-%d"
)

(username, password, connection_string) = petHotel_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_Requests, type SOURCE
# COLUMN COUNT: 32

_sql = f"""
    SELECT
        Requests.RequestId,
        Requests.CartId,
        Requests.ExternalId,
        Requests.StoreNumber,
        Requests.FirstName,
        Requests.LastName,
        Requests.PrimaryPhone,
        Requests.SecondaryPhone,
        Requests.Email,
        Requests.Address1,
        Requests.Address2,
        Requests.City,
        Requests.StateProvince,
        Requests.Postal,
        Requests.CheckIn,
        Requests.CheckOut,
        Requests.Notes,
        Requests.TotalAmount,
        Requests.CustomerCalled,
        Requests.CallReason,
        Requests.CallComment,
        Requests.ClosingComment,
        Requests.Status,
        Requests.LockedBy,
        Requests.LockedAt,
        Requests.CreatedBy,
        Requests.CreatedAt,
        Requests.UpdatedBy,
        Requests.UpdatedAt,
        Requests.IsDeleted,
        Requests.RowVersion,
        Requests.Source
    FROM eReservations.dbo.Requests
    WHERE CreatedAt >=  '{CREATE_DATE}'  OR  UpdatedAt >= '{UPDATE_DATE}'
"""

SQ_Shortcut_to_Requests = SQ_Shortcut_to_AddonCategories = jdbcSqlServerConnection(
    f"({_sql}) as src", username, password, connection_string
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_E_RES_REQUESTS_PRE, type EXPRESSION
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Requests_temp = SQ_Shortcut_to_Requests.toDF(
    *["SQ_Shortcut_to_Requests___" + col for col in SQ_Shortcut_to_Requests.columns]
)

EXP_E_RES_REQUESTS_PRE = SQ_Shortcut_to_Requests_temp.selectExpr(
    "SQ_Shortcut_to_Requests___sys_row_id as sys_row_id",
    "SQ_Shortcut_to_Requests___RequestId as RequestId",
    "SQ_Shortcut_to_Requests___CartId as CartId",
    "SQ_Shortcut_to_Requests___ExternalId as ExternalId",
    "SQ_Shortcut_to_Requests___StoreNumber as StoreNumber",
    "SQ_Shortcut_to_Requests___FirstName as FirstName",
    "SQ_Shortcut_to_Requests___LastName as LastName",
    "SQ_Shortcut_to_Requests___PrimaryPhone as PrimaryPhone",
    "SQ_Shortcut_to_Requests___SecondaryPhone as SecondaryPhone",
    "SQ_Shortcut_to_Requests___Email as Email",
    "SQ_Shortcut_to_Requests___Address1 as Address1",
    "SQ_Shortcut_to_Requests___Address2 as Address2",
    "SQ_Shortcut_to_Requests___City as City",
    "SQ_Shortcut_to_Requests___StateProvince as StateProvince",
    "SQ_Shortcut_to_Requests___Postal as Postal",
    "SQ_Shortcut_to_Requests___CheckIn as CheckIn",
    "SQ_Shortcut_to_Requests___CheckOut as CheckOut",
    "SQ_Shortcut_to_Requests___Notes as Notes",
    "SQ_Shortcut_to_Requests___TotalAmount as TotalAmount",
    "CASE WHEN TRIM(SQ_Shortcut_to_Requests___CustomerCalled) = True THEN '1'  WHEN TRIM(SQ_Shortcut_to_Requests___CustomerCalled) = False THEN '0' ELSE NULL END as O_Customer_Called",
    "regexp_replace(SQ_Shortcut_to_Requests___CallReason, '\\((\\\\r|\\\\n)\\)', '$1') as o_CallReason",
    "regexp_replace(SQ_Shortcut_to_Requests___CallComment, '\\((\\\\r|\\\\n)\\)', '$1') as o_CallComment",
    "regexp_replace(SQ_Shortcut_to_Requests___ClosingComment, '\\((\\\\r|\\\\n)\\)', '$1') as o_ClosingComment",
    "SQ_Shortcut_to_Requests___Status as Status",
    "SQ_Shortcut_to_Requests___LockedBy as LockedBy",
    "SQ_Shortcut_to_Requests___LockedAt as LockedAt",
    "SQ_Shortcut_to_Requests___CreatedBy as CreatedBy",
    "SQ_Shortcut_to_Requests___CreatedAt as CreatedAt",
    "SQ_Shortcut_to_Requests___UpdatedBy as UpdatedBy",
    "SQ_Shortcut_to_Requests___UpdatedAt as UpdatedAt",
    "CASE WHEN TRIM(SQ_Shortcut_to_Requests___IsDeleted) = True THEN '1' WHEN TRIM(SQ_Shortcut_to_Requests___IsDeleted) = False THEN '0' ELSE NULL END as Is_Deleted",
    "SQ_Shortcut_to_Requests___RowVersion as RowVersion",
    "SQ_Shortcut_to_Requests___Source as Source",
    "CURRENT_TIMESTAMP as LOAD_TSTMP",
)

# COMMAND ----------

# Processing node Shortcut_to_E_RES_REQUESTS_PRE, type TARGET
# COLUMN COUNT: 32


Shortcut_to_E_RES_REQUESTS_PRE = EXP_E_RES_REQUESTS_PRE.selectExpr(
    "CAST(RequestId AS INT) as REQUEST_ID",
    "CAST(CartId AS INT) as CART_ID",
    "CAST(ExternalId AS STRING) as EXTERNAL_ID",
    "CAST(StoreNumber AS INT) as STORE_NUMBER",
    "CAST(FirstName AS STRING) as FIRST_NAME",
    "CAST(LastName AS STRING) as LAST_NAME",
    "CAST(PrimaryPhone AS STRING) as PRIMARY_PHONE",
    "CAST(SecondaryPhone AS STRING) as SECONDARY_PHONE",
    "CAST(Email AS STRING) as EMAIL",
    "CAST(Address1 AS STRING) as ADDRESS1",
    "CAST(Address2 AS STRING) as ADDRESS2",
    "CAST(City AS STRING) as CITY",
    "CAST(StateProvince AS STRING) as STATE_PROVINCE",
    "CAST(Postal AS STRING) as POSTAL",
    "CAST(CheckIn AS TIMESTAMP) as CHECK_IN",
    "CAST(CheckOut AS TIMESTAMP) as CHECK_OUT",
    "CAST(Notes AS STRING) as NOTES",
    "CAST(TotalAmount AS DECIMAL(15,2)) as TOTAL_AMOUNT",
    "CAST(O_Customer_Called AS TINYINT) as CUSTOMER_CALLED",
    "CAST(o_CallReason AS STRING) as CALL_REASON",
    "CAST(o_CallComment AS STRING) as CALL_COMMENT",
    "CAST(o_ClosingComment AS STRING) as CLOSING_COMMENT",
    "Status as STATUS",
    "CAST(LockedBy AS STRING) as LOCKED_BY",
    "CAST(LockedAt AS TIMESTAMP) as LOCKED_DT",
    "CAST(CreatedBy AS STRING) as CREATED_BY",
    "CAST(CreatedAt AS TIMESTAMP) as CREATED_DT",
    "CAST(UpdatedBy AS STRING) as UPDATED_BY",
    "CAST(UpdatedAt AS TIMESTAMP) as UPDATED_DT",
    "CAST(Is_Deleted AS TINYINT) as IS_DELETED",
    "CAST(Source AS STRING) as SOURCE",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)

Shortcut_to_E_RES_REQUESTS_PRE.write.mode("overwrite").saveAsTable(
    f"{cust_sensitive}.legacy_e_res_requests_pre"
)
