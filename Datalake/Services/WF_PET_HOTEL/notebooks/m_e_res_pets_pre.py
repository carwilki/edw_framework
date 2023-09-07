#Code converted on 2023-07-28 11:37:39
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime,timedelta
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')

# uncomment before checking in
args = parser.parse_args()
env = args.env

# remove before checking in
# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
cust_sensitive = getEnvPrefix(env) + 'cust_sensitive'

# COMMAND ----------

# Variable_declaration_comment
# CREATE_DATE=args.CREATE_DATE
# UPDATE_DATE=args.UPDATE_DATE

refined_perf_table = "legacy_e_res_pets"
prev_run_dt = genPrevRunDt(refined_perf_table,cust_sensitive,raw)
print("The prev run date is " + prev_run_dt)
CREATE_DATE = (datetime.strptime(prev_run_dt, '%Y-%m-%d') - timedelta(days=1)).strftime("%Y-%m-%d")
UPDATE_DATE = (datetime.strptime(prev_run_dt, '%Y-%m-%d') - timedelta(days=1)).strftime("%Y-%m-%d")

# COMMAND ----------

# Processing node SQ_Shortcut_to_Requests, type SOURCE 
# COLUMN COUNT: 3

(username, password, connection_string) = petHotel_prd_sqlServer(env)

_sql = f"""
SELECT
    Requests.RequestId,
    Requests.CreatedAt,
    Requests.UpdatedAt
FROM eReservations.dbo.Requests
WHERE CreatedAt >= '{CREATE_DATE}' OR UpdatedAt >=  '{UPDATE_DATE}'
"""

SQ_Shortcut_to_Requests = SQ_Shortcut_to_AddonCategories = jdbcSqlServerConnection(f"({_sql}) as src",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_Pets, type SOURCE 
# COLUMN COUNT: 21

_sql = f"""
SELECT
    Pets.PetId,
    Pets.ExternalId,
    Pets.RequestId,
    Pets.Name,
    Pets.SpeciesId,
    Pets.BreedId,
    Pets.ColorId,
    Pets.Gender,
    Pets.Mixed,
    Pets.Fixed,
    Pets.DateOfBirth,
    Pets.Weight,
    Pets.RoomShared,
    Pets.Instructions,
    Pets.RoomType,
    Pets.RoomUnitPrice,
    Pets.RoomTotal,
    Pets.VetName,
    Pets.VetPhone,
    Pets.PetTotal,
    Pets.BookingReference
FROM eReservations.dbo.Pets
"""

SQ_Shortcut_to_Pets = SQ_Shortcut_to_AddonCategories = jdbcSqlServerConnection(f"({_sql}) as src", username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_E_RES_REQUEST, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 24

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Pets_temp = SQ_Shortcut_to_Pets.toDF(*["SQ_Shortcut_to_Pets___" + col for col in SQ_Shortcut_to_Pets.columns])
SQ_Shortcut_to_Requests_temp = SQ_Shortcut_to_Requests.toDF(*["SQ_Shortcut_to_Requests___" + col for col in SQ_Shortcut_to_Requests.columns])

JNR_E_RES_REQUEST = SQ_Shortcut_to_Requests_temp.join(SQ_Shortcut_to_Pets_temp,[SQ_Shortcut_to_Requests_temp.SQ_Shortcut_to_Requests___RequestId == SQ_Shortcut_to_Pets_temp.SQ_Shortcut_to_Pets___RequestId],'inner').selectExpr(
	"SQ_Shortcut_to_Pets___PetId as PetId",
	"SQ_Shortcut_to_Pets___ExternalId as ExternalId",
	"SQ_Shortcut_to_Pets___RequestId as RequestId",
	"SQ_Shortcut_to_Pets___Name as Name",
	"SQ_Shortcut_to_Pets___SpeciesId as SpeciesId",
	"SQ_Shortcut_to_Pets___BreedId as BreedId",
	"SQ_Shortcut_to_Pets___ColorId as ColorId",
	"SQ_Shortcut_to_Pets___Gender as Gender",
	"SQ_Shortcut_to_Pets___Mixed as Mixed",
	"SQ_Shortcut_to_Pets___Fixed as Fixed",
	"SQ_Shortcut_to_Pets___DateOfBirth as DateOfBirth",
	"SQ_Shortcut_to_Pets___Weight as Weight",
	"SQ_Shortcut_to_Pets___RoomShared as RoomShared",
	"SQ_Shortcut_to_Pets___Instructions as Instructions",
	"SQ_Shortcut_to_Pets___RoomType as RoomType",
	"SQ_Shortcut_to_Pets___RoomUnitPrice as RoomUnitPrice",
	"SQ_Shortcut_to_Pets___RoomTotal as RoomTotal",
	"SQ_Shortcut_to_Pets___VetName as VetName",
	"SQ_Shortcut_to_Pets___VetPhone as VetPhone",
	"SQ_Shortcut_to_Pets___PetTotal as PetTotal",
	"SQ_Shortcut_to_Pets___BookingReference as BookingReference",
	"SQ_Shortcut_to_Requests___RequestId as i_RequestId",
	"SQ_Shortcut_to_Requests___CreatedAt as CreatedAt",
	"SQ_Shortcut_to_Requests___UpdatedAt as UpdatedAt")

# COMMAND ----------

JNR_E_RES_REQUEST_temp = JNR_E_RES_REQUEST.toDF(*["JNR_E_RES_REQUEST___" + col for col in JNR_E_RES_REQUEST.columns])
JNR_E_RES_REQUEST_temp.columns

# COMMAND ----------

# Processing node EXP_E_RES_PETS_PRE, type EXPRESSION 
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
JNR_E_RES_REQUEST_temp = JNR_E_RES_REQUEST.toDF(*["JNR_E_RES_REQUEST___" + col for col in JNR_E_RES_REQUEST.columns])

EXP_E_RES_PETS_PRE = JNR_E_RES_REQUEST_temp.selectExpr( 
    # "JNR_E_RES_REQUEST___sys_row_id as sys_row_id", 
    "JNR_E_RES_REQUEST___PetId as PetId", 
    "JNR_E_RES_REQUEST___ExternalId as ExternalId", 
    "JNR_E_RES_REQUEST___RequestId as RequestId", 
    "JNR_E_RES_REQUEST___Name as Name", 
    "JNR_E_RES_REQUEST___SpeciesId as SpeciesId", 
    "JNR_E_RES_REQUEST___BreedId as BreedId", 
    "JNR_E_RES_REQUEST___ColorId as ColorId", 
    "JNR_E_RES_REQUEST___Gender as Gender", 
    "CASE WHEN TRIM(JNR_E_RES_REQUEST___Mixed) = True THEN '1' ELSE '0' END as o_Mixed", 
    "CASE WHEN TRIM(JNR_E_RES_REQUEST___Fixed)  =  True THEN '1' ELSE '0' END as o_Fixed", 
    "JNR_E_RES_REQUEST___DateOfBirth as DateOfBirth", 
    "JNR_E_RES_REQUEST___Weight as Weight", 
    "CASE WHEN TRIM(JNR_E_RES_REQUEST___RoomShared)  = True THEN '1'  WHEN TRIM(JNR_E_RES_REQUEST___RoomShared) = False THEN '0' ELSE NULL END as o_RoomShared",         
    "regexp_replace(JNR_E_RES_REQUEST___Instructions, '\\((\\\\r|\\\\n)\\)', '$1') as o_Instructions", 
    "JNR_E_RES_REQUEST___RoomType as RoomType", 
    "JNR_E_RES_REQUEST___RoomUnitPrice as RoomUnitPrice", 
    "JNR_E_RES_REQUEST___RoomTotal as RoomTotal", 
    "regexp_replace(JNR_E_RES_REQUEST___VetName, '\\((\\\\r|\\\\n)\\)', '$1') as o_VetName",         
    "JNR_E_RES_REQUEST___VetPhone as VetPhone", 
    "JNR_E_RES_REQUEST___PetTotal as PetTotal", 
    "JNR_E_RES_REQUEST___BookingReference as BookingReference", 
    "CURRENT_TIMESTAMP as LOAD_TSTMP" 
)

# COMMAND ----------

# Processing node Shortcut_to_E_RES_PETS_PRE, type TARGET 
# COLUMN COUNT: 22

Shortcut_to_E_RES_PETS_PRE = EXP_E_RES_PETS_PRE.selectExpr(
	"CAST(PetId AS INT) as PET_ID",
	"CAST(ExternalId AS STRING) as EXTERNAL_ID",
	"CAST(RequestId AS INT) as REQUEST_ID",
	"CAST(Name AS STRING) as NAME",
	"CAST(SpeciesId AS INT) as SPECIES_ID",
	"CAST(BreedId AS INT) as BREED_ID",
	"CAST(ColorId AS INT) as COLOR_ID",
	"CAST(Gender AS STRING) as GENDER",
	"CAST(o_Mixed AS TINYINT) as MIXED",
	"CAST(o_Fixed AS TINYINT) as FIXED",
	"CAST(DateOfBirth AS TIMESTAMP) as DATE_OF_BIRTH",
	"CAST(Weight AS DECIMAL(15,2)) as WEIGHT",
	"CAST(o_RoomShared AS TINYINT) as ROOM_SHARED",
	"CAST(o_Instructions AS STRING) as INSTRUCTIONS",
	"CAST(RoomType AS STRING) as ROOM_TYPE",
	"CAST(RoomUnitPrice AS DECIMAL(15,2)) as ROOM_UNIT_PRICE",
	"CAST(RoomTotal AS DECIMAL(15,2)) as ROOM_TOTAL",
	"CAST(o_VetName AS STRING) as VET_NAME",
	"CAST(VetPhone AS STRING) as VET_PHONE",
	"CAST(PetTotal AS DECIMAL(15,2)) as PET_TOTAL",
	"CAST(BookingReference AS STRING) as BOOKING_REFERENCE",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)

Shortcut_to_E_RES_PETS_PRE.write.mode("overwrite").saveAsTable(f'{cust_sensitive}.raw_e_res_pets_pre')
