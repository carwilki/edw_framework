#Code converted on 2023-07-28 07:59:24
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

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

(username,password,connection_string) = PetTraining_prd_sqlServer(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_GS_Travel, type SOURCE 
# COLUMN COUNT: 19

query =f"""(SELECT
GS_Travel.TravelID,
GS_Travel.AirlineDepartDate,
GS_Travel.AirlineDepartTime,
GS_Travel.AirlineReturnDate,
GS_Travel.AirlineReturnTime,
GS_Travel.AirlineSeatPreference,
GS_Travel.HotelCheckinDate,
GS_Travel.HotelCheckoutDate,
GS_Travel.LegalName,
GS_Travel.BirthDate,
GS_Travel.Gender,
GS_Travel.PreferredHotel,
GS_Travel.PreferredHotelAddress,
GS_Travel.PreferredHotelPhone,
GS_Travel.PreferredHotelContact,
GS_Travel.PreferredHotelRate,
GS_Travel.HotelRoomType,
GS_Travel.PreferredRoomMate,
GS_Travel.ModifiedBy
FROM PetTraining.dbo.GS_Travel) as src"""

SQ_Shortcut_to_GS_Travel = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_Travel_temp = SQ_Shortcut_to_GS_Travel.toDF(*["SQ_Shortcut_to_GS_Travel___" + col for col in SQ_Shortcut_to_GS_Travel.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_GS_Travel_temp.selectExpr(
	"SQ_Shortcut_to_GS_Travel___sys_row_id as sys_row_id",
	"CURRENT_TIMESTAMP as LOAD_TSTMP",
	"SQ_Shortcut_to_GS_Travel___TravelID as TravelID",
	"SQ_Shortcut_to_GS_Travel___AirlineDepartDate as AirlineDepartDate",
	"SQ_Shortcut_to_GS_Travel___AirlineDepartTime as AirlineDepartTime",
	"SQ_Shortcut_to_GS_Travel___AirlineReturnDate as AirlineReturnDate",
	"SQ_Shortcut_to_GS_Travel___AirlineReturnTime as AirlineReturnTime",
	"SQ_Shortcut_to_GS_Travel___AirlineSeatPreference as AirlineSeatPreference",
	"SQ_Shortcut_to_GS_Travel___HotelCheckinDate as HotelCheckinDate",
	"SQ_Shortcut_to_GS_Travel___HotelCheckoutDate as HotelCheckoutDate",
	"SQ_Shortcut_to_GS_Travel___LegalName as LegalName",
	"SQ_Shortcut_to_GS_Travel___BirthDate as BirthDate",
	"SQ_Shortcut_to_GS_Travel___Gender as Gender",
	"SQ_Shortcut_to_GS_Travel___PreferredHotel as PreferredHotel",
	"SQ_Shortcut_to_GS_Travel___PreferredHotelAddress as PreferredHotelAddress",
	"SQ_Shortcut_to_GS_Travel___PreferredHotelPhone as PreferredHotelPhone",
	"SQ_Shortcut_to_GS_Travel___PreferredHotelContact as PreferredHotelContact",
	"SQ_Shortcut_to_GS_Travel___PreferredHotelRate as PreferredHotelRate",
	"SQ_Shortcut_to_GS_Travel___HotelRoomType as HotelRoomType",
	"SQ_Shortcut_to_GS_Travel___PreferredRoomMate as PreferredRoomMate",
	"SQ_Shortcut_to_GS_Travel___ModifiedBy as ModifiedBy"
)

# COMMAND ----------
# Processing node Shortcut_to_GS_PT_TRAVEL_PRE, type TARGET 
# COLUMN COUNT: 20


Shortcut_to_GS_PT_TRAVEL_PRE = EXP_LOAD_TSTMP.selectExpr(
	"CAST(TravelID as BIGINT) as GS_PT_TRAVEL_ID",
	"CAST(AirlineDepartDate AS TIMESTAMP) as AIRLINE_DEPART_DT",
	"CAST(AirlineDepartTime AS STRING) as AIRLINE_DEPART_TIME",
	"CAST(AirlineReturnDate AS TIMESTAMP) as AIRLINE_RETURN_DT",
	"CAST(AirlineReturnTime AS STRING) as AIRLINE_RETURN_TIME",
	"CAST(AirlineSeatPreference AS STRING) as AIRLINE_SEAT_PREFERENCE",
	"CAST(HotelCheckinDate AS TIMESTAMP) as HOTEL_CHECKIN_DT",
	"CAST(HotelCheckoutDate AS TIMESTAMP) as HOTEL_CHECKOUT_DT",
	"CAST(LegalName AS STRING) as LEGAL_NAME",
	"CAST(BirthDate AS TIMESTAMP) as BIRTHDATE",
	"CAST(Gender AS SMALLINT) as GENDER",
	"CAST(PreferredHotel AS STRING) as PREFERRED_HOTEL",
	"CAST(PreferredHotelAddress AS STRING) as PREFERRED_HOTEL_ADDRESS",
	"CAST(PreferredHotelPhone AS STRING) as PREFERRED_HOTEL_PHONE",
	"CAST(PreferredHotelContact AS STRING) as PREFERRED_HOTEL_CONTACT",
	"CAST(PreferredHotelRate AS STRING) as PREFERRED_HOTEL_RATE",
	"CAST(HotelRoomType AS SMALLINT) as HOTEL_ROOM_TYPE",
	"CAST(PreferredRoomMate AS STRING) as PREFERRED_ROOMMATE",
	"CAST(ModifiedBy AS STRING) as MODIFIED_BY",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_GS_PT_TRAVEL_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GS_PT_TRAVEL_PRE')
