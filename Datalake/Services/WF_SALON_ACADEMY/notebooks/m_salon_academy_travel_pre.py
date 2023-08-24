#Code converted on 2023-07-26 09:54:09
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

(username,password,connection_string) = SalonAcademy_prd_sqlServer(env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_GS_Travel, type SOURCE 
# COLUMN COUNT: 18

query = f"""(SELECT
GS_Travel.TravelID,
GS_Travel.AirlineDepartDate,
GS_Travel.AirlineDepartTime,
GS_Travel.AirlineReturnDate,
GS_Travel.AirlineReturnTime,
GS_Travel.AirlineSeatPreference,
GS_Travel.HotelCheckinDate,
GS_Travel.HotelCheckoutDate,
GS_Travel.LegalName,
GS_Travel.PreferredHotel,
GS_Travel.PreferredHotelAddress,
GS_Travel.PreferredHotelPhone,
GS_Travel.PreferredHotelContact,
GS_Travel.PreferredHotelRate,
GS_Travel.ModifiedBy,
GS_Travel.ModifiedDate,
GS_Travel.CreatedBy,
GS_Travel.CreatedDate
FROM SalonAcademy.dbo.GS_Travel) as src"""

SQ_Shortcut_to_GS_Travel = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SALON_ACADEMY_TRAVEL_PRE, type EXPRESSION 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_Travel_temp = SQ_Shortcut_to_GS_Travel.toDF(*["SQ_Shortcut_to_GS_Travel___" + col for col in SQ_Shortcut_to_GS_Travel.columns])

EXP_SALON_ACADEMY_TRAVEL_PRE = SQ_Shortcut_to_GS_Travel_temp.selectExpr(
	"SQ_Shortcut_to_GS_Travel___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_GS_Travel___TravelID as TravelID",
	"SQ_Shortcut_to_GS_Travel___AirlineDepartDate as AirlineDepartDate",
	"SQ_Shortcut_to_GS_Travel___AirlineDepartTime as AirlineDepartTime",
	"SQ_Shortcut_to_GS_Travel___AirlineReturnDate as AirlineReturnDate",
	"SQ_Shortcut_to_GS_Travel___AirlineReturnTime as AirlineReturnTime",
	"SQ_Shortcut_to_GS_Travel___AirlineSeatPreference as AirlineSeatPreference",
	"SQ_Shortcut_to_GS_Travel___HotelCheckinDate as HotelCheckinDate",
	"SQ_Shortcut_to_GS_Travel___HotelCheckoutDate as HotelCheckoutDate",
	"SQ_Shortcut_to_GS_Travel___LegalName as LegalName",
	"SQ_Shortcut_to_GS_Travel___PreferredHotel as PreferredHotel",
	"SQ_Shortcut_to_GS_Travel___PreferredHotelAddress as PreferredHotelAddress",
	"SQ_Shortcut_to_GS_Travel___PreferredHotelPhone as PreferredHotelPhone",
	"SQ_Shortcut_to_GS_Travel___PreferredHotelContact as PreferredHotelContact",
	"SQ_Shortcut_to_GS_Travel___PreferredHotelRate as PreferredHotelRate",
	"SQ_Shortcut_to_GS_Travel___ModifiedBy as ModifiedBy",
	"SQ_Shortcut_to_GS_Travel___ModifiedDate as ModifiedDate",
	"SQ_Shortcut_to_GS_Travel___CreatedBy as CreatedBy",
	"SQ_Shortcut_to_GS_Travel___CreatedDate as CreatedDate",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node Shortcut_to_SALON_ACADEMY_TRAVEL_PRE, type TARGET 
# COLUMN COUNT: 19


Shortcut_to_SALON_ACADEMY_TRAVEL_PRE = EXP_SALON_ACADEMY_TRAVEL_PRE.selectExpr(
	"CAST(TravelID AS BIGINT) as SALON_ACADEMY_TRAVEL_ID",
	"CAST(AirlineDepartDate AS DATE) as AIRLINE_DEPART_DT",
	"CAST(AirlineDepartTime AS STRING) as AIRLINE_DEPART_TIME_TX",
	"CAST(AirlineReturnDate AS DATE) as AIRLINE_RETURN_DT",
	"CAST(AirlineReturnTime AS STRING) as AIRLINE_RETURN_TIME_TX",
	"CAST(AirlineSeatPreference AS STRING) as AIRLINE_SEAT_PREFERENCE",
	"CAST(HotelCheckinDate AS DATE) as HOTEL_CHECK_IN_DT",
	"CAST(HotelCheckoutDate AS DATE) as HOTEL_CHECK_OUT_DT",
	"CAST(LegalName AS STRING) as LEGAL_NAME",
	"CAST(PreferredHotel AS STRING) as PREFERRED_HOTEL",
	"CAST(PreferredHotelAddress AS STRING) as PREFERRED_HOTEL_ADDR",
	"CAST(PreferredHotelPhone AS STRING) as PREFERRED_HOTEL_PHONE_NBR",
	"CAST(PreferredHotelContact AS STRING) as PREFERRED_HOTEL_CONTACT",
	"CAST(PreferredHotelRate AS STRING) as PREFERRED_HOTEL_RATE",
	"CAST(CreatedDate AS TIMESTAMP) as CREATED_TSTMP",
	"CAST(CreatedBy AS STRING) as CREATED_BY",
	"CAST(ModifiedDate AS TIMESTAMP) as MODIFIED_TSTMP",
	"CAST(ModifiedBy AS STRING) as MODIFIED_BY",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SALON_ACADEMY_TRAVEL_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SALON_ACADEMY_TRAVEL_PRE')
