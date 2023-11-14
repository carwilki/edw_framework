# Databricks notebook source
#Code converted on 2023-10-18 17:16:46
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'
sensitive = getEnvPrefix(env) + 'empl_sensitive'

# Set global variables
starttime = datetime.now() #start timestamp of the script
# Read in relation source variables
(username, password, connection_string) = Get_Smart_Training_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_GS_TRAVEL1, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_GS_TRAVEL1 = spark.sql(f"""SELECT
GS_TRAVEL_ID,
SYS_MODIFIED_TSTMP,
LOAD_DT
FROM {sensitive}.legacy_GS_TRAVEL""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GS_TRAVEL1 = SQ_Shortcut_to_GS_TRAVEL1 \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAVEL1.columns[0],'GS_TRAVEL_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAVEL1.columns[1],'SYS_MODIFIED_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAVEL1.columns[2],'LOAD_DT')

# COMMAND ----------

# Processing node SQ_Shortcut_to_GS_Travel, type SOURCE 
# COLUMN COUNT: 29

SQ_Shortcut_to_GS_Travel = jdbcSqlServerConnection(f"""(SELECT
travel.TravelID,
travel.AssocContactPhone,
travel.HotelPartnerStoreCityTo,
travel.AirlineDepartDate,
travel.AirlineDepartTime,
travel.AirlineReturnDate,
travel.AirlineReturnTime,
travel.AirlineSeatPreference,
travel.HotelCheckinDate,
travel.HotelCheckoutDate,
travel.HotelSmokingText,
travel.CarPickupDate,
travel.CarPickupTime,
travel.CarReturnDate,
travel.DriversLicense,
travel.LegalName,
travel.BirthDate,
travel.Gender,
travel.PreferredHotel,
travel.PreferredHotelAddress,
travel.PreferredHotelPhone,
travel.PreferredHotelContact,
travel.PreferredHotelRate,
travel.PreferredRoomMate,
travel.ModifiedBy,
travel.HomeStoreCityTo,
travel.TrainingStoreCityTo,
train.DateCreated,
train.DateModified
FROM GetSmartTraining_V1.dbo.GS_Training train, GetSmartTraining_V1.dbo.GS_Travel travel
WHERE train.TravelID = travel.TravelID) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GS_Travel = SQ_Shortcut_to_GS_Travel \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[0],'TravelID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[1],'AssocContactPhone') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[2],'HotelPartnerStoreCityTo') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[3],'AirlineDepartDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[4],'AirlineDepartTime') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[5],'AirlineReturnDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[6],'AirlineReturnTime') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[7],'AirlineSeatPreference') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[8],'HotelCheckinDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[9],'HotelCheckoutDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[10],'HotelSmokingText') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[11],'CarPickupDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[12],'CarPickupTime') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[13],'CarReturnDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[14],'DriversLicense') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[15],'LegalName') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[16],'BirthDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[17],'Gender') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[18],'PreferredHotel') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[19],'PreferredHotelAddress') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[20],'PreferredHotelPhone') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[21],'PreferredHotelContact') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[22],'PreferredHotelRate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[23],'PreferredRoomMate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[24],'ModifiedBy') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[25],'HomeStoreCityTo') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[26],'TrainingStoreCityTo') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[27],'DateCreated') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Travel.columns[28],'DateModified')

# COMMAND ----------

# Processing node JNR_ID, type JOINER 
# COLUMN COUNT: 32

JNR_ID = SQ_Shortcut_to_GS_Travel.join(SQ_Shortcut_to_GS_TRAVEL1,[SQ_Shortcut_to_GS_Travel.TravelID == SQ_Shortcut_to_GS_TRAVEL1.GS_TRAVEL_ID],'fullouter')

# COMMAND ----------

# Processing node EXP_Travel, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
JNR_ID_temp = JNR_ID.toDF(*["JNR_ID___" + col for col in JNR_ID.columns])

EXP_Travel = JNR_ID_temp.selectExpr(
	"JNR_ID___TravelID as TravelID",
	"JNR_ID___AssocContactPhone as AssocContactPhone",
	"JNR_ID___HotelPartnerStoreCityTo as HotelPartnerStoreCityTo",
	"JNR_ID___AirlineDepartDate as AirlineDepartDate",
	"JNR_ID___AirlineDepartTime as AirlineDepartTime",
	"JNR_ID___AirlineReturnDate as AirlineReturnDate",
	"JNR_ID___AirlineReturnTime as AirlineReturnTime",
	"JNR_ID___AirlineSeatPreference as AirlineSeatPreference",
	"JNR_ID___HotelCheckinDate as HotelCheckinDate",
	"JNR_ID___HotelCheckoutDate as HotelCheckoutDate",
	"JNR_ID___HotelSmokingText as HotelSmokingText",
	"JNR_ID___CarPickupDate as CarPickupDate",
	"JNR_ID___CarPickupTime as CarPickupTime",
	"JNR_ID___CarReturnDate as CarReturnDate",
	"JNR_ID___DriversLicense as DriversLicense",
	"JNR_ID___LegalName as LegalName",
	"JNR_ID___BirthDate as BirthDate",
	"JNR_ID___Gender as Gender",
	"JNR_ID___PreferredHotel as PreferredHotel",
	"JNR_ID___PreferredHotelAddress as PreferredHotelAddress",
	"JNR_ID___PreferredHotelPhone as PreferredHotelPhone",
	"JNR_ID___PreferredHotelContact as PreferredHotelContact",
	"JNR_ID___PreferredHotelRate as PreferredHotelRate",
	"JNR_ID___PreferredRoomMate as PreferredRoomMate",
	"JNR_ID___ModifiedBy as ModifiedBy",
	"JNR_ID___HomeStoreCityTo as HomeStoreCityTo",
	"JNR_ID___TrainingStoreCityTo as TrainingStoreCityTo",
	"JNR_ID___DateCreated as DateCreated",
	"JNR_ID___DateModified as DateModified",
	"JNR_ID___GS_TRAVEL_ID as GS_TRAVEL_ID",
	"JNR_ID___SYS_MODIFIED_TSTMP as SYS_MODIFIED_TSTMP",
	"JNR_ID___LOAD_DT as i_LOAD_DT").selectExpr(
	# "JNR_ID___sys_row_id as sys_row_id",
	"TravelID as TravelID",
	"AssocContactPhone as AssocContactPhone",
	"HotelPartnerStoreCityTo as HotelPartnerStoreCityTo",
	"AirlineDepartDate as AirlineDepartDate",
	"AirlineDepartTime as AirlineDepartTime",
	"AirlineReturnDate as AirlineReturnDate",
	"AirlineReturnTime as AirlineReturnTime",
	"AirlineSeatPreference as AirlineSeatPreference",
	"HotelCheckinDate as HotelCheckinDate",
	"HotelCheckoutDate as HotelCheckoutDate",
	"HotelSmokingText as HotelSmokingText",
	"CarPickupDate as CarPickupDate",
	"CarPickupTime as CarPickupTime",
	"CarReturnDate as CarReturnDate",
	"DriversLicense as DriversLicense",
	"LegalName as LegalName",
	"BirthDate as BirthDate",
	"Gender as Gender",
	"PreferredHotel as PreferredHotel",
	"PreferredHotelAddress as PreferredHotelAddress",
	"PreferredHotelPhone as PreferredHotelPhone",
	"PreferredHotelContact as PreferredHotelContact",
	"PreferredHotelRate as PreferredHotelRate",
	"PreferredRoomMate as PreferredRoomMate",
	"ModifiedBy as ModifiedBy",
	"HomeStoreCityTo as HomeStoreCityTo",
	"TrainingStoreCityTo as TrainingStoreCityTo",
	"DateModified as DateModified",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"IF (i_LOAD_DT IS NULL , CURRENT_TIMESTAMP, i_LOAD_DT) as LOAD_DT",
	"IF (GS_TRAVEL_ID IS NULL, 0, IF (IF (DateModified IS NULL, TO_DATE ( '01/01/2000' , 'M/d/yyyy' ), DateModified) <> IF (SYS_MODIFIED_TSTMP IS NULL, TO_DATE ( '01/01/2000' , 'M/d/yyyy' ), SYS_MODIFIED_TSTMP), 1, 3)) as UpdateStrategy"
)

# COMMAND ----------

# Processing node FIL_Strategy, type FILTER 
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
EXP_Travel_temp = EXP_Travel.toDF(*["EXP_Travel___" + col for col in EXP_Travel.columns])

FIL_Strategy = EXP_Travel_temp.selectExpr(
	"EXP_Travel___TravelID as TravelID",
	"EXP_Travel___AssocContactPhone as AssocContactPhone",
	"EXP_Travel___HotelPartnerStoreCityTo as HotelPartnerStoreCityTo",
	"EXP_Travel___AirlineDepartDate as AirlineDepartDate",
	"EXP_Travel___AirlineDepartTime as AirlineDepartTime",
	"EXP_Travel___AirlineReturnDate as AirlineReturnDate",
	"EXP_Travel___AirlineReturnTime as AirlineReturnTime",
	"EXP_Travel___AirlineSeatPreference as AirlineSeatPreference",
	"EXP_Travel___HotelCheckinDate as HotelCheckinDate",
	"EXP_Travel___HotelCheckoutDate as HotelCheckoutDate",
	"EXP_Travel___HotelSmokingText as HotelSmokingText",
	"EXP_Travel___CarPickupDate as CarPickupDate",
	"EXP_Travel___CarPickupTime as CarPickupTime",
	"EXP_Travel___CarReturnDate as CarReturnDate",
	"EXP_Travel___DriversLicense as DriversLicense",
	"EXP_Travel___LegalName as LegalName",
	"EXP_Travel___BirthDate as BirthDate",
	"EXP_Travel___Gender as Gender",
	"EXP_Travel___PreferredHotel as PreferredHotel",
	"EXP_Travel___PreferredHotelAddress as PreferredHotelAddress",
	"EXP_Travel___PreferredHotelPhone as PreferredHotelPhone",
	"EXP_Travel___PreferredHotelContact as PreferredHotelContact",
	"EXP_Travel___PreferredHotelRate as PreferredHotelRate",
	"EXP_Travel___PreferredRoomMate as PreferredRoomMate",
	"EXP_Travel___ModifiedBy as ModifiedBy",
	"EXP_Travel___HomeStoreCityTo as HomeStoreCityTo",
	"EXP_Travel___TrainingStoreCityTo as TrainingStoreCityTo",
	"EXP_Travel___DateModified as DateModified",
	"EXP_Travel___UPDATE_DT as UPDATE_DT",
	"EXP_Travel___LOAD_DT as LOAD_DT",
	"EXP_Travel___UpdateStrategy as UpdateStrategy").filter("UpdateStrategy != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node UPD_Strategy, type UPDATE_STRATEGY 
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
FIL_Strategy_temp = FIL_Strategy.toDF(*["FIL_Strategy___" + col for col in FIL_Strategy.columns])

UPD_Strategy = FIL_Strategy_temp.selectExpr(
	"FIL_Strategy___TravelID as TravelID",
	"FIL_Strategy___AssocContactPhone as AssocContactPhone",
	"FIL_Strategy___HotelPartnerStoreCityTo as HotelPartnerStoreCityTo",
	"FIL_Strategy___AirlineDepartDate as AirlineDepartDate",
	"FIL_Strategy___AirlineDepartTime as AirlineDepartTime",
	"FIL_Strategy___AirlineReturnDate as AirlineReturnDate",
	"FIL_Strategy___AirlineReturnTime as AirlineReturnTime",
	"FIL_Strategy___AirlineSeatPreference as AirlineSeatPreference",
	"FIL_Strategy___HotelCheckinDate as HotelCheckinDate",
	"FIL_Strategy___HotelCheckoutDate as HotelCheckoutDate",
	"FIL_Strategy___HotelSmokingText as HotelSmokingText",
	"FIL_Strategy___CarPickupDate as CarPickupDate",
	"FIL_Strategy___CarPickupTime as CarPickupTime",
	"FIL_Strategy___CarReturnDate as CarReturnDate",
	"FIL_Strategy___DriversLicense as DriversLicense",
	"FIL_Strategy___LegalName as LegalName",
	"FIL_Strategy___BirthDate as BirthDate",
	"FIL_Strategy___Gender as Gender",
	"FIL_Strategy___PreferredHotel as PreferredHotel",
	"FIL_Strategy___PreferredHotelAddress as PreferredHotelAddress",
	"FIL_Strategy___PreferredHotelPhone as PreferredHotelPhone",
	"FIL_Strategy___PreferredHotelContact as PreferredHotelContact",
	"FIL_Strategy___PreferredHotelRate as PreferredHotelRate",
	"FIL_Strategy___PreferredRoomMate as PreferredRoomMate",
	"FIL_Strategy___ModifiedBy as ModifiedBy",
	"FIL_Strategy___HomeStoreCityTo as HomeStoreCityTo",
	"FIL_Strategy___TrainingStoreCityTo as TrainingStoreCityTo",
	"FIL_Strategy___DateModified as DateModified",
	"FIL_Strategy___UPDATE_DT as UPDATE_DT",
	"FIL_Strategy___LOAD_DT as LOAD_DT",
	"FIL_Strategy___UpdateStrategy as UpdateStrategy") \
	.withColumn('pyspark_data_action', col('UpdateStrategy'))

# COMMAND ----------

# Processing node Shortcut_to_GS_TRAVEL1, type TARGET 
# COLUMN COUNT: 30


Shortcut_to_GS_TRAVEL1 = UPD_Strategy.selectExpr(
	"CAST(TRAVELID as bigint) as GS_TRAVEL_ID",
	"CAST(AssocContactPhone AS STRING) as ASSOC_CONTACT_PHN",
	"CAST(HotelPartnerStoreCityTo AS STRING) as HOTEL_PARTNER_STORE_CITY_TO",
	"CAST(AirlineDepartDate AS TIMESTAMP) as AIRLINE_DEPART_DT",
	"CAST(AirlineDepartTime AS STRING) as ARILINE_DEPART_TM",
	"CAST(AirlineReturnDate AS TIMESTAMP) as AIRLINE_RETURN_DT",
	"CAST(AirlineReturnTime AS STRING) as ARILINE_RETURN_TM",
	"CAST(AirlineSeatPreference AS STRING) as AIRLINE_SEAT_PREFERENCE",
	"CAST(HotelCheckinDate AS TIMESTAMP) as HOTEL_CHECKIN_DT",
	"CAST(HotelCheckoutDate AS TIMESTAMP) as HOTEL_CHECKOUT_DT",
	"CAST(HotelSmokingText AS STRING) as HOTEL_SMOKING_TEXT",
	"CAST(CarPickupDate AS TIMESTAMP) as CAR_PICKUP_DT",
	"CAST(CarPickupTime AS TIMESTAMP) as CAR_PICKUP_TM",
	"CAST(CarReturnDate AS TIMESTAMP) as CAR_RETURN_DT",
	"CAST(DriversLicense AS STRING) as DRIVERS_LICENSE",
	"CAST(LegalName AS STRING) as LEGAL_NAME",
	"CAST(BirthDate AS TIMESTAMP) as BIRTH_DT",
	"CAST(GENDER as tinyint) as GENDER",
	"CAST(PreferredHotel AS STRING) as PREFERRED_HOTEL",
	"CAST(PreferredHotelAddress AS STRING) as PREFERRED_HOTEL_ADDRESS",
	"CAST(PreferredHotelPhone AS STRING) as PREFERRED_HOTEL_PHONE",
	"CAST(PreferredHotelContact AS STRING) as PREFERRED_HOTEL_CONTACT",
	"CAST(PreferredHotelRate AS STRING) as PREFERRED_HOTEL_RATE",
	"CAST(PreferredRoomMate AS STRING) as PREFERRED_ROOMMATE",
	"CAST(HomeStoreCityTo AS STRING) as HOME_STORE_CITY_TO",
	"CAST(TrainingStoreCityTo AS STRING) as TRAINING_STORE_CITY_TO",
	"CAST(DateModified AS TIMESTAMP) as SYS_MODIFIED_TSTMP",
	"CAST(ModifiedBy AS STRING) as SYS_MODIFIED_BY",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
).filter(col('GS_TRAVEL_ID').isNotNull())

try:
  primary_key = """source.GS_TRAVEL_ID = target.GS_TRAVEL_ID"""
  refined_perf_table = f"{sensitive}.legacy_GS_TRAVEL"
  chk=DuplicateChecker()
  chk.check_for_duplicate_primary_keys(Shortcut_to_GS_TRAVEL1,["GS_TRAVEL_ID"])
  executeMerge(Shortcut_to_GS_TRAVEL1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed")
  logPrevRunDt("GS_TRAVEL", "GS_TRAVEL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("GS_TRAVEL", "GS_TRAVEL","Failed",str(e), f"{raw}.log_run_details", )
  raise e
