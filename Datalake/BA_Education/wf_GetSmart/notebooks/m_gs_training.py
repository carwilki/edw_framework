# Databricks notebook source
#Code converted on 2023-10-18 17:16:40
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

# Set global variables
starttime = datetime.now() #start timestamp of the script
# Read in relation source variables
(username, password, connection_string) = Get_Smart_Training_prd_sqlServer(env)

# COMMAND ----------

# Processing node LKP_SITE_PROFILE_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 1

LKP_SITE_PROFILE_SRC = spark.sql(f"""SELECT
LOCATION_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE""")
# Conforming fields names to the component layout
# LKP_SITE_PROFILE_SRC = LKP_SITE_PROFILE_SRC \
# 	.withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[0],'')

# COMMAND ----------

#  LKP_SITE_PROFILE_SRC.show()

# COMMAND ----------

# Processing node SQ_Shortcut_to_GS_Training, type SOURCE 
# COLUMN COUNT: 27

SQ_Shortcut_to_GS_Training = jdbcSqlServerConnection(f"""(SELECT
ID,
TravelID,
HomeStoreID,
AssociateID,
AssociateFirstName,
AssociateLastName,
NewPosition,
PrevPosition,
SalonAcademyID,
DM,
SalonSafetyCertiDate,
SplashDate,
TrainingStoreID,
DateHSTrainingStart,
DateHSTrainingEnd,
DateTrainingStart,
DateTrainingEnd,
GroomingToolKit,
HotelPartnerStoreID,
HotelPartnerTrainingStartDate,
HotelPartnerTrainingEndDate,
TrainingStatus,
TrainingType,
DateCreated,
CreatedBy,
ModifiedBy,
DateModified
FROM GetSmartTraining_V1.dbo.GS_Training) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GS_Training = SQ_Shortcut_to_GS_Training \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[0],'ID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[1],'TravelID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[2],'HomeStoreID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[3],'AssociateID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[4],'AssociateFirstName') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[5],'AssociateLastName') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[6],'NewPosition') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[7],'PrevPosition') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[8],'SalonAcademyID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[9],'DM') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[10],'SalonSafetyCertiDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[11],'SplashDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[12],'TrainingStoreID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[13],'DateHSTrainingStart') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[14],'DateHSTrainingEnd') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[15],'DateTrainingStart') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[16],'DateTrainingEnd') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[17],'GroomingToolKit') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[18],'HotelPartnerStoreID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[19],'HotelPartnerTrainingStartDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[20],'HotelPartnerTrainingEndDate') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[21],'TrainingStatus') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[22],'TrainingType') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[23],'DateCreated') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[24],'CreatedBy') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[25],'ModifiedBy') \
	.withColumnRenamed(SQ_Shortcut_to_GS_Training.columns[26],'DateModified')

# COMMAND ----------

# Processing node SQ_Shortcut_to_GS_TRAINING1, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_GS_TRAINING1 = spark.sql(f"""SELECT
GS_TRAINING_ID,
GS_TRAINING_STATUS_ID,
GS_TRAINING_TYPE_ID,
SYS_MODIFIED_TSTMP,
LOAD_DT
FROM {legacy}.GS_TRAINING""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GS_TRAINING1 = SQ_Shortcut_to_GS_TRAINING1 \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING1.columns[0],'GS_TRAINING_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING1.columns[1],'GS_TRAINING_STATUS_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING1.columns[2],'GS_TRAINING_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING1.columns[3],'SYS_MODIFIED_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING1.columns[4],'LOAD_DT')

# COMMAND ----------

# Processing node JNR_ID, type JOINER 
# COLUMN COUNT: 32
LKP_home = LKP_SITE_PROFILE_SRC.toDF(*["LKP___home___" + col for col in LKP_SITE_PROFILE_SRC.columns])
LKP_train = LKP_SITE_PROFILE_SRC.toDF(*["LKP___train___" + col for col in LKP_SITE_PROFILE_SRC.columns])
LKP_hotel = LKP_SITE_PROFILE_SRC.toDF(*["LKP___hotel___" + col for col in LKP_SITE_PROFILE_SRC.columns])
JNR_ID = SQ_Shortcut_to_GS_Training.join(SQ_Shortcut_to_GS_TRAINING1,[SQ_Shortcut_to_GS_Training.ID == SQ_Shortcut_to_GS_TRAINING1.GS_TRAINING_ID],'left_outer')\
        .join(LKP_home,[SQ_Shortcut_to_GS_Training.HomeStoreID == LKP_home.LKP___home___LOCATION_ID],'left')\
        .join(LKP_train,[SQ_Shortcut_to_GS_Training.TrainingStoreID == LKP_train.LKP___train___LOCATION_ID],'left')\
        .join(LKP_hotel,[SQ_Shortcut_to_GS_Training.TrainingStoreID == LKP_hotel.LKP___hotel___LOCATION_ID],'left')            

# COMMAND ----------

# JNR_ID.show(truncate=False)

# COMMAND ----------

JNR_ID_temp = JNR_ID.toDF(*["JNR_ID___" + col for col in JNR_ID.columns])

# COMMAND ----------

# Processing node EXP_Location, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 37

# for each involved DataFrame, append the dataframe name to each column
# LKP_temp = LKP.toDF(*["LKP___" + col for col in LKP.columns])
JNR_ID_temp = JNR_ID.toDF(*["JNR_ID___" + col for col in JNR_ID.columns])

EXP_Location = JNR_ID_temp.selectExpr(
	"JNR_ID___ID as ID",
	"JNR_ID___TravelID as TravelID",
	"JNR_ID___HomeStoreID as i_HomeStoreID",
	"JNR_ID___AssociateID as AssociateID",
	"JNR_ID___AssociateFirstName as AssociateFirstName",
	"JNR_ID___AssociateLastName as AssociateLastName",
	"JNR_ID___NewPosition as NewPosition",
	"JNR_ID___PrevPosition as PrevPosition",
	"JNR_ID___SalonAcademyID as SalonAcademyID",
	"JNR_ID___DM as DM",
	"JNR_ID___SalonSafetyCertiDate as SalonSafetyCertiDate",
	"JNR_ID___SplashDate as SplashDate",
	"JNR_ID___TrainingStoreID as i_TrainingStoreID",
	"JNR_ID___DateHSTrainingStart as DateHSTrainingStart",
	"JNR_ID___DateHSTrainingEnd as DateHSTrainingEnd",
	"JNR_ID___DateTrainingStart as DateTrainingStart",
	"JNR_ID___DateTrainingEnd as DateTrainingEnd",
	"JNR_ID___GroomingToolKit as GroomingToolKit",
	"JNR_ID___HotelPartnerStoreID as i_HotelPartnerStoreID",
	"JNR_ID___HotelPartnerTrainingStartDate as HotelPartnerTrainingStartDate",
	"JNR_ID___HotelPartnerTrainingEndDate as HotelPartnerTrainingEndDate",
	"JNR_ID___TrainingStatus as TrainingStatus",
	"JNR_ID___TrainingType as TrainingType",
	"JNR_ID___DateCreated as DateCreated",
	"JNR_ID___CreatedBy as CreatedBy",
	"JNR_ID___ModifiedBy as ModifiedBy",
	"JNR_ID___DateModified as DateModified",
	"JNR_ID___GS_TRAINING_ID as GS_TRAINING_ID",
	"JNR_ID___SYS_MODIFIED_TSTMP as SYS_MODIFIED_TSTMP",
	"JNR_ID___GS_TRAINING_STATUS_ID as GS_TRAINING_STATUS_ID",
	"JNR_ID___GS_TRAINING_TYPE_ID as GS_TRAINING_TYPE_ID",
	"JNR_ID___LOAD_DT as i_LOAD_DT",
 	"JNR_ID___LKP___home___STORE_NBR as HOME_STORE_NBR",
  	"JNR_ID___LKP___train___STORE_NBR as TRAINING_STORE_NBR",
  	"JNR_ID___LKP___hotel___STORE_NBR as HOTEL_PARTNER_STORE_NBR",).selectExpr(
	# "JNR_ID___sys_row_id as sys_row_id",
	"ID as ID",
	"TravelID as TravelID",
	"i_HomeStoreID as HomeStoreID",
	"CAST(i_HomeStoreID as int) as i_HomeStoreID",
	"HOME_STORE_NBR as HOME_STORE_NBR",
	"IF (cast(AssociateID as double) IS NOT NULL, cast(AssociateID as int), 0) as EMPLOYEE_ID",
	"AssociateFirstName as AssociateFirstName",
	"AssociateLastName as AssociateLastName",
	"NewPosition as NewPosition",
	"PrevPosition as PrevPosition",
	"SalonAcademyID as SalonAcademyID",
	"DM as DM",
	"SalonSafetyCertiDate as SalonSafetyCertiDate",
	"SplashDate as SplashDate",
	"i_TrainingStoreID as TrainingStoreID",
	"cast(i_TrainingStoreID as int) as i_TrainingStoreID",
	"TRAINING_STORE_NBR as TRAINING_STORE_NBR",
	"DateHSTrainingStart as DateHSTrainingStart",
	"DateHSTrainingEnd as DateHSTrainingEnd",
	"DateTrainingStart as DateTrainingStart",
	"DateTrainingEnd as DateTrainingEnd",
	"GroomingToolKit as GroomingToolKit",
	"i_HotelPartnerStoreID as HotelPartnerStoreID",
  	"cast(i_HotelPartnerStoreID as int) as i_HotelPartnerStoreID",
	"HOTEL_PARTNER_STORE_NBR as HOTEL_PARTNER_STORE_NBR",
	"HotelPartnerTrainingStartDate as HotelPartnerTrainingStartDate",
	"HotelPartnerTrainingEndDate as HotelPartnerTrainingEndDate",
	"TrainingStatus as TrainingStatus",
	"TrainingType as TrainingType",
	"DateCreated as DateCreated",
	"CreatedBy as CreatedBy",
	"ModifiedBy as ModifiedBy",
	"DateModified as DateModified",
	"GS_TRAINING_ID as GS_TRAINING_ID",
	"SYS_MODIFIED_TSTMP as SYS_MODIFIED_TSTMP",
	"GS_TRAINING_STATUS_ID as GS_TRAINING_STATUS_ID",
	"GS_TRAINING_TYPE_ID as GS_TRAINING_TYPE_ID",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"IF (i_LOAD_DT IS NULL , CURRENT_TIMESTAMP, i_LOAD_DT) as LOAD_DT",
	"IF (GS_TRAINING_ID IS NULL, 0, IF (IF (DateModified IS NULL, TO_DATE ( '01/01/2000' , 'mm/dd/yyyy' ), DateModified) <> IF (SYS_MODIFIED_TSTMP IS NULL, TO_DATE ( '01/01/2000' , 'mm/dd/yyyy' ), SYS_MODIFIED_TSTMP) OR IF (TrainingStatus IS NULL, -1, TrainingStatus) <> IF (GS_TRAINING_STATUS_ID IS NULL, -1, GS_TRAINING_STATUS_ID) OR IF (TrainingType IS NULL, -1, TrainingType) <> IF (GS_TRAINING_TYPE_ID IS NULL, -1, GS_TRAINING_TYPE_ID), 1, 3)) as UpdateStrategy"
)

# Adding deferred logic for dataframe EXP_Location, column HOME_STORE_NBR
# EXP_Location1 = EXP_Location.join(LKP_SITE_PROFILE_SRC,
# 	(LKP_SITE_PROFILE_SRC.LOCATION_ID == EXP_Location.i_HomeStoreID),'left').select(EXP_Location['*'], LKP_SITE_PROFILE_SRC['STORE_NBR'].alias('HOME_STORE_NBR') )
# EXP_Location = EXP_Location.withColumnRenamed('ULKP_RETURN_1','HOME_STORE_NBR')

# Adding deferred logic for dataframe EXP_Location, column TRAINING_STORE_NBR
# EXP_Location2 = EXP_Location1.join(LKP_SITE_PROFILE_SRC,
# 	(LKP_SITE_PROFILE_SRC.LOCATION_ID == EXP_Location1.i_TrainingStoreID),'left').select(EXP_Location1['*'], LKP_SITE_PROFILE_SRC['STORE_NBR'].alias('TRAINING_STORE_NBR') )
# EXP_Location = EXP_Location.withColumnRenamed('ULKP_RETURN_2' , 'TRAINING_STORE_NBR')

# # Adding deferred logic for dataframe EXP_Location, column HOTEL_PARTNER_STORE_NBR
# EXP_Location = EXP_Location.join(LKP_SITE_PROFILE_SRC,
# 	(LKP_SITE_PROFILE_SRC.LOCATION_ID == EXP_Location.i_HotelPartnerStoreID),'left').select(EXP_Location['*'], LKP_SITE_PROFILE_SRC['STORE_NBR'].alias('ULKP_RETURN_3') )
# EXP_Location = EXP_Location.withColumn('HOTEL_PARTNER_STORE_NBR',col('ULKP_RETURN_3'))

# COMMAND ----------

# EXP_Location.show()

# COMMAND ----------

# Processing node FIL_Strategy, type FILTER 
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
EXP_Location_temp = EXP_Location.toDF(*["EXP_Location___" + col for col in EXP_Location.columns])

FIL_Strategy = EXP_Location_temp.selectExpr(
	"EXP_Location___ID as ID",
	"EXP_Location___TravelID as TravelID",
	"EXP_Location___HomeStoreID as HomeStoreID",
	"EXP_Location___HOME_STORE_NBR as HOME_STORE_NBR",
	"EXP_Location___EMPLOYEE_ID as EMPLOYEE_ID",
	"EXP_Location___AssociateFirstName as AssociateFirstName",
	"EXP_Location___AssociateLastName as AssociateLastName",
	"EXP_Location___NewPosition as NewPosition",
	"EXP_Location___PrevPosition as PrevPosition",
	"EXP_Location___SalonAcademyID as SalonAcademyID",
	"EXP_Location___DM as DM",
	"EXP_Location___SalonSafetyCertiDate as SalonSafetyCertiDate",
	"EXP_Location___SplashDate as SplashDate",
	"EXP_Location___TrainingStoreID as TrainingStoreID",
	"EXP_Location___TRAINING_STORE_NBR as TRAINING_STORE_NBR",
	"EXP_Location___DateHSTrainingStart as DateHSTrainingStart",
	"EXP_Location___DateHSTrainingEnd as DateHSTrainingEnd",
	"EXP_Location___DateTrainingStart as DateTrainingStart",
	"EXP_Location___DateTrainingEnd as DateTrainingEnd",
	"EXP_Location___GroomingToolKit as GroomingToolKit",
	"EXP_Location___HotelPartnerStoreID as HotelPartnerStoreID",
	"EXP_Location___HOTEL_PARTNER_STORE_NBR as HOTEL_PARTNER_STORE_NBR",
	"EXP_Location___HotelPartnerTrainingStartDate as HotelPartnerTrainingStartDate",
	"EXP_Location___HotelPartnerTrainingEndDate as HotelPartnerTrainingEndDate",
	"EXP_Location___TrainingStatus as TrainingStatus",
	"EXP_Location___TrainingType as TrainingType",
	"EXP_Location___DateCreated as DateCreated",
	"EXP_Location___CreatedBy as CreatedBy",
	"EXP_Location___ModifiedBy as ModifiedBy",
	"EXP_Location___DateModified as DateModified",
	"EXP_Location___UPDATE_DT as UPDATE_DT",
	"EXP_Location___LOAD_DT as LOAD_DT",
	"EXP_Location___UpdateStrategy as UpdateStrategy").filter("UpdateStrategy != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node UPD_Strategy, type UPDATE_STRATEGY 
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
FIL_Strategy_temp = FIL_Strategy.toDF(*["FIL_Strategy___" + col for col in FIL_Strategy.columns])

UPD_Strategy = FIL_Strategy_temp.selectExpr(
	"FIL_Strategy___ID as ID",
	"FIL_Strategy___TravelID as TravelID",
	"FIL_Strategy___HomeStoreID as HomeStoreID",
	"FIL_Strategy___HOME_STORE_NBR as HOME_STORE_NBR",
	"FIL_Strategy___EMPLOYEE_ID as EMPLOYEE_ID",
	"FIL_Strategy___AssociateFirstName as AssociateFirstName",
	"FIL_Strategy___AssociateLastName as AssociateLastName",
	"FIL_Strategy___NewPosition as NewPosition",
	"FIL_Strategy___PrevPosition as PrevPosition",
	"FIL_Strategy___SalonAcademyID as SalonAcademyID",
	"FIL_Strategy___DM as DM",
	"FIL_Strategy___SalonSafetyCertiDate as SalonSafetyCertiDate",
	"FIL_Strategy___SplashDate as SplashDate",
	"FIL_Strategy___TrainingStoreID as TrainingStoreID",
	"FIL_Strategy___TRAINING_STORE_NBR as TRAINING_STORE_NBR",
	"FIL_Strategy___DateHSTrainingStart as DateHSTrainingStart",
	"FIL_Strategy___DateHSTrainingEnd as DateHSTrainingEnd",
	"FIL_Strategy___DateTrainingStart as DateTrainingStart",
	"FIL_Strategy___DateTrainingEnd as DateTrainingEnd",
	"FIL_Strategy___GroomingToolKit as GroomingToolKit",
	"FIL_Strategy___HotelPartnerStoreID as HotelPartnerStoreID",
	"FIL_Strategy___HOTEL_PARTNER_STORE_NBR as HOTEL_PARTNER_STORE_NBR",
	"FIL_Strategy___HotelPartnerTrainingStartDate as HotelPartnerTrainingStartDate",
	"FIL_Strategy___HotelPartnerTrainingEndDate as HotelPartnerTrainingEndDate",
	"FIL_Strategy___TrainingStatus as TrainingStatus",
	"FIL_Strategy___TrainingType as TrainingType",
	"FIL_Strategy___DateCreated as DateCreated",
	"FIL_Strategy___CreatedBy as CreatedBy",
	"FIL_Strategy___ModifiedBy as ModifiedBy",
	"FIL_Strategy___DateModified as DateModified",
	"FIL_Strategy___UPDATE_DT as UPDATE_DT",
	"FIL_Strategy___LOAD_DT as LOAD_DT",
	"FIL_Strategy___UpdateStrategy as UpdateStrategy") \
	.withColumn('pyspark_data_action', col('UpdateStrategy'))

# COMMAND ----------

# Processing node Shortcut_to_GS_TRAINING1, type TARGET 
# COLUMN COUNT: 32


Shortcut_to_GS_TRAINING1 = UPD_Strategy.selectExpr(
	"CAST(ID as bigint) as GS_TRAINING_ID",
	"CAST(TRAVELID as bigint) as GS_TRAVEL_ID",
	"CAST(HomeStoreID AS INT) as HOME_LOCATION_ID",
	"CAST(HOME_STORE_NBR AS INT) as HOME_STORE_NBR",
	"CAST(EMPLOYEE_ID AS INT) as EMPLOYEE_ID",
	"CAST(AssociateFirstName AS STRING) as EMPLOYEE_FIRST_NAME",
	"CAST(AssociateLastName AS STRING) as EMPLOYEE_LAST_NAME",
	"CAST(NewPosition AS INT) as NEW_GS_POSITION_ID",
	"CAST(PrevPosition AS INT) as PREV_GS_POSITION_ID",
	"CAST(SalonAcademyID as bigint) as GS_SALON_ACADEMY_ID",
	"CAST(DM AS STRING) as DISTRICT_MANAGER_NAME",
	"CAST(SalonSafetyCertiDate AS TIMESTAMP) as SALON_SAFETY_CERT_DT",
	"CAST(SplashDate AS TIMESTAMP) as SPLASH_DT",
	"CAST(TrainingStoreID AS INT) as TRAINING_LOCATION_ID",
	"CAST(TRAINING_STORE_NBR AS INT) as TRAINING_STORE_NBR",
	"CAST(DateHSTrainingStart AS TIMESTAMP) as HS_TRAINING_START_DT",
	"CAST(DateHSTrainingEnd AS TIMESTAMP) as HS_TRAINING_END_DT",
	"CAST(DateTrainingStart AS TIMESTAMP) as TRAINING_START_DT",
	"CAST(DateTrainingEnd AS TIMESTAMP) as TRAINING_END_DT",
	"CAST(GroomingToolKit AS INT) as GROOMING_TOOL_KIT",
	"CAST(HotelPartnerStoreID AS INT) as HOTEL_PARTNER_LOCATION_ID",
	"CAST(HOTEL_PARTNER_STORE_NBR AS INT) as HOTEL_PARTNER_STORE_NBR",
	"CAST(HotelPartnerTrainingStartDate AS TIMESTAMP) as HOTEL_PARTNER_TRAINING_START_DT",
	"CAST(HotelPartnerTrainingEndDate AS TIMESTAMP) as HOTEL_PARTNER_TRAINING_END_DT",
	"CAST(TrainingStatus AS INT) as GS_TRAINING_STATUS_ID",
	"CAST(TrainingType AS INT) as GS_TRAINING_TYPE_ID",
	"CAST(DateCreated AS TIMESTAMP) as SYS_CREATED_TSTMP",
	"CAST(CreatedBy AS STRING) as SYS_CREATED_BY",
	"CAST(DateModified AS TIMESTAMP) as SYS_MODIFIED_TSTMP",
	"CAST(ModifiedBy AS STRING) as SYS_MODIFIED_BY",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.GS_TRAINING_ID = target.GS_TRAINING_ID"""
  refined_perf_table = f"{legacy}.GS_TRAINING"
  chk=DuplicateChecker()
  chk.check_for_duplicate_primary_keys(Shortcut_to_GS_TRAINING1,["GS_TRAINING_ID"])
  executeMerge(Shortcut_to_GS_TRAINING1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed")
  logPrevRunDt("GS_TRAINING", "GS_TRAINING", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("GS_TRAINING", "GS_TRAINING","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------


