# This code has been manually updated (significantly) as per https://petsmart.atlassian.net/browse/DNM-5906
#Code converted on 2023-08-03 13:28:12
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
# env = 'dev'


if env is None or env == '':
    raise ValueError('env is not set')

sensitive = getEnvPrefix(env) + 'cust_sensitive'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
site_profile_table = f"{legacy}.SITE_PROFILE"
# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = mtx_prd_sqlServer(env)

# COMMAND ----------
# Variable_declaration_comment
CREATE_DATE = genPrevRunDt('legacy_PET_TRAINING_RESERVATION',sensitive,raw)
# CREATE_DATE='2023-07-09'
# COMMAND ----------
# Processing node LKP_SITE_PROFILE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 3

# LKP_SITE_PROFILE_SRC = jdbcSqlServerConnection(f"""SELECT
# LOCATION_ID,
# STORE_NBR
# FROM SITE_PROFILE""",username,password,connection_string)
# # Conforming fields names to the component layout
# LKP_SITE_PROFILE_SRC = LKP_SITE_PROFILE_SRC \
# 	.withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[0],'LOCATION_ID') \
# 	.withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[1],'STORE_NBR') \
# 	.withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[2],'i_STORE_NBR')


LKP_SITE_PROFILE_SRC = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node SQ_Shortcut_to_ClassType, type SOURCE 
# COLUMN COUNT: 4

# SQ_Shortcut_to_ClassType = jdbcSqlServerConnection(f"""(SELECT
# ClassType.ClassTypeId,
# ClassType.Name,
# ClassType.UPC,
# ClassType.Duration
# FROM Training.dbo.ClassType) as src """,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_Pet, type SOURCE 
# COLUMN COUNT: 3

# SQ_Shortcut_to_Pet = jdbcSqlServerConnection(f"""(SELECT
# Pet.PetId,
# Pet.Name,
# Pet.Breed
# FROM Training.dbo.Pet) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_Reservation, type SOURCE 
# COLUMN COUNT: 7

# SQ_Shortcut_to_Reservation = jdbcSqlServerConnection(f"""(SELECT
# Reservation.ReservationId,
# Reservation.CustomerId,
# Reservation.PetId,
# Reservation.StoreClassId,
# Reservation.StoreNumber,
# Reservation.EnrollmentDateTime,
# Reservation.CreateDateTime
# FROM Training.dbo.Reservation
# WHERE CreateDateTime>'{CREATE_DATE}') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

SQ_Shortcut_to_Reservation = jdbcSqlServerConnection(f"""(SELECT Reservation.ReservationId AS ReservationId
    ,Reservation.CustomerId AS CustomerId
    ,Reservation.PetId AS PetId
    ,Reservation.StoreClassId
    ,Reservation.StoreNumber
    ,Reservation.EnrollmentDateTime AS EnrollmentDateTime
    ,Reservation.CreateDateTime AS CreateDateTime
    ,Customer.FirstName AS FirstName
    ,Customer.LastName AS LastName
    ,Pet.Name AS Pet_Name
    ,Pet.Breed AS BREED
    ,StoreClass.ClassTypeId AS ClassTypeId
    ,StoreClass.StartDateTime AS StartDateTime
    ,StoreClass.TrainerId AS TrainerId
    ,ClassType.Name AS ClassTypeName
    ,Trainer.FormattedName AS TrainerName
    ,ClassType.UPC AS UPC
    ,ClassType.Duration AS Duration
FROM (
    (
        (
            (
                (
                    Training.dbo.Reservation Reservation LEFT OUTER JOIN Training.dbo.Customer Customer ON (Customer.CustomerId = Reservation.CustomerId)
                    ) LEFT OUTER JOIN Training.dbo.Pet Pet ON (Pet.PetId = Reservation.PetId)
                ) LEFT OUTER JOIN Training.dbo.StoreClass StoreClass ON (StoreClass.StoreClassId = Reservation.StoreClassId)
            ) LEFT OUTER JOIN Training.dbo.ClassType ClassType ON (ClassType.ClassTypeId = StoreClass.ClassTypeId)
        ) LEFT OUTER JOIN Training.dbo.Trainer Trainer ON (Trainer.TrainerId = StoreClass.TrainerId)
    )
WHERE (Reservation.CreateDateTime > '{CREATE_DATE}')
OR StoreClass.LastModified > '{CREATE_DATE}'
OR ClassType.LastModified > '{CREATE_DATE}')as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# display(SQ_Shortcut_to_Reservation)

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
# SQ_Shortcut_to_Reservation_temp = SQ_Shortcut_to_Reservation.toDF(*["SQ_Shortcut_to_Reservation___" + col for col in SQ_Shortcut_to_Reservation.columns])

# EXPTRANS = SQ_Shortcut_to_Reservation_temp.selectExpr( \
# 	"SQ_Shortcut_to_Reservation___sys_row_id as sys_row_id", \
# 	"SQ_Shortcut_to_Reservation___ReservationId as ReservationId", \
# 	"SQ_Shortcut_to_Reservation___CustomerId as CustomerId", \
# 	"SQ_Shortcut_to_Reservation___PetId as PetId", \
# 	"SQ_Shortcut_to_Reservation___StoreClassId as StoreClassId", \
# 	"SQ_Shortcut_to_Reservation___StoreNumber as StoreNumber", \
# 	"SQ_Shortcut_to_Reservation___EnrollmentDateTime as EnrollmentDateTime", \
# 	"SQ_Shortcut_to_Reservation___CreateDateTime as CreateDateTime" \
# )

# COMMAND ----------
# Processing node SQ_Shortcut_to_StoreClass, type SOURCE 
# COLUMN COUNT: 4

# SQ_Shortcut_to_StoreClass = jdbcSqlServerConnection(f"""(SELECT
# StoreClass.StoreClassId,
# StoreClass.ClassTypeId,
# StoreClass.StartDateTime,
# StoreClass.TrainerId
# FROM Training.dbo.StoreClass) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_Customer, type SOURCE 
# COLUMN COUNT: 3

# SQ_Shortcut_to_Customer = jdbcSqlServerConnection(f"""(SELECT
# Customer.CustomerId,
# Customer.FirstName,
# Customer.LastName
# FROM Training.dbo.Customer) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_Trainer, type SOURCE 
# COLUMN COUNT: 2

# SQ_Shortcut_to_Trainer = jdbcSqlServerConnection(f"""(SELECT
# Trainer.TrainerId,
# Trainer.FormattedName
# FROM Training.dbo.Trainer) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_Customer, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
# SQ_Shortcut_to_Customer_temp = SQ_Shortcut_to_Customer.toDF(*["SQ_Shortcut_to_Customer___" + col for col in SQ_Shortcut_to_Customer.columns])
# EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])

# JNR_Customer = SQ_Shortcut_to_Customer_temp.join(EXPTRANS_temp,[SQ_Shortcut_to_Customer_temp.SQ_Shortcut_to_Customer___CustomerId == EXPTRANS_temp.EXPTRANS___CustomerId],'right_outer').selectExpr( \
# 	"EXPTRANS___ReservationId as ReservationId", \
# 	"EXPTRANS___CustomerId as CustomerId", \
# 	"EXPTRANS___PetId as PetId", \
# 	"EXPTRANS___StoreClassId as StoreClassId", \
# 	"EXPTRANS___StoreNumber as StoreNumber", \
# 	"EXPTRANS___EnrollmentDateTime as EnrollmentDateTime", \
# 	"EXPTRANS___CreateDateTime as CreateDateTime", \
# 	"SQ_Shortcut_to_Customer___CustomerId as CustomerId1", \
# 	"SQ_Shortcut_to_Customer___FirstName as FirstName", \
# 	"SQ_Shortcut_to_Customer___LastName as LastName")

# COMMAND ----------
# Processing node JNR_Pet, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
# JNR_Customer_temp = JNR_Customer.toDF(*["JNR_Customer___" + col for col in JNR_Customer.columns])
# SQ_Shortcut_to_Pet_temp = SQ_Shortcut_to_Pet.toDF(*["SQ_Shortcut_to_Pet___" + col for col in SQ_Shortcut_to_Pet.columns])

# JNR_Pet = SQ_Shortcut_to_Pet_temp.join(JNR_Customer_temp,[SQ_Shortcut_to_Pet_temp.SQ_Shortcut_to_Pet___PetId == JNR_Customer_temp.JNR_Customer___PetId],'right_outer').selectExpr( \
# 	"JNR_Customer___ReservationId as ReservationId", \
# 	"JNR_Customer___CustomerId as CustomerId", \
# 	"JNR_Customer___PetId as PetId", \
# 	"JNR_Customer___StoreClassId as StoreClassId", \
# 	"JNR_Customer___StoreNumber as StoreNumber", \
# 	"JNR_Customer___EnrollmentDateTime as EnrollmentDateTime", \
# 	"JNR_Customer___CreateDateTime as CreateDateTime", \
# 	"JNR_Customer___FirstName as FirstName", \
# 	"JNR_Customer___LastName as LastName", \
# 	"SQ_Shortcut_to_Pet___PetId as PetId1", \
# 	"SQ_Shortcut_to_Pet___Name as Name", \
# 	"SQ_Shortcut_to_Pet___Breed as Breed")

# COMMAND ----------
# Processing node JNR_StoreClass, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
# JNR_Pet_temp = JNR_Pet.toDF(*["JNR_Pet___" + col for col in JNR_Pet.columns])
# SQ_Shortcut_to_StoreClass_temp = SQ_Shortcut_to_StoreClass.toDF(*["SQ_Shortcut_to_StoreClass___" + col for col in SQ_Shortcut_to_StoreClass.columns])

# JNR_StoreClass = SQ_Shortcut_to_StoreClass_temp.join(JNR_Pet_temp,[SQ_Shortcut_to_StoreClass_temp.SQ_Shortcut_to_StoreClass___StoreClassId == JNR_Pet_temp.JNR_Pet___StoreClassId],'right_outer').selectExpr( \
# 	"JNR_Pet___ReservationId as ReservationId", \
# 	"JNR_Pet___CustomerId as CustomerId", \
# 	"JNR_Pet___PetId as PetId", \
# 	"JNR_Pet___StoreClassId as StoreClassId", \
# 	"JNR_Pet___StoreNumber as StoreNumber", \
# 	"JNR_Pet___EnrollmentDateTime as EnrollmentDateTime", \
# 	"JNR_Pet___CreateDateTime as CreateDateTime", \
# 	"JNR_Pet___FirstName as FirstName", \
# 	"JNR_Pet___LastName as LastName", \
# 	"JNR_Pet___Name as Name", \
# 	"JNR_Pet___Breed as Breed", \
# 	"SQ_Shortcut_to_StoreClass___StoreClassId as StoreClassId1", \
# 	"SQ_Shortcut_to_StoreClass___ClassTypeId as ClassTypeId", \
# 	"SQ_Shortcut_to_StoreClass___StartDateTime as StartDateTime", \
# 	"SQ_Shortcut_to_StoreClass___TrainerId as TrainerId")

# COMMAND ----------
# Processing node JNR_ClassType, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
# JNR_StoreClass_temp = JNR_StoreClass.toDF(*["JNR_StoreClass___" + col for col in JNR_StoreClass.columns])
# SQ_Shortcut_to_ClassType_temp = SQ_Shortcut_to_ClassType.toDF(*["SQ_Shortcut_to_ClassType___" + col for col in SQ_Shortcut_to_ClassType.columns])

# JNR_ClassType = SQ_Shortcut_to_ClassType_temp.join(JNR_StoreClass_temp,[SQ_Shortcut_to_ClassType_temp.SQ_Shortcut_to_ClassType___ClassTypeId == JNR_StoreClass_temp.JNR_StoreClass___ClassTypeId],'right_outer').selectExpr( \
# 	"JNR_StoreClass___ReservationId as ReservationId", \
# 	"JNR_StoreClass___CustomerId as CustomerId", \
# 	"JNR_StoreClass___PetId as PetId", \
# 	"JNR_StoreClass___StoreClassId as StoreClassId", \
# 	"JNR_StoreClass___StoreNumber as StoreNumber", \
# 	"JNR_StoreClass___EnrollmentDateTime as EnrollmentDateTime", \
# 	"JNR_StoreClass___CreateDateTime as CreateDateTime", \
# 	"JNR_StoreClass___FirstName as FirstName", \
# 	"JNR_StoreClass___LastName as LastName", \
# 	"JNR_StoreClass___Name as Pet_Name", \
# 	"JNR_StoreClass___Breed as Breed", \
# 	"JNR_StoreClass___ClassTypeId as ClassTypeId", \
# 	"JNR_StoreClass___StartDateTime as StartDateTime", \
# 	"JNR_StoreClass___TrainerId as TrainerId", \
# 	"SQ_Shortcut_to_ClassType___ClassTypeId as ClassTypeId1", \
# 	"SQ_Shortcut_to_ClassType___Name as ClassTypeName", \
# 	"SQ_Shortcut_to_ClassType___UPC as UPC", \
# 	"SQ_Shortcut_to_ClassType___Duration as Duration")

# COMMAND ----------
# Processing node JNR_Trainer, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
# JNR_ClassType_temp = JNR_ClassType.toDF(*["JNR_ClassType___" + col for col in JNR_ClassType.columns])
# SQ_Shortcut_to_Trainer_temp = SQ_Shortcut_to_Trainer.toDF(*["SQ_Shortcut_to_Trainer___" + col for col in SQ_Shortcut_to_Trainer.columns])

# JNR_Trainer = SQ_Shortcut_to_Trainer_temp.join(JNR_ClassType_temp,[SQ_Shortcut_to_Trainer_temp.SQ_Shortcut_to_Trainer___TrainerId == JNR_ClassType_temp.JNR_ClassType___TrainerId],'right_outer').selectExpr( \
# 	"JNR_ClassType___ReservationId as ReservationId", \
# 	"JNR_ClassType___CustomerId as CustomerId", \
# 	"JNR_ClassType___PetId as PetId", \
# 	"JNR_ClassType___StoreClassId as StoreClassId", \
# 	"JNR_ClassType___StoreNumber as StoreNumber", \
# 	"JNR_ClassType___EnrollmentDateTime as EnrollmentDateTime", \
# 	"JNR_ClassType___CreateDateTime as CreateDateTime", \
# 	"JNR_ClassType___FirstName as FirstName", \
# 	"JNR_ClassType___LastName as LastName", \
# 	"JNR_ClassType___Pet_Name as Pet_Name", \
# 	"JNR_ClassType___Breed as Breed", \
# 	"JNR_ClassType___ClassTypeId as ClassTypeId", \
# 	"JNR_ClassType___StartDateTime as StartDateTime", \
# 	"JNR_ClassType___TrainerId as TrainerId", \
# 	"JNR_ClassType___ClassTypeName as ClassTypeName", \
# 	"SQ_Shortcut_to_Trainer___TrainerId as TrainerId1", \
# 	"SQ_Shortcut_to_Trainer___FormattedName as TrainerName", \
# 	"JNR_ClassType___UPC as UPC", \
# 	"JNR_ClassType___Duration as Duration")

# COMMAND ----------
# Processing node LKP_SITE_PROFILE, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3


# LKP_SITE_PROFILE_lookup_result = JNR_Trainer.selectExpr( \
# 	"StoreNumber as i_STORE_NBR").join(LKP_SITE_PROFILE_SRC, (col('STORE_NBR') == col('i_STORE_NBR')), 'left') \
# .withColumn('row_num_LOCATION_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("LOCATION_ID")))

LKP_SITE_PROFILE_lookup_result = SQ_Shortcut_to_Reservation.selectExpr( \
	"StoreNumber as i_STORE_NBR").join(LKP_SITE_PROFILE_SRC, (col('STORE_NBR') == col('i_STORE_NBR')), 'left') \
.withColumn('row_num_LOCATION_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("LOCATION_ID")))

# print('LKP_SITE_PROFILE_lookup_result.printSchema()')
# LKP_SITE_PROFILE_lookup_result.printSchema()

LKP_SITE_PROFILE = LKP_SITE_PROFILE_lookup_result.filter(col("row_num_LOCATION_ID") == 1).select( \
	LKP_SITE_PROFILE_lookup_result.sys_row_id, \
	col('LOCATION_ID'), \
	col('i_STORE_NBR')
)
# print('LKP_SITE_PROFILE.printSchema()')

# LKP_SITE_PROFILE.printSchema()
# COMMAND ----------
# Processing node EXP_TSTMP, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
LKP_SITE_PROFILE_temp = LKP_SITE_PROFILE.toDF(*["LKP_SITE_PROFILE___" + col for col in LKP_SITE_PROFILE.columns])
# JNR_Trainer_temp = JNR_Trainer.toDF(*["JNR_Trainer___" + col for col in JNR_Trainer.columns])
Reservation_temp = SQ_Shortcut_to_Reservation.toDF(*["Reservation___" + col for col in SQ_Shortcut_to_Reservation.columns])
# JNR_Trainer.printSchema()
# LKP_SITE_PROFILE.printSchema()
# StoreNumber
# Joining dataframes JNR_Trainer, LKP_SITE_PROFILE to form EXP_TSTMP
# EXP_TSTMP_joined = JNR_Trainer.join(LKP_SITE_PROFILE, JNR_Trainer.sys_row_id == LKP_SITE_PROFILE.sys_row_id, 'inner')
EXP_TSTMP_joined = SQ_Shortcut_to_Reservation.join(LKP_SITE_PROFILE, SQ_Shortcut_to_Reservation.StoreNumber == LKP_SITE_PROFILE.i_STORE_NBR, 'inner')
# EXP_TSTMP_joined.printSchema()
EXP_TSTMP_joined_temp = EXP_TSTMP_joined.toDF(*["EXP_TSTMP_joined___" + col for col in EXP_TSTMP_joined.columns])
# EXP_TSTMP_joined_temp.printSchema()
EXP_TSTMP = EXP_TSTMP_joined_temp.selectExpr( \
	"EXP_TSTMP_joined___ReservationId as TRAINING_RESERVATION_ID", \
	"EXP_TSTMP_joined___CustomerId as TRAINING_CUSTOMER_ID", \
	"EXP_TSTMP_joined___FirstName as TRAINING_CUSTOMER_FIRST_NAME", \
	"EXP_TSTMP_joined___LastName as TRAINING_CUSTOMER_LAST_NAME", \
	"EXP_TSTMP_joined___PetId as TRAINING_PET_ID", \
	"REGEXP_REPLACE(REGEXP_REPLACE(EXP_TSTMP_joined___Pet_Name, '(' || CHR ( 13 ) || ')', concat ( '\' , CHR ( 13 ) ) ), '(' || CHR ( 10 ) || ')', concat( '\' , CHR ( 10 ) ) ) as O_TRAINING_PET_NAME", \
	"EXP_TSTMP_joined___BREED as BREED", \
	"EXP_TSTMP_joined___StoreClassId as STORE_CLASS_ID", \
	"EXP_TSTMP_joined___LOCATION_ID as LOCATION_ID", \
	"EXP_TSTMP_joined___ClassTypeId as STORE_CLASS_TYPE_ID", \
	"EXP_TSTMP_joined___ClassTypeName as STORE_CLASS_TYPE_DESC", \
	"EXP_TSTMP_joined___EnrollmentDateTime as ENROLLMENT_DT", \
	"EXP_TSTMP_joined___StartDateTime as CLASS_START_DT", \
	"EXP_TSTMP_joined___TrainerId as TRAINER_ID", \
	"EXP_TSTMP_joined___TrainerName as TRAINER_NAME", \
	"EXP_TSTMP_joined___CreateDateTime as CREATED_DT", \
	"CURRENT_TIMESTAMP as LOAD_TSTMP", \
	"EXP_TSTMP_joined___UPC as UPC", \
	"EXP_TSTMP_joined___Duration as Duration" \
)
# .withColumn("v_Inc_create_date", SET {CREATE_DATE} = GREATEST({CREATE_DATE}, CREATED_DT))

# COMMAND ----------
# Processing node Shortcut_to_PET_TRAINING_RESERVATION_PRE, type TARGET 
# COLUMN COUNT: 18


Shortcut_to_PET_TRAINING_RESERVATION_PRE = EXP_TSTMP.selectExpr( \
	"CAST(TRAINING_RESERVATION_ID AS INT) as PET_TRAINING_RESERVATION_ID", \
	"CAST(LOCATION_ID AS INT) as LOCATION_ID", \
	"CAST(ENROLLMENT_DT AS TIMESTAMP) as ENROLLMENT_TSTMP", \
	"CAST(UPC  as BIGINT ) as UPC_ID", \
	"CAST(STORE_CLASS_TYPE_ID AS INT) as STORE_CLASS_TYPE_ID", \
	"CAST(STORE_CLASS_TYPE_DESC AS STRING) as STORE_CLASS_TYPE_DESC", \
	"CAST(CLASS_START_DT AS TIMESTAMP) as CLASS_START_TSTMP", \
	"CAST(Duration  as SMALLINT ) as CLASS_DURATION_WEEKS", \
	"CAST(TRAINING_CUSTOMER_ID AS INT) as PET_TRAINING_CUSTOMER_ID", \
	"CAST(TRAINING_CUSTOMER_FIRST_NAME AS STRING) as CUSTOMER_FIRST_NAME", \
	"CAST(TRAINING_CUSTOMER_LAST_NAME AS STRING) as CUSTOMER_LAST_NAME", \
	"CAST(TRAINING_PET_ID AS INT) as PET_TRAINING_PET_ID", \
	"CAST(O_TRAINING_PET_NAME AS STRING) as PET_NAME", \
	"CAST(BREED AS STRING) as PET_BREED_DESC", \
	"CAST(TRAINER_ID AS INT) as PET_TRAINING_TRAINER_ID", \
	"CAST(TRAINER_NAME AS STRING) as TRAINER_NAME", \
	"CAST(CREATED_DT AS TIMESTAMP) as CREATED_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" \
)

try:
	Shortcut_to_PET_TRAINING_RESERVATION_PRE.write.saveAsTable(f'{sensitive}.raw_PET_TRAINING_RESERVATION_PRE' , mode='overwrite')
	logPrevRunDt("legacy_PET_TRAINING_RESERVATION_PRE", "legacy_PET_TRAINING_RESERVATION_PRE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("legacy_PET_TRAINING_RESERVATION_PRE", "legacy_PET_TRAINING_RESERVATION_PRE", "Failed", "N/A", f"{raw}.log_run_details")
	raise e

