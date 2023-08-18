#Code converted on 2023-07-21 16:48:10
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
#from pyspark.dbutils import DBUtils
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

# parser = argparse.ArgumentParser()

spark = SparkSession.getActiveSession()
#dbutils = DBUtils(spark)

# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
cust_sensitive = getEnvPrefix(env) + 'cust_sensitive'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = mtx_prd_sqlServer(env)

from datetime import date,timedelta
# Variable_declaration_comment
# QueryCond=args.QueryCond
QueryCond=getParameterValue(raw,"Services_Daily_Parameter.prm","INT_Services.WF:wf_Automated_Reminder_Calls_DDC_ff","QueryCond")

# LastRunDate=args.LastRunDate
LastRunDate=getParameterValue(raw,"Services_Daily_Parameter.prm","INT_Services.WF:wf_Automated_Reminder_Calls_DDC_ff","LastRunDate")
print(LastRunDate)

# RunDate=args.RunDate
RunDate="2023-07-24"
#RunDate="2023-08-05"
#print(str(date.today() - timedelta(days = 1)))
# NextRunDate=args.NextRunDate

# PRStoreList=args.PRStoreList
#PRStoreList = "('x')"
PRStoreList=getParameterValue(raw,"Services_Daily_Parameter.prm","INT_Services.WF:wf_Automated_Reminder_Calls_DDC_ff","PRStoreList")

# NormalAppt=args.NormalAppt
#NormalAppt=3
NormalAppt=getParameterValue(raw,"Services_Daily_Parameter.prm","INT_Services.WF:wf_Automated_Reminder_Calls_DDC_ff","NormalAppt")

# HolidayAppt=args.HolidayAppt
#HolidayAppt=2
HolidayAppt=getParameterValue(raw,"Services_Daily_Parameter.prm","INT_Services.WF:wf_Automated_Reminder_Calls_DDC_ff","HolidayAppt")
# DDCSAPClassIds=args.DDCSAPClassIds
#DDCSAPClassIds = '(sap_class_id)'
DDCSAPClassIds=getParameterValue(raw,"Services_Daily_Parameter.prm","INT_Services.WF:wf_Automated_Reminder_Calls_DDC_ff","DDCSAPClassIds")

# DDCSAPCategoryIds=args.DDCSAPCategoryIds
#DDCSAPCategoryIds='(sap_category_id)'
DDCSAPCategoryIds=getParameterValue(raw,"Services_Daily_Parameter.prm","INT_Services.WF:wf_Automated_Reminder_Calls_DDC_ff","DDCSAPCategoryIds")

LKP_CUSTOMERPHONE_SRC = spark.sql(f"""
SELECT
    TP_CUSTOMER_NBR,
    TP_PHONE_TYPE_ID,
    CUST_PHONE_NBR
FROM {cust_sensitive}.refine_tp_customer_phone""") 

# Conforming fields names to the component layout
LKP_CUSTOMERPHONE_SRC = ( LKP_CUSTOMERPHONE_SRC
	.withColumnRenamed(LKP_CUSTOMERPHONE_SRC.columns[0],'TP_CUSTOMER_NBR')
	.withColumnRenamed(LKP_CUSTOMERPHONE_SRC.columns[1],'TP_PHONE_TYPE_ID')
	.withColumnRenamed(LKP_CUSTOMERPHONE_SRC.columns[2],'CUST_PHONE_NBR')
)

# Processing node LKP_FIRSTTIME_DDC_CUSTOMER_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

LKP_FIRSTTIME_DDC_CUSTOMER_SRC = spark.sql(f"""
SELECT a.TP_CUSTOMER_NBR as TP_CUSTOMER_NBR, min(a.APPT_START_TSTMP) as APPT_START_TSTMP        
FROM {cust_sensitive}.refine_tp_invoice_service_rpt a
        join {legacy}.sku_profile_rpt b ON b.product_id = a.product_id 
where b.sap_class_id in {DDCSAPClassIds}
  AND sap_category_Id in {DDCSAPCategoryIds}
group by a.TP_CUSTOMER_NBR """) \

# Conforming fields names to the component layout
LKP_FIRSTTIME_DDC_CUSTOMER_SRC = (LKP_FIRSTTIME_DDC_CUSTOMER_SRC
	.withColumnRenamed(LKP_FIRSTTIME_DDC_CUSTOMER_SRC.columns[0],'TP_CUSTOMER_NBR')
	.withColumnRenamed(LKP_FIRSTTIME_DDC_CUSTOMER_SRC.columns[1],'APPT_START_TSTMP')
)

LKP_HOLIDAYS_SRC = spark.sql(f"""SELECT
SERVICE_NAME,
HOLIDAY_DATE,
COUNTRY_CD,
IS_CLOSED
FROM {legacy}.AUTOMATED_CALL_HOLIDAYS""") 

# Conforming fields names to the component layout
LKP_HOLIDAYS_SRC = (LKP_HOLIDAYS_SRC
	.withColumnRenamed(LKP_HOLIDAYS_SRC.columns[0],'SERVICE_NAME')
	.withColumnRenamed(LKP_HOLIDAYS_SRC.columns[1],'HOLIDAY_DATE')
	.withColumnRenamed(LKP_HOLIDAYS_SRC.columns[2],'COUNTRY_CD')
	.withColumnRenamed(LKP_HOLIDAYS_SRC.columns[3],'IS_CLOSED')
)


LKP_STORE_TIMEZONE_SRC = jdbcSqlServerConnection(f"""
(SELECT t.TimeZoneDesc, p.FacilityNbr 
FROM EnterpriseSiteDataHub.dbo.vw_PetsMartFacility p
join EnterpriseSiteDataHub.dbo.TimeZone t
	on t.TimeZoneId = p.TimeZoneId) as src""",username,password,connection_string) 

# Conforming fields names to the component layout
LKP_STORE_TIMEZONE_SRC = (LKP_STORE_TIMEZONE_SRC
	.withColumnRenamed(LKP_STORE_TIMEZONE_SRC.columns[0],'TimeZoneDesc')
	.withColumnRenamed(LKP_STORE_TIMEZONE_SRC.columns[1],'FacilityNbr')
)    


TP_INVOICE_RPT = spark.sql(f"""
SELECT DISTINCT
  a.TP_INVOICE_NBR,
  a.TP_CUSTOMER_NBR,
  a.CUST_FIRST_NAME,
  a.CUST_LAST_NAME,
  a.TP_PET_NBR,
  a.PET_NAME,
  a.TP_PET_GENDER_DESC,
  a.TP_PET_BREED_DESC,
  a.APPT_START_TSTMP,
  a.APPT_END_TSTMP,
  STORE_NBR,
  SITE_CITY,
  STATE_CD,
  s.COUNTRY_CD,
  POSTAL_CD,
  SITE_MAIN_TELE_NO,
  CUST_EMAIL_ADDRESS,
  REGION_DESC,
  DISTRICT_DESC
FROM {cust_sensitive}.refine_tp_invoice_rpt a JOIN {legacy}.SITE_PROFILE_RPT s ON a.location_id=s.location_id
 and a.TP_APPT_STATUS_DESC = 'Booked'
 and date(a.APPT_START_TSTMP) >= current_date -interval 23 day
 and a.APPT_START_TSTMP <= current_date -interval 23 day  + interval 6 day
 AND s.STORE_NBR NOT IN {PRStoreList}
 JOIN {cust_sensitive}.refine_tp_customer c ON c.tp_customer_nbr = a.tp_customer_nbr
 JOIN {cust_sensitive}.refine_tp_invoice_service_rpt b ON b.tp_invoice_nbr = a.tp_invoice_nbr
 JOIN {legacy}.SKU_PROFILE_RPT p ON p.product_id = b.product_id
 AND p.sap_class_id in {DDCSAPClassIds}
 AND p.sap_category_Id in {DDCSAPCategoryIds}
""").withColumn("sys_row_id", monotonically_increasing_id())

SRT_Reservations = TP_INVOICE_RPT.select('*').sort(col('TP_CUSTOMER_NBR').asc(), col('APPT_START_TSTMP').asc(), col('TP_INVOICE_NBR').asc(), col('STORE_NBR').asc(), col('TP_PET_NBR').asc())

# COMMAND ----------

# Processing node EXP_AutomatedCalls, type EXPRESSION 
# COLUMN COUNT: 29

# for each involved DataFrame, append the dataframe name to each column
# Profile_DDC_Reminder_Calls_temp = Profile_DDC_Reminder_Calls.toDF(*["Profile_DDC_Reminder_Calls___" + col for col in Profile_DDC_Reminder_Calls.columns])

SRT_Reservations = SRT_Reservations.toDF(*["SRT_Reservations___" + col for col in SRT_Reservations.columns])

# COMMAND ----------

from datetime import datetime, timedelta

date_format = '%Y-%m-%d'
#date_format_last_run_dt = '%m/%d/%Y %H:%M:%S'

# Convert 'RunDate' and 'LastRunDate' strings to datetime objects

RunDate_dt = datetime.strptime(RunDate, date_format)
LastRunDate_dt = datetime.strptime(LastRunDate, date_format)

# Check if 'RunDate' is '01/01/1753' and perform the corresponding logic
if RunDate_dt == datetime(1753, 1, 1):
    v_RunDate = LastRunDate_dt.replace(hour=0, minute=0, second=0, microsecond=0)
else:
    v_RunDate = RunDate_dt.replace(hour=0, minute=0, second=0, microsecond=0)

print(v_RunDate)
v_NextRunDate = v_RunDate + timedelta(days=1)

wf_RunDate = v_NextRunDate                           # corresponds to $$RunDate
wf_NextRunDate = v_NextRunDate + timedelta(days=1)   # corresponds to $$NextRunDate

v_ApptDate = v_NextRunDate + timedelta(days=int(NormalAppt))
v_HolidayAppt = v_NextRunDate + timedelta(days=int(HolidayAppt))

# COMMAND ----------



EXP_AutomatedCalls = SRT_Reservations.selectExpr(
	"SRT_Reservations___sys_row_id as sys_row_id",
	"SRT_Reservations___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"SRT_Reservations___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"SRT_Reservations___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"SRT_Reservations___CUST_LAST_NAME as CUST_LAST_NAME",
	"concat(SRT_Reservations___CUST_FIRST_NAME , ' ' , SRT_Reservations___CUST_LAST_NAME ) as CUST_NAME",
	"SRT_Reservations___TP_PET_NBR as TP_PET_NBR",
	"SRT_Reservations___PET_NAME as PET_NAME",
	"SRT_Reservations___TP_PET_GENDER_DESC as TP_PET_GENDER_DESC",
	"SRT_Reservations___TP_PET_BREED_DESC as TP_PET_BREED_DESC",
	"SRT_Reservations___STORE_NBR as STORE_NBR",
	"SRT_Reservations___SITE_CITY as SITE_CITY",
	"SRT_Reservations___STATE_CD as STATE_CD",
	"SRT_Reservations___COUNTRY_CD as COUNTRY_CD",
	"SRT_Reservations___POSTAL_CD as POSTAL_CD",
	"SRT_Reservations___SITE_MAIN_TELE_NO as SITE_MAIN_TELE_NO",
	"SRT_Reservations___CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"SRT_Reservations___APPT_START_TSTMP as APPT_START_TSTMP",
	"SRT_Reservations___APPT_END_TSTMP as APPT_END_TSTMP",
  "DATE_TRUNC('day', SRT_Reservations___APPT_START_TSTMP) AS APPT_Start_Date",
	"DATE_TRUNC('day', SRT_Reservations___APPT_END_TSTMP) AS APPT_End_Date",
  "CONCAT('RoboCalling_', DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddHHmmss'), '.Profile_DDC_Reminder_Calls___txt') AS FILENAME",
  "SRT_Reservations___REGION_DESC as REGION_DESC",
	"SRT_Reservations___DISTRICT_DESC as DISTRICT_DESC"
).withColumn("RunDate", lit(v_RunDate)) \
 .withColumn("NextRunDate", lit(v_NextRunDate)) \
 .withColumn("ApptDate", lit(v_ApptDate)) \
 .withColumn("Holiday_ApptDate", lit(v_HolidayAppt))

print('v_RunDate',v_RunDate)
print('v_NextRunDate',v_NextRunDate)
print('v_ApptDate',v_ApptDate)
print('v_HolidayAppt',v_HolidayAppt)

# COMMAND ----------

# Adding deferred logic for dataframe EXP_AutomatedCalls, column v_HolidayCheck_NextDay

EXP_AutomatedCalls = EXP_AutomatedCalls.join(
    LKP_HOLIDAYS_SRC,
    (LKP_HOLIDAYS_SRC.SERVICE_NAME == lit('DDC')) &
    (LKP_HOLIDAYS_SRC.COUNTRY_CD == EXP_AutomatedCalls.COUNTRY_CD) & 
    (LKP_HOLIDAYS_SRC.HOLIDAY_DATE == date_add(EXP_AutomatedCalls.NextRunDate, lit(1))),
    'left'
).select(EXP_AutomatedCalls['*'], LKP_HOLIDAYS_SRC['IS_CLOSED'].alias('ULKP_RETURN_1'))



EXP_AutomatedCalls = EXP_AutomatedCalls.withColumn('v_HolidayCheck_NextDay', col('ULKP_RETURN_1'))

# COMMAND ----------


# Adding deferred logic for dataframe EXP_AutomatedCalls, column v_HolidayCheck_Today

EXP_AutomatedCalls = EXP_AutomatedCalls.join(
    LKP_HOLIDAYS_SRC,
    (LKP_HOLIDAYS_SRC.SERVICE_NAME == lit('DDC')) & 
    (LKP_HOLIDAYS_SRC.COUNTRY_CD == EXP_AutomatedCalls.COUNTRY_CD) & 
    (LKP_HOLIDAYS_SRC.HOLIDAY_DATE == EXP_AutomatedCalls.NextRunDate),
    'left'
).select(EXP_AutomatedCalls['*'], LKP_HOLIDAYS_SRC['IS_CLOSED'].alias('ULKP_RETURN_2'))

EXP_AutomatedCalls = EXP_AutomatedCalls.withColumn('v_HolidayCheck_Today', col('ULKP_RETURN_2'))

# COMMAND ----------



# Adding deferred logic for dataframe EXP_AutomatedCalls, column Holiday_Store_NextDay
EXP_AutomatedCalls = EXP_AutomatedCalls.withColumn('Holiday_Store_NextDay', expr("v_HolidayCheck_NextDay as Holiday_Store_NextDay"))

# Adding deferred logic for dataframe EXP_AutomatedCalls, column Holiday_Store_Today
EXP_AutomatedCalls = EXP_AutomatedCalls.withColumn('Holiday_Store_Today', expr("v_HolidayCheck_Today as Holiday_Store_Today"))

EXP_AutomatedCalls_temp = EXP_AutomatedCalls.toDF(*["EXP_AutomatedCalls___" + col for col in EXP_AutomatedCalls.columns])

EXP_ReservationDates = EXP_AutomatedCalls_temp.selectExpr(
	"EXP_AutomatedCalls___sys_row_id as sys_row_id",
	"EXP_AutomatedCalls___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"EXP_AutomatedCalls___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"EXP_AutomatedCalls___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"EXP_AutomatedCalls___CUST_LAST_NAME as CUST_LAST_NAME",
	"EXP_AutomatedCalls___CUST_NAME as CUST_NAME",
	"EXP_AutomatedCalls___TP_PET_NBR as TP_PET_NBR",
	"EXP_AutomatedCalls___PET_NAME as PET_NAME",
	"EXP_AutomatedCalls___TP_PET_GENDER_DESC as TP_PET_GENDER_DESC",
	"EXP_AutomatedCalls___TP_PET_BREED_DESC as TP_PET_BREED_DESC",
	"EXP_AutomatedCalls___STORE_NBR as STORE_NBR",
	"EXP_AutomatedCalls___POSTAL_CD as POSTAL_CD",
	"EXP_AutomatedCalls___CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"EXP_AutomatedCalls___APPT_START_TSTMP as APPT_START_TSTMP",
	"EXP_AutomatedCalls___APPT_END_TSTMP as APPT_END_TSTMP",
	"EXP_AutomatedCalls___APPT_Start_Date as APPT_Start_Date",
	"EXP_AutomatedCalls___APPT_End_Date as APPT_End_Date",
	"EXP_AutomatedCalls___FILENAME as FILENAME",
	"EXP_AutomatedCalls___ApptDate as ApptDate",
	"EXP_AutomatedCalls___RunDate as RunDate",
	"EXP_AutomatedCalls___NextRunDate as NextRunDate",
	"EXP_AutomatedCalls___SITE_CITY as SITE_CITY",
	"EXP_AutomatedCalls___STATE_CD as STATE_CD",
	"EXP_AutomatedCalls___COUNTRY_CD as COUNTRY_CD",
	"EXP_AutomatedCalls___SITE_MAIN_TELE_NO as SITE_MAIN_TELE_NO",
	"EXP_AutomatedCalls___REGION_DESC as REGION_DESC",
	"EXP_AutomatedCalls___DISTRICT_DESC as DISTRICT_DESC",
	"EXP_AutomatedCalls___Holiday_Store_NextDay as Holiday_Store_NextDay",
	"EXP_AutomatedCalls___Holiday_Store_Today as Holiday_Store_Today",
	"EXP_AutomatedCalls___Holiday_ApptDate as Holiday_ApptDate"
)


EXP_ReservationDates_temp = EXP_ReservationDates.toDF(*["EXP_ReservationDates___" + col for col in EXP_ReservationDates.columns])

FIL_ReminderCalls = EXP_ReservationDates_temp.selectExpr(
	"EXP_ReservationDates___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"EXP_ReservationDates___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"EXP_ReservationDates___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"EXP_ReservationDates___CUST_LAST_NAME as CUST_LAST_NAME",
	"EXP_ReservationDates___CUST_NAME as CUST_NAME",
	"EXP_ReservationDates___TP_PET_NBR as TP_PET_NBR",
	"EXP_ReservationDates___PET_NAME as PET_NAME",
	"EXP_ReservationDates___TP_PET_GENDER_DESC as TP_PET_GENDER_DESC",
	"EXP_ReservationDates___TP_PET_BREED_DESC as TP_PET_BREED_DESC",
	"EXP_ReservationDates___STORE_NBR as STORE_NBR",
	"EXP_ReservationDates___POSTAL_CD as POSTAL_CD",
	"EXP_ReservationDates___CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"EXP_ReservationDates___APPT_START_TSTMP as APPT_START_TSTMP",
	"EXP_ReservationDates___APPT_END_TSTMP as APPT_END_TSTMP",
	"EXP_ReservationDates___APPT_Start_Date as APPT_Start_Date",
	"EXP_ReservationDates___APPT_End_Date as APPT_End_Date",
	"EXP_ReservationDates___FILENAME as FILENAME",
	"EXP_ReservationDates___ApptDate as ApptDate",
	"EXP_ReservationDates___RunDate as RunDate",
	"EXP_ReservationDates___NextRunDate as NextRunDate",
	"EXP_ReservationDates___SITE_CITY as SITE_CITY",
	"EXP_ReservationDates___STATE_CD as STATE_CD",
	"EXP_ReservationDates___COUNTRY_CD as COUNTRY_CD",
	"EXP_ReservationDates___SITE_MAIN_TELE_NO as SITE_MAIN_TELE_NO",
	"EXP_ReservationDates___REGION_DESC as REGION_DESC",
	"EXP_ReservationDates___DISTRICT_DESC as DISTRICT_DESC",
	"EXP_ReservationDates___Holiday_Store_NextDay as Holiday_Store_NextDay",
	"EXP_ReservationDates___Holiday_Store_Today as Holiday_Store_Today",
	"EXP_ReservationDates___Holiday_ApptDate as Holiday_ApptDate")	.filter(expr("(Holiday_Store_Today IS NULL AND date_trunc('day', APPT_START_TSTMP) = date_trunc('day', ApptDate)) OR ( Holiday_Store_NextDay IS NOT NULL AND date_trunc('day', APPT_START_TSTMP) = date_trunc('day', Holiday_ApptDate))")).withColumn("sys_row_id", monotonically_increasing_id())

AGG_ReminderCalls = (  FIL_ReminderCalls.selectExpr(
	"TP_INVOICE_NBR as TP_INVOICE_NBR",
	"TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"CUST_FIRST_NAME as CUST_FIRST_NAME",
	"CUST_LAST_NAME as CUST_LAST_NAME",
	"CUST_NAME as CUST_NAME",
	"TP_PET_NBR as i_TP_PET_NBR",
	"PET_NAME as i_PET_NAME",
  "APPT_Start_Date AS i_APPT_Start_Date",
	"STORE_NBR as STORE_NBR",
	"TP_PET_BREED_DESC as i_TP_PET_BREED_DESC",
	"TP_PET_GENDER_DESC as i_TP_PET_GENDER_DESC",
	"POSTAL_CD as POSTAL_CD",
	"CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"FILENAME as FILENAME",
	"APPT_START_TSTMP as APPT_START_TSTMP",
	"APPT_END_TSTMP as APPT_END_TSTMP",
	"ApptDate as i_ApptDate",
	"SITE_CITY as SITE_CITY",
	"STATE_CD as STATE_CD",
	"COUNTRY_CD as COUNTRY_CD",
	"SITE_MAIN_TELE_NO as SITE_MAIN_TELE_NO",
	"REGION_DESC as REGION_DESC",
	"DISTRICT_DESC as DISTRICT_DESC"
	).withColumn("New_Cust_Nbr", col("TP_CUSTOMER_NBR")) \
   .withColumn("New_StoreNbr", col("STORE_NBR")) 
).groupBy("TP_CUSTOMER_NBR","i_APPT_Start_Date","STORE_NBR").agg(
  max(col('TP_INVOICE_NBR')).alias('TP_INVOICE_NBR'),
	min(col('CUST_FIRST_NAME')).alias('CUST_FIRST_NAME'),
	min(col('CUST_LAST_NAME')).alias('CUST_LAST_NAME'),
	min(col('CUST_NAME')).alias('CUST_NAME'),
	min(col('POSTAL_CD')).alias('POSTAL_CD'),
	min(col('CUST_EMAIL_ADDRESS')).alias('CUST_EMAIL_ADDRESS'),
	min(col('FILENAME')).alias('FILENAME'),
	min(col('APPT_START_TSTMP')).alias('APPT_START_TSTMP'),
	min(col('APPT_END_TSTMP')).alias('APPT_END_TSTMP'),
	min(col('SITE_CITY')).alias('SITE_CITY'),
	min(col('STATE_CD')).alias('STATE_CD'),
	min(col('COUNTRY_CD')).alias('COUNTRY_CD'),
	min(col('SITE_MAIN_TELE_NO')).alias('SITE_MAIN_TELE_NO'),
	min(col('REGION_DESC')).alias('REGION_DESC'),
	min(col('DISTRICT_DESC')).alias('DISTRICT_DESC'),
  concat_ws(";",collect_list("i_PET_NAME")).alias("Pet_Names"),
  concat_ws(";",collect_list("i_TP_PET_BREED_DESC")).alias("Breed_Desc"),
  concat_ws(";",collect_list("i_TP_PET_GENDER_DESC")).alias("Gender_Desc")
).withColumn("sys_row_id", monotonically_increasing_id()) \
 .withColumn('DF_ROW_ID', monotonically_increasing_id()) 
  
# print(AGG_ReminderCalls.columns)


# COMMAND ----------

# Processing node EXP_Agg_Reminders, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
AGG_ReminderCalls_temp = AGG_ReminderCalls.toDF(*["AGG_ReminderCalls___" + col for col in AGG_ReminderCalls.columns])

EXP_Agg_Reminders = AGG_ReminderCalls_temp.selectExpr(
	"AGG_ReminderCalls___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"AGG_ReminderCalls___TP_INVOICE_NBR as TP_Invoice_Nbr",
	"AGG_ReminderCalls___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"AGG_ReminderCalls___CUST_LAST_NAME as CUST_LAST_NAME",
	"AGG_ReminderCalls___CUST_NAME as CUST_NAME",
	"AGG_ReminderCalls___Pet_Names as Pet_Names",
	"AGG_ReminderCalls___Gender_Desc as TP_PET_GENDER_DESC",
	"AGG_ReminderCalls___Breed_Desc as TP_PET_BREED_DESC",
	"AGG_ReminderCalls___STORE_NBR as STORE_NBR",
	"AGG_ReminderCalls___POSTAL_CD as POSTAL_CD",
	"AGG_ReminderCalls___CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"AGG_ReminderCalls___FILENAME as FILENAME",
	"AGG_ReminderCalls___APPT_START_TSTMP as APPT_START_TSTMP",
	"AGG_ReminderCalls___APPT_END_TSTMP as i_APPT_END_TSTMP",
	"AGG_ReminderCalls___SITE_CITY as SITE_CITY",
	"AGG_ReminderCalls___STATE_CD as STATE_CD",
	"AGG_ReminderCalls___COUNTRY_CD as COUNTRY_CD",
	"AGG_ReminderCalls___SITE_MAIN_TELE_NO as SITE_MAIN_TELE_NO",
	"AGG_ReminderCalls___REGION_DESC as REGION_DESC",
	"AGG_ReminderCalls___DISTRICT_DESC as DISTRICT_DESC",
	"AGG_ReminderCalls___sys_row_id as sys_row_id",
	"'Doggie Day Camp' as Svc_Type",
	"date_format(AGG_ReminderCalls___APPT_START_TSTMP, 'MM/dd/yyyy HH:mm') as Res_Start_Time",
	"date_format(AGG_ReminderCalls___APPT_END_TSTMP, 'MM/dd/yyyy HH:mm') as Res_End_Time"  
)

# COMMAND ----------





# Adding deferred logic for dataframe EXP_Agg_Reminders, column v_TIME_ZONE
EXP_Agg_Reminders = EXP_Agg_Reminders.join(LKP_STORE_TIMEZONE_SRC,
	(LKP_STORE_TIMEZONE_SRC.FacilityNbr == EXP_Agg_Reminders.STORE_NBR),'left').select(EXP_Agg_Reminders['*'], LKP_STORE_TIMEZONE_SRC['TimeZoneDesc'].alias('ULKP_RETURN_1') )
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn('v_TIME_ZONE',col('ULKP_RETURN_1'))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column TimeZone
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn('TimeZone', 
                                                  expr("""CASE v_TIME_ZONE 
                                                             WHEN 'EST' THEN '(UTC-05:00) Eastern Time (US and Canada)' 
                                                             WHEN 'NFL' THEN '(UTC-03:30) Newfoundland' 
                                                             WHEN 'CST' THEN '(UTC-06:00) Central Time (US and Canada)' 
                                                             WHEN 'PST' THEN '(UTC-08:00) Pacific Time (US and Canada)' 
                                                             WHEN 'MS0' THEN '(UTC-07:00) Arizona' 
                                                             WHEN 'MST' THEN '(UTC-07:00) Mountain Time (US and Canada)' 
                                                             WHEN 'CS0' THEN '(UTC-06:00) Central America' 
                                                             WHEN 'ALA' THEN '(UTC-09:00) Alaska' 
                                                             WHEN 'ATL' THEN '(UTC-04:00) Atlantic Time (Canada)' 
                                                             WHEN 'AST' THEN '(UTC-04:00) Georgetown La Paz Manaus San Juan' 
                                                             WHEN 'HST' THEN '(UTC-10:00) Hawaii' 
                                                        ELSE NULL END"""))



# Adding deferred logic for dataframe EXP_Agg_Reminders, column Mobile
EXP_Agg_Reminders = EXP_Agg_Reminders.join(LKP_CUSTOMERPHONE_SRC,
	(LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR) & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(3)),'left').select(EXP_Agg_Reminders['*'], LKP_CUSTOMERPHONE_SRC['CUST_PHONE_NBR'].alias('ULKP_RETURN_2') )
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn('Mobile',col('ULKP_RETURN_2'))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Mobile2
EXP_Agg_Reminders = EXP_Agg_Reminders.join(LKP_CUSTOMERPHONE_SRC,
	(LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR) & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(4)),'left').select(EXP_Agg_Reminders['*'], LKP_CUSTOMERPHONE_SRC['CUST_PHONE_NBR'].alias('ULKP_RETURN_3') )
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn('Mobile2',col('ULKP_RETURN_3'))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Work
EXP_Agg_Reminders = EXP_Agg_Reminders.join(LKP_CUSTOMERPHONE_SRC,
	(LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR) & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(5)),'left').select(EXP_Agg_Reminders['*'], LKP_CUSTOMERPHONE_SRC['CUST_PHONE_NBR'].alias('ULKP_RETURN_4') )
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn('Work',col('ULKP_RETURN_4'))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Work2
EXP_Agg_Reminders = EXP_Agg_Reminders.join(LKP_CUSTOMERPHONE_SRC,
	(LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR) & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(6)),'left').select(EXP_Agg_Reminders['*'], LKP_CUSTOMERPHONE_SRC['CUST_PHONE_NBR'].alias('ULKP_RETURN_5') )
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn('Work2',col('ULKP_RETURN_5'))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Home
EXP_Agg_Reminders = EXP_Agg_Reminders.join(LKP_CUSTOMERPHONE_SRC,
	(LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR) & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(1)),'left').select(EXP_Agg_Reminders['*'], LKP_CUSTOMERPHONE_SRC['CUST_PHONE_NBR'].alias('ULKP_RETURN_6') )
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn('Home',col('ULKP_RETURN_6'))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Home2
EXP_Agg_Reminders = EXP_Agg_Reminders.join(LKP_CUSTOMERPHONE_SRC,
	(LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR) & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(2)),'left').select(EXP_Agg_Reminders['*'], LKP_CUSTOMERPHONE_SRC['CUST_PHONE_NBR'].alias('ULKP_RETURN_7') )
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn('Home2',col('ULKP_RETURN_7'))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column OtherPhone
EXP_Agg_Reminders = EXP_Agg_Reminders.join(LKP_CUSTOMERPHONE_SRC,
	(LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR) & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(8)),'left').select(EXP_Agg_Reminders['*'], LKP_CUSTOMERPHONE_SRC['CUST_PHONE_NBR'].alias('ULKP_RETURN_8') )
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn('OtherPhone',col('ULKP_RETURN_8'))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Min_ApptTime
EXP_Agg_Reminders = EXP_Agg_Reminders.join(LKP_FIRSTTIME_DDC_CUSTOMER_SRC,
	(LKP_FIRSTTIME_DDC_CUSTOMER_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR),'left').select(EXP_Agg_Reminders['*'], LKP_FIRSTTIME_DDC_CUSTOMER_SRC['APPT_START_TSTMP'].alias('ULKP_RETURN_9') )

EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn('Min_ApptTime',col('ULKP_RETURN_9'))

EXP_Agg_Reminders_temp = EXP_Agg_Reminders.toDF(*["EXP_Agg_Reminders___" + col for col in EXP_Agg_Reminders.columns])

FIL_FirstTime_DDCCustomer = EXP_Agg_Reminders_temp.selectExpr(
	"EXP_Agg_Reminders___TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
	"EXP_Agg_Reminders___TP_Invoice_Nbr as TP_Invoice_Nbr",
	"EXP_Agg_Reminders___CUST_FIRST_NAME as CUST_FIRST_NAME",
	"EXP_Agg_Reminders___CUST_LAST_NAME as CUST_LAST_NAME",
	"EXP_Agg_Reminders___CUST_NAME as CUST_NAME",
	"EXP_Agg_Reminders___Pet_Names as Pet_Names",
	"EXP_Agg_Reminders___TP_PET_GENDER_DESC as TP_PET_GENDER_DESC",
	"EXP_Agg_Reminders___TP_PET_BREED_DESC as TP_PET_BREED_DESC",
	"EXP_Agg_Reminders___STORE_NBR as STORE_NBR",
	"EXP_Agg_Reminders___POSTAL_CD as POSTAL_CD",
	"EXP_Agg_Reminders___CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
	"EXP_Agg_Reminders___FILENAME as FILENAME",
	"EXP_Agg_Reminders___SITE_CITY as SITE_CITY",
	"EXP_Agg_Reminders___STATE_CD as STATE_CD",
	"EXP_Agg_Reminders___COUNTRY_CD as COUNTRY_CD",
	"EXP_Agg_Reminders___TimeZone as TimeZone",
	"EXP_Agg_Reminders___SITE_MAIN_TELE_NO as SITE_MAIN_TELE_NO",
	"EXP_Agg_Reminders___Mobile as Mobile",
	"EXP_Agg_Reminders___Mobile2 as Mobile2",
	"EXP_Agg_Reminders___Work as Work",
	"EXP_Agg_Reminders___Work2 as Work2",
	"EXP_Agg_Reminders___Home as Home",
	"EXP_Agg_Reminders___Home2 as Home2",
	"EXP_Agg_Reminders___OtherPhone as OtherPhone",
	"EXP_Agg_Reminders___Svc_Type as Svc_Type",
	"EXP_Agg_Reminders___REGION_DESC as REGION_DESC",
	"EXP_Agg_Reminders___DISTRICT_DESC as DISTRICT_DESC",
	"EXP_Agg_Reminders___Res_Start_Time as Res_Start_Time",
	"EXP_Agg_Reminders___Res_End_Time as Res_End_Time",
	"EXP_Agg_Reminders___Min_ApptTime as Min_ApptTime",
	"EXP_Agg_Reminders___APPT_START_TSTMP as APPT_START_TSTMP").filter(expr("(DATE_TRUNC('day', Min_ApptTime) = DATE_TRUNC('day', APPT_START_TSTMP))")
                                                                    ).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Automated_Reminder_Calls_PetsHotel_FF, type TARGET 
# COLUMN COUNT: 29


Automated_Reminder_Calls_PetsHotel_FF = FIL_FirstTime_DDCCustomer.selectExpr(
	"CAST(TP_CUSTOMER_NBR AS LONG) as TP_Cust_Nbr",
	"CAST(TP_Invoice_Nbr AS LONG) as TP_Invoice_Nbr",
	"CAST(CUST_FIRST_NAME AS STRING) as TP_Cust_First_Name",
	"CAST(CUST_LAST_NAME AS STRING) as TP_Cust_Last_Name",
	"CAST(CUST_NAME AS STRING) as TP_Cust_Name",
	"CAST(CUST_EMAIL_ADDRESS AS STRING) as TP_Cust_Email",
	"CAST(Res_Start_Time AS STRING) as TP_Res_Start_Time",
	"CAST(Res_End_Time AS STRING) as TP_Res_End_Time",
	"CAST(Mobile AS LONG) as TP_Mobile_Phone1",
	"CAST(Mobile2 AS LONG) as TP_Mobile_Phone2",
	"CAST(Home AS LONG) as TP_Home_Phone1",
	"CAST(Home2 AS STRING) as TP_Home_Phone2",
	"CAST(Work AS STRING) as TP_Work_Phone1",
	"CAST(Work2 AS STRING) as TP_Work_Phone2",
	"CAST(OtherPhone AS STRING) as TP_Other_Phone",
	"CAST(Pet_Names AS STRING) as TP_Pet_Name",
	"CAST(TP_PET_GENDER_DESC AS STRING) as TP_Pet_Gender",
	"CAST(TP_PET_BREED_DESC AS STRING) as TP_Pet_Breed",
	"CAST(Svc_Type AS STRING) as TP_Svc_Type",
	"CAST(STORE_NBR AS INTEGER) as TP_Store_Nbr",
	"CAST(POSTAL_CD AS STRING) as TP_Store_Zip",
	"CAST(SITE_CITY AS STRING) as TP_Store_City",
	"CAST(STATE_CD AS STRING) as TP_Store_State",
	"CAST(COUNTRY_CD AS STRING) as TP_Store_Country",
	"CAST(TimeZone AS STRING) as TP_Store_Timezone",
	"CAST(SITE_MAIN_TELE_NO AS LONG) as TP_Store_Phone",
	"CAST(REGION_DESC AS STRING) as TP_Store_District",
	"CAST(DISTRICT_DESC AS STRING) as TP_Store_Region",
	"CAST(FILENAME AS STRING) as FileName"
)

Automated_Reminder_Calls_PetsHotel_FF = Automated_Reminder_Calls_PetsHotel_FF.drop("FileName")

#Automated_Reminder_Calls_PetsHotel_FF.write.mode("overwrite").saveAsTable(f'{raw}.Automated_Reminder_Calls_PetsHotel_FF')
#print(Automated_Reminder_Calls_PetsHotel_FF.count())
#current_date = starttime.strftime("%Y%m%d%H%M%S")
#dateAppendeFileName="RoboCalling_"+current_date+"."+"DDC_Calls.txt"
#filePath="dbfs:/FileStore/users/archana.h@databricks.com/SampleTestDDC"
#writeToFlatFile(Automated_Reminder_Calls_PetsHotel_FF,filePath,dateAppendeFileName,"overwrite")

current_date = starttime.strftime("%Y%m%d%H%M%S")
dateAppendeFileName="RoboCalling_"+current_date+".Profile_DDC_Reminder_Calls.txt"
print(dateAppendeFileName)
if env == "prod":
  dirPath="gs://petm-bdpl-prod-nas-p1-gcs-gbl/nzmigration/automated_reminder_ddc/"
  today = datetime.now()
  filePath=dirPath+today.strftime('%Y%m%d')
  gs_source_path = filePath+"/*.txt"
  nas_target_path = "/mnt/nas05/edwshare/DataLake/Temp_NZ_Migration/" 
else:
  dirPath="gs://petm-bdpl-dev-nas-p1-gcs-gbl/nzmigration/automated_reminder_ddc/"
  today = datetime.now()
  filePath=dirPath+today.strftime('%Y%m%d')
  gs_source_path = filePath+"/*.txt"
  nas_target_path = "/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test/"
try:
  writeToFlatFile(Automated_Reminder_Calls_PetsHotel_FF,filePath,dateAppendeFileName,"overwrite")
  copy_file_to_nas(gs_source_path,nas_target_path)
  #logPrevRunDt("Automated_Reminder_Calls_PetsHotel_FF", "Automated_Reminder_Calls_PetsHotel_FF", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  #logPrevRunDt("Automated_Reminder_Calls_PetsHotel_FF", "Automated_Reminder_Calls_PetsHotel_FF","Failed",str(e), f"{raw}.log_run_details", )
  raise e

  
  
