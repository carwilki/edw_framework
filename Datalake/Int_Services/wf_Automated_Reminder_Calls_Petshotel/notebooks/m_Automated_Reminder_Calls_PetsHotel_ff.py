# Code converted on 2023-07-14 14:59:57
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
from datetime import date

# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()

parser.add_argument("env", type=str, help="Env Variable")
args = parser.parse_args()
env = args.env


if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"
cust_sensitive = getEnvPrefix(env) + "cust_sensitive"

# Set global variables
starttime = datetime.now()  # start timestamp of the script

# Read in relation source variables
(username, password, connection_string) = mtx_prd_sqlServer(env)

# COMMAND ----------

# Variable_declaration_comment

QueryCond = getParameterValue(
    raw,
    "Services_Daily_Parameter.prm",
    "INT_Services.WF:wf_Automated_Reminder_Calls_PetsHotel_ff",
    "QueryCond",
)

# LastRunDate=args.LastRunDate
LastRunDate = getParameterValue(
    raw,
    "Services_Daily_Parameter.prm",
    "INT_Services.WF:wf_Automated_Reminder_Calls_PetsHotel_ff",
    "LastRunDate",
)


RunDate = genPrevRunDtFlatFile("Automated_Reminder_Calls_PetsHotel_FF", raw)
# RunDate="2023-07-24"


PRStoreList = getParameterValue(
    raw,
    "Services_Daily_Parameter.prm",
    "INT_Services.WF:wf_Automated_Reminder_Calls_PetsHotel_ff",
    "PRStoreList",
)

PetsHotelSAPClassIds = getParameterValue(
    raw,
    "Services_Daily_Parameter.prm",
    "INT_Services.WF:wf_Automated_Reminder_Calls_PetsHotel_ff",
    "PetsHotelSAPClassIds",
)

NormalAppt = getParameterValue(
    raw,
    "Services_Daily_Parameter.prm",
    "INT_Services.WF:wf_Automated_Reminder_Calls_PetsHotel_ff",
    "NormalAppt",
)

HolidayAppt = getParameterValue(
    raw,
    "Services_Daily_Parameter.prm",
    "INT_Services.WF:wf_Automated_Reminder_Calls_PetsHotel_ff",
    "HolidayAppt",
)

PeakPeriodAppt = getParameterValue(
    raw,
    "Services_Daily_Parameter.prm",
    "INT_Services.WF:wf_Automated_Reminder_Calls_PetsHotel_ff",
    "PeakPeriodAppt",
)

PeakPeriod_HolidayApptDate = getParameterValue(
    raw,
    "Services_Daily_Parameter.prm",
    "INT_Services.WF:wf_Automated_Reminder_Calls_PetsHotel_ff",
    "PeakPeriod_HolidayApptDate",
)


# i_TP_CUSTOMER_NBR = args.i_TP_CUSTOMER_NBR
# i_TP_PHONE_TYPE_ID = args.i_TP_PHONE_TYPE_ID
# i_Service_Name = args.i_Service_Name
# i_Country_Cd = args.i_Country_Cd
# i_Date = args.i_Date
# i_RuleName = args.i_RuleName
# i_StoreNbr = args.i_StoreNbr

# COMMAND ----------

# Processing node LKP_CUSTOMERPHONE_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 5

LKP_CUSTOMERPHONE_SRC = spark.sql(
    f"""
SELECT
    TP_CUSTOMER_NBR,
    TP_PHONE_TYPE_ID,
    CUST_PHONE_NBR
FROM {cust_sensitive}.refine_tp_customer_phone"""
)


# Conforming fields names to the component layout
LKP_CUSTOMERPHONE_SRC = (
    LKP_CUSTOMERPHONE_SRC.withColumnRenamed(
        LKP_CUSTOMERPHONE_SRC.columns[0], "TP_CUSTOMER_NBR"
    )
    .withColumnRenamed(LKP_CUSTOMERPHONE_SRC.columns[1], "TP_PHONE_TYPE_ID")
    .withColumnRenamed(LKP_CUSTOMERPHONE_SRC.columns[2], "CUST_PHONE_NBR")
)

# COMMAND ----------

# Processing node LKP_HOLIDAYS_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 7

LKP_HOLIDAYS_SRC = spark.sql(
    f"""SELECT
SERVICE_NAME,
HOLIDAY_DATE,
COUNTRY_CD,
IS_CLOSED
FROM {legacy}.AUTOMATED_CALL_HOLIDAYS"""
)


# Conforming fields names to the component layout
LKP_HOLIDAYS_SRC = (
    LKP_HOLIDAYS_SRC.withColumnRenamed(LKP_HOLIDAYS_SRC.columns[0], "SERVICE_NAME")
    .withColumnRenamed(LKP_HOLIDAYS_SRC.columns[1], "HOLIDAY_DATE")
    .withColumnRenamed(LKP_HOLIDAYS_SRC.columns[2], "COUNTRY_CD")
    .withColumnRenamed(LKP_HOLIDAYS_SRC.columns[3], "IS_CLOSED")
)

# COMMAND ----------

# Processing node LKP_RULES_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 11

LKP_RULES_SRC = spark.sql(
    f"""SELECT
SERVICE_NAME,
RULE_NAME,
PEAK_START_DATE,
PEAK_END_DATE,
COUNTRY_CD,
DAY_OFFSET,
LOAD_TSTMP
FROM {legacy}.AUTOMATED_CALL_RULES"""
)


# Conforming fields names to the component layout
LKP_RULES_SRC = (
    LKP_RULES_SRC.withColumnRenamed(LKP_RULES_SRC.columns[0], "SERVICE_NAME")
    .withColumnRenamed(LKP_RULES_SRC.columns[1], "RULE_NAME")
    .withColumnRenamed(LKP_RULES_SRC.columns[2], "PEAK_START_DATE")
    .withColumnRenamed(LKP_RULES_SRC.columns[3], "PEAK_END_DATE")
    .withColumnRenamed(LKP_RULES_SRC.columns[4], "COUNTRY_CD")
    .withColumnRenamed(LKP_RULES_SRC.columns[5], "DAY_OFFSET")
    .withColumnRenamed(LKP_RULES_SRC.columns[6], "LOAD_TSTMP")
)

# COMMAND ----------

# Processing node LKP_STORE_TIMEZONE_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

LKP_STORE_TIMEZONE_SRC = jdbcSqlServerConnection(
    f"""
(SELECT t.TimeZoneDesc, p.FacilityNbr 
FROM EnterpriseSiteDataHub.dbo.vw_PetsMartFacility p
join EnterpriseSiteDataHub.dbo.TimeZone t
	on t.TimeZoneId = p.TimeZoneId) as src""",
    username,
    password,
    connection_string,
)


# Conforming fields names to the component layout
LKP_STORE_TIMEZONE_SRC = LKP_STORE_TIMEZONE_SRC.withColumnRenamed(
    LKP_STORE_TIMEZONE_SRC.columns[0], "TimeZoneDesc"
).withColumnRenamed(LKP_STORE_TIMEZONE_SRC.columns[1], "FacilityNbr")


# COMMAND ----------

# Processing node TP_INVOICE_RPT, type SOURCE
# COLUMN COUNT: 19

TP_INVOICE_RPT = spark.sql(
    f"""
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
  AND a.TP_APPT_STATUS_DESC = 'Booked'
  AND date(a.APPT_START_TSTMP) >= current_date 
  AND a.APPT_START_TSTMP <= current_date + interval 12 day
  AND s.STORE_NBR NOT IN {PRStoreList}
 JOIN {cust_sensitive}.refine_tp_customer c ON c.tp_customer_nbr = a.tp_customer_nbr
 JOIN {cust_sensitive}.refine_tp_invoice_service_rpt b ON b.tp_invoice_nbr = a.tp_invoice_nbr
 JOIN {legacy}.SKU_PROFILE_RPT p ON p.product_id = b.product_id
  AND p.sap_class_id in {PetsHotelSAPClassIds}"""
).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node SRT_Reservations, type SORTER
# COLUMN COUNT: 19

SRT_Reservations = TP_INVOICE_RPT.select("*").sort(
    col("TP_CUSTOMER_NBR").asc(),
    col("TP_INVOICE_NBR").asc(),
    col("APPT_START_TSTMP").asc(),
    col("STORE_NBR").asc(),
    col("TP_PET_NBR").asc(),
)

SRT_Reservations_1 = (
    TP_INVOICE_RPT.select(
        "TP_CUSTOMER_NBR",
        "APPT_START_TSTMP",
        "APPT_END_TSTMP",
        "TP_INVOICE_NBR",
        "STORE_NBR",
    )
    .withColumn(
        "rownum",
        row_number().over(
            Window.partitionBy(
                col("TP_CUSTOMER_NBR"),
                to_date(col("APPT_START_TSTMP")),
                col("STORE_NBR"),
            ).orderBy(desc(col("APPT_START_TSTMP")), desc(col("TP_INVOICE_NBR")))
        ),
    )
    .filter(col("rownum") == 1)
)


# Processing node EXP_AutomatedCalls, type EXPRESSION
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
# Profile_PetsHotel_Reminder_Calls_temp = Profile_PetsHotel_Reminder_Calls.toDF(*["Profile_PetsHotel_Reminder_Calls___" + col for col in Profile_PetsHotel_Reminder_Calls.columns])

SRT_Reservations = SRT_Reservations.toDF(
    *["SRT_Reservations___" + col for col in SRT_Reservations.columns]
)

# COMMAND ----------

from datetime import datetime, timedelta

date_format = "%Y-%m-%d"

# Convert 'RunDate' and 'LastRunDate' strings to datetime objects
RunDate_dt = datetime.strptime(RunDate, date_format)
LastRunDate_dt = datetime.strptime(LastRunDate, date_format)

# Check if 'RunDate' is '01/01/1753' and perform the corresponding logic
if RunDate_dt == datetime(1753, 1, 1):
    v_RunDate = LastRunDate_dt.replace(hour=0, minute=0, second=0, microsecond=0)
else:
    v_RunDate = RunDate_dt.replace(hour=0, minute=0, second=0, microsecond=0)

v_NextRunDate = v_RunDate + timedelta(days=1)

wf_RunDate = v_NextRunDate  # corresponds to $$RunDate
wf_NextRunDate = v_NextRunDate + timedelta(days=1)  # corresponds to $$NextRunDate

v_ApptDate = v_NextRunDate + timedelta(days=int(NormalAppt))
v_HolidayAppt = v_NextRunDate + timedelta(days=int(NormalAppt))

v_PeakPeriodAppt = v_NextRunDate + timedelta(days=int(PeakPeriodAppt))
v_PeakPeriod_HolidayApptDate = v_NextRunDate + timedelta(
    days=int(PeakPeriod_HolidayApptDate)
)

# COMMAND ----------

EXP_AutomatedCalls = (
    SRT_Reservations.selectExpr(
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
        "CONCAT('RoboCalling_', DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddHHmmss'), '.Profile_PetsHotel_Reminder_Calls___txt') AS FILENAME",
        "SRT_Reservations___REGION_DESC as REGION_DESC",
        "SRT_Reservations___DISTRICT_DESC as DISTRICT_DESC",
    )
    .withColumn("RunDate", lit(v_RunDate))
    .withColumn("NextRunDate", lit(v_NextRunDate))
    .withColumn("ApptDate", lit(v_ApptDate))
    .withColumn("Holiday_ApptDate", lit(v_HolidayAppt))
    .withColumn("PeakPeriod_ApptDate", lit(v_PeakPeriodAppt))
    .withColumn("PeakPeriod_HolidayApptDate", lit(v_PeakPeriod_HolidayApptDate))
    .withColumn("v_RuleServiceName", lit("PetsHotel"))
    .withColumn("v_RuleName", lit("PEAK_PERIOD"))
)


# COMMAND ----------


# Adding deferred logic for dataframe EXP_AutomatedCalls, column v_HolidayCheck_NextDay

EXP_AutomatedCalls = EXP_AutomatedCalls.join(
    LKP_HOLIDAYS_SRC,
    (LKP_HOLIDAYS_SRC.SERVICE_NAME == lit("PetsHotel"))
    & (LKP_HOLIDAYS_SRC.COUNTRY_CD == EXP_AutomatedCalls.COUNTRY_CD)
    & (
        LKP_HOLIDAYS_SRC.HOLIDAY_DATE
        == date_add(EXP_AutomatedCalls.NextRunDate, lit(1))
    ),
    "left",
).select(EXP_AutomatedCalls["*"], LKP_HOLIDAYS_SRC["IS_CLOSED"].alias("ULKP_RETURN_1"))

EXP_AutomatedCalls = EXP_AutomatedCalls.withColumn(
    "v_HolidayCheck_NextDay", col("ULKP_RETURN_1")
)

# COMMAND ----------

# Adding deferred logic for dataframe EXP_AutomatedCalls, column v_HolidayCheck_Today

EXP_AutomatedCalls = EXP_AutomatedCalls.join(
    LKP_HOLIDAYS_SRC,
    (LKP_HOLIDAYS_SRC.SERVICE_NAME == lit("PetsHotel"))
    & (LKP_HOLIDAYS_SRC.COUNTRY_CD == EXP_AutomatedCalls.COUNTRY_CD)
    & (LKP_HOLIDAYS_SRC.HOLIDAY_DATE == EXP_AutomatedCalls.NextRunDate),
    "left",
).select(EXP_AutomatedCalls["*"], LKP_HOLIDAYS_SRC["IS_CLOSED"].alias("ULKP_RETURN_2"))

EXP_AutomatedCalls = EXP_AutomatedCalls.withColumn(
    "v_HolidayCheck_Today", col("ULKP_RETURN_2")
)

# COMMAND ----------

# Adding deferred logic for dataframe EXP_AutomatedCalls, column Holiday_Store_NextDay
EXP_AutomatedCalls = EXP_AutomatedCalls.withColumn(
    "Holiday_Store_NextDay", expr("v_HolidayCheck_NextDay as Holiday_Store_NextDay")
)

# Adding deferred logic for dataframe EXP_AutomatedCalls, column Holiday_Store_Today
EXP_AutomatedCalls = EXP_AutomatedCalls.withColumn(
    "Holiday_Store_Today", expr("v_HolidayCheck_Today as Holiday_Store_Today")
)


# Adding deferred logic for dataframe EXP_AutomatedCalls, column v_PeakPeriod_Appt
# EXP_AutomatedCalls = EXP_AutomatedCalls.join(LKP_RULES_SRC,
# 	(LKP_RULES_SRC.SERVICE_NAME == EXP_AutomatedCalls.v_RuleServiceName) & (LKP_RULES_SRC.RULE_NAME == EXP_AutomatedCalls.v_RuleName) AND (LKP_RULES_SRC.COUNTRY_CD == EXP_AutomatedCalls.COUNTRY_CD) AND (LKP_RULES_SRC.PEAK_START_DATE <= trunc(EXP_AutomatedCalls.APPT_START_TSTMP)) AND (LKP_RULES_SRC.PEAK_END_DATE >= trunc(EXP_AutomatedCalls.APPT_START_TSTMP)),'left').select(EXP_AutomatedCalls['*'], LKP_RULES_SRC['PEAK_START_DATE'].alias('ULKP_RETURN_3') )

# Create DataFrame aliases for easier column reference
exp = EXP_AutomatedCalls.alias("exp")
lkp = LKP_RULES_SRC.alias("lkp")

# Corrected join and select using col and date_trunc functions with aliases
EXP_AutomatedCalls = exp.join(
    lkp,
    (
        (lkp["SERVICE_NAME"] == exp["v_RuleServiceName"])
        & (lkp["RULE_NAME"] == exp["v_RuleName"])
        & (lkp["COUNTRY_CD"] == exp["COUNTRY_CD"])
        & (lkp["PEAK_START_DATE"] <= date_trunc("day", exp["APPT_START_TSTMP"]))
        & (lkp["PEAK_END_DATE"] >= date_trunc("day", exp["APPT_START_TSTMP"]))
    ),
    "left",
).select(exp["*"], lkp["PEAK_START_DATE"].alias("ULKP_RETURN_3"))

EXP_AutomatedCalls = EXP_AutomatedCalls.withColumn(
    "v_PeakPeriod_Appt", col("ULKP_RETURN_3")
)

# Adding deferred logic for dataframe EXP_AutomatedCalls, column PeakPeriod_CheckDate
EXP_AutomatedCalls = EXP_AutomatedCalls.withColumn(
    "PeakPeriod_CheckDate", expr("v_PeakPeriod_Appt as PeakPeriod_CheckDate")
)

# COMMAND ----------

# Processing node EXP_ReservationDates, type EXPRESSION
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
EXP_AutomatedCalls_temp = EXP_AutomatedCalls.toDF(
    *["EXP_AutomatedCalls___" + col for col in EXP_AutomatedCalls.columns]
)

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
    "EXP_AutomatedCalls___Holiday_ApptDate as Holiday_ApptDate",
    "EXP_AutomatedCalls___PeakPeriod_CheckDate as PeakPeriod_CheckDate",
    "EXP_AutomatedCalls___PeakPeriod_ApptDate as PeakPeriod_ApptDate",
    "EXP_AutomatedCalls___PeakPeriod_HolidayApptDate as PeakPeriod_HolidayApptDate",
)

# COMMAND ----------

# Processing node FIL_ReminderCalls, type FILTER
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
EXP_ReservationDates_temp = EXP_ReservationDates.toDF(
    *["EXP_ReservationDates___" + col for col in EXP_ReservationDates.columns]
)

FIL_ReminderCalls = (
    EXP_ReservationDates_temp.selectExpr(
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
        "EXP_ReservationDates___Holiday_ApptDate as Holiday_ApptDate",
        "EXP_ReservationDates___PeakPeriod_CheckDate as PeakPeriod_CheckDate",
        "EXP_ReservationDates___PeakPeriod_ApptDate as PeakPeriod_ApptDate",
        "EXP_ReservationDates___PeakPeriod_HolidayApptDate as PeakPeriod_HolidayApptDate",
    )
    .filter(
        expr(
            "(Holiday_Store_Today IS NULL AND DATE_TRUNC('day', APPT_START_TSTMP) = DATE_TRUNC('day', ApptDate)) OR "
            + "(NOT Holiday_Store_NextDay IS NULL AND DATE_TRUNC('day', APPT_START_TSTMP) = DATE_TRUNC('day', Holiday_ApptDate)) OR "
            + "(Holiday_Store_Today IS NULL AND NOT PeakPeriod_CheckDate IS NULL AND DATE_TRUNC('day', APPT_START_TSTMP) = DATE_TRUNC('day', PeakPeriod_ApptDate)) OR "
            + "(NOT Holiday_Store_NextDay IS NULL AND NOT PeakPeriod_CheckDate IS NULL AND DATE_TRUNC('day', APPT_START_TSTMP) = DATE_TRUNC('day', PeakPeriod_HolidayApptDate))"
        )
    )
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node AGG_ReminderCalls, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 21
# TP_CUSTOMER_NBR,APPT_START_TSTMP,TP_INVOICE_NBR,STORE_NBR
FIL_ReminderCalls_a = FIL_ReminderCalls.alias("a")
SRT_Reservations_1_a = SRT_Reservations_1.alias("b")
FIL_ReminderCalls_a = FIL_ReminderCalls_a.join(
    SRT_Reservations_1_a,
    [
        col("b.TP_CUSTOMER_NBR") == col("a.TP_CUSTOMER_NBR"),
        col("b.APPT_START_TSTMP") == col("a.APPT_START_TSTMP"),
        col("b.STORE_NBR") == col("a.STORE_NBR"),
    ],
).selectExpr(
    "b.TP_INVOICE_NBR as TP_INVOICE_NBR",
    "b.TP_CUSTOMER_NBR as TP_CUSTOMER_NBR",
    "CUST_FIRST_NAME as CUST_FIRST_NAME",
    "CUST_LAST_NAME as CUST_LAST_NAME",
    "CUST_NAME as CUST_NAME",
    "TP_PET_NBR as TP_PET_NBR",
    "PET_NAME as PET_NAME",
    "APPT_Start_Date AS APPT_Start_Date",
    "b.STORE_NBR as STORE_NBR",
    "TP_PET_BREED_DESC as TP_PET_BREED_DESC",
    "TP_PET_GENDER_DESC as TP_PET_GENDER_DESC",
    "POSTAL_CD as POSTAL_CD",
    "CUST_EMAIL_ADDRESS as CUST_EMAIL_ADDRESS",
    "FILENAME as FILENAME",
    "b.APPT_START_TSTMP as APPT_START_TSTMP",
    "b.APPT_END_TSTMP as APPT_END_TSTMP",
    "ApptDate as ApptDate",
    "SITE_CITY as SITE_CITY",
    "STATE_CD as STATE_CD",
    "COUNTRY_CD as COUNTRY_CD",
    "SITE_MAIN_TELE_NO as SITE_MAIN_TELE_NO",
    "REGION_DESC as REGION_DESC",
    "DISTRICT_DESC as DISTRICT_DESC",
)


AGG_ReminderCalls = (
    (
        FIL_ReminderCalls_a.selectExpr(
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
            "DISTRICT_DESC as DISTRICT_DESC",
        )
        .withColumn("New_APPT_Date", col("i_APPT_Start_Date"))
        .withColumn("New_Cust_Nbr", col("TP_CUSTOMER_NBR"))
        .withColumn("New_StoreNbr", col("STORE_NBR"))
    )
    .groupBy("TP_CUSTOMER_NBR", "i_APPT_Start_Date", "STORE_NBR")
    .agg(
        max(col("TP_INVOICE_NBR")).alias("TP_INVOICE_NBR"),
        min(col("CUST_FIRST_NAME")).alias("CUST_FIRST_NAME"),
        min(col("CUST_LAST_NAME")).alias("CUST_LAST_NAME"),
        min(col("CUST_NAME")).alias("CUST_NAME"),
        min(col("POSTAL_CD")).alias("POSTAL_CD"),
        min(col("CUST_EMAIL_ADDRESS")).alias("CUST_EMAIL_ADDRESS"),
        min(col("FILENAME")).alias("FILENAME"),
        min(col("APPT_START_TSTMP")).alias("APPT_START_TSTMP"),
        min(col("APPT_END_TSTMP")).alias("APPT_END_TSTMP"),
        min(col("SITE_CITY")).alias("SITE_CITY"),
        min(col("STATE_CD")).alias("STATE_CD"),
        min(col("COUNTRY_CD")).alias("COUNTRY_CD"),
        min(col("SITE_MAIN_TELE_NO")).alias("SITE_MAIN_TELE_NO"),
        min(col("REGION_DESC")).alias("REGION_DESC"),
        min(col("DISTRICT_DESC")).alias("DISTRICT_DESC"),
        concat_ws(";", collect_list("i_PET_NAME")).alias("Pet_Names"),
        concat_ws(";", collect_list("i_TP_PET_BREED_DESC")).alias("Breed_Desc"),
        concat_ws(";", collect_list("i_TP_PET_GENDER_DESC")).alias("Gender_Desc"),
    )
    .withColumn("sys_row_id", monotonically_increasing_id())
    .withColumn("DF_ROW_ID", monotonically_increasing_id())
)

# print(AGG_ReminderCalls.columns)

# COMMAND ----------

# Processing node EXP_Agg_Reminders, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 29

# for each involved DataFrame, append the dataframe name to each column
AGG_ReminderCalls_temp = AGG_ReminderCalls.toDF(
    *["AGG_ReminderCalls___" + col for col in AGG_ReminderCalls.columns]
)
# LKP_temp = LKP.toDF(*["LKP___" + col for col in LKP.columns])

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
    "'PetsHotel' as Svc_Type",
    "date_format(AGG_ReminderCalls___APPT_START_TSTMP, 'MM/dd/yyyy HH:mm') as Res_Start_Time",
    "date_format(AGG_ReminderCalls___APPT_END_TSTMP, 'MM/dd/yyyy HH:mm') as Res_End_Time",
)

# COMMAND ----------

# Adding deferred logic for dataframe EXP_Agg_Reminders, column v_TIME_ZONE
EXP_Agg_Reminders = EXP_Agg_Reminders.join(
    LKP_STORE_TIMEZONE_SRC,
    (LKP_STORE_TIMEZONE_SRC.FacilityNbr == EXP_Agg_Reminders.STORE_NBR),
    "left",
).select(
    EXP_Agg_Reminders["*"],
    LKP_STORE_TIMEZONE_SRC["TimeZoneDesc"].alias("ULKP_RETURN_1"),
)
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn("v_TIME_ZONE", col("ULKP_RETURN_1"))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column TimeZone
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn(
    "TimeZone",
    expr(
        """CASE v_TIME_ZONE 
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
                                                        ELSE NULL END"""
    ),
)

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Mobile
EXP_Agg_Reminders = EXP_Agg_Reminders.join(
    LKP_CUSTOMERPHONE_SRC,
    (LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR)
    & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(3)),
    "left",
).select(
    EXP_Agg_Reminders["*"],
    LKP_CUSTOMERPHONE_SRC["CUST_PHONE_NBR"].alias("ULKP_RETURN_2"),
)
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn("Mobile", col("ULKP_RETURN_2"))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Mobile2
EXP_Agg_Reminders = EXP_Agg_Reminders.join(
    LKP_CUSTOMERPHONE_SRC,
    (LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR)
    & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(4)),
    "left",
).select(
    EXP_Agg_Reminders["*"],
    LKP_CUSTOMERPHONE_SRC["CUST_PHONE_NBR"].alias("ULKP_RETURN_3"),
)
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn("Mobile2", col("ULKP_RETURN_3"))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Work
EXP_Agg_Reminders = EXP_Agg_Reminders.join(
    LKP_CUSTOMERPHONE_SRC,
    (LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR)
    & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(5)),
    "left",
).select(
    EXP_Agg_Reminders["*"],
    LKP_CUSTOMERPHONE_SRC["CUST_PHONE_NBR"].alias("ULKP_RETURN_4"),
)
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn("Work", col("ULKP_RETURN_4"))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Work2
EXP_Agg_Reminders = EXP_Agg_Reminders.join(
    LKP_CUSTOMERPHONE_SRC,
    (LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR)
    & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(6)),
    "left",
).select(
    EXP_Agg_Reminders["*"],
    LKP_CUSTOMERPHONE_SRC["CUST_PHONE_NBR"].alias("ULKP_RETURN_5"),
)
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn("Work2", col("ULKP_RETURN_5"))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Home
EXP_Agg_Reminders = EXP_Agg_Reminders.join(
    LKP_CUSTOMERPHONE_SRC,
    (LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR)
    & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(1)),
    "left",
).select(
    EXP_Agg_Reminders["*"],
    LKP_CUSTOMERPHONE_SRC["CUST_PHONE_NBR"].alias("ULKP_RETURN_6"),
)
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn("Home", col("ULKP_RETURN_6"))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column Home2
EXP_Agg_Reminders = EXP_Agg_Reminders.join(
    LKP_CUSTOMERPHONE_SRC,
    (LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR)
    & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(2)),
    "left",
).select(
    EXP_Agg_Reminders["*"],
    LKP_CUSTOMERPHONE_SRC["CUST_PHONE_NBR"].alias("ULKP_RETURN_7"),
)
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn("Home2", col("ULKP_RETURN_7"))

# Adding deferred logic for dataframe EXP_Agg_Reminders, column OtherPhone
EXP_Agg_Reminders = EXP_Agg_Reminders.join(
    LKP_CUSTOMERPHONE_SRC,
    (LKP_CUSTOMERPHONE_SRC.TP_CUSTOMER_NBR == EXP_Agg_Reminders.TP_CUSTOMER_NBR)
    & (LKP_CUSTOMERPHONE_SRC.TP_PHONE_TYPE_ID == lit(8)),
    "left",
).select(
    EXP_Agg_Reminders["*"],
    LKP_CUSTOMERPHONE_SRC["CUST_PHONE_NBR"].alias("ULKP_RETURN_8"),
)
EXP_Agg_Reminders = EXP_Agg_Reminders.withColumn("OtherPhone", col("ULKP_RETURN_8"))

# COMMAND ----------

# Processing node Automated_Reminder_Calls_PetsHotel_FF, type TARGET
# COLUMN COUNT: 29


Automated_Reminder_Calls_PetsHotel_FF = EXP_Agg_Reminders.selectExpr(
    "CAST(TP_CUSTOMER_NBR AS STRING) as TP_Cust_Nbr",
    "CAST(TP_Invoice_Nbr AS STRING) as TP_Invoice_Nbr",
    "CAST(CUST_FIRST_NAME AS STRING) as TP_Cust_First_Name",
    "CAST(CUST_LAST_NAME AS STRING) as TP_Cust_Last_Name",
    "CAST(CUST_NAME AS STRING) as TP_Cust_Name",
    "CAST(CUST_EMAIL_ADDRESS AS STRING) as TP_Cust_Email",
    "CAST(Res_Start_Time AS STRING) as TP_Res_Start_Time",
    "CAST(Res_End_Time AS STRING) as TP_Res_End_Time",
    "CAST(Mobile AS STRING) as TP_Mobile_Phone1",
    "CAST(Mobile2 AS STRING) as TP_Mobile_Phone2",
    "CAST(Home AS STRING) as TP_Home_Phone1",
    "CAST(Home2 AS STRING) as TP_Home_Phone2",
    "CAST(Work AS STRING) as TP_Work_Phone1",
    "CAST(Work2 AS STRING) as TP_Work_Phone2",
    "CAST(OtherPhone AS STRING) as TP_Other_Phone",
    "CAST(Pet_Names AS STRING) as TP_Pet_Name",
    "CAST(TP_PET_GENDER_DESC AS STRING) as TP_Pet_Gender",
    "CAST(TP_PET_BREED_DESC AS STRING) as TP_Pet_Breed",
    "CAST(Svc_Type AS STRING) as TP_Svc_Type",
    "CAST(STORE_NBR AS STRING) as TP_Store_Nbr",
    "CAST(POSTAL_CD AS STRING) as TP_Store_Zip",
    "CAST(SITE_CITY AS STRING) as TP_Store_City",
    "CAST(STATE_CD AS STRING) as TP_Store_State",
    "CAST(COUNTRY_CD AS STRING) as TP_Store_Country",
    "CAST(TimeZone AS STRING) as TP_Store_Timezone",
    "CAST(SITE_MAIN_TELE_NO AS STRING) as TP_Store_Phone",
    "CAST(DISTRICT_DESC AS STRING) as TP_Store_District",
    "CAST(REGION_DESC AS STRING) as TP_Store_Region",
    "CAST(FILENAME AS STRING) as FileName",
)


Automated_Reminder_Calls_PetsHotel_FF = Automated_Reminder_Calls_PetsHotel_FF.drop(
    "FileName"
)

current_date = starttime.strftime("%Y%m%d%H%M%S")
dateAppendeFileName = "RoboCalling_" + current_date + "." + "Profile_PetsHotel_Reminder_Calls.txt"
print(dateAppendeFileName)
if env == "prod":
    dirPath = (
        "gs://petm-bdpl-prod-nas-p1-gcs-gbl/nzmigration/automated_petshotel_reminder/"
    )
    today=datetime.now()
    filePath = dirPath + today.strftime("%Y%m%d")
    gs_source_path = filePath + "/*.txt"
    nas_target_path = "/mnt/nas05/edwshare/DataLake/Temp_NZ_Migration/"
else:
    dirPath = (
        "gs://petm-bdpl-dev-nas-p1-gcs-gbl/nzmigration/automated_petshotel_reminder/"
    )
    today=datetime.now()
    filePath = dirPath + today.strftime("%Y%m%d")
    gs_source_path = filePath + "/*.txt"
    nas_target_path = "/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test/"
try:
    writeToFlatFile(
        Automated_Reminder_Calls_PetsHotel_FF,
        filePath,
        dateAppendeFileName,
        "overwrite",
    )
    copy_file_to_nas(gs_source_path, nas_target_path)
    logPrevRunDt(
        "Automated_Reminder_Calls_PetsHotel_FF",
        "Automated_Reminder_Calls_PetsHotel_FF",
        "Completed",
        "N/A",
        f"{raw}.log_run_details",
    )
except Exception as e:
    logPrevRunDt(
        "Automated_Reminder_Calls_PetsHotel_FF",
        "Automated_Reminder_Calls_PetsHotel_FF",
        "Failed",
        str(e),
        f"{raw}.log_run_details",
    )
    raise e
