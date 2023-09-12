# Databricks notebook source
# Code converted on 2023-08-03 11:48:59
import os
import argparse
import json
import webbrowser
import requests
import xml.etree.ElementTree as ET
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from requests.structures import CaseInsensitiveDict

# COMMAND ----------

# parser = argparse.ArgumentParser()
# spark = SparkSession.getActiveSession()
# parser.add_argument("env", type=str, help="Env Variable")

# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"

store_nbr = 133

# COMMAND ----------
# Variable_declaration_comment

# mPar_ClientId = "33920cc3f6e042e1ab648b8663196d3b"
# mPar_ClientSecret = "d4E51B043aB14490a77ed96A2ED39374"
scopeName=""
if env.lower() == "prod":
    scopeName='MULESOFT_API_P'
else :
    scopeName='MULESOFT_API_NP'    

mPar_ClientId = secrets.get(scope=scopeName, key="client_id")
mPar_ClientSecret = secrets.get(scope=scopeName, key="secret")
     


# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE
# COLUMN COUNT: 1

SQ_Shortcut_to_SITE_PROFILE = spark.sql(
    f"""SELECT
SITE_PROFILE.STORE_NBR
FROM {legacy}.SITE_PROFILE
WHERE SITE_PROFILE.STORE_NBR=store_nbr"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Exp_ServicesHours, type EXPRESSION
# COLUMN COUNT: 6

api_url = "https://petm-qa-facility-svc.cloudhub.io/facility-svc/v1/store/all/hours"

headers = CaseInsensitiveDict()
headers["Accept"] = "application/xml"
headers["field"] = "1"
headers["client_id"] = mPar_ClientId
headers["client_secret"] = mPar_ClientSecret

response = requests.get(api_url, headers=headers)
with open("/dbfs/FileStore/shared_uploads/text_xml.xml", "w") as f:
    f.write(response.text)

schema = StructType(
    [
        StructField("n3_StoreNumber0", StringType(), True),
        StructField("name", StringType(), True),
        StructField("d2p1_ForDate", StringType(), True),
        StructField("d2p1_OpenTime", StringType(), True),
        StructField("d2p1_CloseTime", StringType(), True),
        StructField("d2p1_IsClosed", StringType(), True),
    ]
)
Store_ServiceHours = spark.createDataFrame([], schema)

df = (
    spark.read.format("com.databricks.spark.xml")
    .option("rootTag", "Stores")
    .option("rowTag", "StoreGetStoreHoursResponseViewModel")
    .load("dbfs:/FileStore/shared_uploads/text_xml.xml")
)

result_dict = {row["StoreNumber"]: row["StoreHours"] for row in df.collect()}

for key in result_dict:
    if key == store_nbr:
        tpl = result_dict[key]
        for key2 in tpl:
            for key3 in key2:
                hourlist = key3.asDict()
                opentime = hourlist["OpenTime"].asDict()
                closetime = hourlist["CloseTime"].asDict()
                newRow = spark.createDataFrame(
                    [
                        (
                            key,
                            None,
                            str(hourlist["ForDate"]),
                            opentime["_VALUE"],
                            closetime["_VALUE"],
                            hourlist["IsClosed"],
                        )
                    ],
                    schema,
                )
                Store_ServiceHoursNew = Store_ServiceHours.union(newRow)
                Store_ServiceHours = Store_ServiceHoursNew

result_dict = {row["StoreNumber"]: row["StoreServices"] for row in df.collect()}

for key in result_dict:
    if key == store_nbr:
        tpl = result_dict[key]
        for key2 in tpl:
            for key3 in key2:
                # print (key3,"\n")
                taglist = key3.asDict()
                # print (taglist,"\n")
                hourlist = taglist["StoreServiceHoursForDateList"].asDict()
                for key4 in hourlist:
                    rowhours = hourlist[key4]
                    for key5 in rowhours:
                        hourstaglist = key5.asDict()
                        opentime = hourstaglist["OpenTime"].asDict()
                        closetime = hourstaglist["CloseTime"].asDict()
                        # print(key,',',taglist['Name'],',',opentime['_VALUE'],,',closetime['_VALUE'],',',hourstaglist['ForDate'],',',hourstaglist['IsClosed'])
                        newRow = spark.createDataFrame(
                            [
                                (
                                    key,
                                    taglist["Name"],
                                    str(hourstaglist["ForDate"]),
                                    opentime["_VALUE"],
                                    closetime["_VALUE"],
                                    hourstaglist["IsClosed"],
                                )
                            ],
                            schema,
                        )
                        Store_ServiceHoursNew = Store_ServiceHours.union(newRow)
                        Store_ServiceHours = Store_ServiceHoursNew

Unn_Store_ServiceHours = Store_ServiceHours.join(
    SQ_Shortcut_to_SITE_PROFILE,
    Store_ServiceHours.n3_StoreNumber0 == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR,
    "inner",
)
# COMMAND ----------
# Processing node Fil_StoreHours, type FILTER
# COLUMN COUNT: 6

# # for each involved DataFrame, append the dataframe name to each column
Unn_Store_ServiceHours_temp = Unn_Store_ServiceHours.toDF(
    *["Unn_Store_ServiceHours___" + col for col in Unn_Store_ServiceHours.columns]
)

Fil_StoreHours = (
    Unn_Store_ServiceHours_temp.selectExpr(
        "Unn_Store_ServiceHours___n3_StoreNumber0 as n3_StoreNumber0",
        "Unn_Store_ServiceHours___name as name",
        "Unn_Store_ServiceHours___d2p1_ForDate as d2p1_ForDate",
        "Unn_Store_ServiceHours___d2p1_CloseTime as d2p1_CloseTime",
        "Unn_Store_ServiceHours___d2p1_OpenTime as d2p1_OpenTime",
        "Unn_Store_ServiceHours___d2p1_IsClosed as d2p1_IsClosed",
    )
    .filter("n3_StoreNumber0 is not null AND d2p1_ForDate is not null")
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------
# Processing node Exp_Site_Hours_Day_Pre, type EXPRESSION
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column

Exp_Site_Hours_Day_Pre = (
    Fil_StoreHours.withColumn(
        "v_Day_Dt", to_date(substring(Fil_StoreHours.d2p1_ForDate, 1, 10), "yyyy-MM-dd")
    )
    .withColumn(
        "v_Is_Closed", expr("""IF (UPPER ( d2p1_IsClosed ) = 'FALSE', '0', '1')""")
    )
    .withColumn(
        "v_OpenHours",
        lpad(
            regexp_extract(
                Fil_StoreHours.d2p1_OpenTime,
                "([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?",
                2,
            ),
            2,
            "0",
        ),
    )
    .withColumn(
        "v_OpenMinutes",
        lpad(
            regexp_extract(
                Fil_StoreHours.d2p1_OpenTime,
                "([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?",
                4,
            ),
            2,
            "0",
        ),
    )
    .withColumn(
        "v_OpenTstmp",
        expr(
            """IF (v_Is_Closed == 0, to_timestamp (year(v_Day_Dt) || lpad(month(v_Day_Dt),2,'0') || lpad(day(v_Day_Dt),2,'0') || v_OpenHours || v_OpenMinutes || '00', "yyyyMMddHHmmss"), to_timestamp (v_Day_Dt))"""
        ),
    )
    .withColumn(
        "v_CloseHours",
        lpad(
            regexp_extract(
                Fil_StoreHours.d2p1_CloseTime,
                "([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?",
                2,
            ),
            2,
            "0",
        ),
    )
    .withColumn(
        "v_CloseMinutes",
        lpad(
            regexp_extract(
                Fil_StoreHours.d2p1_CloseTime,
                "([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?",
                4,
            ),
            2,
            "0",
        ),
    )
    .withColumn(
        "v_CloseTstmp",
        expr(
            """IF (v_Is_Closed == 0, to_timestamp (year(v_Day_Dt) || lpad(month(v_Day_Dt),2,'0') || lpad(day(v_Day_Dt),2,'0') || v_CloseHours || v_CloseMinutes || '00', "yyyyMMddHHmmss"), to_timestamp (v_Day_Dt))"""
        ),
    )
    .selectExpr(
        "sys_row_id as sys_row_id",
        "v_Day_Dt as DAY_DT",
        "LPAD (n3_StoreNumber0 , 4 , '0' ) as LOCATION_NBR",
        "8 as LOCATION_TYPE_ID",
        "IF (name IS NULL, 'Store', name) as BUSINESS_AREA",
        "v_Is_Closed as IS_CLOSED",
        "v_OpenTstmp as OPEN_TSTMP",
        "v_CloseTstmp as CLOSE_TSTMP",
        "CURRENT_TIMESTAMP as LOAD_TSTMP",
    )
)
# COMMAND ----------
# Processing node Shortcut_to_SITE_HOURS_DAY_PRE, type TARGET
# COLUMN COUNT: 8

Shortcut_to_SITE_HOURS_DAY_PRE = Exp_Site_Hours_Day_Pre.selectExpr(
    "CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
    "CAST(LOCATION_NBR AS STRING) as LOCATION_NBR",
    "CAST(LOCATION_TYPE_ID AS TINYINT) as LOCATION_TYPE_ID",
    "CAST(BUSINESS_AREA AS STRING) as BUSINESS_AREA",
    "CAST(OPEN_TSTMP AS TIMESTAMP) as OPEN_TSTMP",
    "CAST(CLOSE_TSTMP AS TIMESTAMP) as CLOSE_TSTMP",
    "CAST(IS_CLOSED AS SMALLINT) as IS_CLOSED",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
)
# overwriteDeltaPartition(Shortcut_to_SITE_HOURS_DAY_PRE,'DC_NBR',dcnbr,f'{raw}.SITE_HOURS_DAY_PRE')
Shortcut_to_SITE_HOURS_DAY_PRE.write.mode("overwrite").saveAsTable(
    f"{raw}.SITE_HOURS_DAY_PRE"
)
