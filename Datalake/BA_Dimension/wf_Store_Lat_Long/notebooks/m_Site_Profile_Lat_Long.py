# Databricks notebook source
#Code converted on 2023-09-26 11:24:09
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

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node ASQ_Store_Lat_Long, type SOURCE 
# COLUMN COUNT: 3

ASQ_Store_Lat_Long = spark.sql(f"""
SELECT SITE.LOCATION_ID,PRE.LAT,PRE.LON
FROM {legacy}.SITE_PROFILE SITE, {raw}.STORE_LAT_LONG_PRE PRE
WHERE SITE.STORE_NBR = PRE.STORE_NBR
AND (PRE.LAT <> SITE.GEO_LATITUDE_NBR or PRE.LON <> SITE.GEO_LONGITUDE_NBR)""").withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
ASQ_Store_Lat_Long = ASQ_Store_Lat_Long \
	.withColumnRenamed(ASQ_Store_Lat_Long.columns[0],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Store_Lat_Long.columns[1],'PRE_LAT') \
	.withColumnRenamed(ASQ_Store_Lat_Long.columns[2],'PRE_LON')

# COMMAND ----------

# Processing node Shortcut_to_SITE_PROFILE1, type TARGET 
# COLUMN COUNT: 101


Shortcut_to_SITE_PROFILE1 = ASQ_Store_Lat_Long.selectExpr(
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(NULL AS INT) as LOCATION_TYPE_ID",
	"CAST(NULL AS INT) as STORE_NBR",
	"CAST(NULL AS STRING) as STORE_NAME",
	"CAST(NULL AS STRING) as STORE_TYPE_ID",
	"CAST(NULL AS STRING) as STORE_OPEN_CLOSE_FLAG",
	"CAST(NULL AS INT) as COMPANY_ID",
	"CAST(NULL AS BIGINT) as REGION_ID",
	"CAST(NULL AS BIGINT) as DISTRICT_ID",
	"CAST(NULL AS STRING) as PRICE_ZONE_ID",
	"CAST(NULL AS STRING) as PRICE_AD_ZONE_ID",
	"CAST(NULL AS INT) as REPL_DC_NBR",
	"CAST(NULL AS INT) as REPL_FISH_DC_NBR",
	"CAST(NULL AS INT) as REPL_FWD_DC_NBR",
	"CAST(NULL AS DOUBLE) as SQ_FEET_RETAIL",
	"CAST(NULL AS DOUBLE) as SQ_FEET_TOTAL",
	"CAST(NULL AS STRING) as SITE_ADDRESS",
	"CAST(NULL AS STRING) as SITE_CITY",
	"CAST(NULL AS STRING) as STATE_CD",
	"CAST(NULL AS STRING) as COUNTRY_CD",
	"CAST(NULL AS STRING) as POSTAL_CD",
	"CAST(NULL AS STRING) as SITE_MAIN_TELE_NO",
	"CAST(NULL AS STRING) as SITE_GROOM_TELE_NO",
	"CAST(NULL AS STRING) as SITE_EMAIL_ADDRESS",
	"CAST(NULL AS STRING) as SITE_SALES_FLAG",
	"CAST(NULL AS INT) as EQUINE_MERCH_ID",
	"CAST(NULL AS INT) as EQUINE_SITE_ID",
	"CAST(NULL AS TIMESTAMP) as EQUINE_SITE_OPEN_DT",
	"CAST(PRE_LAT AS DECIMAL(12,6)) as GEO_LATITUDE_NBR",
	"CAST(PRE_LON AS DECIMAL(12,6)) as GEO_LONGITUDE_NBR",
	"CAST(NULL AS STRING) as PETSMART_DMA_CD",
	"CAST(NULL AS INT) as LOYALTY_PGM_TYPE_ID",
	"CAST(NULL AS INT) as LOYALTY_PGM_STATUS_ID",
	"CAST(NULL AS TIMESTAMP) as LOYALTY_PGM_START_DT",
	"CAST(NULL AS TIMESTAMP) as LOYALTY_PGM_CHANGE_DT",
	"CAST(NULL AS INT) as BP_COMPANY_NBR",
	"CAST(NULL AS INT) as BP_GL_ACCT",
	"CAST(NULL AS STRING) as TP_LOC_FLAG",
	"CAST(NULL AS INT) as TP_ACTIVE_CNT",
	"CAST(NULL AS STRING) as PROMO_LABEL_CD",
	"CAST(NULL AS BIGINT) as PARENT_LOCATION_ID",
	"CAST(NULL AS STRING) as LOCATION_NBR",
	"CAST(NULL AS STRING) as TIME_ZONE_ID",
	"CAST(NULL AS STRING) as DELV_SERVICE_CLASS_ID",
	"CAST(NULL AS STRING) as PICK_SERVICE_CLASS_ID",
	"CAST(NULL AS STRING) as SITE_LOGIN_ID",
	"CAST(NULL AS BIGINT) as SITE_MANAGER_ID",
	"CAST(NULL AS DECIMAL(5,2)) as SITE_OPEN_YRS_AMT",
	"CAST(NULL AS INT) as HOTEL_FLAG",
	"CAST(NULL AS INT) as DAYCAMP_FLAG",
	"CAST(NULL AS INT) as VET_FLAG",
	"CAST(NULL AS STRING) as DIST_MGR_NAME",
	"CAST(NULL AS STRING) as DIST_SVC_MGR_NAME",
	"CAST(NULL AS STRING) as REGION_VP_NAME",
	"CAST(NULL AS STRING) as REGION_TRAINER_NAME",
	"CAST(NULL AS STRING) as ASSET_PROTECT_NAME",
	"CAST(NULL AS STRING) as SITE_COUNTY",
	"CAST(NULL AS STRING) as SITE_FAX_NO",
	"CAST(NULL AS TIMESTAMP) as SFT_OPEN_DT",
	"CAST(NULL AS STRING) as DM_EMAIL_ADDRESS",
	"CAST(NULL AS STRING) as DSM_EMAIL_ADDRESS",
	"CAST(NULL AS STRING) as RVP_EMAIL_ADDRESS",
	"CAST(NULL AS STRING) as TRADE_AREA",
	"CAST(NULL AS STRING) as FDLPS_NAME",
	"CAST(NULL AS STRING) as FDLPS_EMAIL",
	"CAST(NULL AS STRING) as OVERSITE_MGR_NAME",
	"CAST(NULL AS STRING) as OVERSITE_MGR_EMAIL",
	"CAST(NULL AS STRING) as SAFETY_DIRECTOR_NAME",
	"CAST(NULL AS STRING) as SAFETY_DIRECTOR_EMAIL",
	"CAST(NULL AS STRING) as RETAIL_MANAGER_SAFETY_NAME",
	"CAST(NULL AS STRING) as RETAIL_MANAGER_SAFETY_EMAIL",
	"CAST(NULL AS STRING) as AREA_DIRECTOR_NAME",
	"CAST(NULL AS STRING) as AREA_DIRECTOR_EMAIL",
	"CAST(NULL AS STRING) as DC_GENERAL_MANAGER_NAME",
	"CAST(NULL AS STRING) as DC_GENERAL_MANAGER_EMAIL",
	"CAST(NULL AS STRING) as ASST_DC_GENERAL_MANAGER_NAME1",
	"CAST(NULL AS STRING) as ASST_DC_GENERAL_MANAGER_EMAIL1",
	"CAST(NULL AS STRING) as ASST_DC_GENERAL_MANAGER_NAME2",
	"CAST(NULL AS STRING) as ASST_DC_GENERAL_MANAGER_EMAIL2",
	"CAST(NULL AS STRING) as REGIONAL_DC_SAFETY_MGR_NAME",
	"CAST(NULL AS STRING) as REGIONAL_DC_SAFETY_MGR_EMAIL",
	"CAST(NULL AS STRING) as DC_PEOPLE_SUPERVISOR_NAME",
	"CAST(NULL AS STRING) as DC_PEOPLE_SUPERVISOR_EMAIL",
	"CAST(NULL AS STRING) as PEOPLE_MANAGER_NAME",
	"CAST(NULL AS STRING) as PEOPLE_MANAGER_EMAIL",
	"CAST(NULL AS STRING) as ASSET_PROT_DIR_NAME",
	"CAST(NULL AS STRING) as ASSET_PROT_DIR_EMAIL",
	"CAST(NULL AS STRING) as SR_REG_ASSET_PROT_MGR_NAME",
	"CAST(NULL AS STRING) as SR_REG_ASSET_PROT_MGR_EMAIL",
	"CAST(NULL AS STRING) as REG_ASSET_PROT_MGR_NAME",
	"CAST(NULL AS STRING) as REG_ASSET_PROT_MGR_EMAIL",
	"CAST(NULL AS STRING) as ASSET_PROTECT_EMAIL",
	"CAST(NULL AS TIMESTAMP) as TP_START_DT",
	"CAST(NULL AS TIMESTAMP) as OPEN_DT",
	"CAST(NULL AS TIMESTAMP) as GR_OPEN_DT",
	"CAST(NULL AS TIMESTAMP) as CLOSE_DT",
	"CAST(NULL AS TIMESTAMP) as HOTEL_OPEN_DT",
	"CAST(NULL AS TIMESTAMP) as ADD_DT",
	"CAST(NULL AS TIMESTAMP) as DELETE_DT",
	"CAST(NULL AS TIMESTAMP) as UPDATE_DT",
	"CAST(NULL AS TIMESTAMP) as LOAD_DT"
)

# COMMAND ----------

def execute_update(dataframe, target_table, join_condition):
  # Create a temporary view from the DataFrame
  sourceTempView = "temp_source_" + target_table.split(".")[1]
  
  dataframe.createOrReplaceTempView(sourceTempView)
 
  merge_sql =  f"""MERGE INTO {target_table} target
                   USING {sourceTempView} source
                   ON {join_condition}
                   WHEN MATCHED THEN UPDATE
                  SET target.GEO_LATITUDE_NBR= source.GEO_LATITUDE_NBR , target.GEO_LONGITUDE_NBR =source.GEO_LONGITUDE_NBR
                """
 
  spark.sql(merge_sql) 

# COMMAND ----------

try:
	primary_key = "source.LOCATION_ID = target.LOCATION_ID"
	refined_perf_table = f"{legacy}.SITE_PROFILE"
	execute_update(Shortcut_to_SITE_PROFILE1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed")
	logPrevRunDt("SITE_PROFILE", "SITE_PROFILE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("SITE_PROFILE", "SITE_PROFILE","Failed",str(e), f"{raw}.log_run_details", )
	raise e
    
    

# COMMAND ----------


