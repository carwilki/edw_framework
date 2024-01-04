# Databricks notebook source
# Code converted on 2023-11-09 07:58:55
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
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
enterprise = getEnvPrefix(env) + 'enterprise'
empl_protected = getEnvPrefix(env) + 'empl_protected'


# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 22

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""select

SP.LOCATION_NBR LOC, SP.STORE_NAME,

LC.LOCATION_TYPE_DESC LOC_TYPE_DESC,

DATE_FORMAT(SP.OPEN_DT,'MM/dd/yyyy') OpenDate,

DATE_FORMAT(SP.OPEN_DT,'yyyy') OpenYear,

decode(SP.CLOSE_DT,'9999-12-31 00:00:00',null,DATE_FORMAT(SP.CLOSE_DT,'MM/dd/yyyy')) ClosedDate,

decode(SP.CLOSE_DT,'9999-12-31 00:00:00',null,DATE_FORMAT(SP.CLOSE_DT,'yyyy')) ClosedYear,

case when SP.HOTEL_FLAG = 1

     then DATE_FORMAT(SP.HOTEL_OPEN_DT,'MM/dd/yyyy')

     else null

     end as HotelOpenDate,

case when SP.HOTEL_OPEN_DT is not null then Null

     when SP.CLOSE_DT is not null then DATE_FORMAT(SP.CLOSE_DT,'MM/dd/yyyy')

     else null

     end as HotelClosedDate,

EP.EMPL_FIRST_NAME||' '||EP.EMPL_LAST_NAME Manager,

SP.DIST_MGR_NAME DM,

SP.REGION_VP_NAME RVP,

DATE_FORMAT(SS.COMP_EFF_DT,'MM/dd/yyyy') CompStart,

D.FISCAL_WK CompWk,

SS.COMP_CURR_FLAG Comp,

SP.HOTEL_FLAG Hotel,

SP.VET_FLAG Vet,

DATE_FORMAT(SP.ADD_DT,'MM/dd/yyyy') CreateDate,

sqft.TTL_SQ_FT,

sp.state_cd as state,

sp.site_sales_flag as sitesalesflag,

case when dfy.fiscal_yr is not null

	then cast(dfy.fiscal_yr as varchar(4))

	when DATE_FORMAT(sp.open_dt,'MM') = '01'

	then DATE_FORMAT(date_add(sp.open_dt,-40),'yyyy')

	else DATE_FORMAT(sp.open_dt,'yyyy')

	end as siteopenfyr	

from {legacy}.site_profile sp

     left outer join {legacy}.slscmp_store ss

     on sp.location_id = ss.location_id

     left outer join {enterprise}.days dfy

     on dfy.day_dt = sp.open_dt

     left outer join {enterprise}.days d

     on d.day_dt = ss.comp_eff_dt

     left outer join {empl_protected}.legacy_employee_profile ep

     on sp.SITE_MANAGER_ID = ep.employee_id

     join {legacy}.LOCATION_TYPE LC

     on SP.LOCATION_TYPE_ID = LC.LOCATION_TYPE_ID

     left outer join (select       a11.WEEK_DT  WEEK_DT_CUSTCOL,

                                a11.location_id,

                                sum(a11.SQ_FT_AMT)  TTL_SQ_FT

                      from      {legacy}.LOCATION_AREA_VIEW a11

                                join {legacy}.SITE_PROFILE_RPT a12

                                  on (a11.LOCATION_ID = a12.LOCATION_ID)

                      where     (a11.AREA_ID in (1)

                      and       a11.WEEK_DT = ((CURRENT_DATE -7) - (date_part('dow', CURRENT_DATE -7)-1)))

                      group by  a11.WEEK_DT,

                                a11.location_id) sqft

     on SP.location_id = sqft.location_id

order by  1""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[0],'LOCATION_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[1],'STORE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[2],'LOCATION_TYPE_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[3],'OpenDate') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[4],'OpenYear') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[5],'ClosedDate') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[6],'ClosedYear') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[7],'HotelOpenDate') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[8],'HotelClosedDate') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[9],'EMPL_FIRST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[10],'DIST_MGR_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[11],'REGION_VP_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[12],'CompStart') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[13],'CompWk') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[14],'COMP_CURR_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[15],'HOTEL_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[16],'VET_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[17],'CreateDate') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[18],'SQ_FT_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[19],'STATE_CD') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[20],'SITE_SALES_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[21],'FISCAL_YR')

# COMMAND ----------

current_date = datetime.today().strftime('%Y%m%d')
target_bucket=getParameterValue(raw,'wf_BPC','m_bpc_ff','target_bucket')
key=getParameterValue(raw,'wf_BPC','m_bpc_ff','key')

target_bucket=target_bucket + str(current_date) + f'/' 

target_file=target_bucket + key

nas_target_path=getParameterValue(raw,'wf_BPC','m_bpc_ff','nas_target_path')



# COMMAND ----------

# Processing node SQ_bpc_sqft_ff, type TARGET 
# COLUMN COUNT: 22


SQ_bpc_sqft_ff = SQ_Shortcut_to_SITE_PROFILE.selectExpr(
	"CAST(LOCATION_NBR AS STRING) as LOCATION_NBR",
	"CAST(STORE_NAME AS STRING) as STORE_NAME",
	"CAST(LOCATION_TYPE_DESC AS STRING) as LOCATION_TYPE_DESC",
	"CAST(OpenDate AS STRING) as OpenDate",
	"CAST(OpenYear AS STRING) as OpenYear",
	"CAST(ClosedDate AS STRING) as ClosedDate",
	"CAST(ClosedYear AS STRING) as ClosedYear",
	"CAST(HotelOpenDate AS STRING) as HotelOpenDate",
	"CAST(HotelClosedDate AS STRING) as HotelClosedDate",
	"CAST(EMPL_FIRST_NAME AS STRING) as EMPL_FIRST_NAME",
	"CAST(DIST_MGR_NAME AS STRING) as DIST_MGR_NAME",
	"CAST(REGION_VP_NAME AS STRING) as REGION_VP_NAME",
	"CAST(CompStart AS STRING) as CompStart",
	"CAST(CompWk AS STRING) as CompWk",
	"COMP_CURR_FLAG as COMP_CURR_FLAG",
	"HOTEL_FLAG as HOTEL_FLAG",
	"VET_FLAG as VET_FLAG",
	"CAST(CreateDate AS STRING) as CreateDate",
	"SQ_FT_AMT as SQ_FT_AMT",
	"CAST(STATE_CD AS STRING) as STATE",
	"CAST(SITE_SALES_FLAG AS STRING) as SITE_SALES_FLAG",
	"CAST(FISCAL_YR AS STRING) as FISCAL_YR"
).withColumn("STORE_NAME",regexp_replace(col('STORE_NAME'), r'[^\x00-\x7F]', ' '))



# COMMAND ----------

try:
	SQ_bpc_sqft_ff.repartition(1).write.mode('overwrite').options(header='False', delimiter='|').csv(target_bucket.strip("/") + "/" + key[:-4])
	removeTransactionFiles(target_bucket.strip("/") + "/" + key[:-4])
	newFilePath = target_bucket.strip("/") + "/" + key[:-4]
	renamePartFileNames(newFilePath, newFilePath,'.out')
	copy_file_to_nas(target_file ,nas_target_path)
	logPrevRunDt("wf_BPC", "m_BPC_ff", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("wf_BPC", "m_BPC_ff","Failed",str(e), f"{raw}.log_run_details", )
	raise e
