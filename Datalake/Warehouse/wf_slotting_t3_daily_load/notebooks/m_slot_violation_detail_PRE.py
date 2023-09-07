# Databricks notebook source
# Code converted on 2023-08-24 09:26:43
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

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

(username,password,connection_string) = or_kro_read_edhp1(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SL_ITEM_SCORE, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_SL_ITEM_SCORE = jdbcOracleConnection(f"""select s.LOCATION_ID, trunc(sysdate+6,'DY') week_dt, i.SLOT_ITEM_SCORE_ID, si.SLOT_ID, i.SLOTITEM_ID, sp.PRODUCT_ID,
	   decode(c.CAT_ID,5,'Slotting Group',6,'Package Type','4','Status Code',to_char(sc.cat_name)) category,
       sc.CAT_NAME,
	   cg.CNSTR_GRP_NAME, c.CNSTR_NAME, c.CNSTR_TYPE,
	   i.SCORE, c.IMPORTANCE, si.LEGAL_FIT, si.LEGAL_FIT_REASON
  from wmspadm.sl_item_score i,
  	   wmspadm.sl_item si,
	   wmspadm.sl_item_master im,
	   biwpadm.sku_profile sp,
	   biwpadm.site_profile s,
  	   wmspadm.sl_constraints c,
       wmspadm.sl_category sc, 
       wmspadm.sl_cnstr_grp cg

where i.DAY_OF_WK_NBR = to_char(sysdate-1,'D') 
  and i.whse=c.whse and i.DAY_OF_WK_NBR = c.DAY_OF_WK_NBR and i.CNSTR_ID = c.CNSTR_ID
  and c.whse = cg.whse and c.CNSTR_GRP_ID = cg.CNSTR_GRP_ID
  and c.whse = sc.WHSE(+) and c.CAT_ID = sc.CAT_ID(+)
  and i.whse = si.whse and i.DAY_OF_WK_NBR = si.DAY_OF_WK_NBR and i.SLOTITEM_ID = si.SLOTITEM_ID
  and si.WHSE= im.WHSE and si.SKU_ID = im.SKU_ID 
  and im.SKU_NAME = to_char(sp.SKU_NBR) 
  and i.WHSE = s.STORE_NBR and nvl(sc.DEL_FLG,0)=0
  and i.DEL_FLG = 0 and si.DEL_FLG = 0 and c.DEL_FLG = 0 and cg.DEL_FLG=0
  and i.score > 0""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SL_ITEM_SCORE = SQ_Shortcut_to_SL_ITEM_SCORE \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[1],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[2],'SLOT_ITEM_SCORE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[3],'SLOT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[4],'SLOTITEM_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[5],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[6],'UD_CAT_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[7],'CAT_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[8],'CNSTR_GRP_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[9],'CNSTR_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[10],'CAT_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[11],'SCORE') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[12],'IMPORTANCE') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[13],'LEGAL_FIT') \
	.withColumnRenamed(SQ_Shortcut_to_SL_ITEM_SCORE.columns[14],'LEGAL_FIT_REASON')

# COMMAND ----------

# Processing node Shortcut_to_SLOT_VIOLATION_DETAIL_PRE, type TARGET 
# COLUMN COUNT: 15


Shortcut_to_SLOT_VIOLATION_DETAIL_PRE = SQ_Shortcut_to_SL_ITEM_SCORE.selectExpr(
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(SLOT_ITEM_SCORE_ID AS INT) as SLOT_ITEM_SCORE_ID",
	"CAST(SLOT_ID AS INT) as SLOT_ID",
	"CAST(SLOTITEM_ID AS INT) as SLOTITEM_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(UD_CAT_NAME AS STRING) as CATEGORY",
	"CAST(CAT_NAME AS STRING) as CAT_NAME",
	"CAST(CNSTR_GRP_NAME AS STRING) as CNSTR_GRP_NAME",
	"CAST(CNSTR_NAME AS STRING) as CNSTR_NAME",
	"CAST(CAT_TYPE AS STRING) as CNSTR_TYPE",
	"CAST(SCORE AS DECIMAL(9,4)) as SCORE",
	"CAST(IMPORTANCE AS DECIMAL(9,2)) as IMPORTANCE",
	"CAST(LEGAL_FIT AS TINYINT) as LEGAL_FIT",
	"CAST(LEGAL_FIT_REASON AS INT) as LEGAL_FIT_REASON"
)
# overwriteDeltaPartition(Shortcut_to_SLOT_VIOLATION_DETAIL_PRE,'DC_NBR',dcnbr,f'{raw}.SLOT_VIOLATION_DETAIL_PRE')
Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SLOT_VIOLATION_DETAIL_PRE')
