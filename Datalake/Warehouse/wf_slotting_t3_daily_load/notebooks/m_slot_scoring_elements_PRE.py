# Databricks notebook source
# Code converted on 2023-08-24 09:26:54
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

# Processing node SQTRANS, type SOURCE 
# COLUMN COUNT: 6

SQTRANS = jdbcOracleConnection(f"""select ch.color_code_nbr, cd.GROUP_NBR,  ch.description,

       case when nvl(group_max,0) = 0 then 'Dk Green'

	   		when group_nbr = 1 then 'Red'

	   		when group_nbr = 2 then 'Orange'

			when group_nbr = 3 then 'Yellow'

			when group_nbr = 4 then 'Lt Green'

			when group_nbr = 5 then 'Dk Green'

			else 'N/A'  END grid_color,

CASE WHEN GROUP_NBR=5 AND GROUP_MAX is not null then 0 

	 WHEN GROUP_NBR=5 AND GROUP_MAX is null then 0

	 WHEN GROUP_NBR=1 AND LAG(GROUP_MAX) over (partition by ch.color_code_nbr order by group_nbr desc) = 0 THEN 99.9

	 ELSE LAG(GROUP_MAX) over (partition by ch.color_code_nbr order by group_nbr desc) END group_min,

	 CASE WHEN GROUP_NBR = 5 AND GROUP_MAX is null THEN 99.9 ELSE cd.GROUP_MAX END group_max

from wmspadm.sl_color_cd_hdr ch,

     wmspadm.sl_color_cd_dtl cd

where ch.WHSE        = cd.WHSE

  and ch.COLOR_CODE_ID = cd.COLOR_CODE_ID

/*  Only gather color grid from 1 DC... (They're all the same) */
  and ch.WHSE ='041' 

  and active  = 1

order by 1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQTRANS = SQTRANS \
	.withColumnRenamed(SQTRANS.columns[0],'COLOR_CODE_ID') \
	.withColumnRenamed(SQTRANS.columns[1],'GROUP_NBR') \
	.withColumnRenamed(SQTRANS.columns[2],'DESCRIPTION') \
	.withColumnRenamed(SQTRANS.columns[3],'DESCRIPTION1') \
	.withColumnRenamed(SQTRANS.columns[4],'GROUP_MIN') \
	.withColumnRenamed(SQTRANS.columns[5],'GROUP_MAX')

# COMMAND ----------

# Processing node Shortcut_to_SLOT_SCORING_ELEMENTS_PRE, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_SLOT_SCORING_ELEMENTS_PRE = SQTRANS.selectExpr(
	"CAST(COLOR_CODE_ID AS INT) as COLOR_CODE_NBR",
	"CAST(GROUP_NBR AS INT) as GROUP_NBR",
	"CAST(DESCRIPTION AS STRING) as COLOR_CODE_DESC",
	"CAST(DESCRIPTION1 AS STRING) as SLOT_COLOR",
	"CAST(GROUP_MIN AS DECIMAL(9,2)) as GROUP_MIN",
	"CAST(GROUP_MAX AS DECIMAL(9,2)) as GROUP_MAX"
)
# overwriteDeltaPartition(Shortcut_to_SLOT_SCORING_ELEMENTS_PRE,'DC_NBR',dcnbr,f'{raw}.SLOT_SCORING_ELEMENTS_PRE')
Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SLOT_SCORING_ELEMENTS_PRE')

# COMMAND ----------


