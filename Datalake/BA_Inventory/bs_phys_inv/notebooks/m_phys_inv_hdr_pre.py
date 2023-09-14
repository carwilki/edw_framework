# Databricks notebook source
# Code converted on 2023-08-22 11:02:00
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


# COMMAND ----------

dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/phys_inv/')
source_bucket = dbutils.widgets.get('source_bucket')

# COMMAND ----------

# Processing node SQ_Shortcut_To_IKPF_Physical_Inventory, type SOURCE 
# COLUMN COUNT: 9

_bucket=getParameterValue(raw,'BA_Inventory_Parameter.prm','BA_Inventory.WF:bs_phys_inv','source_bucket')
key ="phys"

def get_source_file(key, _bucket):
    import builtins
    lst = dbutils.fs.ls(_bucket)
    dirs = [item for item in lst if item.isDir()]
    fldr = builtins.max(dirs, key=lambda x: x.name).name
    lst = dbutils.fs.ls(_bucket + fldr)
    files = [x.path for x in lst if x.name.startswith(key)]
    return files[0] if files else None

source_file = get_source_file(key, _bucket)
# TESTED WITH SPECIFIC FILE BELOW
#source_file = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/phys_inv/20230825/physinv20230824_170352.dat"
print(source_file)

csv_options = {
    "sep": ",",
    "header": False,           # The first row contains column names
}

SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT = spark.read.csv(source_file, **csv_options)



# COMMAND ----------

SQ_Shortcut_To_IKPF_Physical_Inventory = SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT.select(
    SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT._c0.substr(1,1).alias('DELETE_IND'),
    SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT._c0.substr(2,10).alias('PHYS_INV_DOC_NBR'),
    SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT._c0.substr(12,4).alias('FISCAL_YR'),
    SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT._c0.substr(16,4).alias('STORE_NBR'),
    SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT._c0.substr(20,8).alias('PLANNED_COUNT_DT'),
    SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT._c0.substr(28,8).alias('LAST_COUNT_DT'),
    SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT._c0.substr(36,8).alias('DOC_POSTING_DT'),
    SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT._c0.substr(44,12).alias('USER_NAME'),
    SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT._c0.substr(56,16).alias('PHYS_INV_DESC')
).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------


PLANNED_COUNT_DATE = SQ_Shortcut_To_IKPF_Physical_Inventory \
  .withColumn("v_EFF_DT", lit('00010101')) \
	.withColumn("v_END_DT", lit('99991231')) \
	.selectExpr(
	"sys_row_id as sys_row_id",
	"IF (PLANNED_COUNT_DT = '00000000', TO_DATE ( v_EFF_DT , 'yyyyMMdd' ), TO_DATE ( PLANNED_COUNT_DT , 'yyyyMMdd' )) as o_PLANNED_COUNT_DT"
)

# COMMAND ----------


EXPTRANS = SQ_Shortcut_To_IKPF_Physical_Inventory.selectExpr(
	"sys_row_id as sys_row_id",
	"rtrim ( STORE_NBR ) as o_STORE_NBR",
	"rtrim ( PHYS_INV_DESC ) as o_PHYS_INV_DESC"
)

# COMMAND ----------


LAST_COUNT_DATE = SQ_Shortcut_To_IKPF_Physical_Inventory \
  .withColumn('i_TIME_ONLY', lit(None)) \
  .withColumn("v_EFF_DT", lit('00010101')) \
	.withColumn("v_END_DT", lit('99991231')) \
	.selectExpr(
	"sys_row_id as sys_row_id",
	"IF (LAST_COUNT_DT = '00000000', TO_DATE ( v_EFF_DT , 'yyyyMMdd' ), TO_DATE ( LAST_COUNT_DT , 'yyyyMMdd' )) as o_LAST_COUNT_DT"
)

# COMMAND ----------


DOC_POSTING_DATE_TRANS = SQ_Shortcut_To_IKPF_Physical_Inventory \
  .withColumn("v_EFF_DT", lit('00010101')) \
	.withColumn("v_END_DT", lit('99991231')) \
	.selectExpr(
	"sys_row_id as sys_row_id",
	"IF (DOC_POSTING_DT = '00000000', TO_DATE ( v_EFF_DT , 'yyyyMMdd' ), TO_DATE ( DOC_POSTING_DT , 'yyyyMMdd' )) as o_DOC_POSTING_DT",
)

# COMMAND ----------

# Processing node Shortcut_To_PHYS_INV_HDR_PRE, type TARGET 
# COLUMN COUNT: 8

# Joining dataframes SQ_Shortcut_To_IKPF_Physical_Inventory, PLANNED_COUNT_DATE, EXPTRANS, LAST_COUNT_DATE, DOC_POSTING_DATE_TRANS to form Shortcut_To_PHYS_INV_HDR_PRE
Shortcut_To_PHYS_INV_HDR_PRE_joined = SQ_Shortcut_To_IKPF_Physical_Inventory.join(PLANNED_COUNT_DATE, SQ_Shortcut_To_IKPF_Physical_Inventory.sys_row_id == PLANNED_COUNT_DATE.sys_row_id, 'inner') \
.join(EXPTRANS, PLANNED_COUNT_DATE.sys_row_id == EXPTRANS.sys_row_id, 'inner')\
.join(LAST_COUNT_DATE, EXPTRANS.sys_row_id == LAST_COUNT_DATE.sys_row_id, 'inner')\
.join(DOC_POSTING_DATE_TRANS, LAST_COUNT_DATE.sys_row_id == DOC_POSTING_DATE_TRANS.sys_row_id, 'inner')

Shortcut_To_PHYS_INV_HDR_PRE = Shortcut_To_PHYS_INV_HDR_PRE_joined.selectExpr(
	"CAST(PHYS_INV_DOC_NBR AS BIGINT) as PHYS_INV_DOC_NBR",
	"CAST(FISCAL_YR AS SMALLINT) as FISCAL_YR",
	"CAST(o_STORE_NBR AS INT) as STORE_NBR",
	"CAST(o_PLANNED_COUNT_DT AS TIMESTAMP) as PLANNED_COUNT_DT",
	"CAST(o_LAST_COUNT_DT AS TIMESTAMP) as LAST_COUNT_DT",
	"CAST(o_DOC_POSTING_DT AS TIMESTAMP) as DOC_POSTING_DT",
	"CAST(USER_NAME AS STRING) as USER_NAME",
	"CAST(o_PHYS_INV_DESC AS STRING) as PHYS_INV_DESC"
)
# overwriteDeltaPartition(Shortcut_To_PHYS_INV_HDR_PRE,'DC_NBR',dcnbr,f'{raw}.PHYS_INV_HDR_PRE')
Shortcut_To_PHYS_INV_HDR_PRE.write.mode("overwrite").saveAsTable(f'{raw}.PHYS_INV_HDR_PRE')

# COMMAND ----------


