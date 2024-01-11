# Databricks notebook source
# Code converted on 2023-10-27 08:45:14
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


# COMMAND ----------

# Processing node SQ_Shortcut_to_WM_E_JOB_FUNCTION, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WM_E_JOB_FUNCTION = spark.sql(f"""SELECT DISTINCT
WM_E_JOB_FUNCTION.WM_JOB_FUNCTION_NAME
FROM {refine}.WM_E_JOB_FUNCTION""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_JOIN1, type EXPRESSION 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column

EXP_JOIN1 = SQ_Shortcut_to_WM_E_JOB_FUNCTION.selectExpr(
	"sys_row_id as sys_row_id",
	"1 as JOIN_COL1",
	"UPPER ( WM_JOB_FUNCTION_NAME ) as WM_JOB_FUNC_NAME_O"
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_WMS_INV_NEED_TYPE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WMS_INV_NEED_TYPE = spark.sql(f"""SELECT
WMS_INV_NEED_TYPE.WMS_INV_NEED_TYPE_ID,
WMS_INV_NEED_TYPE.WMS_INV_NEED_TYPE_DESC
FROM {legacy}.WMS_INV_NEED_TYPE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_JOIN2, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WMS_INV_NEED_TYPE_temp = SQ_Shortcut_to_WMS_INV_NEED_TYPE.toDF(*["SQ_Shortcut_to_WMS_INV_NEED_TYPE___" + col for col in SQ_Shortcut_to_WMS_INV_NEED_TYPE.columns])

EXP_JOIN2 = SQ_Shortcut_to_WMS_INV_NEED_TYPE_temp.selectExpr(
	"SQ_Shortcut_to_WMS_INV_NEED_TYPE___sys_row_id as sys_row_id",
	"1 as JOIN_COL2",
	"SQ_Shortcut_to_WMS_INV_NEED_TYPE___WMS_INV_NEED_TYPE_ID as WMS_INV_NEED_TYPE_ID",
	"SQ_Shortcut_to_WMS_INV_NEED_TYPE___WMS_INV_NEED_TYPE_DESC as WMS_INV_NEED_TYPE_DESC"
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_WMS_JOB_FUNCTION, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WMS_JOB_FUNCTION = spark.sql(f"""SELECT
WMS_JOB_FUNCTION.WMS_JOB_FUNCTION_ID,
WMS_JOB_FUNCTION.WMS_JOB_FUNCTION_NAME
FROM {legacy}.WMS_JOB_FUNCTION""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_INV_NEED_TYPE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_JOIN1_temp = EXP_JOIN1.toDF(*["EXP_JOIN1___" + col for col in EXP_JOIN1.columns])
EXP_JOIN2_temp = EXP_JOIN2.toDF(*["EXP_JOIN2___" + col for col in EXP_JOIN2.columns])

JNR_INV_NEED_TYPE = EXP_JOIN2_temp.join(EXP_JOIN1_temp,[EXP_JOIN2_temp.EXP_JOIN2___JOIN_COL2 == EXP_JOIN1_temp.EXP_JOIN1___JOIN_COL1],'inner').selectExpr(
	"EXP_JOIN1___sys_row_id as sys_row_id",
	"EXP_JOIN1___JOIN_COL1 as JOIN_COL1",
	"EXP_JOIN1___WM_JOB_FUNC_NAME_O as WM_JOB_FUNC_NAME",
	"EXP_JOIN2___JOIN_COL2 as JOIN_COL2",
	"EXP_JOIN2___WMS_INV_NEED_TYPE_ID as WMS_INV_NEED_TYPE_ID",
	"EXP_JOIN2___WMS_INV_NEED_TYPE_DESC as WMS_INV_NEED_TYPE_DESC")

# COMMAND ----------

# Processing node EXP_INV_NEED_TYPE, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
JNR_INV_NEED_TYPE_temp = JNR_INV_NEED_TYPE.toDF(*["JNR_INV_NEED_TYPE___" + col for col in JNR_INV_NEED_TYPE.columns])

EXP_INV_NEED_TYPE = JNR_INV_NEED_TYPE_temp.selectExpr(
	"JNR_INV_NEED_TYPE___sys_row_id as sys_row_id",
	"JNR_INV_NEED_TYPE___WM_JOB_FUNC_NAME as WM_JOB_FUNC_NAME",
	"JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_ID as WMS_INV_NEED_TYPE_ID",
	"JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_DESC as WMS_INV_NEED_TYPE_DESC",
	"DECODE ( TRUE , INSTR ( upper ( JNR_INV_NEED_TYPE___WM_JOB_FUNC_NAME ) , 'ORDERFILL' ) > 0 , IF (JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_DESC = 'Order-Fill', JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_ID, - 1) , INSTR ( upper ( JNR_INV_NEED_TYPE___WM_JOB_FUNC_NAME ) , 'PUTAWAY' ) > 0 , IF (JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_DESC = 'Putaway', JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_ID, - 1) , INSTR ( upper ( JNR_INV_NEED_TYPE___WM_JOB_FUNC_NAME ) , 'RECEIVING' ) > 0 , IF (JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_DESC = 'Receiving', JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_ID, - 1) , INSTR ( upper ( JNR_INV_NEED_TYPE___WM_JOB_FUNC_NAME ) , 'REPLEN' ) > 0 OR upper ( JNR_INV_NEED_TYPE___WM_JOB_FUNC_NAME ) IN ( 'RSR','MODULE STOCKING' ) , IF (JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_DESC = 'Replenishment', JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_ID, - 1) , INSTR ( upper ( JNR_INV_NEED_TYPE___WM_JOB_FUNC_NAME ) , 'SHIP' ) > 0 OR upper ( JNR_INV_NEED_TYPE___WM_JOB_FUNC_NAME ) IN ( 'MODULE CONSOLIDATION' ) , IF (JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_DESC = 'Shipping', JNR_INV_NEED_TYPE___WMS_INV_NEED_TYPE_ID, - 1) , 0 ) as INV_NEED_TYPE_ID"
)

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WMS_JOB_FUNCTION_temp = SQ_Shortcut_to_WMS_JOB_FUNCTION.toDF(*["SQ_Shortcut_to_WMS_JOB_FUNCTION___" + col for col in SQ_Shortcut_to_WMS_JOB_FUNCTION.columns])

EXPTRANS = SQ_Shortcut_to_WMS_JOB_FUNCTION_temp.selectExpr(
	"SQ_Shortcut_to_WMS_JOB_FUNCTION___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_WMS_JOB_FUNCTION___WMS_JOB_FUNCTION_ID as WMS_JOB_FUNCTION_ID",
	"UPPER ( SQ_Shortcut_to_WMS_JOB_FUNCTION___WMS_JOB_FUNCTION_NAME ) as WMS_JOB_FUNCTION_NAME_O"
)

# COMMAND ----------

# Processing node FIL_INV_NEED_TYPE, type FILTER 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
EXP_INV_NEED_TYPE_temp = EXP_INV_NEED_TYPE.toDF(*["EXP_INV_NEED_TYPE___" + col for col in EXP_INV_NEED_TYPE.columns])

FIL_INV_NEED_TYPE = EXP_INV_NEED_TYPE_temp.selectExpr(
	"EXP_INV_NEED_TYPE___WM_JOB_FUNC_NAME as WM_JOB_FUNC_NAME",
	"EXP_INV_NEED_TYPE___WMS_INV_NEED_TYPE_ID as WMS_INV_NEED_TYPE_ID",
	"EXP_INV_NEED_TYPE___WMS_INV_NEED_TYPE_DESC as WMS_INV_NEED_TYPE_DESC",
	"EXP_INV_NEED_TYPE___INV_NEED_TYPE_ID as INV_NEED_TYPE_ID").filter("INV_NEED_TYPE_ID != - 1").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_WMS_JOB_FUNCTION, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])
FIL_INV_NEED_TYPE_temp = FIL_INV_NEED_TYPE.toDF(*["FIL_INV_NEED_TYPE___" + col for col in FIL_INV_NEED_TYPE.columns])

JNR_WMS_JOB_FUNCTION = EXPTRANS_temp.join(FIL_INV_NEED_TYPE_temp,[EXPTRANS_temp.EXPTRANS___WMS_JOB_FUNCTION_NAME_O == FIL_INV_NEED_TYPE_temp.FIL_INV_NEED_TYPE___WM_JOB_FUNC_NAME],'right_outer').selectExpr(
	"FIL_INV_NEED_TYPE___WM_JOB_FUNC_NAME as WM_JOB_FUNC_NAME",
	"FIL_INV_NEED_TYPE___WMS_INV_NEED_TYPE_ID as WMS_INV_NEED_TYPE_ID",
	"FIL_INV_NEED_TYPE___WMS_INV_NEED_TYPE_DESC as WMS_INV_NEED_TYPE_DESC",
	"FIL_INV_NEED_TYPE___INV_NEED_TYPE_ID as INV_NEED_TYPE_ID",
	"EXPTRANS___WMS_JOB_FUNCTION_ID as i_WMS_JOB_FUNCTION_ID",
	"EXPTRANS___WMS_JOB_FUNCTION_NAME_O as i_WMS_JOB_FUNCTION_NAME")

# COMMAND ----------

# Processing node FIL_JOB_FUNCTION_ID, type FILTER 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
JNR_WMS_JOB_FUNCTION_temp = JNR_WMS_JOB_FUNCTION.toDF(*["JNR_WMS_JOB_FUNCTION___" + col for col in JNR_WMS_JOB_FUNCTION.columns])

FIL_JOB_FUNCTION_ID = JNR_WMS_JOB_FUNCTION_temp.selectExpr(
	"JNR_WMS_JOB_FUNCTION___WM_JOB_FUNC_NAME as WM_JOB_FUNC_NAME",
	"JNR_WMS_JOB_FUNCTION___INV_NEED_TYPE_ID as INV_NEED_TYPE_ID",
	"JNR_WMS_JOB_FUNCTION___i_WMS_JOB_FUNCTION_ID as i_WMS_JOB_FUNCTION_ID").filter("i_WMS_JOB_FUNCTION_ID IS NULL").withColumn("sys_row_id", monotonically_increasing_id())



# COMMAND ----------

# Processing node SRT_JOB_FUNCtION_ID, type SORTER 
# COLUMN COUNT: 2

SRT_JOB_FUNCtION_ID = FIL_JOB_FUNCTION_ID.sort(col('WM_JOB_FUNC_NAME').asc(), col('INV_NEED_TYPE_ID').asc())



# COMMAND ----------

# Processing node SQL_JOB_FUNCTION_ID, type SQL_TRANSFORM 
# COLUMN COUNT: 6

NEXTVAL_DF = spark.sql(f"""
         SELECT NVL(MAX(WMS_JOB_FUNCTION_ID),0) + 1 as job_func_id FROM {legacy}.WMS_JOB_FUNCTION                      
""")
job_func_id=NEXTVAL_DF.first()["job_func_id"]



# COMMAND ----------

# Processing node EXP_WMS_JOB_FUNCTION, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQL_JOB_FUNCTION_ID_temp = SRT_JOB_FUNCtION_ID.toDF(*["SQL_JOB_FUNCTION_ID___" + col for col in SRT_JOB_FUNCtION_ID.columns])

EXP_WMS_JOB_FUNCTION = SQL_JOB_FUNCTION_ID_temp \
    .selectExpr(
	"SQL_JOB_FUNCTION_ID___WM_JOB_FUNC_NAME as WM_JOB_FUNC_NAME",
	"SQL_JOB_FUNCTION_ID___INV_NEED_TYPE_ID as INV_NEED_TYPE_ID",
	"CURRENT_TIMESTAMP ()  as UPDATE_DT",
	"CURRENT_TIMESTAMP ()  as LOAD_DT" 
    ).withColumn("WMS_JOB_FUNCTION_ID", monotonically_increasing_id()+job_func_id)


# COMMAND ----------

# Processing node Shortcut_to_WMS_JOB_FUNCTION1, type TARGET 
# COLUMN COUNT: 5

Shortcut_to_WMS_JOB_FUNCTION1 = EXP_WMS_JOB_FUNCTION.selectExpr(
	"CAST(WMS_JOB_FUNCTION_ID AS SMALLINT) as WMS_JOB_FUNCTION_ID",
	"CAST(WM_JOB_FUNC_NAME AS STRING) as WMS_JOB_FUNCTION_NAME",
	"CAST(INV_NEED_TYPE_ID AS SMALLINT) as WMS_INV_NEED_TYPE_ID",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
try:
	chk=DuplicateChecker()
	chk.check_for_duplicate_primary_keys(Shortcut_to_WMS_JOB_FUNCTION1,["WMS_JOB_FUNCTION_ID"])
	Shortcut_to_WMS_JOB_FUNCTION1.write.mode("append").saveAsTable(f'{legacy}.WMS_JOB_FUNCTION')
except Exception as e:
	raise e

# COMMAND ----------


