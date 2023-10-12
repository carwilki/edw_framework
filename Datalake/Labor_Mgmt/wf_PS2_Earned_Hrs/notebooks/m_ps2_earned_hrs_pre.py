# Databricks notebook source
# Code converted on 2023-09-13 14:38:35
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

def or_kro_read(env):
    if env.lower() == "dev" or env.lower() == "qa":
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.238"
        portnumber = "1800"
        db = "krop1"
        password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"temp_krop1_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username, password, connection_string)

(username, password, connection_string) = or_kro_read(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_LABFORECASTTOT, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_LABFORECASTTOT = jdbcOracleConnection(f"""SELECT lab.laborfcdt

      ,lab.orgidsid

      ,lab.laborhoursamt

  FROM labforecasttot lab

 WHERE lab.laborfctypeid = 4

   AND lab.enteredondtm > TRUNC(SYSDATE) -7

   AND lab.laborfcdt > TRUNC(SYSDATE) - 35""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_LABFORECASTTOT = SQ_Shortcut_to_LABFORECASTTOT \
	.withColumnRenamed(SQ_Shortcut_to_LABFORECASTTOT.columns[0],'LABORFCDT') \
	.withColumnRenamed(SQ_Shortcut_to_LABFORECASTTOT.columns[1],'ORGIDSID') \
	.withColumnRenamed(SQ_Shortcut_to_LABFORECASTTOT.columns[2],'LABORHOURSAMT')

# COMMAND ----------

# Processing node Shortcut_to_PS2_EARNED_HRS_PRE, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_PS2_EARNED_HRS_PRE = SQ_Shortcut_to_LABFORECASTTOT.selectExpr(
	"CAST(LABORFCDT AS TIMESTAMP) as DAY_DT",
	"CAST(ORGIDSID AS BIGINT) as ORG_IDS_ID",
	"CAST(LABORHOURSAMT AS DECIMAL(16,6)) as EARNED_HRS"
)
Shortcut_to_PS2_EARNED_HRS_PRE.write.saveAsTable(f'{raw}.PS2_EARNED_HRS_PRE',mode='overwrite')

# COMMAND ----------


