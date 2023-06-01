# Databricks notebook source
from pyspark.dbutils import DBUtils
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext;
from pyspark.sql.session import SparkSession
from datetime import datetime

# COMMAND ----------
dbutils: DBUtils = dbutils
spark: SparkSession = spark

dbutils.widgets.text(name="DC_NBR", defaultValue="")
dbutils.widgets.text(name="env", defaultValue="")


dcnbr = dbutils.widgets.get("DC_NBR")
env = dbutils.widgets.get("env")


# COMMAND ----------


####################################################################
# foreach mapping in maplet/worklet call the corresponding notebook
# that is created.
####################################################################
def run_notebook(name, timeout, params):
    dbutils.notebook.run(name, timeout, params)


# COMMAND ----------

####################################################################
## main section
####################################################################
run_notebook("./m_WM_Ucl_User_PRE", 3090, {"DC_NBR": f"{dcnbr}", "env": f"{env}"})
run_notebook("./m_WM_E_Dept_PRE", 8000, {"DC_NBR": f"{dcnbr}", "env": f"{env}"})
run_notebook(
    "./m_WM_E_Consol_Perf_Smry_PRE", 8000, {"DC_NBR": f"{dcnbr}", "env": f"{env}"}
)

