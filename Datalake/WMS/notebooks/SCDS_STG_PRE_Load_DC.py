from pyspark.dbutils import DBUtils
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from datetime import datetime
from Datalake.WMS.notebooks.utils.genericUtilities import getEnvPrefix
from Datalake.WMS.notebooks.utils.configs import getConfig

spark: SparkSession = SparkSession.getActiveSession()
dbutils: DBUtils = DBUtils(spark)

dcnbr = dbutils.jobs.taskValues.get(key='DC_NBR', defaultValue='')
env = dbutils.jobs.taskValues.get(key='env', defaultValue='')

if dcnbr is None or dcnbr == "":
    raise Exception("DC_NBR is not set")

if env is None or env == "":
    raise Exception("env is not set")


####################################################################
# foreach mapping in maplet/worklet call the corresponding notebook
# that is created.
####################################################################
def run_notebook(name, timeout, params):
    dbutils.notebook.run(name, timeout, params)

####################################################################
## main section
####################################################################
run_notebook("./m_WM_Ucl_User_PRE", 3090, {"DC_NBR": f"{dcnbr}", "env": f"{env}"})
run_notebook("./m_WM_E_Dept_PRE", 8000, {"DC_NBR": f"{dcnbr}", "env": f"{env}"})
run_notebook("./m_WM_E_Consol_Perf_Smry_PRE", 8000, {"DC_NBR": f"{dcnbr}", "env": f"{env}"})
