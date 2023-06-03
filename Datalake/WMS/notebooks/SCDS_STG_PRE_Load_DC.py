#
from pyspark.dbutils import DBUtils
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession







dbutils: DBUtils = dbutils
spark: SparkSession = spark

dbutils.widgets.text(name="DC_NBR", defaultValue="")
dbutils.widgets.text(name="env", defaultValue="")

dcnbr = dbutils.widgets.get("DC_NBR")
env = dbutils.widgets.get("env")




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
