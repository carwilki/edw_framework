from pyspark.dbutils import DBUtils
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext;
from pyspark.sql.session import SparkSession
from datetime import datetime

##########################################################################
# this file is a good example of how to map a informatica worklet
# of maplet to a databricks notebook
##########################################################################

# COMMAND ----------
# configure spark and dbutils. This is required when building notebooks outside of
# the notebook itself.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
dbutils = DBUtils(sc)

# COMMAND ----------
# Variable_declaration_comment
# Read in job variables
# read_infa_paramfile('', 'm_WM_E_Consol_Perf_Smry_PRE') ProcessingUtils
# for params that are need for this create widgets that are bound to the names
dbutils.widgets.text(name="DC_NBR", defaultValue="")
dbutils.widgets.text(name="Prev_Run_Dt", defaultValue="01/01/1901")
dbutils.widgets.text(name="Initial_Load", defaultValue="")

# set all the local variables to the inputs from the job parameters
starttime = datetime.now()  # start timestamp of the script
dcnbr = dbutils.widgets.get("DC_NBR", as_type=str)
prev_run_dt = dbutils.widgets.get("Prev_Run_Dt", as_type=str)
initial_load = dbutils.widgets.get("Initial_Load", as_type=str)

####################################################################
# we use dbutils.notebook.run to run the notebook, passing in the
# parameters to the notebook
####################################################################
# COMMAND ----------


####################################################################
# foreach mapping in maplet/worklet call the corresponding notebook
# that is created.
####################################################################
def run_notebook(name, timeout, params):
    dbutils.notebook.run(name, timeout, params)


####################################################################
## main section
####################################################################
run_notebook(
    "m_WM_Ucl_User_PRE.py",
    "60",
    {
        "DC_NBR": f"{dcnbr}",
        "Prev_Run_Dt": f"{prev_run_dt}",
        "Initial_Load": f"{initial_load}",
    },
)
run_notebook(
    "m_WM_E_Dept_PRE.py",
    "60",
    {
        "DC_NBR": f"{dcnbr}",
        "Prev_Run_Dt": f"{prev_run_dt}",
        "Initial_Load": f"{initial_load}",
    },
)
run_notebook(
    "m_WM_E_Consol_Perf_Smry_PRE.py",
    "60",
    {
        "DC_NBR": f"{dcnbr}",
        "Prev_Run_Dt": f"{prev_run_dt}",
        "Initial_Load": f"{initial_load}",
    },
)
