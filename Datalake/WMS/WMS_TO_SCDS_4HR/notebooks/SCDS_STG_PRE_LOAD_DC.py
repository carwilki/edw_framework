from pyspark.dbutils import DBUtils
from pyspark.sql.functions import current_timestamp, lit,monotonically_increasing_id
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from datetime import datetime
from Datalake.utils.genericUtilities import getEnvPrefix
from Datalake.utils.configs import getConfig
from Datalake.utils.configs import getMaxDate,getConfig
from Datalake.WMS.WMS_TO_SCDS_4HR.notebooks.m_WM_E_Consol_Perf_Smry_PRE import perf_smry
from Datalake.WMS.WMS_TO_SCDS_4HR.notebooks.m_WM_Ucl_User import user_pre
from Datalake.WMS.WMS_TO_SCDS_4HR.notebooks.m_WM_E_Dept.py import dept_pre

import argparse
parser = argparse.ArgumentParser()

spark: SparkSession = SparkSession.getActiveSession()
dbutils: DBUtils = DBUtils(spark)

parser.add_argument('DC_NBR',type=str, help = "DC number")
parser.add_argument('env',type=str, help = "Env Variable")
args = parser.parse_args()
dcnbr=args.DC_NBR
env = args.env	

# dcnbr = dbutils.widgets.get('DC_NBR')	
# env = dbutils.widgets.get('env')

if dcnbr is None or dcnbr == "":
    raise Exception("DC_NBR is not set")

if env is None or env == "":
    raise Exception("env is not set")


####################################################################
# foreach mapping in maplet/worklet call the corresponding notebook
# that is created.
####################################################################

####################################################################
## main section ##
####################################################################

user_pre(dcnbr,env)
dept_pre(dcnbr,env)
perf_smry(dcnbr,env)
