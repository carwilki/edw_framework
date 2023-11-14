#Code converted on 2023-08-08 15:40:52
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

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')

args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

(username,password,connection_string) = or_kro_read_krap1(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_ORG_PRE, type SOURCE 
# COLUMN COUNT: 10

SQ_Shortcut_to_WFA_ORG_PRE = spark.sql(f"""SELECT
WFA_ORG_PRE.ORG_ID,
SITE_PROFILE.LOCATION_ID,
WFA_BUSINESS_AREA.WFA_BUSN_AREA_ID,
WFA_BUSINESS_AREA.WFA_BUSN_AREA_DESC,
WFA_DEPARTMENT.WFA_DEPT_ID,
WFA_DEPARTMENT.WFA_DEPT_DESC,
WFA_TASK.WFA_TASK_ID,
WFA_TASK.WFA_TASK_DESC,
WFA_ORG_PRE.ORG_SKEY,
WFA_ORG_PRE.ORG_LVL_NBR
FROM {raw}.WFA_ORG_PRE, {legacy}.WFA_TASK, {legacy}.SITE_PROFILE, {legacy}.WFA_DEPARTMENT, {legacy}.WFA_BUSINESS_AREA
WHERE wfa_org_pre.org_lvl06_nam = site_profile.store_nbr

    AND wfa_org_pre.org_lvl07_nam = wfa_business_area.wfa_busn_area_desc

    AND wfa_org_pre.org_lvl08_nam = wfa_department.wfa_dept_desc

    AND wfa_org_pre.org_lvl09_nam = wfa_task.wfa_task_desc""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_WFA_ORG_FACT_PRE, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_WFA_ORG_FACT_PRE = SQ_Shortcut_to_WFA_ORG_PRE.selectExpr(
	"CAST(ORG_ID AS BIGINT) as ORG_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(WFA_BUSN_AREA_ID AS SMALLINT) as WFA_BUSN_AREA_ID",
	"WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
	"CAST(WFA_DEPT_ID AS SMALLINT) as WFA_DEPT_ID",
	"WFA_DEPT_DESC as WFA_DEPT_DESC",
	"CAST(WFA_TASK_ID AS SMALLINT) as WFA_TASK_ID",
	"WFA_TASK_DESC as WFA_TASK_DESC",
	"CAST(ORG_SKEY AS BIGINT) as ORG_SKEY",
	"CAST(ORG_LVL_NBR AS BIGINT) as ORG_LVL_NBR"
)
# overwriteDeltaPartition(Shortcut_to_WFA_ORG_FACT_PRE,'DC_NBR',dcnbr,f'{raw}.WFA_ORG_FACT_PRE')
Shortcut_to_WFA_ORG_FACT_PRE.write.mode("overwrite").saveAsTable(f'{raw}.WFA_ORG_FACT_PRE')