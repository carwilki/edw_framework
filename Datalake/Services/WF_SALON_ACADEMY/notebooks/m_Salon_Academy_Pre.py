#Code converted on 2023-07-26 09:54:30
import os
import argparse
from  pyspark.sql import DataFrameWriter
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

(username,password,connection_string) = SalonAcademy_prd_sqlServer(env)
# COMMAND ----------
# Processing node LKP_ACADEMY_TYPE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 3

LKP_ACADEMY_TYPE_SRC = spark.sql(f"""SELECT
SALON_ACADEMY_TYPE_ID,
SALON_ACADEMY_TYPE_DESC
FROM {legacy}.SALON_ACADEMY_TYPE ORDER BY SALON_ACADEMY_TYPE_ID""")

#LKP_ACADEMY_TYPE_SRC = LKP_ACADEMY_TYPE_SRC \
#	.withColumnRenamed(LKP_ACADEMY_TYPE_SRC.columns[0],'SALON_ACADEMY_TYPE_ID') \
#	.withColumnRenamed(LKP_ACADEMY_TYPE_SRC.columns[1],'ii_SALON_ACADEMY_TYPE_ID') \
#	.withColumnRenamed(LKP_ACADEMY_TYPE_SRC.columns[2],'SALON_ACADEMY_TYPE_DESC') 


# COMMAND ----------
# Processing node SQ_Shortcut_to_GS_SalonAcademy, type SOURCE 
# COLUMN COUNT: 11

query = f"""(SELECT
GS_SalonAcademy.ID,
GS_SalonAcademy.StoreLocationID,
GS_SalonAcademy.StartDate,
GS_SalonAcademy.EndDate,
GS_SalonAcademy.Active,
GS_SalonAcademy.SeatLimit,
GS_SalonAcademy.Cancelled,
GS_SalonAcademy.InstructorID,
GS_SalonAcademy.InstructorName,
GS_SalonAcademy.ApprovalDM,
GS_SalonAcademy.AcademyType
FROM SalonAcademy.dbo.GS_SalonAcademy) as src"""

SQ_Shortcut_to_GS_SalonAcademy = jdbcSqlServerConnection(query,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# COMMAND ----------
# Processing node EXP_LOAD_TSTMP, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_SalonAcademy_temp = SQ_Shortcut_to_GS_SalonAcademy.toDF(*["SQ_Shortcut_to_GS_SalonAcademy___" + col for col in SQ_Shortcut_to_GS_SalonAcademy.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_GS_SalonAcademy_temp.selectExpr(
	"SQ_Shortcut_to_GS_SalonAcademy___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_GS_SalonAcademy___ID as ID",
	"SQ_Shortcut_to_GS_SalonAcademy___StoreLocationID as StoreLocationID",
	"SQ_Shortcut_to_GS_SalonAcademy___StartDate as StartDate",
	"SQ_Shortcut_to_GS_SalonAcademy___EndDate as EndDate",
	"SQ_Shortcut_to_GS_SalonAcademy___Active as Active",
	"SQ_Shortcut_to_GS_SalonAcademy___SeatLimit as SeatLimit",
	"SQ_Shortcut_to_GS_SalonAcademy___Cancelled as Cancelled",
	"SQ_Shortcut_to_GS_SalonAcademy___InstructorID as InstructorID",
	"SQ_Shortcut_to_GS_SalonAcademy___InstructorName as InstructorName",
	"SQ_Shortcut_to_GS_SalonAcademy___ApprovalDM as ApprovalDM",
	"SQ_Shortcut_to_GS_SalonAcademy___AcademyType as AcademyType",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node LKP_ACADEMY_TYPE, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

LKP_ACADEMY_TYPE_lookup_result = EXP_LOAD_TSTMP.selectExpr("AcademyType as i_SALON_ACADEMY_TYPE_ID", "sys_row_id").join(LKP_ACADEMY_TYPE_SRC, (col('SALON_ACADEMY_TYPE_ID') == col('i_SALON_ACADEMY_TYPE_ID')), 'left') \
        .withColumn('row_num_SALON_ACADEMY_TYPE_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("SALON_ACADEMY_TYPE_ID")))
    
LKP_ACADEMY_TYPE = LKP_ACADEMY_TYPE_lookup_result.filter("row_num_SALON_ACADEMY_TYPE_ID = 1").select( 
        LKP_ACADEMY_TYPE_lookup_result.sys_row_id, 
        col('SALON_ACADEMY_TYPE_DESC') 
)

# COMMAND ----------
# Processing node Shortcut_to_SALON_ACADEMY_PRE, type TARGET 
# COLUMN COUNT: 13

# Joining dataframes EXP_LOAD_TSTMP, LKP_ACADEMY_TYPE to form Shortcut_to_SALON_ACADEMY_PRE
Shortcut_to_SALON_ACADEMY_PRE_joined = EXP_LOAD_TSTMP.join(LKP_ACADEMY_TYPE, EXP_LOAD_TSTMP.sys_row_id == LKP_ACADEMY_TYPE.sys_row_id, 'inner')

Shortcut_to_SALON_ACADEMY_PRE = Shortcut_to_SALON_ACADEMY_PRE_joined.selectExpr(
	"CAST(ID AS BIGINT) as SALON_ACADEMY_ID",
	"CAST(StoreLocationID AS INT) as LOCATION_ID",
	"CAST(StartDate AS DATE) as START_DT",
	"CAST(EndDate AS DATE) as END_DT",
	"CAST(Active AS TINYINT) as ACTIVE_FLAG",
	"CAST(SeatLimit AS INT) as SEAT_LIMIT",
	"CAST(Cancelled AS TINYINT) as CANCELLED_FLAG",
	"CAST(InstructorID AS INT) as TRAINER_EMPLOYEE_ID",
	"CAST(InstructorName AS STRING) as TRAINER_NAME",
	"CAST(AcademyType AS INT) as SALON_ACADEMY_TYPE_ID",
	"CAST(SALON_ACADEMY_TYPE_DESC AS STRING) as SALON_ACADEMY_TYPE_DESC",
	"CAST(ApprovalDM AS TINYINT) as APPROVED_BY_DISTRICT_LEADER_FLAG",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SALON_ACADEMY_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SALON_ACADEMY_PRE')
