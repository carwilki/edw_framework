# Databricks notebook source
# Code converted on 2023-11-09 07:57:35
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

# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT

  'SSG US' department_id

,

     'SSG US' department_name

,

     '99999'  parent_id

,

     'PetSmart DEFAULT Cost Center' parent_name

,

     NULL TYPE

,

     'Active' status

,

     NULL business_unit

,

     '1000' company

,

     NULL location

,

     NULL department

,

     NULL accounting_system

,

     '1000-SSG US' glaccount

,

     NULL mgr_employee_id

,

     NULL manager_as_approver

,

     NULL original_department_id

,

     NULL original_department_name

,

     NULL show_as_business_unit

UNION

SELECT

  'PetSmart US' department_id

,

     'PetSmart US' department_name

,

     '99999'  parent_id

,

     'PetSmart DEFAULT Cost Center' parent_name

,

     NULL TYPE

,

     'Active' status

,

     NULL business_unit

,

     '1000' company

,

     NULL location

,

     NULL department

,

     NULL accounting_system

,

     '1000-PetSmart US' glaccount

,

     NULL mgr_employee_id

,

     NULL manager_as_approver

,

     NULL original_department_id

,

     NULL original_department_name

,

     NULL show_as_business_unit

UNION

SELECT

  'PetSmart CA' department_id

,

     'PetSmart CA' department_name

,

     '99999'  parent_id

,

     'PetSmart DEFAULT Cost Center' parent_name

,

     NULL TYPE

,

     'Active' status

,

     NULL business_unit

,

     '2000' company

,

     NULL location

,

     NULL department

,

     NULL accounting_system

,

     '2000-PetSmart CA' glaccount

,

     NULL mgr_employee_id

,

     NULL manager_as_approver

,

     NULL original_department_id

,

     NULL original_department_name

,

     NULL show_as_business_unit

UNION

SELECT

  'PetSmart Other' department_id

,

     'PetSmart Other' department_name

,

     '99999'  parent_id

,

     'PetSmart DEFAULT Cost Center' parent_name

,

     NULL TYPE

,

     'Active' status

,

     NULL business_unit

,

     '1000' company

,

     NULL location

,

     NULL department

,

     NULL accounting_system

,

     '1000-PetSmart Other' glaccount

,

     NULL mgr_employee_id

,

     NULL manager_as_approver

,

     NULL original_department_id

,

     NULL original_department_name

,

     NULL show_as_business_unit

UNION

SELECT

  'PetSmart Distribution Center' department_id

,

     'PetSmart Distribution Center' department_name

,

     '99999'  parent_id

,

     'PetSmart DEFAULT Cost Center' parent_name

,

     NULL TYPE

,

     'Active' status

,

     NULL business_unit

,

     '1000' company

,

     NULL location

,

     NULL department

,

     NULL accounting_system

,

     '1000-PetSmart Distribution Center' glaccount

,

     NULL mgr_employee_id

,

     NULL manager_as_approver

,

     NULL original_department_id

,

     NULL original_department_name

,

     NULL show_as_business_unit

UNION

SELECT

  'PetSmart Closed' department_id

,

     'PetSmart Closed' department_name

,

     '99999'  parent_id

,

     'PetSmart DEFAULT Cost Center' parent_name

,

     NULL TYPE

,

     'Active' status

,

     NULL business_unit

,

     '1000' company

,

     NULL location

,

     NULL department

,

     NULL accounting_system

,

     '1000-PetSmart Closed' glaccount

,

     NULL mgr_employee_id

,

     NULL manager_as_approver

,

     NULL original_department_id

,

     NULL original_department_name

,

     NULL show_as_business_unit

UNION

SELECT

  '99999' department_id

,

     'PetSmart DEFAULT Cost Center' department_name

,

     NULL  parent_id

,

     NULL parent_name

,

     NULL TYPE

,

     'Active' status

,

     NULL business_unit

,

     '1000' company

,

     NULL location

,

     NULL department

,

     NULL accounting_system

,

     '1000-PetSmart DEFAULT Cost Center' glaccount

,

     NULL mgr_employee_id

,

     NULL manager_as_approver

,

     NULL original_department_id

,

     NULL original_department_name

,

     NULL show_as_business_unit

UNION

SELECT b.department_id,

    b.department_name,

    b.parent_id,

    b.PARENT_NAME,

    b.TYPE,

    b.status,

    b.business_unit,

    b.company,

    b.location,

    b.department,

    b.accounting_system,

    b.glaccount,

    b.mgr_employee_id,

    b.manager_as_approver,

    b.original_department_id,

    b.original_department_name,

    b.show_as_business_unit

FROM

(SELECT a.department_id,

    a.department_name,

    a.parent_id,

    a.PARENT_NAME,

    a.TYPE,

    a.status,

    a.business_unit,

    a.company,

    a.location,

    a.department,

    a.accounting_system,

    a.glaccount,

    a.mgr_employee_id,

    a.manager_as_approver,

    a.original_department_id,

    a.original_department_name,

    a.show_as_business_unit

FROM

(SELECT

  (CASE WHEN RTRIM(STORE_TYPE_ID) = '100' THEN '38' || LPAD(CAST(STORE_NBR AS VARCHAR(10)),2,'0')

ELSE LPAD(CAST(STORE_NBR AS VARCHAR(10)),4,'0')

END) AS department_id

  ,

    (RANK() OVER (PARTITION BY (CASE WHEN RTRIM(STORE_TYPE_ID) = '100' THEN '38' || LPAD(CAST(STORE_NBR AS VARCHAR(10)),2,'0')

ELSE LPAD(CAST(STORE_NBR AS VARCHAR(10)),4,'0')

END)

ORDER BY STORE_NAME DESC)) Department_Rank

  ,

    (DECODE(RTRIM(STORE_TYPE_ID), '100','38'||LPAD(CAST(STORE_NBR AS VARCHAR(10)),2,'0'),LPAD(CAST(STORE_NBR AS VARCHAR(10)),4,'0')) || '-' ||  STORE_NAME) AS department_name

  ,

    (CASE WHEN (RTRIM(store_type_id) <> '100'

    AND s.store_nbr <> 99999

    AND RTRIM(STORE_TYPE_ID) <> '999'

    AND store_open_close_flag='O')

                 THEN decode(region_id,8000,'SSG US',decode(COUNTRY_CD, 'US ','PetSmart US', 'CA ', 'PetSmart CA', 'PetSmart Other'))

              WHEN (RTRIM(store_type_id) = '100'

    AND store_open_close_flag='O')

                 THEN 'PetSmart Distribution Center'

              WHEN ((s.store_nbr = 99999

    OR  RTRIM(STORE_TYPE_ID) = '999')

    AND store_open_close_flag='O')

                 THEN 'PetSmart Other'

              WHEN  (store_open_close_flag<>'O')

                 THEN 'PetSmart Closed'

       END )  parent_id

,

      (CASE WHEN (RTRIM(store_type_id) <> '100'

    AND s.store_nbr <> 99999

    AND RTRIM(STORE_TYPE_ID) <> '999'

    AND store_open_close_flag='O')

                 THEN decode(region_id,8000,'SSG US',decode(COUNTRY_CD, 'US ','PetSmart US', 'CA ', 'PetSmart CA', 'PetSmart Other'))

              WHEN (RTRIM(store_type_id) = '100'

    AND store_open_close_flag='O')

                 THEN 'PetSmart Distribution Center'

              WHEN ((s.store_nbr = 99999

    OR  RTRIM(STORE_TYPE_ID) = '999')

    AND store_open_close_flag='O')

                 THEN 'PetSmart Other'

              WHEN  (store_open_close_flag<>'O')

                 THEN 'PetSmart Closed'

       END ) PARENT_NAME

,

     NULL TYPE

,

     (CASE  WHEN STORE_OPEN_CLOSE_FLAG = 'O' THEN 'Active' WHEN STORE_OPEN_CLOSE_FLAG='X' THEN 'Pending Deactivation' WHEN STORE_OPEN_CLOSE_FLAG='C' THEN  'Inactive'

END) status

,

     NULL business_unit

,

     (CASE WHEN COUNTRY_CD = 'CA ' THEN '2000'

ELSE '1000'

END) company

,

     NULL location

,

     NULL department

,

     NULL accounting_system

,

    (CASE WHEN COUNTRY_CD = 'CA ' THEN '2000'

ELSE '1000'

END) || '-' || DECODE(RTRIM(STORE_TYPE_ID), '100','38'||LPAD(CAST(STORE_NBR AS VARCHAR(10)),2,'0'),LPAD(CAST(STORE_NBR  AS VARCHAR(10)),4,'0')) glaccount

,

     NULL mgr_employee_id

,

     NULL manager_as_approver

,

     NULL original_department_id

,

     NULL original_department_name

,

     NULL show_as_business_unit

FROM

 {legacy}.SITE_PROFILE s

WHERE s.store_nbr <> 99999

       AND  cast(s.store_nbr AS varchar (10)) NOT LIKE '38__'

       AND RTRIM(STORE_TYPE_ID) <> '999')a

WHERE DEPARTMENT_RANK = 1)b""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[0],'DEPARTMENT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[1],'DEPARTMENT_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[2],'PARENT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[3],'PARENT_NAME1') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[4],'TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[5],'STATUS1') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[6],'BUSINESS_UNIT') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[7],'COMPANY') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[8],'LOCATION') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[9],'DEPARTMENT') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[10],'ACCOUNTING_SYSTEM') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[11],'GLACCOUNT') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[12],'MGR_EMPLOYEE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[13],'MANAGER_IS_APPROVER') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[14],'ORIGINAL_DEPARTMENT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[15],'ORIGINAL_DEPARTMENT_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[16],'SHOW_AS_BUSINESS_UNIT')

# COMMAND ----------

target_bucket=getParameterValue(raw,'wf_ecova','m_ecova_department','target_bucket')
key=getParameterValue(raw,'wf_ecova','m_ecova_department','key')

target_file=target_bucket + key


# COMMAND ----------

# Processing node Shortcut_to_ECOVA_DEPARTMENT, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_ECOVA_DEPARTMENT = SQ_Shortcut_to_SITE_PROFILE.selectExpr(
	"CAST(DEPARTMENT_ID AS STRING) as DEPARTMENT_ID",
	"CAST(DEPARTMENT_NAME AS STRING) as DEPARTMENT_NAME",
	"CAST(PARENT_ID AS STRING) as PARENT_ID",
	"CAST(PARENT_NAME1 AS STRING) as PARENT_NAME",
	"CAST(TYPE AS STRING) as TYPE",
	"CAST(STATUS1 AS STRING) as STATUS",
	"CAST(BUSINESS_UNIT AS STRING) as BUSINESS_UNIT",
	"CAST(COMPANY AS STRING) as COMPANY",
	"CAST(LOCATION AS STRING) as LOCATION",
	"CAST(DEPARTMENT AS STRING) as DEPARTMENT",
	"CAST(ACCOUNTING_SYSTEM AS STRING) as ACCOUNTING_SYSTEM",
	"CAST(GLACCOUNT AS STRING) as GLACCOUNT",
	"CAST(MGR_EMPLOYEE_ID AS STRING) as MGR_EMPLOYEE_ID",
	"CAST(MANAGER_IS_APPROVER AS STRING) as MANAGER_IS_APPROVER",
	"CAST(ORIGINAL_DEPARTMENT_ID AS STRING) as ORIGINAL_DEPARTMENT_ID",
	"CAST(ORIGINAL_DEPARTMENT_NAME AS STRING) as ORIGINAL_DEPARTMENT_NAME",
	"CAST(SHOW_AS_BUSINESS_UNIT AS STRING) as SHOW_AS_BUSINESS_UNIT"
)

Shortcut_to_ECOVA_DEPARTMENT=Shortcut_to_ECOVA_DEPARTMENT.withColumnRenamed("SHOW_AS_BUSINESS_UNIT", "SHOW_AS_BUSINESS_UNIT ")
cols = Shortcut_to_ECOVA_DEPARTMENT.columns
cols =[col.lower() for col in cols ]
Shortcut_to_ECOVA_DEPARTMENT=Shortcut_to_ECOVA_DEPARTMENT.toDF(*cols)

Shortcut_to_ECOVA_DEPARTMENT.write.mode('overwrite').option("ignoreLeadingWhiteSpace","false").option("ignoreTrailingWhiteSpace", "false").options(header='True', delimiter='|').csv(target_file)


# COMMAND ----------


