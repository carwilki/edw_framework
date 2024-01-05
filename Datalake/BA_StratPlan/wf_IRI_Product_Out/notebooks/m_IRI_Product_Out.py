# Databricks notebook source
# Code converted on 2023-11-27 11:17:05
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
from datetime import datetime

# COMMAND ----------

def writeToFlatFile_NoHeader(df, filePath, fileName, mode):
    print(filePath)
    if mode == "overwrite":
        dbutils.fs.rm(filePath.strip("/") + "/", True)

    df.repartition(1).write.mode(mode).option("header", "false").option(
        "inferSchema", "true"
    ).option("delimiter", "|").option("ignoreTrailingWhiteSpace", "False").csv(filePath)
    print("File added to GCS Path")
    removeTransactionFiles(filePath)
    newFilePath = filePath.strip("/") + "/" + fileName
    renamePartFileName(filePath, newFilePath)

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

# Variable_declaration_comment
target_bucket=getParameterValue(raw,'wf_IRI_Product_Out','m_IRI_Product_Out','target_bucket')
target_file=getParameterValue(raw,'wf_IRI_Product_Out','m_IRI_Product_Out','key')

#target_file=target_bucket + key
Last_Run_date=getParameterValue(raw,'wf_IRI_Product_Out','m_IRI_Product_Out','Last_Run_date')
SKU_Date_Filter=getParameterValue(raw,'wf_IRI_Product_Out','m_IRI_Product_Out','SKU_Date_Filter')

if str(Last_Run_date).startswith('1900') :
    v_max_update_dt=str(spark.sql(f"Select max(UPDATE_DT) as max_dt from {legacy}.SKU_PROFILE_RPT").first()[0])
    Last_Run_date=v_max_update_dt
    SKU_Date_Filter=f"SKU_PROFILE_RPT.UPDATE_DT>to_date('{v_max_update_dt}')"
    print('Initial Run')
else:
    Last_Run_date=getParameterValue(raw,'wf_IRI_Product_Out','m_IRI_Product_Out','Last_Run_date')
    SKU_Date_Filter=getParameterValue(raw,'wf_IRI_Product_Out','m_IRI_Product_Out','SKU_Date_Filter')

print(Last_Run_date)
print(SKU_Date_Filter)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT1, type SOURCE 
# COLUMN COUNT: 29

SQ_Shortcut_to_SKU_PROFILE_RPT1 = spark.sql(f"""SELECT DISTINCT
UPC.UPC_CD,
UPC.UPC_ID,
SKU_PROFILE_RPT.SKU_NBR,
SKU_PROFILE_RPT.SKU_DESC,
SKU_PROFILE_RPT.PRIMARY_VENDOR_ID,
SKU_PROFILE_RPT.PRIMARY_VENDOR_NAME,
SKU_PROFILE_RPT.BRAND_CD,
SKU_PROFILE_RPT.BRAND_NAME,
SKU_PROFILE_RPT.STATUS_NAME,
SKU_PROFILE_RPT.ADD_DT,
SKU_PROFILE_RPT.DELETE_DT,
SKU_PROFILE_RPT.CONTENTS,
SKU_PROFILE_RPT.CONTENTS_UNITS,
SKU_PROFILE_RPT.SIZE_DESC,
SKU_PROFILE_RPT.WEIGHT_NET_AMT,
TRIM(SKU_PROFILE_RPT.WEIGHT_UOM_CD) as WEIGHT_UOM_CD,
SKU_PROFILE_RPT.FLAVOR_DESC,
SKU_PROFILE_RPT.OWNBRAND_FLAG,
SKU_PROFILE_RPT.VP_ID,
SKU_PROFILE_RPT.VP_DESC,
SKU_PROFILE_RPT.SAP_DIVISION_ID,
SKU_PROFILE_RPT.SAP_DIVISION_DESC,
SKU_PROFILE_RPT.SAP_DEPT_ID,
SKU_PROFILE_RPT.SAP_DEPT_DESC,
SKU_PROFILE_RPT.SAP_CLASS_ID,
SKU_PROFILE_RPT.SAP_CLASS_DESC,
SKU_PROFILE_RPT.SAP_CATEGORY_ID,
SKU_PROFILE_RPT.SAP_CATEGORY_DESC,
SKU_PROFILE_RPT.UPDATE_DT
FROM {legacy}.UPC, {legacy}.SKU_PROFILE_RPT
WHERE SKU_PROFILE_RPT.PRODUCT_ID=UPC.PRODUCT_ID
--AND SKU_PROFILE_RPT.UPDATE_DT > '2023-12-06 01:53:17'
AND {SKU_Date_Filter} ORDER BY "UPC_CD" ASC
""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Date_FMT_Status, type EXPRESSION 
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SKU_PROFILE_RPT1_temp = SQ_Shortcut_to_SKU_PROFILE_RPT1.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT1___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT1.columns])

EXP_Date_FMT_Status = SQ_Shortcut_to_SKU_PROFILE_RPT1_temp.selectExpr(
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___UPC_CD as UPC_CD",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___UPC_ID as UPC_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SKU_NBR as SKU_NBR",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SKU_DESC as SKU_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___PRIMARY_VENDOR_ID as PRIMARY_VENDOR_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___PRIMARY_VENDOR_NAME as PRIMARY_VENDOR_NAME",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___BRAND_CD as BRAND_CD",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___BRAND_NAME as BRAND_NAME",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___CONTENTS as CONTENTS",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___CONTENTS_UNITS as CONTENTS_UNITS",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SIZE_DESC as SIZE_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___WEIGHT_NET_AMT as WEIGHT_NET_AMT",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___WEIGHT_UOM_CD as WEIGHT_UOM_CD",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___FLAVOR_DESC as FLAVOR_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___OWNBRAND_FLAG as OWNBRAND_FLAG",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___VP_ID as VP_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___VP_DESC as VP_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SAP_DIVISION_ID as SAP_DIVISION_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SAP_DIVISION_DESC as SAP_DIVISION_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SAP_DEPT_ID as SAP_DEPT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SAP_DEPT_DESC as SAP_DEPT_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SAP_CLASS_ID as SAP_CLASS_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SAP_CATEGORY_ID as SAP_CATEGORY_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SAP_CATEGORY_DESC as SAP_CATEGORY_DESC",
	"IF (UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_SKU_PROFILE_RPT1___STATUS_NAME ) ) ) like '%DISCONTINUED%', 'Y', 'N') as o_STATUS_NAME",
	"date_format(SQ_Shortcut_to_SKU_PROFILE_RPT1___ADD_DT, 'yyyy-MM-dd') as ADD_DT_FMT",
	"date_format(SQ_Shortcut_to_SKU_PROFILE_RPT1___DELETE_DT, 'yyyy-MM-dd') as DELETE_DT_FMT"
)

# COMMAND ----------

# Processing node Shortcut_to_IRI_Product_FF, type TARGET 
# COLUMN COUNT: 28


Shortcut_to_IRI_Product_FF = EXP_Date_FMT_Status.selectExpr(
	"CAST(UPC_CD AS STRING) as UPC_Code",
	"CAST(UPC_ID AS STRING) as UPC_ID",
	"CAST(SKU_NBR AS STRING) as Retailer_Product_Identifier",
	"CAST(SKU_DESC AS STRING) as Item_Description",
	"CAST(PRIMARY_VENDOR_ID AS STRING) as Manufacturer_Number",
	"CAST(PRIMARY_VENDOR_NAME AS STRING) as Manufacturer_Name",
	"CAST(BRAND_CD AS STRING) as Brand_Number",
	"CAST(BRAND_NAME AS STRING) as Brand_Name",
	"CAST(o_STATUS_NAME AS STRING) as Discontinued_Indicator",
	"CAST(ADD_DT_FMT AS STRING) as Add_Date",
	"CAST(DELETE_DT_FMT AS STRING) as Delete_Date",
	"CAST(CONTENTS AS STRING) as Contents",
	"CAST(CONTENTS_UNITS AS STRING) as Unit_of_Measure_Description",
	"CAST(SIZE_DESC AS STRING) as Retail_Pack_Size",
	"CAST(WEIGHT_NET_AMT AS STRING) as Weight_Net_Amt",
	"CAST(WEIGHT_UOM_CD AS STRING) as Weight_UOM_CD",
	"CAST(FLAVOR_DESC AS STRING) as Flavor_Desc",
	"CAST(OWNBRAND_FLAG AS STRING) as Private_Label_Indicator",
	"CAST(VP_ID AS STRING) as VP_ID",
	"CAST(VP_DESC AS STRING) as VP_Desc",
	"CAST(SAP_DIVISION_ID AS STRING) as Division_ID",
	"CAST(SAP_DIVISION_DESC AS STRING) as Division_Desc",
	"CAST(SAP_DEPT_ID AS STRING) as Dept_ID",
	"CAST(SAP_DEPT_DESC AS STRING) as Dept_Desc",
	"CAST(SAP_CLASS_ID AS STRING) as Class_ID",
	"CAST(SAP_CLASS_DESC AS STRING) as Class_Desc",
	"CAST(SAP_CATEGORY_ID AS STRING) as Category_ID",
	"CAST(SAP_CATEGORY_DESC AS STRING) as Category_Desc"
)
Shortcut_to_IRI_Product_FF=Shortcut_to_IRI_Product_FF.orderBy("UPC_Code")
target_bucket=target_bucket+'/'+datetime.now().strftime("%Y%m%d")+'/'
target_file =  target_file.replace('.dat','') + datetime.today().strftime("_%Y-%m-%d-%H-%m-%S") + '.dat'
writeToFlatFile_NoHeader(Shortcut_to_IRI_Product_FF, target_bucket, target_file, 'overwrite')


# COMMAND ----------

# source = "gs://petm-bdpl-qa-apps-p1-gcs-gbl/nas_outbound/IRI/Petsmart_Product/20231220/Petsmart_Product_2023-12-20-14-12-12.dat"
# target = "/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test/IRI/Petsmart_Product/"
# source


# COMMAND ----------

# print(target_bucket)
# print(target_file)

# COMMAND ----------

if env == "prod":
    gs_source_path = target_bucket + target_file
    today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/nas05/edwshare/DataLake/Temp_NZ_Migration/IRI/Petsmart_Product/" + today + '/'
else:
    gs_source_path = target_bucket + target_file
    #today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test/IRI/Petsmart_Product/"

# COMMAND ----------

copy_file_to_nas(gs_source_path, nas_target_path)

# COMMAND ----------

# COMMAND ----------

# updating parm config table with latest run date
param_file_name='wf_IRI_Product_Out'
param_section='m_IRI_Product_Out'

v_param_value_Last_Run_date=spark.sql(f"Select max(UPDATE_DT) as max_dt from {legacy}.SKU_PROFILE_RPT").first()[0]
v_param_value_SKU_Date_Filter=f"SKU_PROFILE_RPT.UPDATE_DT > to_date('{v_param_value_Last_Run_date}','MM/dd/yyyy')"

print(v_param_value_Last_Run_date)
print(v_param_value_SKU_Date_Filter)

#spark.sql(f"""Update {raw}.parameter_config set parameter_value="SKU_PROFILE_RPT.UPDATE_DT > '{v_parameter_value}'" where parameter_file_name='{param_file_name}' and parameter_section='{param_section}' and parameter_key in ('SKU_Date_Filter')""")

#spark.sql(f"""Update {raw}.parameter_config set parameter_value='{v_parameter_value}' where parameter_file_name='{param_file_name}' and parameter_section='{param_section}' and parameter_key in ('Last_Run_date')""")

#raw, parameter_file_name, parameter_section, parameter_key, parameter_value
update_param_config(raw, param_file_name, param_section, 'SKU_Date_Filter',v_param_value_SKU_Date_Filter)
update_param_config(raw, param_file_name, param_section, 'Last_Run_date',v_param_value_Last_Run_date)

