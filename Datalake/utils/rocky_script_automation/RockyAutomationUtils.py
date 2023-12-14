from logging import INFO, getLogger

import pyspark.sql.functions as F
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

import Datalake.utils.secrets as secrets

logger = getLogger()
logger.setLevel(INFO)

spark: SparkSession = SparkSession.getActiveSession()
dbutils: DBUtils = DBUtils(spark)
def create_metadata_dic(metadata_path, table_name):
    from pyspark.sql.functions import lower, col
    harness_metadata = spark.read.option("header", True).csv(metadata_path)
    table_row_count = spark.table(table_name)
    rocky_metadata = harness_metadata.join(
        table_row_count,
        lower(harness_metadata["name"]) == lower(table_row_count["TABLENAME"]),
        "left",
    )
    data_list = rocky_metadata.collect()
    table_metadata = [
        {
            "table_name": row.name,
            "source_db": row.source_schema,
            "target_schema": row.test_target_schema[3:],
            "primary_key": row.primary_key.split("|"),
            "record_count": int(row.ROWS),
        }
        for row in data_list
        if row.name[-3:].upper() != "PRE"
    ]
    return table_metadata

def add_rocky_metadata(table_metadata):
  for table in table_metadata:
    if table["record_count"] ==None or table["record_count"] < 15000000:
      table["source_type"]="NZ_Mako8"
      table["load_type"]="full"
      table["rocky_p_key"]= 'null'
    elif  table["record_count"] < 1000000000:
      table["source_type"]="NZ_Export_Mako8"
      table["load_type"]="full"
      table["rocky_p_key"]= 'null'
    else:
      table["source_type"]="NZ_Export_Mako8"
      table["load_type"]="upsert"
      table["rocky_p_key"]= "\'" + ','.join(table["primary_key"]) + "\'"

    if table["target_schema"].lower()=="legacy" or table["target_schema"].lower()=="refine":
      table['is_pii'] ='FALSE'
      table["pii_type"] = "null"
    else:
      table['is_pii'] ='TRUE'
      if table["target_schema"].lower() == "cust_sensitive":
        table["pii_type"] = "\'customer\'"
      elif table["target_schema"].lower() == "empl_sensitive":
        table["pii_type"] = "\'employee\'"
      else:
        table["pii_type"] = "\'employee_protected\'"
        
  return table_metadata


def check_exists(list_of_tables):
  already_exists = []
  not_exists = []
  for table in list_of_tables:
    target_schema = table["target_schema"]
    table_name = table["table_name"]
    if spark.catalog.tableExists(f"hive_metastore.{target_schema}.{table_name}") or spark.catalog.tableExists(f"hive_metastore.{target_schema}.legacy_{table_name}")or spark.catalog.tableExists(f"hive_metastore.{target_schema}.refine_{table_name}"):
      #checks whether the table is already present in prod
      table["created"] = True
      already_exists.append(table["table_name"])
    else:
      not_exists.append(table["table_name"])
      table["created"] = False
      
  if len(already_exists)>0:
    print("Not creating rocky script for the following tables as they already exists")
    for x in already_exists:
      print(x)
  
  print("\nCreating rocky script for the following tables")
  for x in not_exists:
    print(x)

  return list_of_tables


def find_missing_records(list_of_tables):
  missing_table_list = []
  for table in list_of_tables:

    if table["created"] == True:
      continue

    missing_data =[]
    if table["table_name"] == None:
      missing_data.append("table_name")
    if table["source_db"] == None:
      missing_data.append("source_db")
    if table["target_schema"] == None:
      missing_data.append("target_schema")
    if table["record_count"] == None:
      missing_data.append("record_count")
    table["missing_data"] = missing_data
    if len(missing_data)>0:
      missing_table_list.append(table["table_name"])
   
    table["missing_data"] = missing_data
  
  if len(missing_table_list)>0:
    raise Exception("\n The following tables have missing values",missing_table_list)

  return list_of_tables



def get_rocky_insert_statements(list_of_tables):
  insert_statements = {}
  for table in list_of_tables:
    if table["created"]:
      continue
    insert = f"""
    INSERT INTO work.rocky_ingestion_metadata(table_group,table_group_desc,source_type,source_db,source_table,table_desc,is_pii,pii_type,has_hard_deletes,target_sink,target_db,target_schema,target_table_name,load_type,source_delta_column,primary_key,initial_load_filter,load_frequency,load_cron_expr,tidal_dependencies,tidal_trigger_condition,expected_start_time,job_watchers,max_retry,disable_no_record_failure,job_tag,is_scheduled,job_id,snowflake_ddl,snowflake_pre_sql,snowflake_post_sql,additional_config)
      VALUES('NZ_Migration' --table_group
      , null --table_group_desc
      , '{table["source_type"]}' --source_type
      , '{table["source_db"]}' --source_db
      , '{table["table_name"]}' --source_table
      , null --table_desc
      , '{table["is_pii"]}' --is_pii
      , {table["pii_type"]} --pii_type
      , 'FALSE' --has_hard_deletes
      , 'delta' --target_sink
      , 'refine' --target_db
      , 'NULL' --target_schema
      , '{table["table_name"]}_history' --target_table_name
      , '{table["load_type"]}' --load_type
      , null --source_delta_column
      , {table["rocky_p_key"]} --primary_key
      , null --initial_load_filter
      , 'one-time' --load_frequency
      , '0 0 6 ? * *' --load_cron_expr
      , array('NULL') --tidal_dependencies
      , null --tidal_trigger_condition
      , null --expected_start_time
      , array(\"rrajamani@petsmart.com\", \"APipewala@PetSmart.com\") --job_watchers
      , 0 --max_retry
      , 'TRUE' --disable_no_record_failure
      ,'{{\"Department\":\"Netezza-Migration\"}}' --job_tag
      , 'FALSE' --is_scheduled
      , null --job_id
      , null --snowflake_ddl
      , null --snowflake_pre_sql
      , null --snowflake_post_sql
      , null --additional_config
      );
      
    """
    insert_statements[table["table_name"]] = insert
  return insert_statements


def create_table_metadata(metadata_path,table_name):
  table_metadata1 = create_metadata_dic(metadata_path,table_name)
  table_metadata2 = add_rocky_metadata(table_metadata1)
  table_metadata3 = check_exists(table_metadata2)
  table_metadata4 = find_missing_records(table_metadata3)
  return table_metadata4
 

def trigger_rocky_job(payload, instance_id, token):
  import json
  import requests
  api_version = "/api/2.1"
  api_command = "/jobs/run-now"
  url = f"https://{instance_id}{api_version}{api_command}"
  params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
  response = requests.post(url=url, headers=params, data=payload)

  return response.text


def trigger_rocky(tables):
  import json
  import requests
  
  env = "prod"
  if env == "prod":
      work_db = "work"
      platform_db = "stranger_things"
      token = dbutils.secrets.get(scope="db-token-jobsapi", key="password")
      instance_id = "3609071286715921.1.gcp.databricks.com"
  else:
      work_db = "qa_work"
      platform_db = "qa_stranger_things"
      # token = '2577d49a1c78bd890daa9daaad0cd342'
      instance_id = "3986616729757273.3.gcp.databricks.com"
  
  for table in tables:
      print(table)
      query = f"""select job_id from {work_db}.rocky_ingestion_metadata where source_table='{table}' and table_group='NZ_Migration'"""
      print(query)
      job_id = spark.sql(query).collect()[0][0]
      print(job_id)
      try:
          run_info = trigger_rocky_job(json.dumps({"job_id": job_id}), instance_id, token)
          print("response:", run_info)
          # run_id = json.loads(run_info)['run_id']
          # print(run_id)
      except Exception as e:
          print("failed for", table, e)
          

def copy_hist_to_refine(tables):
  from pyspark.sql.functions import col
  for table in tables:
      print(table)
      rocky_table = f"refine.{table}_history"
      target_table = f"refine.{table}"
      df = spark.sql(f"select * from {rocky_table}")
      df = df.drop(
          col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
      )
      df.write.insertInto(f"{target_table}", overwrite=True)
 
def copy_hist_to_legacy(tables):
  from pyspark.sql.functions import col
  for table in tables:
      print(table)
      rocky_table = f"refine.{table}_history"
      target_table = f"legacy.{table}"
      df = spark.sql(f"select * from {rocky_table}")
      df = df.drop(
          col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
      )
      df.write.insertInto(f"{target_table}", overwrite=True)

def copy_hist_to_empl_protected(tables):
  from pyspark.sql.functions import col
  for table in tables:
      print(table)
      rocky_table = f"refine.{table}_history"
      target_table = f"empl_protected.legacy_{table}"
      df = spark.sql(f"select * from {rocky_table}")
      df = df.drop(
          col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
      )
      df.write.insertInto(f"{target_table}", overwrite=True)

def copy_hist_to_empl_sensitive(tables):
  from pyspark.sql.functions import col
  for table in tables:
      print(table)
      rocky_table = f"refine.{table}_history"
      target_table = f"empl_sensitive.legacy_{table}"
      df = spark.sql(f"select * from {rocky_table}")
      df = df.drop(
          col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
      )
      df.write.insertInto(f"{target_table}", overwrite=True)
      
def copy_hist_to_cust_sensitive(tables):
  from pyspark.sql.functions import col
  for table in tables:
      print(table)
      rocky_table = f"refine.{table}_history"
      target_table = f"cust_sensitive.legacy_{table}"
      df = spark.sql(f"select * from {rocky_table}")
      df = df.drop(
          col("bd_create_dt_tm"), col("bd_update_dt_tm"), col("source_file_name")
      )
      df.write.insertInto(f"{target_table}", overwrite=True)
      

def get_trigger_job_script(workflow_name,table_metadata):
    table_list = [] 
    for table in table_metadata:
        if table["created"]:
            continue
        table_list.append(table['table_name'])
    rocky_script = f"""# Databricks notebook source
from Datalake.utils.rocky_script_automation.RockyAutomationUtils import *
from pyspark.sql.functions import *

# COMMAND ----------


# COMMAND ----------

tables = {table_list}

# COMMAND ----------
trigger_rocky(tables)

"""
    print(rocky_script)
    with open(f"{workflow_name}_rocky_trigger.py","w") as file1:
        file1.writelines(rocky_script)


def get_copy_script(workflow_name,table_metadata):
    from pyspark.sql.functions import lower
    refine_tables = [] 
    legacy_tables = []
    empl_protected_tables =[]
    empl_sensitive_tables =[]
    cust_sensitive_tables =[]
 
    table_list = []
    for table in table_metadata:
        if table["created"]:
            continue
        table_list.append(table['table_name'])
 
    for table in table_metadata:
        if table["created"]:
            continue
        if table['target_schema'].lower() == 'refine':
            refine_tables.append(table['table_name'])
        elif table['target_schema'].lower() == 'legacy':
            legacy_tables.append(table['table_name'])
        elif table['target_schema'].lower() == 'empl_protected':
            empl_protected_tables.append(table['table_name'])
        elif table['target_schema'].lower() == 'empl_sensitive':
            empl_sensitive_tables.append(table['table_name'])
        elif table['target_schema'].lower() == 'cust_sensitive':
            cust_sensitive_tables.append(table['table_name'])
        else:
            raise Exception("Schema not found",table['target_schema']) 

    with open(f"{workflow_name}_rocky_copy_hist_to_target.py","w") as file1:
        imports = """# Databricks notebook source
from Datalake.utils.rocky_script_automation.RockyAutomationUtils import *
from pyspark.sql.functions import *"""
        file1.writelines(imports)
        if len(refine_tables)>0:
            scripts =f"""
# COMMAND ----------
refine_tables = {refine_tables}
copy_hist_to_refine(refine_tables)"""
            print(scripts)
            file1.writelines(scripts)

        if len(legacy_tables)>0:
            scripts =f"""
# COMMAND ----------
legacy_tables = {legacy_tables}
copy_hist_to_legacy(legacy_tables)"""
            print(scripts)
            file1.writelines(scripts)

        if len(empl_protected_tables)>0:
            scripts =f"""
# COMMAND ----------
empl_protected_tables = {empl_protected_tables}
copy_hist_to_empl_protected(empl_protected_tables)"""
            print(scripts)
            file1.writelines(scripts)

        if len(empl_sensitive_tables)>0:
            scripts =f"""
# COMMAND ----------
empl_sensitive_tables = {empl_sensitive_tables}
copy_hist_to_empl_sensitive(empl_sensitive_tables)"""
            print(scripts)
            file1.writelines(scripts)

        if len(cust_sensitive_tables)>0:
            scripts =f"""
# COMMAND ----------
cust_sensitive_tables = {cust_sensitive_tables}
copy_hist_to_cust_sensitive(cust_sensitive_tables)"""
            print(scripts)
            file1.writelines(scripts)
        #creating delete scripts
        delete_scripts =f"""
# COMMAND ----------
table_list = {table_list} 
# COMMAND ----------  
for table in table_list:
    try:
      spark.sql(f\"drop table  refine.{{table}}_history\")
      print(f"refine.{{table}}_history deleted")
    except Exception as e:
      print(\"failed for \", table, e)"""
        file1.writelines(delete_scripts)


def getLargeTables(table_metadata):
    large_tables= []
    for table in table_metadata:
        if table["created"]:
            continue
        if table["record_count"] > 1000000000:
            large_tables.append(table['table_name'])
    return large_tables