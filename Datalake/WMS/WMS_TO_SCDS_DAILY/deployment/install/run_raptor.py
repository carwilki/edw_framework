# Databricks notebook source
# MAGIC %run /Shared/DataRaptor/Method_Defintion_submit_raptor_request

# COMMAND ----------

from datetime import datetime, timedelta

def generate_select(schema_name, table_name,prev_run_dt):
  df = spark.sql(f'show columns in {schema_name}.{table_name}').collect()
  return ('SELECT '+ ','.join([row[0] for row in df])+f' FROM {table_name}_nz'+ f""" where date_trunc('DAY',cast(UPDATE_TSTMP as date)) >= '{prev_run_dt}'""")


df = spark.sql("select distinct tableName,pKeys from raw.sf2delta_pKey_tstcols where tableName in (select distinct table_name from raw.historical_run_details_from_sf)").collect()
for row in df:
  deltaTable=row["tableName"]
  prev_run_dt = spark.sql(f"""select max(run_date) from raw.historical_run_details_from_sf where lower(table_name)=lower('{deltaTable}') and lower(status)= lower('Succeeded')""").collect()[0][0]
  prev_run_dt = datetime.strptime(str(prev_run_dt), "%Y-%m-%d %H:%M:%S")
  prev_run_dt = prev_run_dt - timedelta(days=30)
  prev_run_dt = prev_run_dt.strftime("%Y-%m-%d")
  table=row["tableName"]
  pKey=row["pKeys"].split("|")
  pkey_joined= ','.join(pKey)
  primary_key = pkey_joined
  print(primary_key)
  source_query_without_double_quotes=generate_select("refine",table,prev_run_dt)
  source_query = source_query_without_double_quotes
  print(source_query)
  email_address="pshekhar@petsmart.com"
  teams_cahnnel=""
  source_system_type="sf"  
  target_query=f"select * from refine.{table}"+f" where date_trunc('DAY',cast(UPDATE_TSTMP as date)) >= '{prev_run_dt}'" 
  target_system_type="bd"  
  output_table_name_format=table
  submit_dataRaptor_job(email_address, primary_key, source_query, source_system_type, output_table_name_format, target_query, target_system_type)
