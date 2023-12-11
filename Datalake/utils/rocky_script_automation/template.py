# Databricks notebook source
from Datalake.utils.rocky_script_automation.RockyAutomationUtils import *
from pyspark.sql import *

# COMMAND ----------

dbutils.widgets.text(name ="Workflow_name",defaultValue="")
workflow_name = dbutils.widgets.get("Workflow_name")
dbutils.widgets.text(name ="table_name", defaultValue = "qa_raw.nz_table_stats")
table_name = dbutils.widgets.get("table_name")
dbutils.widgets.text(name ="metadata_path",defaultValue="")
metadata_path = dbutils.widgets.get("metadata_path")
#metadat path should have file:/ as prefix

# COMMAND ----------

# extracts the metadat from the file and table
table_metadata = create_table_metadata(metadata_path,table_name)

# COMMAND ----------

rocky_insert_scripts = get_rocky_insert_statements(table_metadata)

# COMMAND ----------

with open(f"{workflow_name}_rocky_scripts.sql","w") as file1:
  for x in rocky_insert_scripts:
    file1.writelines(rocky_insert_scripts[x])

# COMMAND ----------


table_list = [] 
for table in table_metadata:
    if table["created"]:
        continue
    table_list.append(table['table_name'])
print(table_list)
#please check the table list before triggering the next function

# COMMAND ----------

#trigger_rocky(table_list)


# COMMAND ----------


from pyspark.sql.functions import lower
refine_tables = [] 
legacy_tables = []
empl_protected_tables =[]
empl_sensitive_tables =[]
cust_sensitive_tables =[]

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



if len(refine_tables)>0:
  print("Refine_tables =",refine_table)
  #copy_hist_to_refine(refine_tables)


if len(legacy_tables)>0:
  print("Legacy_tables =",legacy_tables)
  #copy_hist_to_legacy(legacy_tables)
       
if len(empl_protected_tables)>0:
  print("Empl_protected_tables =",empl_protected_tables)
  #copy_hist_to_empl_protected(empl_protected_tables)

if len(empl_sensitive_tables)>0:
  print("Empl_sensitive_tables =",empl_sensitive_tables)
  #copy_hist_to_empl_sensitive(empl_sensitive_tables)

if len(cust_sensitive_tables)>0:
  print("Cust_sensitive_tables =",cust_sensitive_tables)
  #copy_hist_to_cust_sensitive(cust_sensitive_tables)




# COMMAND ----------

# run this script to generate scripts for trigering rocky job
get_trigger_job_script(workflow_name,table_metadata)

# COMMAND ----------

# run this script to copy the history tables to target tables.
get_copy_script(workflow_name,table_metadata)
